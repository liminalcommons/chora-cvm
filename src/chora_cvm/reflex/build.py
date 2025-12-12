"""
Build Reflex Arc: Detect build quality regressions and emit signals.

The Build Reflex is the autonomic nervous system for build governance:
1. Runs quality checks (lint, type, test) on packages
2. Detects regressions from previous known-good state
3. Emits signals for failures
4. Auto-resolves signals when builds pass again

This makes build health visible to the Loom - CI failures become
entities that can be queried, tracked, and reasoned about.

Usage:
    from chora_cvm.reflex.build import run_build_reflex
    result = run_build_reflex(db_path, packages=["chora-cvm"], dry_run=False)
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

from ..store import EventStore
from ..std import manifest_entity
from ..schema import ExecutionContext
from ..lib.attention import focus_create


# Packages to check by default
PYTHON_PACKAGES = ["chora-cvm", "chora-crypto", "chora-inference", "chora-sync"]

# Signal types for different failure modes
SIGNAL_TYPES = {
    "lint": "signal-lint-regression",
    "typecheck": "signal-type-regression",
    "test": "signal-test-regression",
    "coverage": "signal-coverage-regression",
}


@dataclass
class CheckResult:
    """Result of a single quality check."""
    passed: bool
    output: str
    exit_code: int


@dataclass
class BuildReflexResult:
    """Result of running the build reflex arc."""
    packages_checked: list[str] = field(default_factory=list)
    signals_emitted: list[str] = field(default_factory=list)
    signals_resolved: list[str] = field(default_factory=list)
    focuses_triggered: list[str] = field(default_factory=list)
    learnings_harvested: list[str] = field(default_factory=list)
    failures: list[dict] = field(default_factory=list)
    passes: list[dict] = field(default_factory=list)


def run_check(package: str, check_type: str, workspace_root: Path) -> CheckResult:
    """
    Run a quality check on a package.

    Args:
        package: Package name (e.g., "chora-cvm")
        check_type: Type of check ("lint", "typecheck", "test")
        workspace_root: Path to workspace root

    Returns:
        CheckResult with passed status, output, and exit code
    """
    pkg_path = workspace_root / "packages" / package

    if not pkg_path.exists():
        return CheckResult(
            passed=False,
            output=f"Package not found: {pkg_path}",
            exit_code=-1
        )

    commands = {
        "lint": ["ruff", "check", "."],
        "typecheck": ["mypy", "src/", "--ignore-missing-imports"],
        "test": ["pytest", "-v", "--tb=short"],
    }

    if check_type not in commands:
        return CheckResult(
            passed=False,
            output=f"Unknown check type: {check_type}",
            exit_code=-1
        )

    try:
        result = subprocess.run(
            commands[check_type],
            cwd=pkg_path,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )
        return CheckResult(
            passed=result.returncode == 0,
            output=result.stdout + result.stderr,
            exit_code=result.returncode,
        )
    except subprocess.TimeoutExpired:
        return CheckResult(passed=False, output="Timeout expired", exit_code=-1)
    except FileNotFoundError as e:
        return CheckResult(passed=False, output=f"Command not found: {e}", exit_code=-1)


def get_active_build_signals(db_path: str) -> list[dict]:
    """Get all active build-related signals."""
    store = EventStore(db_path)
    cur = store._conn.cursor()

    cur.execute("""
        SELECT id, json_extract(data_json, '$.title') as title,
               json_extract(data_json, '$.signal_type') as signal_type,
               json_extract(data_json, '$.package') as package,
               json_extract(data_json, '$.check_type') as check_type
        FROM entities
        WHERE type = 'signal'
        AND (
            json_extract(data_json, '$.signal_type') LIKE 'signal-%-regression'
            OR json_extract(data_json, '$.void_type') LIKE 'build%'
        )
    """)

    signals = []
    for row in cur.fetchall():
        signals.append({
            "id": row[0],
            "title": row[1],
            "signal_type": row[2],
            "package": row[3],
            "check_type": row[4],
        })

    store.close()
    return signals


def emit_build_signal(
    db_path: str,
    package: str,
    check_type: str,
    output: str,
    dry_run: bool = False,
) -> Optional[str]:
    """Emit a signal for a build failure."""
    signal_type = SIGNAL_TYPES.get(check_type, f"signal-{check_type}-regression")
    signal_id = f"signal-{check_type}-{package}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    title = f"{check_type.title()} failing in {package}"

    if dry_run:
        return None

    manifest_entity(
        db_path=db_path,
        entity_type="signal",
        entity_id=signal_id,
        data={
            "title": title,
            "signal_type": signal_type,
            "package": package,
            "check_type": check_type,
            "output_snippet": output[:500] if output else "",
            "void_type": "build-integrity",
            "severity": "warning" if check_type == "lint" else "critical",
            "detected_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    return signal_id


def resolve_signal(db_path: str, signal_id: str, dry_run: bool = False) -> bool:
    """Mark a signal as resolved (delete it)."""
    if dry_run:
        return True

    store = EventStore(db_path)
    cur = store._conn.cursor()

    # Delete the signal entity
    cur.execute("DELETE FROM entities WHERE id = ?", (signal_id,))
    store._conn.commit()
    store.close()

    return True


def trigger_focus(
    db_path: str,
    signal_id: str,
    signal_title: str,
    package: str,
    check_type: str,
    dry_run: bool = False,
) -> Optional[str]:
    """
    Create a Focus entity triggered by a build signal.

    Part of Phase 2B: When a signal is emitted, auto-create a Focus that
    captures attention. This creates the triggers bond: signal --triggers--> focus.

    Args:
        db_path: Path to the Loom database
        signal_id: The signal that triggered this focus
        signal_title: Title of the signal
        package: Package that failed
        check_type: Type of check that failed
        dry_run: If True, don't create entities

    Returns:
        Focus ID if created, None if dry_run or already exists
    """
    if dry_run:
        return None

    focus_title = f"Investigate: {signal_title}"
    ctx = ExecutionContext(db_path=db_path, persona_id="build-reflex")

    # Check if focus already exists for this signal
    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("""
        SELECT id FROM entities
        WHERE type = 'focus'
        AND json_extract(data_json, '$.triggered_by') = ?
    """, (signal_id,))
    existing = cur.fetchone()
    store.close()

    if existing:
        return None  # Focus already exists

    # Create focus with triggers bond
    result = focus_create(
        title=focus_title,
        _ctx=ctx,
        description=f"Build {check_type} is failing in {package}. Investigate and fix.",
        signal_id=signal_id,
        data={
            "build_package": package,
            "build_check_type": check_type,
            "focus_type": "build-investigation",
        },
    )

    if result.get("status") == "success":
        return result.get("id")
    return None


def get_failure_pattern(db_path: str, package: str, check_type: str) -> dict:
    """
    Detect patterns of repeated failures for the same package/check.

    Part of Phase 2C: Learning harvesting - when the same behavior fails
    multiple times, we harvest a Learning entity.

    Args:
        db_path: Path to the Loom database
        package: Package name
        check_type: Type of check (lint, typecheck, test)

    Returns:
        Dict with pattern info: {"count": n, "signals": [...]}
    """
    store = EventStore(db_path)
    cur = store._conn.cursor()

    # Count historical signals for this package/check
    # Look at both active signals and learnings that mention this failure
    cur.execute("""
        SELECT COUNT(*) FROM entities
        WHERE type = 'learning'
        AND json_extract(data_json, '$.domain') = 'build-reflex'
        AND json_extract(data_json, '$.package') = ?
        AND json_extract(data_json, '$.check_type') = ?
    """, (package, check_type))
    learning_count = cur.fetchone()[0]

    store.close()

    return {
        "count": learning_count,
        "package": package,
        "check_type": check_type,
    }


def harvest_learning(
    db_path: str,
    package: str,
    check_type: str,
    failure_count: int,
    dry_run: bool = False,
) -> Optional[str]:
    """
    Harvest a Learning entity from repeated build failures.

    Part of Phase 2C: When the same behavior fails repeatedly, capture
    this as a Learning for the system to remember.

    Args:
        db_path: Path to the Loom database
        package: Package that frequently fails
        check_type: Type of check that frequently fails
        failure_count: How many times this has failed
        dry_run: If True, don't create entities

    Returns:
        Learning ID if created, None otherwise
    """
    if dry_run:
        return None

    # Only harvest learning after 3+ failures
    if failure_count < 3:
        return None

    learning_id = f"learning-build-{check_type}-{package}-frequent-failures"
    title = f"{check_type.title()} frequently fails in {package}"

    # Check if learning already exists
    store = EventStore(db_path)
    existing = store.get_entity(learning_id)
    if existing:
        store.close()
        return None  # Learning already exists

    store.close()

    manifest_entity(
        db_path=db_path,
        entity_type="learning",
        entity_id=learning_id,
        data={
            "title": title,
            "insight": f"The {check_type} check in {package} has failed {failure_count}+ times. "
                       f"This suggests a structural issue that may need deeper investigation.",
            "domain": "build-reflex",
            "package": package,
            "check_type": check_type,
            "failure_count": failure_count,
            "harvested_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    return learning_id


def get_build_learnings(db_path: str) -> list[dict]:
    """
    Get all build-related learnings for observability.

    Part of Phase 2D: Observability surface for build learnings.

    Args:
        db_path: Path to the Loom database

    Returns:
        List of learning dicts with id, title, insight, package, check_type
    """
    store = EventStore(db_path)
    cur = store._conn.cursor()

    cur.execute("""
        SELECT id,
               json_extract(data_json, '$.title') as title,
               json_extract(data_json, '$.insight') as insight,
               json_extract(data_json, '$.package') as package,
               json_extract(data_json, '$.check_type') as check_type,
               json_extract(data_json, '$.harvested_at') as harvested_at
        FROM entities
        WHERE type = 'learning'
        AND json_extract(data_json, '$.domain') = 'build-reflex'
        ORDER BY json_extract(data_json, '$.harvested_at') DESC
    """)

    learnings = []
    for row in cur.fetchall():
        learnings.append({
            "id": row[0],
            "title": row[1],
            "insight": row[2],
            "package": row[3],
            "check_type": row[4],
            "harvested_at": row[5],
        })

    store.close()
    return learnings


def run_build_reflex(
    db_path: str,
    packages: Optional[list[str]] = None,
    checks: Optional[list[str]] = None,
    dry_run: bool = False,
    verbose: bool = True,
    workspace_root: Optional[Path] = None,
) -> BuildReflexResult:
    """
    Run the build reflex arc.

    1. Get current build signals
    2. Run quality checks on packages
    3. Emit new signals for failures
    4. Auto-resolve signals for passes

    Args:
        db_path: Path to the Loom database
        packages: List of packages to check (default: all Python packages)
        checks: List of check types to run (default: lint, typecheck, test)
        dry_run: If True, don't emit/resolve signals
        verbose: If True, print progress output
        workspace_root: Path to workspace root (auto-detected if not provided)

    Returns:
        BuildReflexResult with summary of actions taken
    """
    if workspace_root is None:
        # Auto-detect: assume we're in chora-cvm/src/chora_cvm/reflex/
        workspace_root = Path(__file__).parent.parent.parent.parent.parent.parent

    packages = packages or PYTHON_PACKAGES
    checks = checks or ["lint", "typecheck", "test"]

    result = BuildReflexResult()

    # Get existing signals
    existing_signals = get_active_build_signals(db_path)
    existing_by_key = {
        (s["package"], s.get("check_type", "")): s
        for s in existing_signals
        if s.get("package") and s.get("check_type")
    }

    if verbose:
        print("Build Reflex Arc")
        print("=" * 60)
        print()

    for package in packages:
        if verbose:
            print(f"Package: {package}")
            print("-" * 40)
        result.packages_checked.append(package)

        for check_type in checks:
            if verbose:
                print(f"  {check_type}... ", end="", flush=True)

            check_result = run_check(package, check_type, workspace_root)

            if check_result.passed:
                if verbose:
                    print("PASS")
                result.passes.append({"package": package, "check": check_type})

                # Check if there was a signal for this - auto-resolve
                key = (package, check_type)
                if key in existing_by_key:
                    signal = existing_by_key[key]
                    if verbose:
                        print(f"    Auto-resolving signal: {signal['id']}")
                    resolve_signal(db_path, signal["id"], dry_run)
                    result.signals_resolved.append(signal["id"])

            else:
                if verbose:
                    print("FAIL")
                result.failures.append({
                    "package": package,
                    "check": check_type,
                    "output": check_result.output[:200],
                })

                # Emit signal if not already present
                key = (package, check_type)
                if key not in existing_by_key:
                    signal_id = emit_build_signal(
                        db_path, package, check_type, check_result.output, dry_run
                    )
                    if signal_id:
                        result.signals_emitted.append(signal_id)
                        if verbose:
                            print(f"    Emitted signal: {signal_id}")

                        # Phase 2B: Trigger Focus for attention
                        signal_title = f"{check_type.title()} failing in {package}"
                        focus_id = trigger_focus(
                            db_path, signal_id, signal_title, package, check_type, dry_run
                        )
                        if focus_id:
                            result.focuses_triggered.append(focus_id)
                            if verbose:
                                print(f"    Triggered focus: {focus_id}")

                        # Phase 2C: Check for patterns and harvest learning
                        pattern = get_failure_pattern(db_path, package, check_type)
                        if pattern["count"] >= 2:  # 3rd+ failure triggers learning
                            learning_id = harvest_learning(
                                db_path, package, check_type, pattern["count"] + 1, dry_run
                            )
                            if learning_id:
                                result.learnings_harvested.append(learning_id)
                                if verbose:
                                    print(f"    Harvested learning: {learning_id}")
                else:
                    if verbose:
                        print(f"    Signal already exists: {existing_by_key[key]['id']}")

        if verbose:
            print()

    # Summary
    if verbose:
        print("=" * 60)
        print("Summary")
        print("-" * 40)
        print(f"  Packages checked: {len(result.packages_checked)}")
        print(f"  Passes: {len(result.passes)}")
        print(f"  Failures: {len(result.failures)}")
        print(f"  Signals emitted: {len(result.signals_emitted)}")
        print(f"  Signals resolved: {len(result.signals_resolved)}")
        print(f"  Focuses triggered: {len(result.focuses_triggered)}")
        print(f"  Learnings harvested: {len(result.learnings_harvested)}")

        if result.failures:
            print()
            print("Failures:")
            for f in result.failures:
                print(f"  - {f['package']}/{f['check']}")

    return result
