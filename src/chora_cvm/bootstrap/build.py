"""
Bootstrap Build Governance: Manifest structural entities for build integrity.

This module creates the principles, patterns, behaviors, and tools that encode
build practices as structural governance in the Loom.

The Build Bootstrap is idempotent - running twice will update existing entities
rather than creating duplicates (upsert semantics from save_entity/save_bond).

Usage:
    from chora_cvm.bootstrap.build import bootstrap_build_entities
    result = bootstrap_build_entities(db_path)
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..store import EventStore
from ..std import manifest_entity


@dataclass
class BuildBootstrapResult:
    """Result of bootstrapping build governance entities."""
    principles_created: list[str] = field(default_factory=list)
    patterns_created: list[str] = field(default_factory=list)
    behaviors_created: list[str] = field(default_factory=list)
    tools_created: list[str] = field(default_factory=list)
    bonds_created: list[str] = field(default_factory=list)
    bonds_skipped: list[str] = field(default_factory=list)

    @property
    def total_entities(self) -> int:
        return (
            len(self.principles_created) +
            len(self.patterns_created) +
            len(self.behaviors_created) +
            len(self.tools_created)
        )

    @property
    def total_bonds(self) -> int:
        return len(self.bonds_created)


# =============================================================================
# Entity Definitions
# =============================================================================

PRINCIPLES = [
    {
        "id": "principle-shift-left-quality",
        "title": "Shift-Left Quality",
        "data": {
            "statement": "Quality issues are caught at the earliest possible stage - at commit time, not in CI or production.",
            "rationale": "Earlier detection means cheaper fixes, faster feedback, and maintained flow state.",
        },
    },
    {
        "id": "principle-defense-in-depth",
        "title": "Defense in Depth",
        "data": {
            "statement": "Security and quality validation occurs at multiple checkpoints: developer machine, CI pipeline, and release gates.",
            "rationale": "No single checkpoint is infallible. Layered validation catches what earlier layers miss.",
        },
    },
    {
        "id": "principle-ci-as-physics",
        "title": "CI as Physics",
        "data": {
            "statement": "Build validation is not external tooling but part of the system's physics - violations emit Signals, successes create Learnings.",
            "rationale": "When CI is physics, the system can reason about and heal its own build health.",
        },
    },
    {
        "id": "principle-reproducible-builds",
        "title": "Reproducible Builds",
        "data": {
            "statement": "Build outputs are deterministic given the same inputs. Lockfiles pin dependencies, environment is declarative.",
            "rationale": "Reproducibility enables debugging, security auditing, and confident deployments.",
        },
    },
    {
        "id": "principle-zero-trust-dependencies",
        "title": "Zero Trust Dependencies",
        "data": {
            "statement": "External dependencies are verified, not blindly trusted. Hash checking, vulnerability scanning, and license auditing.",
            "rationale": "Supply chain attacks exploit implicit trust. Verification makes trust explicit.",
        },
    },
]

PATTERNS = [
    {
        "id": "pattern-pre-commit-hooks",
        "title": "Pre-commit Hooks Pattern",
        "data": {
            "target": "developer-workflow",
            "template": """Layer 1 of Defense in Depth: Developer machine validation.

Hooks:
- trailing-whitespace, end-of-file-fixer (basic hygiene)
- ruff (Python linting + formatting)
- mypy (type checking)
- bandit (security linting)
- gitleaks (secret scanning)

Install: pre-commit install
Run: pre-commit run --all-files""",
        },
    },
    {
        "id": "pattern-ci-pipeline-layers",
        "title": "CI Pipeline Layers Pattern",
        "data": {
            "target": "github-actions",
            "template": """Layer 2 of Defense in Depth: CI pipeline validation.

Jobs:
- lint: ruff check across all packages
- python-matrix: pytest across Python 3.10-3.12 x packages
- typescript: npm test for TS packages
- security: CodeQL analysis
- emit-signal: On failure, emit Signal to Loom

Matrix strategy enables parallel validation across versions.""",
        },
    },
    {
        "id": "pattern-release-gates",
        "title": "Release Gates Pattern",
        "data": {
            "target": "publishing",
            "template": """Layer 3 of Defense in Depth: Release gates.

Steps:
- All CI checks must pass
- SBOM generation (CycloneDX)
- Provenance attestation
- Trusted publishing (OIDC, no stored tokens)
- Version tagging

Release only on v* tags to main branch.""",
        },
    },
    {
        "id": "pattern-signal-on-failure",
        "title": "Signal on Failure Pattern",
        "data": {
            "target": "reflex",
            "template": """The Reflex Arc for build integrity.

When a build check fails:
1. Detect the failure (test, lint, security)
2. Emit a typed Signal (signal-test-regression, etc.)
3. Signal persists until the void is resolved
4. Auto-resolve when checks pass again

This makes build health visible to the Loom.""",
        },
    },
]

BEHAVIORS = [
    {
        "id": "behavior-lint-passes",
        "title": "Lint passes",
        "data": {
            "given": "A Python package with source code",
            "when": "ruff check is executed",
            "then": "Exit code is 0 and no violations reported",
        },
    },
    {
        "id": "behavior-types-check",
        "title": "Types check",
        "data": {
            "given": "A Python package with type hints",
            "when": "mypy is executed",
            "then": "Exit code is 0 and no type errors",
        },
    },
    {
        "id": "behavior-tests-pass",
        "title": "Tests pass",
        "data": {
            "given": "A package with test suite",
            "when": "pytest is executed",
            "then": "All tests pass with exit code 0",
        },
    },
    {
        "id": "behavior-coverage-threshold",
        "title": "Coverage threshold met",
        "data": {
            "given": "A package with pytest-cov configured",
            "when": "Tests complete",
            "then": "Coverage is >= 80%",
        },
    },
    {
        "id": "behavior-security-scan-clean",
        "title": "Security scan clean",
        "data": {
            "given": "Package source code",
            "when": "bandit/CodeQL is executed",
            "then": "No high/critical vulnerabilities detected",
        },
    },
    {
        "id": "behavior-secrets-not-committed",
        "title": "Secrets not committed",
        "data": {
            "given": "A staged commit",
            "when": "gitleaks scan runs",
            "then": "No secrets detected in diff",
        },
    },
    {
        "id": "behavior-pre-commit-enforced",
        "title": "Pre-commit enforced",
        "data": {
            "given": "A developer making a commit",
            "when": "git commit is invoked",
            "then": "Pre-commit hooks execute automatically",
        },
    },
    {
        "id": "behavior-pr-cannot-merge-broken",
        "title": "PR cannot merge when broken",
        "data": {
            "given": "A PR with failing CI checks",
            "when": "Merge is attempted",
            "then": "Merge is blocked by branch protection",
        },
    },
]

TOOLS = [
    {
        "id": "tool-lint",
        "title": "Lint",
        "data": {
            "handler": "subprocess:ruff check",
            "phenomenology": "When code quality needs verification",
            "cognition": {
                "ready_at_hand": "Use to verify Python code style and catch common errors",
            },
        },
    },
    {
        "id": "tool-typecheck",
        "title": "Type Check",
        "data": {
            "handler": "subprocess:mypy",
            "phenomenology": "When type safety needs verification",
            "cognition": {
                "ready_at_hand": "Use to verify Python type annotations are consistent",
            },
        },
    },
    {
        "id": "tool-test",
        "title": "Test",
        "data": {
            "handler": "subprocess:pytest",
            "phenomenology": "When behavior verification is needed",
            "cognition": {
                "ready_at_hand": "Use to run package test suite and verify behaviors",
            },
        },
    },
    {
        "id": "tool-security-scan",
        "title": "Security Scan",
        "data": {
            "handler": "subprocess:bandit",
            "phenomenology": "When security posture needs validation",
            "cognition": {
                "ready_at_hand": "Use to detect security vulnerabilities in Python code",
            },
        },
    },
    {
        "id": "tool-pre-commit",
        "title": "Pre-commit",
        "data": {
            "handler": "subprocess:pre-commit run --all-files",
            "phenomenology": "When local quality gates need execution",
            "cognition": {
                "ready_at_hand": "Use before committing to run all quality checks",
            },
        },
    },
]

# Bonds wire the structure
BONDS = [
    # Principles govern Patterns
    ("governs", "principle-shift-left-quality", "pattern-pre-commit-hooks"),
    ("governs", "principle-defense-in-depth", "pattern-ci-pipeline-layers"),
    ("governs", "principle-defense-in-depth", "pattern-release-gates"),
    ("governs", "principle-ci-as-physics", "pattern-signal-on-failure"),
    ("governs", "principle-reproducible-builds", "pattern-release-gates"),
    ("governs", "principle-zero-trust-dependencies", "pattern-release-gates"),
    # Tools implement behaviors
    ("implements", "tool-lint", "behavior-lint-passes"),
    ("implements", "tool-typecheck", "behavior-types-check"),
    ("implements", "tool-test", "behavior-tests-pass"),
    ("implements", "tool-test", "behavior-coverage-threshold"),
    ("implements", "tool-security-scan", "behavior-security-scan-clean"),
    ("implements", "tool-pre-commit", "behavior-pre-commit-enforced"),
    # Tools verify behaviors
    ("verifies", "tool-lint", "behavior-lint-passes"),
    ("verifies", "tool-typecheck", "behavior-types-check"),
    ("verifies", "tool-test", "behavior-tests-pass"),
    ("verifies", "tool-security-scan", "behavior-security-scan-clean"),
]


# =============================================================================
# Bootstrap Function
# =============================================================================

def bootstrap_build_entities(
    db_path: str,
    verbose: bool = True,
) -> BuildBootstrapResult:
    """
    Bootstrap build governance entities into the Loom.

    Creates principles, patterns, behaviors, tools, and bonds that encode
    build practices as structural governance.

    This function is idempotent - running twice will update existing entities
    rather than creating duplicates (upsert semantics).

    Args:
        db_path: Path to the Loom database
        verbose: If True, print progress output

    Returns:
        BuildBootstrapResult with summary of created entities and bonds
    """
    result = BuildBootstrapResult()

    if verbose:
        print("Build Governance Bootstrap")
        print("=" * 60)
        print()

    # Manifest Principles
    if verbose:
        print("Principles (truths about building)")
        print("-" * 40)
    for p in PRINCIPLES:
        manifest_entity(
            db_path=db_path,
            entity_type="principle",
            entity_id=p["id"],
            data={"title": p["title"], **p["data"]},
        )
        result.principles_created.append(p["id"])
        if verbose:
            print(f"  + {p['id']}")

    if verbose:
        print()

    # Manifest Patterns
    if verbose:
        print("Patterns (reusable build forms)")
        print("-" * 40)
    for p in PATTERNS:
        manifest_entity(
            db_path=db_path,
            entity_type="pattern",
            entity_id=p["id"],
            data={"title": p["title"], **p["data"]},
        )
        result.patterns_created.append(p["id"])
        if verbose:
            print(f"  + {p['id']}")

    if verbose:
        print()

    # Manifest Behaviors
    if verbose:
        print("Behaviors (quality expectations)")
        print("-" * 40)
    for b in BEHAVIORS:
        manifest_entity(
            db_path=db_path,
            entity_type="behavior",
            entity_id=b["id"],
            data={"title": b["title"], **b["data"]},
        )
        result.behaviors_created.append(b["id"])
        if verbose:
            print(f"  + {b['id']}")

    if verbose:
        print()

    # Manifest Tools
    if verbose:
        print("Tools (build operations)")
        print("-" * 40)
    for t in TOOLS:
        manifest_entity(
            db_path=db_path,
            entity_type="tool",
            entity_id=t["id"],
            data={"title": t["title"], **t["data"]},
        )
        result.tools_created.append(t["id"])
        if verbose:
            print(f"  + {t['id']}")

    if verbose:
        print()

    # Create Bonds
    if verbose:
        print("Bonds (wiring the structure)")
        print("-" * 40)

    store = EventStore(db_path)
    for verb, from_id, to_id in BONDS:
        bond_id = f"relationship-{verb}-{from_id}-{to_id}".replace("_", "-")
        try:
            store.save_bond(
                bond_id=bond_id,
                bond_type=verb,
                from_id=from_id,
                to_id=to_id,
                status="active",
                confidence=1.0,
                data={},
            )
            result.bonds_created.append(bond_id)
            if verbose:
                print(f"  {from_id} --{verb}--> {to_id}")
        except Exception as e:
            result.bonds_skipped.append(f"{bond_id}: {e}")
            if verbose:
                print(f"  SKIP {verb}: {from_id} -> {to_id} ({e})")

    store.close()

    # Summary
    if verbose:
        print()
        print("=" * 60)
        print("BUILD GOVERNANCE ENTITIES BOOTSTRAPPED")
        print("=" * 60)
        print(f"  Principles: {len(result.principles_created)}")
        print(f"  Patterns:   {len(result.patterns_created)}")
        print(f"  Behaviors:  {len(result.behaviors_created)}")
        print(f"  Tools:      {len(result.tools_created)}")
        print(f"  Bonds:      {len(result.bonds_created)}")
        if result.bonds_skipped:
            print(f"  Skipped:    {len(result.bonds_skipped)}")
        print()
        print("Run 'just orient' to see build tools in cognitive compass")

    return result
