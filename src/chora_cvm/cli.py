"""
Universal Dispatcher: The Single Membrane between Human Intent and the Protocol Graph.

This is the mouth and ears of the CVM. It bridges human commands to protocol execution.

Usage:
    python -m chora_cvm.cli invoke <protocol_id> [--input '{"key": "value"}'] [--db path] [--persona id]
    python -m chora_cvm.cli invoke <protocol_id> --async    # Queue for background execution
    python -m chora_cvm.cli worker                          # Start background worker
    python -m chora_cvm.cli status <task_id>                # Check async task status
    python -m chora_cvm.cli login <persona_id>
    python -m chora_cvm.cli context
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from .kernel.engine import CvmEngine
from .kernel.runner import execute_protocol
from .kernel.store import EventStore


# =============================================================================
# Context Resolution
# =============================================================================

def get_context_file() -> Path:
    """Get the path to the context file."""
    return Path.cwd() / ".chora" / "context.json"


def load_context() -> Dict[str, Any]:
    """Load context from .chora/context.json if it exists."""
    context_file = get_context_file()
    if context_file.exists():
        return json.loads(context_file.read_text())
    return {}


def save_context(context: Dict[str, Any]) -> None:
    """Save context to .chora/context.json."""
    context_file = get_context_file()
    context_file.parent.mkdir(parents=True, exist_ok=True)
    context_file.write_text(json.dumps(context, indent=2))


def resolve_persona(explicit: Optional[str], store: EventStore) -> Optional[str]:
    """
    Resolve persona using hierarchy:
    1. Explicit flag
    2. Environment variable CHORA_PERSONA
    3. Local context file
    4. Implicit (single persona in DB)
    """
    # 1. Explicit flag
    if explicit:
        return explicit

    # 2. Environment variable
    env_persona = os.environ.get("CHORA_PERSONA")
    if env_persona:
        return env_persona

    # 3. Local context file
    context = load_context()
    if context.get("persona_id"):
        return context["persona_id"]

    # 4. Implicit fallback - query personas from DB
    import sqlite3
    conn = sqlite3.connect(store.path)
    cur = conn.execute("SELECT id FROM entities WHERE type = 'persona'")
    personas = [row[0] for row in cur.fetchall()]
    conn.close()

    if len(personas) == 1:
        return personas[0]
    elif len(personas) > 1:
        # Don't auto-select, let it be None
        return None

    return None


def resolve_db_path(explicit: Optional[str]) -> str:
    """
    Resolve database path:
    1. Explicit flag
    2. Environment variable CHORA_DB
    3. Default: chora-cvm-manifest.db in current directory
    """
    if explicit:
        return explicit

    env_db = os.environ.get("CHORA_DB")
    if env_db:
        return env_db

    return str(Path.cwd() / "chora-cvm-manifest.db")


# =============================================================================
# Commands
# =============================================================================

def cmd_dispatch(args: argparse.Namespace) -> int:
    """
    Dispatch through CvmEngine - the unified entry point.

    This routes to protocols OR primitives through the same interface,
    demonstrating the Event Horizon pattern where all interfaces converge.
    """
    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Parse inputs
    inputs: Dict[str, Any] = {}
    if args.input:
        try:
            inputs = json.loads(args.input)
        except json.JSONDecodeError as e:
            print(f"✗ Invalid JSON input: {e}", file=sys.stderr)
            return 1

    # Resolve persona
    store = EventStore(db_path)
    persona_id = resolve_persona(getattr(args, "persona", None), store)
    store.close()

    # Use CvmEngine for dispatch
    engine = CvmEngine(db_path)
    try:
        result = engine.dispatch(
            intent=args.intent,
            inputs=inputs,
            output_sink=print,
            persona_id=persona_id,
        )
    finally:
        engine.close()

    if not result.ok:
        print(f"✗ {result.error_kind}: {result.error_message}", file=sys.stderr)
        return 1

    # Output result (protocols with ui_render will have already printed)
    if result.data and not result.data.get("rendered"):
        print(json.dumps(result.data, indent=2, default=str))

    return 0


def cmd_capabilities(args: argparse.Namespace) -> int:
    """List all available capabilities (protocols and primitives)."""
    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    engine = CvmEngine(db_path)
    try:
        capabilities = engine.list_capabilities()
    finally:
        engine.close()

    # Group by kind
    protocols = [c for c in capabilities if c.kind.value == "protocol"]
    primitives = [c for c in capabilities if c.kind.value == "primitive"]

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  CVM Capabilities                                          │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    print(f"  Protocols ({len(protocols)}):")
    for p in sorted(protocols, key=lambda x: x.id):
        short = p.id[9:] if p.id.startswith("protocol-") else p.id
        print(f"    {short:30} {p.description[:40]}")

    print()
    print(f"  Primitives ({len(primitives)}):")
    for p in sorted(primitives, key=lambda x: x.id)[:20]:  # Show first 20
        short = p.id[10:] if p.id.startswith("primitive-") else p.id
        print(f"    {short:30} {p.description[:40]}")

    if len(primitives) > 20:
        print(f"    ... and {len(primitives) - 20} more")

    print()
    return 0


def cmd_invoke(args: argparse.Namespace) -> int:
    """Invoke a protocol."""
    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Parse inputs
    inputs: Dict[str, Any] = {}
    if args.input:
        try:
            inputs = json.loads(args.input)
        except json.JSONDecodeError as e:
            print(f"✗ Invalid JSON input: {e}", file=sys.stderr)
            return 1

    # Resolve persona
    store = EventStore(db_path)
    persona_id = resolve_persona(args.persona, store)
    store.close()

    # Async mode: enqueue for background execution
    if getattr(args, "async_mode", False):
        from .worker import enqueue_protocol

        task_id = enqueue_protocol(
            db_path=db_path,
            protocol_id=args.protocol_id,
            inputs=inputs,
            persona_id=persona_id,
        )
        print(f"✓ Protocol queued for async execution")
        print(f"  Task ID: {task_id}")
        print(f"  Check status: python -m chora_cvm.cli status {task_id}")
        return 0

    # Synchronous mode: execute immediately
    # CLI explicitly routes output to stdout via the I/O Membrane
    result = execute_protocol(
        db_path=db_path,
        protocol_id=args.protocol_id,
        inputs=inputs,
        persona_id=persona_id,
        state_id=args.state_id,
        output_sink=print,
    )

    # Handle errors
    if result.get("status") == "error":
        print(f"✗ {result.get('error_kind', 'Error')}: {result.get('error_message', 'Unknown')}", file=sys.stderr)
        return 1

    # Output result (protocols with ui_render will have already printed)
    # For protocols that return data without rendering, output JSON
    if result and not result.get("rendered"):
        print(json.dumps(result, indent=2, default=str))

    return 0


def cmd_login(args: argparse.Namespace) -> int:
    """Set the current persona context."""
    context = load_context()
    context["persona_id"] = args.persona_id
    save_context(context)
    print(f"✓ Logged in as {args.persona_id}")
    return 0


def cmd_context(args: argparse.Namespace) -> int:
    """Show current context."""
    context = load_context()
    db_path = resolve_db_path(args.db if hasattr(args, "db") else None)

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  CVM Context                                               │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()
    print(f"  Database: {db_path}")
    print(f"  Persona:  {context.get('persona_id', '(not set)')}")
    print(f"  Circle:   {context.get('circle_id', '(not set)')}")
    print()

    # Show env overrides if present
    if os.environ.get("CHORA_PERSONA"):
        print(f"  ENV CHORA_PERSONA: {os.environ['CHORA_PERSONA']}")
    if os.environ.get("CHORA_DB"):
        print(f"  ENV CHORA_DB: {os.environ['CHORA_DB']}")

    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    """Start the background worker."""
    from .worker import run_worker

    workers = getattr(args, "workers", 1)
    run_worker(workers=workers, verbose=True)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Check status of an async task."""
    from .worker import get_task_status, get_worker_db_path

    worker_db = get_worker_db_path()
    status = get_task_status(worker_db, args.task_id)

    if not status:
        print(f"✗ Task not found: {args.task_id}", file=sys.stderr)
        return 1

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  Task Status                                               │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()
    print(f"  Task ID:    {status['task_id']}")
    print(f"  Protocol:   {status['protocol_id']}")
    print(f"  Status:     {status['status']}")
    print(f"  Enqueued:   {status['enqueued_at']}")
    if status.get("started_at"):
        print(f"  Started:    {status['started_at']}")
    if status.get("completed_at"):
        print(f"  Completed:  {status['completed_at']}")
    print()

    if status.get("error"):
        print(f"  Error: {status['error']}")
        print()

    if status.get("result") and status["status"] == "completed":
        print("  Result:")
        print(json.dumps(status["result"], indent=4, default=str))
        print()

    return 0


def cmd_pulse_status(args: argparse.Namespace) -> int:
    """Show recent pulse history."""
    from .worker import get_pulse_status, get_worker_db_path

    worker_db = get_worker_db_path()
    status = get_pulse_status(worker_db, limit=args.limit)

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  Pulse Status (Last {} heartbeats){}│".format(
        args.limit,
        " " * (26 - len(str(args.limit)))
    ))
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    pulses = status.get("pulses", [])
    if not pulses:
        print("  No pulse history yet.")
        print()
        return 0

    # Header
    print("  Time       │ Found │ Processed │ Protocols │ Errors")
    print("  ───────────┼───────┼───────────┼───────────┼────────")

    for pulse in pulses:
        # Extract time portion from ISO timestamp
        pulse_time = pulse["pulse_at"].split("T")[1][:8] if "T" in pulse["pulse_at"] else pulse["pulse_at"][:8]
        found = pulse["signals_found"]
        processed = pulse["signals_processed"]
        protocols = pulse["protocols_triggered"]
        errors = pulse["errors"]

        # Format with error indicator
        error_str = str(errors) if errors == 0 else f"*{errors}*"

        print(f"  {pulse_time}  │  {found:3d}  │    {processed:3d}    │    {protocols:3d}    │  {error_str}")

    print()
    print(f"  Total pulses recorded: {status['total_pulses']}")
    print()

    return 0


def cmd_pulse_preview(args: argparse.Namespace) -> int:
    """Preview what the next pulse would process."""
    from .std import pulse_preview

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    preview = pulse_preview(db_path, limit=args.limit)

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  Pulse Preview                                             │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    would_process = preview.get("would_process", [])
    signals_without = preview.get("signals_without_triggers", 0)

    if not would_process and signals_without == 0:
        print("  No active signals to process.")
        print()
        return 0

    if would_process:
        print(f"  Would process {len(would_process)} signal(s):")
        print()
        for item in would_process:
            signal_id = item["signal_id"]
            triggers = item.get("triggers", "(no protocol)")
            print(f"    {signal_id}")
            print(f"      → {triggers}")
        print()

    if signals_without > 0:
        print(f"  {signals_without} signal(s) have no triggers bond (will be skipped)")
        print()

    return 0


def cmd_integrity(args: argparse.Namespace) -> int:
    """Check system integrity - verify behaviors have tests and they pass."""
    from .std import integrity_discover_scenarios, integrity_report

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    features_dir = args.features_dir or str(Path(__file__).parent.parent.parent.parent / "tests" / "features")

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  System Integrity Check                                    │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    # Discover scenarios
    discovery = integrity_discover_scenarios(db_path, features_dir)
    behaviors = discovery["behaviors"]

    if not behaviors:
        print("  No behaviors found in database.")
        print()
        return 0

    # Count statuses
    verified = sum(1 for b in behaviors.values() if b.get("has_scenarios"))
    unverified = sum(1 for b in behaviors.values() if not b.get("has_scenarios"))
    total = len(behaviors)
    coverage = int((verified / total) * 100) if total > 0 else 0

    # Show summary
    print(f"  Behaviors: {total}")
    print(f"  With scenarios: {verified}")
    print(f"  Without scenarios: {unverified}")
    print(f"  Coverage: {coverage}%")
    print()

    # Show behaviors with scenarios
    if verified > 0:
        print("  ✓ Behaviors with test scenarios:")
        for bid, bdata in behaviors.items():
            if bdata.get("has_scenarios"):
                feature = bdata.get("feature_file", "unknown")
                print(f"    • {bid}")
                print(f"      → {feature}")
        print()

    # Show behaviors without scenarios
    if unverified > 0:
        print("  ⚠ Behaviors without test scenarios:")
        for bid, bdata in behaviors.items():
            if not bdata.get("has_scenarios"):
                print(f"    • {bid}")
        print()

    return 0


def cmd_prune_via_protocol(args: argparse.Namespace) -> int:
    """
    Prune detection via CvmEngine protocol dispatch.

    This is the Phase 2 migration path - routing through protocol-prune-detect
    instead of the legacy prune.py functions.
    """
    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    engine = CvmEngine(db_path)
    try:
        result = engine.dispatch(
            "prune-detect",
            {"db_path": db_path},
            output_sink=print,
        )
    finally:
        engine.close()

    if not result.ok:
        print(f"✗ {result.error_kind}: {result.error_message}", file=sys.stderr)
        return 1

    # Format output
    data = result.data.get("data", result.data)

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  Prune Detect (via Protocol)                               │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    orphans = data.get("orphan_tools", [])
    deprecated = data.get("deprecated_tools", [])
    summary = data.get("summary", {})

    print(f"  Orphan tools:       {summary.get('orphan_count', len(orphans))}")
    print(f"  Deprecated tools:   {summary.get('deprecated_count', len(deprecated))}")
    print()

    if orphans:
        print("  Orphan Tools:")
        for tool in orphans[:10]:
            tool_id = tool["id"] if isinstance(tool, dict) else tool
            print(f"    • {tool_id}")
        if len(orphans) > 10:
            print(f"    ... and {len(orphans) - 10} more")
        print()

    if deprecated:
        print("  Deprecated Tools:")
        for tool in deprecated[:10]:
            tool_id = tool["id"] if isinstance(tool, dict) else tool
            print(f"    • {tool_id}")
        print()

    return 0


def cmd_rhythm(args: argparse.Namespace) -> int:
    """
    Rhythm: Kairotic Phase Detection via protocol.

    Sense the system's current kairotic state, satiation level,
    and temporal health metrics through the protocol-sense-rhythm.
    """
    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    engine = CvmEngine(db_path)
    try:
        result = engine.dispatch(
            "sense-rhythm",
            {"db_path": db_path},
            output_sink=print,
        )
    finally:
        engine.close()

    if not result.ok:
        print(f"✗ {result.error_kind}: {result.error_message}", file=sys.stderr)
        return 1

    # Format output
    data = result.data.get("data", result.data)

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  Rhythm: Kairotic Phase Detection (via Protocol)          │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    # Kairotic state
    kairotic = data.get("kairotic", {})
    phases = kairotic.get("phases", {})
    dominant = kairotic.get("dominant", "unknown")
    side = kairotic.get("side", "unknown")

    print(f"  System Phase: {dominant.upper()} ({side} side)")
    print()
    print("  Phase Weights:")
    print(f"    Pioneer:    {phases.get('pioneer', 0):.2f} │ Steward:   {phases.get('steward', 0):.2f}")
    print(f"    Cultivator: {phases.get('cultivator', 0):.2f} │ Curator:   {phases.get('curator', 0):.2f}")
    print(f"    Regulator:  {phases.get('regulator', 0):.2f} │ Scout:     {phases.get('scout', 0):.2f}")
    print()

    # Satiation
    satiation = data.get("satiation", {})
    score = satiation.get("score", 0.0)
    label = satiation.get("label", "unknown")
    print(f"  Satiation: {score:.2f} ({label})")
    print()

    # Temporal health
    health = data.get("health", {})
    growth_rate = health.get("growth_rate", 0.0)
    metabolic_balance = health.get("metabolic_balance", 0.0)
    metrics = health.get("metrics", {})

    print(f"  Growth Rate:       {growth_rate:+.1f} entities/week")
    print(f"  Metabolic Balance: {metabolic_balance:.1f} (anabolic / catabolic)")
    print()
    print("  Recent Activity (7 days):")
    print(f"    Entities created:   {metrics.get('entities_created', 0)}")
    print(f"    Bonds created:      {metrics.get('bonds_created', 0)}")
    print(f"    Learnings captured: {metrics.get('learnings_captured', 0)}")
    print(f"    Entities composted: {metrics.get('entities_composted', 0)}")
    print()

    return 0


def cmd_prune(args: argparse.Namespace) -> int:
    """Prune: Physics-Driven Code Lifecycle.

    Detect prunable entities based on graph axioms:
    - Orphan tools (no behavior implements them)
    - Deprecated tools (marked for removal)
    - Broken handlers (code not found)
    - Dark matter (code without entities)
    """
    db_path = resolve_db_path(args.db)

    # New protocol path (Phase 2 migration)
    if getattr(args, "via_protocol", False):
        return cmd_prune_via_protocol(args)

    from .prune import detect_prunable, emit_prune_signals, propose_prune

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Determine src_dir (chora_cvm package location)
    src_dir = Path(__file__).parent

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  Prune: Physics-Driven Code Lifecycle                      │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    if args.dry_run:
        print("  [DRY RUN - no signals/focuses will be created]")
        print()

    # Phase 1: Detect
    print("  Detection Phase")
    print("  ───────────────────────────────────────────────")
    report = detect_prunable(db_path, src_dir)

    print(f"    Orphan tools:       {len(report.orphan_tools)}")
    print(f"    Deprecated tools:   {len(report.deprecated_tools)}")
    print(f"    Broken handlers:    {len(report.broken_handlers)}")
    print(f"    Dark matter:        {len(report.dark_matter)}")
    print()

    # Show details if any found
    if report.orphan_tools:
        print("  Orphan Tools (no behavior implements them):")
        for tool in report.orphan_tools[:10]:
            print(f"    • {tool.id}")
            if tool.handler:
                print(f"      handler: {tool.handler}")
        if len(report.orphan_tools) > 10:
            print(f"    ... and {len(report.orphan_tools) - 10} more")
        print()

    if report.deprecated_tools:
        print("  Deprecated Tools (marked for removal):")
        for tool in report.deprecated_tools:
            print(f"    • {tool.id}")
            if tool.reason:
                print(f"      reason: {tool.reason}")
        print()

    if report.broken_handlers:
        print("  Broken Handlers (code not found):")
        for tool in report.broken_handlers:
            print(f"    • {tool.id}")
            print(f"      handler: {tool.handler}")
        print()

    if report.dark_matter:
        print("  Dark Matter (code without entities):")
        for dm in report.dark_matter[:10]:
            print(f"    • {dm['name']} ({dm['file']}:{dm['line']})")
        if len(report.dark_matter) > 10:
            print(f"    ... and {len(report.dark_matter) - 10} more")
        print()

    # Phase 2: Emit signals or propose focuses
    if args.propose:
        print("  Proposal Phase (creating Focus entities)")
        print("  ───────────────────────────────────────────────")
        focuses = propose_prune(db_path, report, dry_run=args.dry_run)

        if focuses:
            for focus in focuses:
                print(f"    + {focus['id']}: {focus['category']}")
            print()
            if not args.dry_run:
                print("  To approve: just prune-approve <focus-id>")
                print("  To reject:  just prune-reject <focus-id> <reason>")
        else:
            print("    (no items require approval)")
        print()
    else:
        print("  Signal Phase (emitting for threshold breaches)")
        print("  ───────────────────────────────────────────────")
        signals = emit_prune_signals(db_path, report, dry_run=args.dry_run)

        if signals:
            for sig in signals:
                print(f"    + {sig['id']}: {sig['category']} (count={sig['count']})")
        else:
            print("    (no threshold breaches)")
        print()

    # Summary
    total_prunable = (
        len(report.orphan_tools) +
        len(report.deprecated_tools) +
        len(report.broken_handlers)
    )

    print("  ───────────────────────────────────────────────")
    print(f"  Total prunable entities:  {total_prunable}")
    print(f"  Dark matter functions:    {len(report.dark_matter)}")
    print()

    return 0


def cmd_prune_approve(args: argparse.Namespace) -> int:
    """Approve a prune proposal: finalize entity removal with captured wisdom.

    Approving a prune:
    1. Composts the target entity (archives with provenance)
    2. Creates a learning capturing wisdom from the entity
    3. Resolves the Focus with outcome "completed"
    """
    from .prune import prune_approve

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print()
    print("  Prune Approve: Finalize Entity Removal")
    print("  " + "=" * 50)
    print()
    print(f"  Focus: {args.focus_id}")
    print()

    result = prune_approve(db_path, args.focus_id)

    if result.get("error"):
        print(f"  ERROR: {result['error']}")
        print()
        return 1

    print("  Result:")
    print("  " + "-" * 50)
    print(f"    Archived:    {result.get('archived')}")
    print(f"    Archive ID:  {result.get('archive_id')}")
    print(f"    Learning ID: {result.get('learning_id')}")
    print(f"    Tool ID:     {result.get('tool_id')}")
    print()
    print("  The entity has been composted and wisdom captured.")
    print()

    return 0


def cmd_prune_reject(args: argparse.Namespace) -> int:
    """Reject a prune proposal: decline removal and capture why.

    Rejecting a prune:
    1. Creates a learning capturing why the entity should stay
    2. Resolves the Focus with outcome "abandoned"
    3. The target entity remains unchanged
    """
    from .prune import prune_reject

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    reason = getattr(args, 'reason', None)

    print()
    print("  Prune Reject: Decline Entity Removal")
    print("  " + "=" * 50)
    print()
    print(f"  Focus: {args.focus_id}")
    if reason:
        print(f"  Reason: {reason}")
    print()

    result = prune_reject(db_path, args.focus_id, reason)

    if result.get("error"):
        print(f"  ERROR: {result['error']}")
        print()
        return 1

    print("  Result:")
    print("  " + "-" * 50)
    print(f"    Rejected:    {result.get('rejected')}")
    print(f"    Learning ID: {result.get('learning_id')}")
    print(f"    Tool ID:     {result.get('tool_id')}")
    print(f"    Reason:      {result.get('reason')}")
    print()
    print("  The prune was rejected and a learning was captured.")
    print("  The entity remains in the graph.")
    print()

    return 0


def cmd_bond(args: argparse.Namespace) -> int:
    """Bond: Create a relationship between two entities.

    The fundamental force carrier of the Loom.
    """
    from .std import manage_bond

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    result = manage_bond(
        db_path=db_path,
        bond_type=args.verb,
        from_id=args.from_id,
        to_id=args.to_id,
        confidence=args.confidence,
        enforce_physics=not args.no_physics,
    )

    if "error" in result:
        print(f"✗ {result['error']}", file=sys.stderr)
        return 1

    # Success output
    print(f"✓ Bond created: {result['id']}")
    print(f"  {result['from']} --{result['type']}--> {result['to']}")
    print(f"  confidence: {result['confidence']}")

    if result.get("signal_id"):
        print(f"  ⚡ Signal emitted: {result['signal_id']}")

    return 0


def cmd_entropy(args: argparse.Namespace) -> int:
    """Entropy: Report system metabolic health."""
    from .kernel.runner import execute_protocol

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Invoke protocol-sense-entropy via VM
    # CLI explicitly routes output to stdout via the I/O Membrane
    result = execute_protocol(
        db_path=db_path,
        protocol_id="protocol-sense-entropy",
        inputs={"db_path": db_path},
        output_sink=print,
    )

    # Handle protocol errors
    if result.get("status") == "error":
        print(f"✗ {result.get('error_kind', 'Error')}: {result.get('error_message', 'Unknown')}", file=sys.stderr)
        return 1

    health = result.get("health", {})
    signals = result.get("signals_emitted", [])

    print()
    print("  Metabolic Health Report")
    print("  " + "=" * 50)
    print()
    print(f"    Total entities:     {health.get('total_entities', 0)}")
    print(f"    Total bonds:        {health.get('total_bonds', 0)}")
    print()
    print(f"    Orphan count:       {health.get('orphan_count', 0)}")
    print(f"    Deprecated:         {health.get('deprecated_count', 0)}")
    print(f"    Learning count:     {health.get('learning_count', 0)}")
    print()

    if signals:
        print("  Signals Emitted:")
        print("  " + "-" * 50)
        for sig in signals:
            print(f"    - {sig.get('id')}: {sig.get('type')} (count={sig.get('count')})")
        print()

    return 0


def cmd_digest(args: argparse.Namespace) -> int:
    """Digest: Transform an entity into a learning."""
    from .kernel.runner import execute_protocol

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Invoke protocol-digest via VM
    # CLI explicitly routes output to stdout via the I/O Membrane
    result = execute_protocol(
        db_path=db_path,
        protocol_id="protocol-digest",
        inputs={"db_path": db_path, "entity_id": args.entity_id},
        output_sink=print,
    )

    # Handle protocol errors
    if result.get("status") == "error":
        print(f"✗ {result.get('error_kind', 'Error')}: {result.get('error_message', 'Unknown')}", file=sys.stderr)
        return 1

    if "error" in result:
        print(f"✗ {result['error']}", file=sys.stderr)
        return 1

    print()
    print("  Digest Complete")
    print("  " + "=" * 50)
    print()
    print(f"    Source:     {args.entity_id}")
    print(f"    Learning:   {result.get('learning_id')}")
    print(f"    Bond:       {result.get('bond_id')}")
    print()
    wisdom = result.get("wisdom", {})
    insight = wisdom.get('insight', '')
    if insight and len(insight) > 80:
        insight = insight[:80] + "..."
    print(f"    Insight:    {insight}")
    print(f"    Domain:     {wisdom.get('domain')}")
    print()

    return 0


def cmd_compost(args: argparse.Namespace) -> int:
    """Compost: Archive an orphan entity."""
    from .metabolic import compost

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    result = compost(db_path, args.entity_id, force=args.force)

    if "error" in result:
        print(f"✗ {result['error']}", file=sys.stderr)
        if result.get("bond_count"):
            print(f"  Entity has {result['bond_count']} active bonds.")
            print("  Use --force to archive anyway, or use 'just digest' instead.")
        return 1

    print()
    print("  Compost Complete")
    print("  " + "=" * 50)
    print()
    print(f"    Entity:         {args.entity_id}")
    print(f"    Archived:       {result.get('archived')}")
    print(f"    Archive ID:     {result.get('archive_id')}")
    print(f"    Learning:       {result.get('learning_id')}")
    print(f"    Bonds archived: {result.get('bonds_archived', 0)}")
    print()

    return 0


def cmd_induce(args: argparse.Namespace) -> int:
    """Induce: Propose a pattern from clustered learnings."""
    from .kernel.runner import execute_protocol

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    if len(args.learning_ids) < 3:
        print("✗ Minimum 3 learnings required for pattern induction.", file=sys.stderr)
        return 1

    # Invoke protocol-induce via VM
    # CLI explicitly routes output to stdout via the I/O Membrane
    result = execute_protocol(
        db_path=db_path,
        protocol_id="protocol-induce",
        inputs={"db_path": db_path, "learning_ids": args.learning_ids},
        output_sink=print,
    )

    # Handle protocol errors
    if result.get("status") == "error":
        print(f"✗ {result.get('error_kind', 'Error')}: {result.get('error_message', 'Unknown')}", file=sys.stderr)
        return 1

    if "error" in result:
        print(f"✗ {result['error']}", file=sys.stderr)
        return 1

    print()
    print("  Induction Complete")
    print("  " + "=" * 50)
    print()
    print(f"    Pattern:          {result.get('pattern_id')}")
    print(f"    Domain:           {result.get('domain')}")
    print(f"    Source learnings: {result.get('crystallized_from_count')}")
    print(f"    Review signal:    {result.get('review_signal_id')}")
    print()
    print("  Pattern is in 'proposed' status. Review and adopt or reject.")
    print()

    return 0


def cmd_reflex_build(args: argparse.Namespace) -> int:
    """Reflex Build: Detect build quality regressions and emit signals.

    The Build Reflex is the autonomic nervous system for build governance:
    - Runs quality checks (lint, typecheck, test) on packages
    - Detects regressions from previous known-good state
    - Emits signals for failures
    - Auto-resolves signals when builds pass again
    """
    from .reflex.build import run_build_reflex, PYTHON_PACKAGES

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Parse packages
    packages = [args.package] if args.package else None
    checks = [args.check] if args.check else None

    # Run the reflex
    result = run_build_reflex(
        db_path=db_path,
        packages=packages,
        checks=checks,
        dry_run=args.dry_run,
        verbose=True,
    )

    # Return non-zero if there are failures
    return 1 if result.failures else 0


def cmd_bootstrap_build(args: argparse.Namespace) -> int:
    """Bootstrap Build: Manifest build governance entities.

    Creates principles, patterns, behaviors, tools, and bonds that encode
    build practices as structural governance in the Loom.

    Also bootstraps build primitives and protocols for graph-defined build checking.

    This command is idempotent - running twice will update existing entities
    rather than creating duplicates (upsert semantics).
    """
    from .bootstrap.build import bootstrap_build_entities
    from .genesis_build import bootstrap_build_governance
    from .store import EventStore

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Bootstrap build governance entities (principles, patterns, behaviors, tools)
    result = bootstrap_build_entities(
        db_path=db_path,
        verbose=True,
    )

    # Bootstrap build primitives and protocol
    print()
    store = EventStore(db_path)
    try:
        governance_result = bootstrap_build_governance(store, verbose=True)
    finally:
        store.close()

    # Return success if any entities were created
    total = result.total_entities + len(governance_result.get("primitives", [])) + len(governance_result.get("protocols", []))
    return 0 if total > 0 else 1


def cmd_build_check(args: argparse.Namespace) -> int:
    """Check build integrity across all packages.

    Runs lint, typecheck, and tests on all packages. Optionally emits
    signals for any failures to the Loom.

    This uses allow-listed build primitives (ruff, mypy, pytest) rather
    than generic shell execution.
    """
    from .std import check_build_integrity

    db_path = resolve_db_path(args.db) if args.db else None
    workspace_path = args.workspace if args.workspace else None
    emit_signals = not args.no_signals and db_path is not None

    print("Build Integrity Check")
    print("=" * 60)
    print()

    result = check_build_integrity(
        workspace_path=workspace_path,
        db_path=db_path,
        emit_signals=emit_signals,
    )

    # Display results
    for pkg_name, pkg_results in result.get("results", {}).items():
        print(f"Package: {pkg_name}")
        print("-" * 40)
        for check_name, check_result in pkg_results.items():
            status = "✓" if check_result.get("success") else "✗"
            print(f"  {status} {check_name}: exit_code={check_result.get('exit_code')}")
            if not check_result.get("success") and check_result.get("stderr"):
                # Show first few lines of error
                stderr_lines = check_result["stderr"].strip().split("\n")[:5]
                for line in stderr_lines:
                    print(f"      {line}")
        print()

    # Summary
    print("=" * 60)
    if result.get("healthy"):
        print("Build Health: ✓ All checks passed")
    else:
        print(f"Build Health: ✗ Failures detected")
        if result.get("signals_emitted"):
            print(f"Signals emitted: {len(result['signals_emitted'])}")
            for sig in result["signals_emitted"]:
                print(f"  - {sig}")

    return 0 if result.get("healthy") else 1


def cmd_create(args: argparse.Namespace) -> int:
    """Create an entity (manifest into the Loom)."""
    import re
    from .std import manifest_entity

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Parse data JSON
    try:
        data = json.loads(args.data) if args.data else {}
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {e}", file=sys.stderr)
        return 1

    # Add title to data
    data["title"] = args.title

    # Generate entity ID
    slug = re.sub(r"[^a-z0-9]+", "-", args.title.lower()).strip("-")
    entity_id = f"{args.type}-{slug}"

    result = manifest_entity(
        db_path=db_path,
        entity_type=args.type,
        entity_id=entity_id,
        data=data,
    )

    print(f"Manifested: {result['id']} ({result['type']})")

    # Show bond suggestions (graceful degradation)
    try:
        from .semantic import embed_entity, suggest_bonds

        embed_result = embed_entity(db_path, result['id'])
        if embed_result.get("method") == "semantic":
            print(f"  Embedded: {embed_result.get('dimension', 0)} dimensions")

        suggest_result = suggest_bonds(db_path, result['id'], limit=5)
        candidates = suggest_result.get("candidates", [])

        if candidates:
            print("\n  Suggested bonds:")
            for c in candidates[:5]:
                sim = c.get("similarity", 0)
                sim_str = f" ({sim:.0%})" if sim else ""
                print(f"    just {c['bond_type']} {result['id']} {c['to_id']}{sim_str}")
        elif suggest_result.get("method") == "fallback":
            print("\n  (No bond suggestions - inference unavailable)")
    except Exception:
        pass  # Graceful degradation

    return 0


def cmd_garden(args: argparse.Namespace) -> int:
    """Auto-Gardener: Propose bonds for entities with voids."""
    import uuid
    from datetime import datetime, timezone
    from .store import EventStore
    from .semantic import suggest_bonds
    from .std import manifest_entity, manage_bond

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    confidence_threshold = args.confidence
    dry_run = args.dry_run
    auto_create = args.auto

    print()
    print("  Auto-Gardener: Bond Proposal Engine")
    print("  " + "=" * 50)
    print()
    print(f"  Confidence threshold: {confidence_threshold:.0%}")
    if dry_run:
        print("  [DRY RUN - no signals or bonds will be created]")
    if auto_create:
        print("  [AUTO-CREATE - bonds ≥95% will be created automatically]")
    print()

    # Find void entities
    store = EventStore(db_path)
    cur = store._conn.cursor()

    void_entities = []

    # Behaviors without implementing tools
    cur.execute("""
        SELECT e.id, e.type, json_extract(e.data_json, '$.title') as title
        FROM entities e
        WHERE e.type = 'behavior'
        AND NOT EXISTS (
            SELECT 1 FROM bonds b WHERE b.to_id = e.id AND b.type = 'implements'
        )
    """)
    for row in cur.fetchall():
        void_entities.append({
            "id": row[0], "type": row[1], "title": row[2],
            "void": "needs implementing tool",
        })

    # Stories without specifying behaviors
    cur.execute("""
        SELECT e.id, e.type, json_extract(e.data_json, '$.title') as title
        FROM entities e
        WHERE e.type = 'story'
        AND NOT EXISTS (
            SELECT 1 FROM bonds b WHERE b.from_id = e.id AND b.type = 'specifies'
        )
    """)
    for row in cur.fetchall():
        void_entities.append({
            "id": row[0], "type": row[1], "title": row[2],
            "void": "needs specifying behavior",
        })

    # Tools without verifying behaviors
    cur.execute("""
        SELECT e.id, e.type, json_extract(e.data_json, '$.title') as title
        FROM entities e
        WHERE e.type = 'tool'
        AND NOT EXISTS (
            SELECT 1 FROM bonds b WHERE b.from_id = e.id AND b.type = 'verifies'
        )
    """)
    for row in cur.fetchall():
        void_entities.append({
            "id": row[0], "type": row[1], "title": row[2],
            "void": "needs verifying behavior",
        })

    store.close()

    # Generate proposals
    proposals = []
    bonds_created = []

    for void_entity in void_entities:
        result = suggest_bonds(db_path, void_entity["id"], limit=5)
        if result.get("error"):
            continue

        for candidate in result.get("candidates", []):
            similarity = candidate.get("similarity", 0)
            if similarity < confidence_threshold:
                continue

            proposal = {
                "from_id": void_entity["id"],
                "to_id": candidate["to_id"],
                "bond_type": candidate["bond_type"],
                "similarity": similarity,
                "void": void_entity["void"],
            }
            proposals.append(proposal)

            if auto_create and similarity >= 0.95 and not dry_run:
                bond_result = manage_bond(
                    db_path, candidate["bond_type"],
                    void_entity["id"], candidate["to_id"],
                    confidence=similarity,
                )
                bonds_created.append({
                    "bond_id": bond_result.get("id"),
                    "type": candidate["bond_type"],
                    "from": void_entity["id"],
                    "to": candidate["to_id"],
                })

    # Emit signal if proposals exist
    signals_emitted = []
    if proposals and not dry_run:
        signal_id = f"signal-garden-proposals-{uuid.uuid4().hex[:8]}"
        manifest_entity(
            db_path, "signal", signal_id, {
                "title": f"Garden: {len(proposals)} bond proposals above {confidence_threshold:.0%}",
                "status": "active",
                "signal_type": "garden-proposal",
                "proposal_count": len(proposals),
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        signals_emitted.append(signal_id)

    # Output
    print(f"  Entities with voids:  {len(void_entities)}")
    print(f"  Proposals generated:  {len(proposals)}")
    print()

    if proposals:
        print("  Proposed Bonds:")
        print("  " + "-" * 50)
        for p in proposals[:20]:
            print(f"    {p['similarity']:.0%}  {p['bond_type']:12}  {p['from_id'][:30]}...")
            print(f"          → {p['to_id'][:40]}...")
        if len(proposals) > 20:
            print(f"    ... and {len(proposals) - 20} more")
        print()

    if bonds_created:
        print(f"  Bonds auto-created:   {len(bonds_created)}")
        for b in bonds_created:
            print(f"    ✓ {b['bond_id']}")
        print()

    if signals_emitted:
        print(f"  Signals emitted:      {len(signals_emitted)}")
        for s in signals_emitted:
            print(f"    + {s}")
        print()

    return 0


def cmd_horizon(args: argparse.Namespace) -> int:
    """Horizon: What wants attention (unverified tools near recent learnings)."""
    from .kernel.runner import execute_protocol

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # CLI explicitly routes output to stdout via the I/O Membrane
    result = execute_protocol(
        db_path=db_path,
        protocol_id="protocol-horizon",
        inputs={"db_path": db_path, "days": args.days, "limit": args.limit},
        output_sink=print,
    )

    method = result.get("method", "unknown")
    recommendations = result.get("recommendations", [])
    recent_learnings = result.get("recent_learnings", [])
    unverified_tools = result.get("unverified_tools", [])

    print()
    print("═" * 60)
    print("  HORIZON: What wants attention")
    print("═" * 60)
    print()
    print(f"  Method: {method}")
    print(f"  Recent learnings (last {args.days} days): {len(recent_learnings)}")
    print(f"  Unverified tools: {len(unverified_tools)}")
    print()

    if result.get("note"):
        print(f"  Note: {result['note']}")
        print()
        return 0

    if not recommendations:
        print("  No recommendations - all tools are verified!")
        return 0

    print("  Recommendations (unverified tools close to recent learnings):")
    print("  " + "-" * 56)
    print()

    for rec in recommendations:
        tool_id = rec.get("tool_id", "?")
        similarity = rec.get("similarity", 0)
        reasoning = rec.get("reasoning", "")

        short_id = tool_id.replace("tool-", "")
        if len(short_id) > 40:
            short_id = short_id[:37] + "..."

        print(f"    {short_id:<42} ({similarity:.0%})")
        if reasoning:
            print(f"      → {reasoning}")
        print()

    print("  To verify a tool, wire it to a behavior:")
    print("    just verifies <tool-id> <behavior-id>")
    print()

    return 0


def cmd_reflex_arc(args: argparse.Namespace) -> int:
    """Reflex Arc: Autonomic void detection and signal emission."""
    import uuid
    from datetime import datetime, timezone
    from .store import EventStore
    from .std import manifest_entity
    from .metabolic import detect_stagnation, check_void_resolution

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    dry_run = args.dry_run

    print()
    print("  Reflex Arc: Autonomic Void Detection")
    print("  " + "=" * 50)
    print()

    if dry_run:
        print("  [DRY RUN - no signals will be emitted]")
        print()

    # Load axioms for homoiconic void detection
    store = EventStore(db_path)
    cur = store._conn.cursor()

    cur.execute("""
        SELECT
            json_extract(data_json, '$.verb') as verb,
            json_extract(data_json, '$.subject_type') as subject_type,
            json_extract(data_json, '$.object_type') as object_type
        FROM entities WHERE type = 'axiom'
        AND json_extract(data_json, '$.verb') IS NOT NULL
    """)
    axioms = {row[0]: {"subject_type": row[1], "object_type": row[2]}
              for row in cur.fetchall() if row[0] and row[1]}

    # Phase 1: Detect generative chain voids
    print("  Phase 1: Detecting generative chain voids...")
    voids = {
        "behaviors_without_tools": [],
        "stories_without_behaviors": [],
        "learnings_undigested": [],
        "principles_ungoverning": [],
    }

    if "implements" in axioms:
        ax = axioms["implements"]
        cur.execute("""
            SELECT e.id, json_extract(e.data_json, '$.title') as title
            FROM entities e WHERE e.type = ?
            AND NOT EXISTS (SELECT 1 FROM bonds b WHERE b.from_id = e.id AND b.type = ?)
        """, (ax["subject_type"], "implements"))
        voids["behaviors_without_tools"] = [{"id": r[0], "title": r[1]} for r in cur.fetchall()]

    if "specifies" in axioms:
        ax = axioms["specifies"]
        cur.execute("""
            SELECT e.id, json_extract(e.data_json, '$.title') as title
            FROM entities e WHERE e.type = ?
            AND NOT EXISTS (SELECT 1 FROM bonds b WHERE b.from_id = e.id AND b.type = ?)
        """, (ax["subject_type"], "specifies"))
        voids["stories_without_behaviors"] = [{"id": r[0], "title": r[1]} for r in cur.fetchall()]

    if "surfaces" in axioms or "induces" in axioms:
        cur.execute("""
            SELECT e.id, json_extract(e.data_json, '$.title') as title
            FROM entities e WHERE e.type = 'learning'
            AND NOT EXISTS (SELECT 1 FROM bonds b WHERE b.from_id = e.id AND b.type IN ('surfaces', 'induces'))
            AND COALESCE(json_extract(e.data_json, '$.status'), '') != 'digested'
        """)
        voids["learnings_undigested"] = [{"id": r[0], "title": r[1]} for r in cur.fetchall()]

    if "governs" in axioms:
        ax = axioms["governs"]
        cur.execute("""
            SELECT e.id, json_extract(e.data_json, '$.title') as title
            FROM entities e WHERE e.type = ?
            AND NOT EXISTS (SELECT 1 FROM bonds b WHERE b.from_id = e.id AND b.type = ?)
        """, (ax["subject_type"], "governs"))
        voids["principles_ungoverning"] = [{"id": r[0], "title": r[1]} for r in cur.fetchall()]

    store.close()

    print(f"    Behaviors without tools:    {len(voids['behaviors_without_tools'])}")
    print(f"    Stories without behaviors:  {len(voids['stories_without_behaviors'])}")
    print(f"    Learnings undigested:       {len(voids['learnings_undigested'])}")
    print(f"    Principles ungoverning:     {len(voids['principles_ungoverning'])}")
    print()

    # Phase 2: Emit signals for threshold breaches
    print("  Phase 2: Emitting signals for threshold breaches...")
    void_signals = []

    thresholds = [
        ("behaviors_without_tools", 5, "behavior-tool-gap"),
        ("stories_without_behaviors", 3, "story-behavior-gap"),
        ("learnings_undigested", 10, "learning-wisdom-gap"),
    ]

    for void_key, threshold, category in thresholds:
        count = len(voids[void_key])
        if count > threshold:
            signal_id = f"signal-{void_key.replace('_', '-')}-{uuid.uuid4().hex[:8]}"
            if not dry_run:
                manifest_entity(db_path, "signal", signal_id, {
                    "title": f"Generative void: {count} {void_key.replace('_', ' ')}",
                    "status": "active",
                    "signal_type": "generative-void",
                    "category": category,
                    "count": count,
                    "sample_ids": [v["id"] for v in voids[void_key][:5]],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })
            void_signals.append({"id": signal_id, "category": category, "count": count})

    if void_signals:
        for sig in void_signals:
            print(f"    + {sig['id']}: {sig['category']} (count={sig['count']})")
    else:
        print("    (no threshold breaches)")
    print()

    # Phase 3: Detect metabolic stagnation
    print("  Phase 3: Detecting metabolic stagnation...")
    if not dry_run:
        stagnation = detect_stagnation(db_path)
        if stagnation["signals_emitted"]:
            for sig in stagnation["signals_emitted"]:
                print(f"    + {sig['id']}: {sig.get('category', 'stagnation')}")
        else:
            print("    (no stagnation detected)")
    else:
        print("    (skipped in dry run)")
    print()

    # Phase 4: Self-healing
    print("  Phase 4: Self-healing (auto-resolving cleared voids)...")
    if not dry_run:
        resolution = check_void_resolution(db_path)
        if resolution["resolved_signals"]:
            for sig_id in resolution["resolved_signals"]:
                print(f"    ✓ Auto-resolved: {sig_id}")
        else:
            print("    (no voids cleared)")
    else:
        print("    (skipped in dry run)")
    print()

    # Summary
    total_voids = sum(len(voids[k]) for k in voids)
    print("  " + "-" * 50)
    print(f"  Total generative voids:     {total_voids}")
    print(f"  Signals emitted:            {len(void_signals)}")
    print()

    return 0


def cmd_search(args: argparse.Namespace) -> int:
    """Semantic search across the Loom."""
    from .semantic import semantic_search

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    result = semantic_search(
        db_path=db_path,
        query=args.query,
        entity_type=args.type,
        limit=args.limit,
    )

    method = result.get("method", "unknown")
    results = result.get("results", [])

    print(f"\nSearch: \"{args.query}\"")
    print(f"Method: {method}")
    if args.type:
        print(f"Filter: {args.type}")
    print()

    if not results:
        print("  No results found.")
        return 0

    for r in results:
        entity_type = r.get("type", "?")
        entity_id = r.get("id", "?")
        score = r.get("similarity") or r.get("rank")
        score_str = f" ({score:.0%})" if isinstance(score, float) else ""

        print(f"  {entity_type:12} {entity_id}{score_str}")

        preview = r.get("preview", "")
        if preview:
            preview = preview[:60].replace("\n", " ")
            if len(preview) == 60:
                preview += "..."
            print(f"               {preview}")

    return 0


def cmd_confidence(args: argparse.Namespace) -> int:
    """Update the confidence of an existing bond."""
    from .std import update_bond_confidence

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    result = update_bond_confidence(
        db_path=db_path,
        bond_id=args.bond_id,
        confidence=args.confidence,
    )

    if "error" in result:
        print(f"✗ {result['error']}", file=sys.stderr)
        return 1

    previous = result["previous"]
    new = result["new"]

    if new < previous:
        print(f"↓ Confidence reduced: {previous:.2f} → {new:.2f}")
    elif new > previous:
        print(f"↑ Confidence increased: {previous:.2f} → {new:.2f}")
    else:
        print(f"= Confidence unchanged: {new:.2f}")

    if result.get("signal_id"):
        print(f"  ⚡ Signal emitted: {result['signal_id']}")

    return 0


def cmd_harvest_entities(args: argparse.Namespace) -> int:
    """Harvest entities from legacy ~/.chora databases."""
    from .harvest.schema import init_legacy_db, get_db_stats
    from .harvest.entity_extractor import harvest_entities_to_db, search_legacy_entities
    import sqlite3

    db_path = args.db_path
    chora_dir = os.path.expanduser("~/.chora")

    legacy_databases = [
        "chora-old-v3.db",
        "chora-old.db",
        "chora.db",
        "chora-v3.db",
        "chora-v2.db",
    ]

    # Search mode
    if args.search:
        if not os.path.exists(db_path):
            print(f"✗ Database not found: {db_path}", file=sys.stderr)
            print(f"  Run: cvm harvest entities to create it", file=sys.stderr)
            return 1

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        print(f"[*] Searching legacy entities for: {args.search}")
        print()

        results = search_legacy_entities(conn, args.search, args.limit)

        if not results:
            print("  No results found.")
            conn.close()
            return 0

        for i, r in enumerate(results, 1):
            print(f"{i:2}. [{r['source_db']}] {r['entity_type']}: {r['title']}")
            print(f"    ID: {r['entity_id']}")
            if r.get("status"):
                print(f"    Status: {r['status']}")
            snippet = r.get("snippet", "")
            if snippet:
                snippet = snippet.replace("\n", " ")[:200]
                print(f"    {snippet}")
            print()

        conn.close()
        return 0

    # Stats mode
    if args.stats:
        if not os.path.exists(db_path):
            print(f"✗ Database not found: {db_path}", file=sys.stderr)
            return 1

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        stats = get_db_stats(conn)

        print(f"[*] Legacy Entity Statistics: {db_path}")
        print()
        print(f"    Legacy entities: {stats.get('legacy_entities', 0)}")
        print(f"    Legacy relationships: {stats.get('legacy_relationships', 0)}")
        print()

        if stats.get("legacy_by_type"):
            print("    Entities by type:")
            for etype, count in sorted(stats["legacy_by_type"].items()):
                print(f"      {etype}: {count}")

        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT source_db, COUNT(*) as count
                FROM legacy_entities
                GROUP BY source_db
                ORDER BY count DESC
            """)
            print()
            print("    Entities by source:")
            for row in cur.fetchall():
                print(f"      {row['source_db']}: {row['count']}")
        except sqlite3.OperationalError:
            pass

        conn.close()
        return 0

    # Harvest mode (default)
    print(f"[*] Legacy Entity Harvester")
    print(f"    Target database: {db_path}")
    print(f"    Source directory: {chora_dir}")
    print()

    available_dbs = []
    print(f"[*] Checking for legacy databases:")
    for db_name in legacy_databases:
        source_path = os.path.join(chora_dir, db_name)
        if os.path.exists(source_path):
            try:
                conn = sqlite3.connect(source_path)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM entities WHERE type != 'relationship'")
                entity_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM entities WHERE type = 'relationship'")
                rel_count = cur.fetchone()[0]
                conn.close()
                print(f"    [found] {db_name:<20} {entity_count:>5} entities, {rel_count:>5} relationships")
                available_dbs.append(source_path)
            except Exception as e:
                print(f"    [error] {db_name:<20} {e}")
        else:
            print(f"    [skip]  {db_name:<20} not found")

    if not available_dbs:
        print("\n! No legacy databases found to harvest")
        return 1

    print()

    conn = init_legacy_db(db_path)
    total_entities = 0
    total_relationships = 0

    for source_path in available_dbs:
        print(f"[*] Harvesting: {os.path.basename(source_path)}")
        result = harvest_entities_to_db(source_path, conn)

        print(f"    Entities: {result['entities']}")
        for etype, count in sorted(result["by_type"].items()):
            print(f"      {etype}: {count}")
        print(f"    Relationships: {result['relationships']}")

        total_entities += result["entities"]
        total_relationships += result["relationships"]
        print()

    conn.close()

    print("=" * 60)
    print(f"[*] Harvest Complete!")
    print(f"    Total entities: {total_entities}")
    print(f"    Total relationships: {total_relationships}")
    print()
    print(f"[*] Search with:")
    print(f"    cvm harvest entities --search 'your query'")

    return 0


def cmd_harvest_legacy(args: argparse.Namespace) -> int:
    """Harvest content from legacy repository packages."""
    from .harvest import LegacyHarvester, init_legacy_db
    from .harvest.config import get_legacy_repo_configs, get_archive_repo_configs
    from .harvest.harvester import search_legacy
    from .harvest.schema import get_db_stats
    import sqlite3

    db_path = args.db_path
    workspace_root = args.workspace

    # Search mode
    if args.search:
        if not os.path.exists(db_path):
            print(f"✗ Database not found: {db_path}", file=sys.stderr)
            print(f"  Run: cvm harvest legacy to create it", file=sys.stderr)
            return 1

        print(f"[*] Searching for: {args.search}")
        print()

        results = search_legacy(db_path, args.search, args.limit)

        if not results:
            print("  No results found.")
            return 0

        for i, r in enumerate(results, 1):
            print(f"{i:2}. [{r['repo_name']}] {r['doc_title']}")
            print(f"    Section: {r['section_title']} ({r['chunk_type']})")
            print(f"    Path: {r['relative_path']}")
            snippet = r['snippet'].replace('\n', ' ')[:200]
            print(f"    {snippet}...")
            print()

        return 0

    # Stats mode
    if args.stats:
        if not os.path.exists(db_path):
            print(f"✗ Database not found: {db_path}", file=sys.stderr)
            return 1

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        stats = get_db_stats(conn)

        print(f"[*] Database Statistics: {db_path}")
        print()
        print(f"    Repositories: {stats['repositories']}")
        print(f"    Documents: {stats['documents']} ({stats['duplicates']} duplicates)")
        print(f"    Chunks: {stats['chunks']}")
        print(f"    Tag types: {stats['tag_types']} ({stats['total_tags']} total tags)")
        print()
        print("    Documents by type:")
        for content_type, count in stats.get('by_type', {}).items():
            print(f"      {content_type}: {count}")
        print()

        cur = conn.cursor()
        cur.execute("""
            SELECT r.name, r.priority, r.total_files,
                   COUNT(DISTINCT d.id) as docs,
                   COUNT(DISTINCT CASE WHEN d.is_duplicate_of IS NOT NULL THEN d.id END) as dups
            FROM repositories r
            LEFT JOIN documents d ON d.repository_id = r.id
            GROUP BY r.id
            ORDER BY r.priority DESC
        """)

        print("    Repositories:")
        for row in cur.fetchall():
            print(f"      {row['name']:<25} priority={row['priority']:>2}  docs={row['docs']:>4}  dups={row['dups']:>3}")

        conn.close()
        return 0

    # Harvest mode (default)
    print(f"[*] Legacy Content Harvester")
    print(f"    Database: {db_path}")
    print(f"    Workspace: {workspace_root}")
    if args.archive:
        print(f"    Mode: Including archive repositories")
    print()

    configs = get_legacy_repo_configs()
    if args.archive:
        configs = configs + get_archive_repo_configs()

    print(f"[*] Configured repositories:")
    for config in configs:
        repo_path = config.get_absolute_path(workspace_root)
        exists = "✓" if repo_path.exists() else "✗"
        print(f"    {exists} {config.name:<25} priority={config.priority:>2}  {config.description[:50]}")
    print()

    harvester = LegacyHarvester(db_path, configs, workspace_root)
    report = harvester.harvest_all()

    print()
    print("=" * 60)
    print(f"[*] Harvest Complete!")
    print(f"    Duration: {report.duration_seconds:.1f}s")
    print(f"    Files processed: {report.total_files}")
    print(f"    Chunks created: {report.total_chunks}")
    print(f"    Duplicates found: {report.total_duplicates}")
    print()
    print(f"[*] Search with:")
    print(f"    cvm harvest legacy --search 'your query'")

    return 0


def cmd_harvest_plans(args: argparse.Namespace) -> int:
    """Harvest plans corpus from ~/.claude/plans into FTS database."""
    import re
    import sqlite3
    from datetime import datetime
    from hashlib import sha256

    db_path = args.db_path
    plans_dir = os.path.expanduser("~/.claude/plans")

    def init_plans_db(path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                path TEXT NOT NULL,
                title TEXT,
                size_bytes INTEGER,
                line_count INTEGER,
                harvested_at TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                section_title TEXT,
                content TEXT NOT NULL,
                line_start INTEGER,
                line_end INTEGER,
                chunk_type TEXT DEFAULT 'section',
                FOREIGN KEY (document_id) REFERENCES documents(id)
            )
        """)

        try:
            cur.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts
                USING fts5(id, document_id, section_title, content, tokenize='porter unicode61')
            """)
        except sqlite3.OperationalError:
            pass

        cur.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id TEXT PRIMARY KEY,
                chunk_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                confidence TEXT DEFAULT 'low',
                promoted INTEGER DEFAULT 0,
                promoted_to TEXT,
                FOREIGN KEY (chunk_id) REFERENCES chunks(id)
            )
        """)

        conn.commit()
        return conn

    def parse_markdown_sections(content: str) -> list:
        lines = content.split('\n')
        sections = []
        current_section = {'title': 'Preamble', 'lines': [], 'line_start': 1}

        for i, line in enumerate(lines, 1):
            header_match = re.match(r'^(#{1,6})\s+(.+)$', line)
            if header_match:
                if current_section['lines']:
                    current_section['line_end'] = i - 1
                    current_section['content'] = '\n'.join(current_section['lines']).strip()
                    if current_section['content']:
                        sections.append(current_section)
                current_section = {
                    'title': header_match.group(2).strip(),
                    'level': len(header_match.group(1)),
                    'lines': [],
                    'line_start': i
                }
            else:
                current_section['lines'].append(line)

        if current_section['lines']:
            current_section['line_end'] = len(lines)
            current_section['content'] = '\n'.join(current_section['lines']).strip()
            if current_section['content']:
                sections.append(current_section)

        return sections

    def harvest_file(conn: sqlite3.Connection, filepath: str) -> dict:
        filename = os.path.basename(filepath)
        doc_id = f"doc-{sha256(filepath.encode()).hexdigest()[:12]}"

        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

        lines = content.split('\n')
        line_count = len(lines)
        size_bytes = len(content.encode('utf-8'))

        title_match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
        title = title_match.group(1).strip() if title_match else filename

        cur = conn.cursor()

        cur.execute("""
            INSERT INTO documents (id, filename, path, title, size_bytes, line_count, harvested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title, size_bytes=excluded.size_bytes,
                line_count=excluded.line_count, harvested_at=excluded.harvested_at
        """, (doc_id, filename, filepath, title, size_bytes, line_count, datetime.now().isoformat()))

        cur.execute("DELETE FROM chunks WHERE document_id = ?", (doc_id,))
        try:
            cur.execute("DELETE FROM chunks_fts WHERE document_id = ?", (doc_id,))
        except sqlite3.OperationalError:
            pass

        sections = parse_markdown_sections(content)
        chunk_count = 0

        for i, section in enumerate(sections):
            chunk_id = f"{doc_id}-chunk-{i:03d}"
            cur.execute("""
                INSERT INTO chunks (id, document_id, section_title, content, line_start, line_end, chunk_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (chunk_id, doc_id, section.get('title', 'Untitled'), section['content'],
                  section.get('line_start', 0), section.get('line_end', 0), 'section'))

            try:
                cur.execute("""
                    INSERT INTO chunks_fts (id, document_id, section_title, content)
                    VALUES (?, ?, ?, ?)
                """, (chunk_id, doc_id, section.get('title', ''), section['content']))
            except sqlite3.OperationalError:
                pass

            chunk_count += 1

        conn.commit()
        return {'doc_id': doc_id, 'filename': filename, 'title': title, 'chunks': chunk_count, 'lines': line_count}

    print(f"[*] Harvesting plans corpus into {db_path}...")
    print(f"    Source: {plans_dir}")

    if not os.path.exists(plans_dir):
        print(f"✗ Plans directory not found: {plans_dir}", file=sys.stderr)
        return 1

    conn = init_plans_db(db_path)

    md_files = sorted(Path(plans_dir).glob("*.md"))
    print(f"    Found {len(md_files)} markdown files")
    print()

    total_chunks = 0
    total_lines = 0

    for filepath in md_files:
        result = harvest_file(conn, str(filepath))
        print(f"    ✓ {result['filename'][:40]:<40} | {result['chunks']:>3} chunks | {result['lines']:>5} lines")
        total_chunks += result['chunks']
        total_lines += result['lines']

    conn.close()

    print()
    print(f"[*] Harvest complete!")
    print(f"    Documents: {len(md_files)}")
    print(f"    Chunks: {total_chunks}")
    print(f"    Lines: {total_lines}")
    print()
    print(f"[*] Search with:")
    print(f"    sqlite3 {db_path} \"SELECT document_id, section_title, substr(content, 1, 100) FROM chunks_fts WHERE chunks_fts MATCH 'query'\"")

    return 0


def cmd_harvest_principles(args: argparse.Namespace) -> int:
    """Harvest principles from legacy archive into the Loom."""
    from .std import manifest_entity
    import sqlite3

    loom_db = resolve_db_path(args.db)
    legacy_db = os.path.expanduser("~/.chora/chora.db")

    if not os.path.exists(legacy_db):
        print(f"✗ Legacy DB not found at {legacy_db}", file=sys.stderr)
        return 1

    print(f"[*] Harvesting principles from {legacy_db} into {loom_db}...")

    legacy_conn = sqlite3.connect(legacy_db)
    legacy_conn.row_factory = sqlite3.Row

    cursor = legacy_conn.cursor()
    cursor.execute("SELECT id, type, title, status, data FROM entities WHERE type = 'principle'")
    principles = cursor.fetchall()

    print(f"    Found {len(principles)} principles in Archive")

    harvested = []
    for row in principles:
        entity_id = row["id"]
        base_data = json.loads(row["data"]) if row["data"] else {}
        data = {"title": row["title"], "status": row["status"], **base_data}

        print(f"    -> Harvesting {entity_id}...")
        try:
            result = manifest_entity(
                db_path=loom_db,
                entity_type="principle",
                entity_id=entity_id,
                data=data
            )
            if result.get("id"):
                harvested.append(entity_id)
                print(f"       SUCCESS: {result.get('id')}")
            else:
                print(f"       SKIP: unexpected return value")
        except Exception as e:
            print(f"       ERROR: {e}")

    legacy_conn.close()

    print(f"\n[*] Harvest complete. {len(harvested)} principles now in Loom.")
    print("\n[*] Harvested principles:")
    for p in harvested:
        print(f"    - {p}")

    return 0


def cmd_harvest_setup(args: argparse.Namespace) -> int:
    """Bootstrap harvest primitives and protocol."""
    import uuid
    from .registry import PrimitiveRegistry
    from .schema import (
        EventClock, EventOp, EventRecord, EventType,
        PrimitiveEntity, ProtocolEntity, StateStatus,
    )
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)

    def _run_manifest(store, registry, protocol, inputs):
        vm = ProtocolVM(registry)
        state = vm.spawn(protocol, inputs)
        state.id = str(uuid.uuid4())
        state.status = StateStatus.RUNNING

        spawn_evt = EventRecord(
            id=str(uuid.uuid4()),
            clock=EventClock(actor="harvest-setup", seq=1),
            type=EventType.PROTOCOL_SPAWN,
            op=EventOp.SUCCESS,
            persona_id=None,
            signature=None,
            payload={"protocol_id": protocol.id, "state_id": state.id},
        )
        store.append(spawn_evt)
        store.save_state(state)

        seq = 2
        while state.status == StateStatus.RUNNING:
            vm.step(protocol, state)
            step_evt = EventRecord(
                id=str(uuid.uuid4()),
                clock=EventClock(actor="harvest-setup", seq=seq),
                type=EventType.PROTOCOL_STEP,
                op=EventOp.SUCCESS if state.status != StateStatus.STRESSED else EventOp.ERROR,
                persona_id=None,
                signature=None,
                payload={"state_id": state.id, "cursor_after": state.data.cursor},
            )
            store.append(step_evt)
            store.save_state(state)
            seq += 1
            if seq > 20:
                print("! SAFETY BRAKE: loop limit exceeded during manifest.")
                break

    print(f"[*] Booting CVM for Harvest setup using {db_path}...")
    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    for prim_id in ("primitive-sys-log", "primitive-manifest-entity"):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)

    protocol_manifest = store.load_entity("protocol-manifest-entity", ProtocolEntity)
    if protocol_manifest is None:
        print("✗ CRITICAL: protocol-manifest-entity not found. Run genesis first.", file=sys.stderr)
        store.close()
        return 1

    print("    -> Manifesting primitive-sqlite-query...")
    sqlite_prim_inputs = {
        "db_path": db_path,
        "entity_type": "primitive",
        "entity_id": "primitive-sqlite-query",
        "data": {
            "python_ref": "chora_cvm.std.sqlite_query",
            "description": "Execute read-only SQL query",
            "interface": {
                "inputs": {"type": "object", "required": ["db_path", "sql"],
                           "properties": {"db_path": {"type": "string"}, "sql": {"type": "string"},
                                          "params": {"type": "object"}}},
                "outputs": {"type": "object", "properties": {"rows": {"type": "array"}}},
            },
        },
    }
    _run_manifest(store, registry, protocol_manifest, sqlite_prim_inputs)

    print("    -> Manifesting primitive-json-parse...")
    json_parse_inputs = {
        "db_path": db_path,
        "entity_type": "primitive",
        "entity_id": "primitive-json-parse",
        "data": {
            "python_ref": "chora_cvm.std.json_parse",
            "description": "Parse JSON string into structured data",
            "interface": {
                "inputs": {"type": "object", "required": ["json_str"],
                           "properties": {"json_str": {"type": "string"}}},
                "outputs": {"type": "object", "properties": {"data": {"type": "object"}}},
            },
        },
    }
    _run_manifest(store, registry, protocol_manifest, json_parse_inputs)

    print("    -> Registering new primitives into registry...")
    for prim_id in ("primitive-sqlite-query", "primitive-json-parse"):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)

    print("    -> Manifesting protocol-harvest-pattern...")
    harvest_proto_inputs = {
        "db_path": db_path,
        "entity_type": "protocol",
        "entity_id": "protocol-harvest-pattern",
        "data": {
            "interface": {
                "inputs": {"legacy_db": {"type": "string"}, "target_db": {"type": "string"},
                           "pattern_id": {"type": "string"}},
                "outputs": {"status": {"type": "string"}},
            },
            "graph": {
                "start": "node_read",
                "nodes": {
                    "node_read": {"kind": "call", "ref": "primitive-sqlite-query",
                                  "inputs": {"db_path": "$.inputs.legacy_db",
                                             "sql": "SELECT * FROM entities WHERE id = ?",
                                             "params": "$.inputs.pattern_id"}},
                    "node_parse": {"kind": "call", "ref": "primitive-json-parse",
                                   "inputs": {"json_str": "$.node_read.rows.0.data"}},
                    "node_write": {"kind": "call", "ref": "protocol-manifest-entity",
                                   "inputs": {"db_path": "$.inputs.target_db", "entity_type": "pattern",
                                              "entity_id": "$.inputs.pattern_id", "data": "$.node_parse.data"}},
                    "node_return": {"kind": "return", "outputs": {"status": "Harvested {$.inputs.pattern_id}"}},
                },
                "edges": [
                    {"from": "node_read", "to": "node_parse", "default": True},
                    {"from": "node_parse", "to": "node_write", "default": True},
                    {"from": "node_write", "to": "node_return", "default": True},
                ],
            },
        },
    }
    _run_manifest(store, registry, protocol_manifest, harvest_proto_inputs)

    print("[*] Harvest setup complete. protocol-harvest-pattern is now available in the Loom.")
    store.close()
    return 0


def cmd_harvest_pattern(args: argparse.Namespace) -> int:
    """Execute harvest protocol for a pattern from legacy DB."""
    import uuid
    from typing import Dict, List, Optional
    from .registry import PrimitiveRegistry
    from .schema import (
        EventClock, EventOp, EventRecord, EventType,
        PrimitiveEntity, ProtocolEntity, StateEntity, StateStatus,
    )
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)
    legacy_db = os.path.expanduser("~/.chora/chora.db")

    if not os.path.exists(legacy_db):
        print(f"✗ Legacy DB not found at {legacy_db}", file=sys.stderr)
        return 1

    target_pattern = args.pattern_id or "pattern-self-extension-pattern"

    print("[*] Booting CVM for Harvest execution...")
    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    primitives = ["primitive-sys-log", "primitive-manifest-entity",
                  "primitive-sqlite-query", "primitive-json-parse"]

    for pid in primitives:
        entity = store.load_entity(pid, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)
        else:
            print(f"✗ Missing primitive: {pid}. Run cvm harvest setup first.", file=sys.stderr)
            store.close()
            return 1

    protocol_id = "protocol-harvest-pattern"
    protocol = store.load_entity(protocol_id, ProtocolEntity)

    def protocol_loader(pid: str) -> Optional[ProtocolEntity]:
        return store.load_entity(pid, ProtocolEntity)

    if protocol is None:
        print(f"✗ Protocol {protocol_id} not found. Run cvm harvest setup first.", file=sys.stderr)
        store.close()
        return 1

    vm = ProtocolVM(registry, protocol_loader=protocol_loader)

    inputs = {"legacy_db": legacy_db, "target_db": db_path, "pattern_id": target_pattern}

    print(f"[*] Harvesting {target_pattern} from {legacy_db} into {db_path}...")

    root_state = vm.spawn(protocol, inputs)
    root_state.id = str(uuid.uuid4())
    root_state.status = StateStatus.RUNNING
    store.save_state(root_state)

    stack: List[StateEntity] = [root_state]
    child_result: Optional[Dict[str, object]] = None

    seq = 1
    while stack:
        current = stack[-1]
        current_proto = protocol_loader(current.data.protocol_id)
        if current_proto is None:
            print(f"✗ Missing protocol for state {current.id}", file=sys.stderr)
            break

        updated_state, new_child = vm.step(current_proto, current, child_result)

        if current.status != StateStatus.SUSPENDED:
            child_result = None

        step_evt = EventRecord(
            id=str(uuid.uuid4()),
            clock=EventClock(actor="harvest-exec", seq=seq),
            type=EventType.PROTOCOL_STEP,
            op=EventOp.SUCCESS if updated_state.status != StateStatus.STRESSED else EventOp.ERROR,
            persona_id=None,
            signature=None,
            payload={"state_id": updated_state.id, "cursor_after": updated_state.data.cursor},
        )
        store.append(step_evt)
        store.save_state(updated_state)

        if new_child:
            print(f"    -> Recursion: {new_child.data.protocol_id}")
            new_child.id = str(uuid.uuid4())
            new_child.status = StateStatus.RUNNING
            store.save_state(new_child)
            stack.append(new_child)
            child_result = None
        elif updated_state.status == StateStatus.FULFILLED:
            print(f"    <- Fulfilled: {updated_state.data.protocol_id}")
            child_result = vm.extract_output(current_proto, updated_state)
            stack.pop()
        elif updated_state.status == StateStatus.STRESSED:
            print(f"    ! Error: {updated_state.data.error}")
            break

        seq += 1
        if seq > 50:
            print("! Loop limit reached during harvest execution.")
            break

    print("[*] Harvest execution complete. Verifying Loom state...")

    row = store._conn.execute(
        "SELECT id, type, data_json FROM entities WHERE id = ?", (target_pattern,),
    ).fetchone()
    if row:
        print(f"    SUCCESS: {row['id']} ({row['type']}) exists in Loom.")
        snippet = str(row["data_json"])
        print(f"    Data snippet: {snippet[:120]}...")
    else:
        print(f"    FAILURE: {target_pattern} not found in Loom.")

    store.close()
    return 0


def cmd_orient(args: argparse.Namespace) -> int:
    """Orient: Summarize the Loom's current entity landscape via protocol."""
    import uuid
    from typing import List
    from .registry import PrimitiveRegistry
    from .schema import (
        EventClock, EventOp, EventRecord, EventType,
        PrimitiveEntity, ProtocolEntity, StateStatus,
    )
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Booting CVM for Orient vNext using {db_path}...")
    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    for prim_id in ("primitive-sqlite-query",):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)
        else:
            print(f"✗ Missing primitive: {prim_id}. Run cvm harvest setup first.", file=sys.stderr)
            store.close()
            return 1

    protocol = store.load_entity("protocol-orient-vnext", ProtocolEntity)
    if protocol is None:
        print("✗ protocol-orient-vnext not found. Run orient setup first.", file=sys.stderr)
        store.close()
        return 1

    vm = ProtocolVM(registry)

    inputs = {"db_path": db_path}
    state = vm.spawn(protocol, inputs)
    state.id = str(uuid.uuid4())
    state.status = StateStatus.RUNNING

    spawn_evt = EventRecord(
        id=str(uuid.uuid4()),
        clock=EventClock(actor="orient-runner", seq=1),
        type=EventType.PROTOCOL_SPAWN,
        op=EventOp.SUCCESS,
        persona_id=None,
        signature=None,
        payload={"protocol_id": protocol.id, "state_id": state.id},
    )
    store.append(spawn_evt)
    store.save_state(state)

    seq = 2
    while state.status == StateStatus.RUNNING:
        vm.step(protocol, state)
        step_evt = EventRecord(
            id=str(uuid.uuid4()),
            clock=EventClock(actor="orient-runner", seq=seq),
            type=EventType.PROTOCOL_STEP,
            op=EventOp.SUCCESS if state.status != StateStatus.STRESSED else EventOp.ERROR,
            persona_id=None,
            signature=None,
            payload={"state_id": state.id, "cursor_after": state.data.cursor},
        )
        store.append(step_evt)
        store.save_state(state)
        seq += 1
        if seq > 10:
            print("! SAFETY BRAKE: loop limit exceeded (orient).")
            break

    print(f"[*] Orient protocol finished. Status: {state.status.value}")
    if state.status == StateStatus.STRESSED:
        print(f"    Error: {state.data.error}")
        store.close()
        return 1

    summary = vm.extract_output(protocol, state)
    counts: List[dict] = summary.get("summary", [])
    personas: List[dict] = summary.get("personas", [])
    recent: List[dict] = summary.get("recent", [])

    print("    Entity counts by type in Loom:")
    for row in counts:
        print(f"      - {row.get('type')}: {row.get('count')}")

    if personas:
        print("    Personas:")
        for p in personas:
            print(f"      - {p.get('id')}")

    if recent:
        print("    Recent entities (id, type, data_snippet):")
        for r in recent:
            print(f"      - {r.get('id')} [{r.get('type')}] :: {r.get('data_snippet')}")

    store.close()
    return 0


def cmd_teach(args: argparse.Namespace) -> int:
    """Teach: Explain an entity in Diataxis-shaped format via protocol."""
    import uuid
    from .registry import PrimitiveRegistry
    from .schema import PrimitiveEntity, ProtocolEntity, StateStatus
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)
    entity_id = args.entity_id

    if not entity_id:
        print("Usage: cvm teach <entity_id> [--db path]", file=sys.stderr)
        return 1

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Teaching entity {entity_id} from {db_path}...")
    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    # Register primitives needed by protocol-teach-me
    for prim_id in (
        "primitive-sys-log",
        "primitive-entity-doc-bundle",
        "primitive-teach-format",
    ):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)

    vm = ProtocolVM(registry)
    protocol = store.load_entity("protocol-teach-me", ProtocolEntity)
    if protocol is None:
        print("✗ protocol-teach-me not found. Run docs setup first.", file=sys.stderr)
        store.close()
        return 1

    inputs = {"db_path": db_path, "entity_id": entity_id}

    state = vm.spawn(protocol, inputs)
    state.id = str(uuid.uuid4())
    state.status = StateStatus.RUNNING
    store.save_state(state)

    while state.status == StateStatus.RUNNING:
        vm.step(protocol, state)
        store.save_state(state)

    if state.status == StateStatus.FULFILLED:
        memory = state.data.memory or {}
        # protocol-teach-me writes the formatted text at node_format
        node_format = memory.get("node_format") or {}
        text = node_format.get("text") or ""
        print(text)
    else:
        print(f"✗ Teach-me ended in status: {state.status.value}", file=sys.stderr)
        store.close()
        return 1

    store.close()
    return 0


def cmd_circle_orient(args: argparse.Namespace) -> int:
    """Circle-orient: Resolve active Circle and surface its assets and tools."""
    import uuid
    from . import context
    from .registry import PrimitiveRegistry
    from .schema import PrimitiveEntity, ProtocolEntity, StateStatus
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    # Resolve repo root (workspace directory)
    repo_root = Path.cwd()
    print(f"[*] Circle-orient using DB {db_path} and repo {repo_root}...")

    circle_id = context.resolve_circle(db_path, cwd=str(repo_root))
    if circle_id is None:
        print("✗ No circle resolved for this workspace. Run `just manifest-circle` first.", file=sys.stderr)
        return 1

    print(f"    Active circle: {circle_id}")

    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    # Hydrate primitives used by protocol-circle-orient
    for prim_id in ("primitive-sys-log", "primitive-entities-query"):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)

    def load_protocol(pid: str) -> ProtocolEntity | None:
        return store.load_entity(pid, ProtocolEntity)

    vm = ProtocolVM(registry, protocol_loader=load_protocol)

    protocol = store.load_entity("protocol-circle-orient", ProtocolEntity)
    if protocol is None:
        print("✗ protocol-circle-orient not found. Run circle-orient-setup first.", file=sys.stderr)
        store.close()
        return 1

    inputs = {
        "db_path": db_path,
        "circle_id": circle_id,
        "limit": 50,
    }

    state = vm.spawn(protocol, inputs)
    state.id = str(uuid.uuid4())
    state.status = StateStatus.RUNNING
    store.save_state(state)

    while state.status == StateStatus.RUNNING:
        vm.step(protocol, state)
        store.save_state(state)

    print(f"[*] Circle-orient finished with status: {state.status.value}")
    mem = state.data.memory or {}
    node_assets = mem.get("node_assets") or {}
    node_tools = mem.get("node_tools") or {}
    assets = node_assets.get("rows") or node_assets.get("assets") or []
    tools = node_tools.get("rows") or node_tools.get("entities") or []

    print("    Circle:", circle_id)

    if assets:
        print("    Assets:")
        for a in assets:
            title = (a.get("data") or {}).get("title") or a.get("id")
            print(f"      - {a.get('id')}: {title}")
    else:
        print("    Assets: (none)")

    if tools:
        print("    Tools:")
        for t in tools:
            data = t.get("data") or {}
            title = data.get("title") or t.get("id")
            print(f"      - {t.get('id')}: {title}")
    else:
        print("    Tools: (none)")

    store.close()
    return 0


def cmd_manifest_circle(args: argparse.Namespace) -> int:
    """Manifest a Circle and its local repo Asset into the Loom."""
    import json as json_lib
    import uuid
    from .registry import PrimitiveRegistry
    from .schema import PrimitiveEntity, ProtocolEntity, StateStatus
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    repo_root = Path.cwd()
    circle_id = "circle-chora-workspace"
    asset_id = "asset-repo-local"

    print(f"[*] Manifesting circle + asset into {db_path}...")

    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    for prim_id in ("primitive-sys-log", "primitive-manifest-entity"):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)

    vm = ProtocolVM(registry)
    protocol = store.load_entity("protocol-manifest-entity", ProtocolEntity)
    if protocol is None:
        print("✗ protocol-manifest-entity not found. Run bootstrap first.", file=sys.stderr)
        store.close()
        return 1

    # 1. Manifest the Circle entity
    circle_inputs = {
        "db_path": db_path,
        "entity_type": "circle",
        "entity_id": circle_id,
        "data": {
            "title": "Chora Workspace",
            "description": "The local Chora workspace on this machine.",
            "kind": "workspace",
        },
    }

    print(f"    Ensuring circle entity {circle_id}...")
    circle_state = vm.spawn(protocol, circle_inputs)
    circle_state.id = str(uuid.uuid4())
    circle_state.status = StateStatus.RUNNING
    store.save_state(circle_state)

    while circle_state.status == StateStatus.RUNNING:
        vm.step(protocol, circle_state)
        store.save_state(circle_state)

    # 2. Manifest the Asset entity for this repo
    source_uri = str(repo_root)
    asset_inputs = {
        "db_path": db_path,
        "entity_type": "asset",
        "entity_id": asset_id,
        "data": {
            "title": "Local Repo",
            "description": "The local git workspace for this Chora instance.",
            "asset_type": "git-repo",
            "source_uri": source_uri,
            "circle_id": circle_id,
        },
    }

    print(f"    Ensuring asset entity {asset_id}...")
    asset_state = vm.spawn(protocol, asset_inputs)
    asset_state.id = str(uuid.uuid4())
    asset_state.status = StateStatus.RUNNING
    store.save_state(asset_state)

    while asset_state.status == StateStatus.RUNNING:
        vm.step(protocol, asset_state)
        store.save_state(asset_state)

    store.close()

    # 3. Ensure the local marker file
    marker_dir = repo_root / ".chora"
    marker_dir.mkdir(parents=True, exist_ok=True)
    marker_path = marker_dir / "circle.json"
    marker_path.write_text(json_lib.dumps({"circle_id": circle_id}, indent=2))
    print(f"    Circle marker written to {marker_path}")

    return 0


def cmd_bootstrap_physics(args: argparse.Namespace) -> int:
    """Genesis: Crystallize the Laws of Nature as Axiom Entities."""
    from .schema import GenericEntity
    from .store import EventStore

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Physics Genesis: Crystallizing Laws of Nature into {db_path}...")

    PHYSICS_AXIOMS = [
        {"id": "axiom-physics-yields", "verb": "yields", "subject_type": "inquiry", "object_type": "learning", "category": "generative-chain", "description": "Exploration produces insight."},
        {"id": "axiom-physics-surfaces", "verb": "surfaces", "subject_type": "learning", "object_type": "principle", "category": "generative-chain", "description": "Insight reveals truth."},
        {"id": "axiom-physics-induces", "verb": "induces", "subject_type": "learning", "object_type": "pattern", "category": "generative-chain", "description": "Insight suggests form."},
        {"id": "axiom-physics-governs", "verb": "governs", "subject_type": "principle", "object_type": "pattern", "category": "generative-chain", "description": "Truth constrains form."},
        {"id": "axiom-physics-clarifies", "verb": "clarifies", "subject_type": "principle", "object_type": "story", "category": "generative-chain", "description": "Truth clarifies desire."},
        {"id": "axiom-physics-structures", "verb": "structures", "subject_type": "pattern", "object_type": "story", "category": "generative-chain", "description": "Blueprint shapes desire."},
        {"id": "axiom-physics-specifies", "verb": "specifies", "subject_type": "story", "object_type": "behavior", "category": "generative-chain", "description": "Desire becomes expectation."},
        {"id": "axiom-physics-implements", "verb": "implements", "subject_type": "behavior", "object_type": "tool", "category": "generative-chain", "description": "Expectation becomes capability."},
        {"id": "axiom-physics-verifies", "verb": "verifies", "subject_type": "tool", "object_type": "behavior", "category": "generative-chain", "description": "Capability proves expectation."},
        {"id": "axiom-physics-emits", "verb": "emits", "subject_type": "tool", "object_type": "signal", "category": "reflex-arc", "description": "Action generates impulse."},
        {"id": "axiom-physics-triggers", "verb": "triggers", "subject_type": "signal", "object_type": "focus", "category": "reflex-arc", "description": "Impulse captures attention."},
        {"id": "axiom-physics-crystallized-from", "verb": "crystallized-from", "subject_type": None, "object_type": None, "category": "provenance", "description": "Tracks origin.", "constraint_mode": "flexible"},
        {"id": "axiom-physics-inhabits", "verb": "inhabits", "subject_type": None, "object_type": "circle", "category": "circle-physics", "description": "Membership.", "constraint_mode": "flexible"},
        {"id": "axiom-physics-belongs-to", "verb": "belongs-to", "subject_type": None, "object_type": "circle", "category": "circle-physics", "description": "Ownership.", "constraint_mode": "flexible"},
        {"id": "axiom-physics-stewards", "verb": "stewards", "subject_type": None, "object_type": "circle", "category": "circle-physics", "description": "Responsibility.", "constraint_mode": "flexible"},
    ]

    store = EventStore(db_path)
    created = 0
    skipped = 0

    for axiom_data in PHYSICS_AXIOMS:
        axiom_id = axiom_data["id"]
        existing = store.load_entity(axiom_id, GenericEntity)
        if existing:
            print(f"  ○ {axiom_id} (exists)")
            skipped += 1
            continue

        data = {
            "title": f"Physics: {axiom_data['verb']}",
            "verb": axiom_data["verb"],
            "subject_type": axiom_data["subject_type"],
            "object_type": axiom_data["object_type"],
            "category": axiom_data["category"],
            "description": axiom_data["description"],
            "constraint_mode": axiom_data.get("constraint_mode", "strict"),
        }

        entity = GenericEntity(id=axiom_id, type="axiom", data=data)
        store.save_entity(entity)
        print(f"  ✓ {axiom_id}")
        created += 1

    store.close()
    print(f"\n[*] Genesis complete: {created} created, {skipped} existed")
    return 0


def cmd_bootstrap_circle_orient(args: argparse.Namespace) -> int:
    """Manifest protocol-circle-orient for circle-aware orientation."""
    import uuid
    from .registry import PrimitiveRegistry
    from .schema import PrimitiveEntity, ProtocolEntity, StateStatus
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Manifesting protocol-circle-orient into {db_path}...")
    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    for prim_id in ("primitive-sys-log", "primitive-manifest-entity", "primitive-entities-query"):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)

    vm = ProtocolVM(registry)
    protocol = store.load_entity("protocol-manifest-entity", ProtocolEntity)
    if protocol is None:
        print("✗ protocol-manifest-entity not found. Run bootstrap first.", file=sys.stderr)
        store.close()
        return 1

    circle_orient_protocol = {
        "id": "protocol-circle-orient",
        "data": {
            "interface": {
                "inputs": {"db_path": {"type": "string"}, "circle_id": {"type": "string"}, "limit": {"type": "integer"}},
                "outputs": {"assets": {"type": "array"}, "tools": {"type": "array"}},
            },
            "graph": {
                "start": "node_assets",
                "nodes": {
                    "node_assets": {"kind": "call", "ref": "primitive-entities-query", "inputs": {"db_path": "$.inputs.db_path", "entity_type": "asset", "circle_id": "$.inputs.circle_id", "limit": "$.inputs.limit"}},
                    "node_tools": {"kind": "call", "ref": "primitive-entities-query", "inputs": {"db_path": "$.inputs.db_path", "entity_type": "tool", "limit": "$.inputs.limit"}},
                    "node_return": {"kind": "return", "outputs": {"assets": "$.node_assets.rows", "tools": "$.node_tools.rows"}},
                },
                "edges": [{"from": "node_assets", "to": "node_tools", "default": True}, {"from": "node_tools", "to": "node_return", "default": True}],
            },
        },
    }

    inputs = {
        "db_path": db_path,
        "entity_type": "protocol",
        "entity_id": circle_orient_protocol["id"],
        "data": circle_orient_protocol["data"],
    }

    state = vm.spawn(protocol, inputs)
    state.id = str(uuid.uuid4())
    state.status = StateStatus.RUNNING
    store.save_state(state)

    while state.status == StateStatus.RUNNING:
        vm.step(protocol, state)
        store.save_state(state)

    store.close()
    print("[*] protocol-circle-orient manifest complete.")
    return 0


def cmd_semantic_setup(args: argparse.Namespace) -> int:
    """Setup semantic primitives and protocols in the CVM database."""
    from .schema import (
        PrimitiveData, PrimitiveEntity, ProtocolData, ProtocolEntity,
        ProtocolGraph, ProtocolInterface, ProtocolNode, ProtocolNodeKind, ProtocolEdge,
    )
    from .store import EventStore

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Setting up semantic primitives and protocols in {db_path}...")
    store = EventStore(db_path)

    # Register semantic primitives
    primitives = [
        ("primitive-embed-entity", "chora_cvm.semantic.embed_entity", "Compute and store embedding for an entity"),
        ("primitive-suggest-bonds", "chora_cvm.semantic.suggest_bonds", "Suggest potential bonds for an entity"),
        ("primitive-semantic-search", "chora_cvm.semantic.semantic_search", "Search entities by semantic similarity"),
        ("primitive-detect-clusters", "chora_cvm.semantic.detect_clusters", "Detect clusters of similar entities"),
    ]

    print("\n[1] Registering semantic primitives...")
    for prim_id, python_ref, description in primitives:
        prim = PrimitiveEntity(
            id=prim_id,
            data=PrimitiveData(python_ref=python_ref, description=description, interface={"inputs": {}, "outputs": {}}),
        )
        store.save_entity(prim)
        print(f"    + {prim_id}")

    print("\n[2] Creating semantic protocols...")
    # protocol-manifest-with-suggestions
    proto1 = ProtocolEntity(
        id="protocol-manifest-with-suggestions",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={"db_path": {"type": "string"}, "entity_type": {"type": "string"}, "entity_id": {"type": "string"}, "data": {"type": "object"}},
                outputs={"id": {"type": "string"}, "type": {"type": "string"}, "suggestions": {"type": "array"}},
            ),
            graph=ProtocolGraph(
                start="node_manifest",
                nodes={
                    "node_manifest": ProtocolNode(kind=ProtocolNodeKind.CALL, ref="primitive-manifest-entity", inputs={"db_path": "$.inputs.db_path", "entity_type": "$.inputs.entity_type", "entity_id": "$.inputs.entity_id", "data": "$.inputs.data"}),
                    "node_embed": ProtocolNode(kind=ProtocolNodeKind.CALL, ref="primitive-embed-entity", inputs={"db_path": "$.inputs.db_path", "entity_id": "$.inputs.entity_id"}),
                    "node_suggest": ProtocolNode(kind=ProtocolNodeKind.CALL, ref="primitive-suggest-bonds", inputs={"db_path": "$.inputs.db_path", "entity_id": "$.inputs.entity_id", "limit": 5}),
                    "node_return": ProtocolNode(kind=ProtocolNodeKind.RETURN, outputs={"id": "$.node_manifest.id", "type": "$.node_manifest.type", "suggestions": "$.node_suggest.candidates"}),
                },
                edges=[ProtocolEdge(**{"from": "node_manifest", "to": "node_embed"}), ProtocolEdge(**{"from": "node_embed", "to": "node_suggest"}), ProtocolEdge(**{"from": "node_suggest", "to": "node_return"})],
            ),
        ),
    )
    store.save_entity(proto1)
    print(f"    + {proto1.id}")

    # protocol-semantic-search
    proto2 = ProtocolEntity(
        id="protocol-semantic-search",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={"db_path": {"type": "string"}, "query": {"type": "string"}, "entity_type": {"type": "string"}, "limit": {"type": "integer"}},
                outputs={"results": {"type": "array"}, "method": {"type": "string"}},
            ),
            graph=ProtocolGraph(
                start="node_search",
                nodes={
                    "node_search": ProtocolNode(kind=ProtocolNodeKind.CALL, ref="primitive-semantic-search", inputs={"db_path": "$.inputs.db_path", "query": "$.inputs.query", "entity_type": "$.inputs.entity_type", "limit": "$.inputs.limit"}),
                    "node_return": ProtocolNode(kind=ProtocolNodeKind.RETURN, outputs={"results": "$.node_search.results", "method": "$.node_search.method"}),
                },
                edges=[ProtocolEdge(**{"from": "node_search", "to": "node_return"})],
            ),
        ),
    )
    store.save_entity(proto2)
    print(f"    + {proto2.id}")

    store.close()
    print("\n[*] Semantic setup complete.")
    return 0


def cmd_docs_setup(args: argparse.Namespace) -> int:
    """Setup docs/teach primitives and protocols."""
    import uuid
    from .registry import PrimitiveRegistry
    from .schema import PrimitiveEntity, ProtocolEntity, StateStatus
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Setting up docs/teach primitives in {db_path}...")
    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    for prim_id in ("primitive-sys-log", "primitive-manifest-entity"):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)

    vm = ProtocolVM(registry)
    protocol = store.load_entity("protocol-manifest-entity", ProtocolEntity)
    if protocol is None:
        print("✗ protocol-manifest-entity not found.", file=sys.stderr)
        store.close()
        return 1

    def _run_manifest(inputs: dict) -> None:
        state = vm.spawn(protocol, inputs)
        state.id = str(uuid.uuid4())
        state.status = StateStatus.RUNNING
        store.save_state(state)
        while state.status == StateStatus.RUNNING:
            vm.step(protocol, state)
            store.save_state(state)

    # Manifest teach primitives
    teach_primitives = [
        {"id": "primitive-entity-doc-bundle", "python_ref": "chora_cvm.std.entity_doc_bundle", "description": "Load entity and linked Diataxis docs."},
        {"id": "primitive-teach-format", "python_ref": "chora_cvm.std.teach_format", "description": "Format Diataxis explanation from doc bundle."},
    ]

    for prim in teach_primitives:
        print(f"    Ensuring {prim['id']}...")
        _run_manifest({
            "db_path": db_path, "entity_type": "primitive", "entity_id": prim["id"],
            "data": {"python_ref": prim["python_ref"], "description": prim["description"], "interface": {"inputs": {}, "outputs": {}}},
        })

    # Manifest protocol-teach-me
    print("    Ensuring protocol-teach-me...")
    teach_protocol = {
        "id": "protocol-teach-me",
        "data": {
            "interface": {"inputs": {"db_path": {"type": "string"}, "entity_id": {"type": "string"}}, "outputs": {"text": {"type": "string"}}},
            "graph": {
                "start": "node_bundle",
                "nodes": {
                    "node_bundle": {"kind": "call", "ref": "primitive-entity-doc-bundle", "inputs": {"db_path": "$.inputs.db_path", "entity_id": "$.inputs.entity_id"}},
                    "node_format": {"kind": "call", "ref": "primitive-teach-format", "inputs": {"bundle": "$.node_bundle"}},
                    "node_return": {"kind": "return", "outputs": {"text": "$.node_format.text"}},
                },
                "edges": [{"from": "node_bundle", "to": "node_format", "default": True}, {"from": "node_format", "to": "node_return", "default": True}],
            },
        },
    }
    _run_manifest({"db_path": db_path, "entity_type": "protocol", "entity_id": teach_protocol["id"], "data": teach_protocol["data"]})

    store.close()
    print("[*] Docs setup complete.")
    return 0


def cmd_docs_check(args: argparse.Namespace) -> int:
    """Check Diataxis completeness for tools in the Loom."""
    from . import std as cvm_std
    from .store import EventStore

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Checking Diataxis completeness for tools in {db_path}...")
    store = EventStore(db_path)
    try:
        cur = store._conn.execute("SELECT id FROM entities WHERE type = 'tool' ORDER BY id;")
        tool_ids = [row["id"] for row in cur.fetchall()]
    finally:
        store.close()

    if not tool_ids:
        print("No tools found.")
        return 0

    print("tool_id,story,pattern,principle")
    for tid in tool_ids:
        bundle = cvm_std.entity_doc_bundle(db_path, tid)
        story = "yes" if bundle.get("story") else "no"
        pattern = "yes" if bundle.get("pattern") else "no"
        principle = "yes" if bundle.get("principle") else "no"
        print(f"{tid},{story},{pattern},{principle}")

    return 0


def cmd_docs_generate(args: argparse.Namespace) -> int:
    """Generate browsable Markdown docs for Loom tools."""
    from . import std as cvm_std
    from .store import EventStore

    db_path = resolve_db_path(args.db)
    rel_output = args.output or "docs/loom.md"

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    repo_root = Path.cwd()
    base_dir = str(repo_root)
    print(f"[*] Generating Loom docs into {rel_output}...")

    store = EventStore(db_path)
    try:
        cur = store._conn.execute("SELECT id FROM entities WHERE type = 'tool' ORDER BY id;")
        tool_ids = [row["id"] for row in cur.fetchall()]
    finally:
        store.close()

    sections: list = []
    for tool_id in tool_ids:
        bundle = cvm_std.entity_doc_bundle(db_path, tool_id)
        formatted = cvm_std.teach_format(bundle)
        sections.append(formatted.get("text", ""))

    if not sections:
        doc_text = "# Loom Tools\n\n_No tools found in the Loom database yet._\n"
    else:
        doc_text = "# Loom Tools\n\n" + "\n\n---\n\n".join(sections) + "\n"

    result = cvm_std.write_file(base_dir, rel_output, doc_text)
    print(f"[*] Docs written to {result['path']}")
    return 0


def cmd_docs_core(args: argparse.Namespace) -> int:
    """Manifest core Diataxis docs for key tools."""
    import json as json_lib
    import uuid
    from .registry import PrimitiveRegistry
    from .schema import GenericEntity, PrimitiveEntity, ProtocolEntity, StateStatus
    from .store import EventStore
    from .vm import ProtocolVM

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Manifesting core docs into {db_path}...")
    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    for prim_id in ("primitive-sys-log", "primitive-manifest-entity"):
        entity = store.load_entity(prim_id, PrimitiveEntity)
        if entity:
            registry.register_from_entity(entity)

    vm = ProtocolVM(registry)
    protocol = store.load_entity("protocol-manifest-entity", ProtocolEntity)
    if protocol is None:
        print("✗ protocol-manifest-entity not found.", file=sys.stderr)
        store.close()
        return 1

    def _run_manifest(inputs: dict) -> None:
        state = vm.spawn(protocol, inputs)
        state.id = str(uuid.uuid4())
        state.status = StateStatus.RUNNING
        store.save_state(state)
        while state.status == StateStatus.RUNNING:
            vm.step(protocol, state)
            store.save_state(state)

    def _ensure_links(tool_id: str, story_id: str | None, pattern_id: str | None, principle_id: str | None) -> None:
        cur = store._conn.execute("SELECT id, type, data_json FROM entities WHERE id = ?", (tool_id,))
        row = cur.fetchone()
        if not row:
            return
        data = json_lib.loads(row["data_json"])
        cognition = data.get("cognition") or {}
        links = cognition.get("links") or {}
        if story_id:
            links["story_id"] = story_id
        if pattern_id:
            links["pattern_id"] = pattern_id
        if principle_id:
            links["principle_id"] = principle_id
        cognition["links"] = links
        data["cognition"] = cognition
        entity = GenericEntity(id=row["id"], type=row["type"], data=data)
        store.save_entity(entity)

    # Docs for tool-manifest
    print("    Ensuring docs for tool-manifest...")
    _run_manifest({"db_path": db_path, "entity_type": "story", "entity_id": "story-tool-manifest-first-use", "data": {"title": "First time using tool-manifest", "description": "Start by deciding if capability is primitive or protocol, then call tool-manifest.", "tags": ["docs", "tutorial"]}})
    _run_manifest({"db_path": db_path, "entity_type": "pattern", "entity_id": "pattern-tool-manifest-howto", "data": {"title": "How to add capability via tool-manifest", "description": "1. Decide if protocol over existing primitives. 2. Draft JSON. 3. Call tool-manifest.", "tags": ["docs", "howto"]}})
    _run_manifest({"db_path": db_path, "entity_type": "principle", "entity_id": "principle-tool-manifest-role", "data": {"title": "tool-manifest as gateway", "statement": "tool-manifest is boundary between Python and Loom.", "tags": ["docs", "principle"]}})
    _ensure_links("tool-manifest", "story-tool-manifest-first-use", "pattern-tool-manifest-howto", "principle-tool-manifest-role")

    # Docs for tool-teach-me
    print("    Ensuring docs for tool-teach-me...")
    _run_manifest({"db_path": db_path, "entity_type": "story", "entity_id": "story-tool-teach-me-first-use", "data": {"title": "First time using tool-teach-me", "description": "Call tool-teach-me with entity id for Diataxis explanation.", "tags": ["docs", "tutorial"]}})
    _run_manifest({"db_path": db_path, "entity_type": "pattern", "entity_id": "pattern-tool-teach-me-howto", "data": {"title": "How to learn with tool-teach-me", "description": "Run just teach <entity_id> and read facets.", "tags": ["docs", "howto"]}})
    _run_manifest({"db_path": db_path, "entity_type": "principle", "entity_id": "principle-tool-teach-me-role", "data": {"title": "tool-teach-me as conversational manual", "statement": "tool-teach-me is the Loom's way of explaining itself.", "tags": ["docs", "principle"]}})
    _ensure_links("tool-teach-me", "story-tool-teach-me-first-use", "pattern-tool-teach-me-howto", "principle-tool-teach-me-role")

    store.close()
    print("[*] Core docs manifest complete.")
    return 0


def cmd_whoami(args: argparse.Namespace) -> int:
    """Show personas from the Loom."""
    import json as json_lib
    from .store import EventStore

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    print(f"[*] Reading personas from {db_path}...")
    store = EventStore(db_path)

    rows = store._conn.execute(
        "SELECT id, type, data_json FROM entities WHERE id IN ('persona-victor', 'persona-resident-architect')"
    ).fetchall()

    if not rows:
        print("! No personas found. Run persona manifest first.")
        store.close()
        return 0

    for row in rows:
        print(f"\nPersona: {row['id']} ({row['type']})")
        data = json_lib.loads(row["data_json"])
        if data.get("title"):
            print(f"  Title: {data['title']}")
        if data.get("description"):
            print(f"  Description: {data['description']}")
        if data.get("roles"):
            print(f"  Roles: {data['roles']}")
        if data.get("policy"):
            print(f"  Policy: {data['policy']}")
        if data.get("preferences"):
            print(f"  Preferences: {data['preferences']}")

    store.close()
    return 0


# =============================================================================
# Provenance Verification Commands
# =============================================================================

def cmd_provenance_audit(args: argparse.Namespace) -> int:
    """Audit tool provenance: check implements, verifies, crystallized-from bonds."""
    import json as json_lib
    from .store import EventStore

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    store = EventStore(db_path)

    # Get all tools
    tools = store._conn.execute(
        "SELECT id, data_json FROM entities WHERE type = 'tool' ORDER BY id"
    ).fetchall()

    # Get all bonds
    bonds = store._conn.execute(
        "SELECT from_id, to_id, type FROM bonds"
    ).fetchall()

    # Build lookup tables
    implements_to_tool = {}  # behavior -> tool (implements bond)
    tool_verifies = {}  # tool -> [behaviors]
    tool_origins = {}  # tool -> [origins]

    for b in bonds:
        if b["type"] == "implements":
            implements_to_tool[b["from_id"]] = b["to_id"]
        elif b["type"] == "verifies":
            tool_verifies.setdefault(b["from_id"], []).append(b["to_id"])
        elif b["type"] == "crystallized-from":
            tool_origins.setdefault(b["from_id"], []).append(b["to_id"])

    # Reverse lookup: tool -> behaviors that implement it
    tool_implemented_by = {}
    for behavior_id, tool_id in implements_to_tool.items():
        tool_implemented_by.setdefault(tool_id, []).append(behavior_id)

    # Analyze each tool
    results = []
    complete = 0
    missing_implements = []
    missing_verifies = []
    missing_origin = []
    missing_cognition = []

    for row in tools:
        tool_id = row["id"]
        data = json_lib.loads(row["data_json"])

        has_implements = tool_id in tool_implemented_by
        has_verifies = tool_id in tool_verifies
        has_origin = tool_id in tool_origins
        has_cognition = bool(data.get("cognition", {}).get("ready_at_hand"))

        score = sum([has_implements, has_verifies, has_origin, has_cognition])

        results.append({
            "id": tool_id,
            "implements": has_implements,
            "verifies": has_verifies,
            "origin": has_origin,
            "cognition": has_cognition,
            "score": score,
        })

        if score == 4:
            complete += 1
        if not has_implements:
            missing_implements.append(tool_id)
        if not has_verifies:
            missing_verifies.append(tool_id)
        if not has_origin:
            missing_origin.append(tool_id)
        if not has_cognition:
            missing_cognition.append(tool_id)

    store.close()

    total = len(tools)

    # Output
    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  Tool Provenance Audit                                     │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    # Summary
    print(f"  Total tools: {total}")
    print(f"  Complete provenance (4/4): {complete} ({int(complete/total*100) if total else 0}%)")
    print()
    print(f"  With implements bond: {total - len(missing_implements)} ({int((total-len(missing_implements))/total*100) if total else 0}%)")
    print(f"  With verifies bond: {total - len(missing_verifies)} ({int((total-len(missing_verifies))/total*100) if total else 0}%)")
    print(f"  With crystallized-from: {total - len(missing_origin)} ({int((total-len(missing_origin))/total*100) if total else 0}%)")
    print(f"  With cognition.ready_at_hand: {total - len(missing_cognition)} ({int((total-len(missing_cognition))/total*100) if total else 0}%)")
    print()

    # Detailed output if verbose
    if args.verbose:
        print("  ┌─────────────────────────────────────────────────────────┐")
        print("  │ Tool                                    Impl Verf Orig Cog │")
        print("  ├─────────────────────────────────────────────────────────┤")
        for r in sorted(results, key=lambda x: x["score"]):
            impl = "✓" if r["implements"] else "✗"
            verf = "✓" if r["verifies"] else "✗"
            orig = "✓" if r["origin"] else "✗"
            cog = "✓" if r["cognition"] else "✗"
            name = r["id"][:38].ljust(38)
            print(f"  │ {name}  {impl}    {verf}    {orig}    {cog}  │")
        print("  └─────────────────────────────────────────────────────────┘")
        print()

    # CSV output
    if args.csv:
        print("tool_id,implements,verifies,origin,cognition,score")
        for r in results:
            impl = "yes" if r["implements"] else "no"
            verf = "yes" if r["verifies"] else "no"
            orig = "yes" if r["origin"] else "no"
            cog = "yes" if r["cognition"] else "no"
            print(f"{r['id']},{impl},{verf},{orig},{cog},{r['score']}")

    # Show gaps if requested
    if args.gaps:
        if missing_implements:
            print("  Missing implements bonds:")
            for tid in sorted(missing_implements)[:20]:
                print(f"    • {tid}")
            if len(missing_implements) > 20:
                print(f"    ... and {len(missing_implements) - 20} more")
            print()

        if missing_origin:
            print("  Missing crystallized-from bonds:")
            for tid in sorted(missing_origin)[:20]:
                print(f"    • {tid}")
            if len(missing_origin) > 20:
                print(f"    ... and {len(missing_origin) - 20} more")
            print()

    return 0


def cmd_provenance_check(args: argparse.Namespace) -> int:
    """Check provenance for a specific tool."""
    import json as json_lib
    from .store import EventStore

    db_path = resolve_db_path(args.db)
    tool_id = args.tool_id

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    store = EventStore(db_path)

    # Get tool
    tool_row = store._conn.execute(
        "SELECT id, data_json FROM entities WHERE id = ?", (tool_id,)
    ).fetchone()

    if not tool_row:
        print(f"✗ Tool not found: {tool_id}", file=sys.stderr)
        store.close()
        return 1

    data = json_lib.loads(tool_row["data_json"])

    # Get bonds
    bonds_from = store._conn.execute(
        "SELECT to_id, type FROM bonds WHERE from_id = ?", (tool_id,)
    ).fetchall()

    bonds_to = store._conn.execute(
        "SELECT from_id, type FROM bonds WHERE to_id = ?", (tool_id,)
    ).fetchall()

    store.close()

    # Analyze
    implements_behaviors = [b["from_id"] for b in bonds_to if b["type"] == "implements"]
    verifies_behaviors = [b["to_id"] for b in bonds_from if b["type"] == "verifies"]
    origins = [b["to_id"] for b in bonds_from if b["type"] == "crystallized-from"]
    emits_signals = [b["to_id"] for b in bonds_from if b["type"] == "emits"]

    cognition = data.get("cognition", {})
    ready_at_hand = cognition.get("ready_at_hand")

    print()
    print(f"╭─ Provenance Check: {tool_id}")
    print("│")

    # Implements
    if implements_behaviors:
        print(f"│  ✓ IMPLEMENTS (behavior → tool)")
        for bid in implements_behaviors:
            print(f"│      {bid} --implements--> {tool_id}")
    else:
        print(f"│  ✗ IMPLEMENTS: No behavior implements this tool")
        print(f"│      Suggestion: Create a behavior entity and bond it")

    print("│")

    # Verifies
    if verifies_behaviors:
        print(f"│  ✓ VERIFIES (tool → behavior)")
        for bid in verifies_behaviors:
            print(f"│      {tool_id} --verifies--> {bid}")
    else:
        print(f"│  ✗ VERIFIES: Tool does not verify any behavior")
        print(f"│      Suggestion: just verifies {tool_id} <behavior_id>")

    print("│")

    # Origin
    if origins:
        print(f"│  ✓ CRYSTALLIZED-FROM (origin tracing)")
        for oid in origins:
            print(f"│      {tool_id} --crystallized-from--> {oid}")
    else:
        print(f"│  ✗ CRYSTALLIZED-FROM: No origin trace")
        print(f"│      Suggestion: just bond crystallized-from {tool_id} <learning_id|pattern_id>")

    print("│")

    # Cognition
    if ready_at_hand:
        print(f"│  ✓ COGNITION (teaching metadata)")
        print(f"│      ready_at_hand: \"{ready_at_hand}\"")
    else:
        print(f"│  ✗ COGNITION: No ready_at_hand teaching")
        print(f"│      Suggestion: Update tool data with cognition.ready_at_hand")

    print("│")

    # Emits
    if emits_signals:
        print(f"│  ✓ EMITS (reflex arc)")
        for sid in emits_signals:
            print(f"│      {tool_id} --emits--> {sid}")
    else:
        print(f"│  ○ EMITS: No signals (optional)")

    # Score
    score = sum([
        bool(implements_behaviors),
        bool(verifies_behaviors),
        bool(origins),
        bool(ready_at_hand)
    ])

    print("│")
    print(f"╰─ Score: {score}/4 {'(Complete!)' if score == 4 else ''}")
    print()

    return 0


def cmd_provenance_heal(args: argparse.Namespace) -> int:
    """Suggest and optionally apply provenance fixes for tools."""
    import json as json_lib
    from .store import EventStore

    db_path = resolve_db_path(args.db)

    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}", file=sys.stderr)
        return 1

    store = EventStore(db_path)

    # Get all tools without origin
    tools_without_origin = store._conn.execute("""
        SELECT t.id FROM entities t
        WHERE t.type = 'tool'
        AND NOT EXISTS (
            SELECT 1 FROM bonds b WHERE b.from_id = t.id AND b.type = 'crystallized-from'
        )
        ORDER BY t.id
    """).fetchall()

    # Get all tools without implements
    tools_without_implements = store._conn.execute("""
        SELECT t.id FROM entities t
        WHERE t.type = 'tool'
        AND NOT EXISTS (
            SELECT 1 FROM bonds b WHERE b.to_id = t.id AND b.type = 'implements'
        )
        ORDER BY t.id
    """).fetchall()

    # Get all tools without verifies
    tools_without_verifies = store._conn.execute("""
        SELECT t.id FROM entities t
        WHERE t.type = 'tool'
        AND NOT EXISTS (
            SELECT 1 FROM bonds b WHERE b.from_id = t.id AND b.type = 'verifies'
        )
        ORDER BY t.id
    """).fetchall()

    # Get learnings (potential origin targets)
    recent_learnings = store._conn.execute("""
        SELECT id, data_json FROM entities
        WHERE type = 'learning'
        ORDER BY id DESC
        LIMIT 20
    """).fetchall()

    # Get patterns (potential origin targets)
    patterns = store._conn.execute("""
        SELECT id, data_json FROM entities WHERE type = 'pattern' ORDER BY id
    """).fetchall()

    store.close()

    print()
    print("╭────────────────────────────────────────────────────────────╮")
    print("│  Provenance Heal - Suggested Fixes                         │")
    print("╰────────────────────────────────────────────────────────────╯")
    print()

    # Prioritize by category
    if args.category == "origin" or not args.category:
        print(f"  Tools missing crystallized-from ({len(tools_without_origin)}):")
        print()
        print("  Available targets for origin tracing:")
        print("    Learnings (recent):")
        for row in recent_learnings[:5]:
            data = json_lib.loads(row["data_json"])
            title = data.get("title", "")[:50]
            print(f"      • {row['id']}: {title}")
        print("    Patterns:")
        for row in patterns[:5]:
            data = json_lib.loads(row["data_json"])
            title = data.get("title", "")[:50]
            print(f"      • {row['id']}: {title}")
        print()
        print("  To fix, run:")
        print("    just bond crystallized-from <tool_id> <learning_id|pattern_id>")
        print()

    if args.category == "implements" or not args.category:
        print(f"  Tools missing implements bond ({len(tools_without_implements)}):")
        for row in tools_without_implements[:10]:
            print(f"    • {row['id']}")
        if len(tools_without_implements) > 10:
            print(f"    ... and {len(tools_without_implements) - 10} more")
        print()
        print("  To fix:")
        print("    1. Create behavior: just create behavior \"Tool does X when Y\"")
        print("    2. Bond: just implements <behavior_id> <tool_id>")
        print()

    if args.category == "verifies" or not args.category:
        print(f"  Tools missing verifies bond ({len(tools_without_verifies)}):")
        for row in tools_without_verifies[:10]:
            print(f"    • {row['id']}")
        if len(tools_without_verifies) > 10:
            print(f"    ... and {len(tools_without_verifies) - 10} more")
        print()
        print("  To fix:")
        print("    just verifies <tool_id> <behavior_id>")
        print()

    return 0


# =============================================================================
# Main Entry Point
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="cvm",
        description="Chora Virtual Machine - Universal Dispatcher",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # dispatch command (CvmEngine unified entry point)
    dispatch_parser = subparsers.add_parser(
        "dispatch",
        help="Dispatch through CvmEngine (protocols OR primitives)"
    )
    dispatch_parser.add_argument("intent", help="Intent to dispatch (protocol or primitive name)")
    dispatch_parser.add_argument("--input", "-i", help="JSON input")
    dispatch_parser.add_argument("--db", help="Database path")
    dispatch_parser.add_argument("--persona", help="Persona ID")

    # capabilities command
    caps_parser = subparsers.add_parser(
        "capabilities",
        help="List all available protocols and primitives"
    )
    caps_parser.add_argument("--db", help="Database path")

    # invoke command (legacy, routes directly to execute_protocol)
    invoke_parser = subparsers.add_parser("invoke", help="Invoke a protocol")
    invoke_parser.add_argument("protocol_id", help="Protocol ID to invoke")
    invoke_parser.add_argument("--input", "-i", help="JSON input for the protocol")
    invoke_parser.add_argument("--db", help="Database path")
    invoke_parser.add_argument("--persona", help="Persona ID")
    invoke_parser.add_argument("--state-id", help="State ID for resumption")
    invoke_parser.add_argument(
        "--async", dest="async_mode", action="store_true",
        help="Queue for background execution (requires worker)"
    )

    # worker command
    worker_parser = subparsers.add_parser("worker", help="Start background worker")
    worker_parser.add_argument(
        "--workers", "-w", type=int, default=1,
        help="Number of worker threads (default: 1)"
    )

    # status command
    status_parser = subparsers.add_parser("status", help="Check async task status")
    status_parser.add_argument("task_id", help="Task ID to check")

    # login command
    login_parser = subparsers.add_parser("login", help="Set current persona")
    login_parser.add_argument("persona_id", help="Persona ID to use")

    # context command
    subparsers.add_parser("context", help="Show current context")

    # pulse-status command
    pulse_status_parser = subparsers.add_parser("pulse-status", help="Show recent pulse history")
    pulse_status_parser.add_argument(
        "--limit", "-n", type=int, default=10,
        help="Number of recent pulses to show (default: 10)"
    )

    # pulse-preview command
    pulse_preview_parser = subparsers.add_parser("pulse-preview", help="Preview what pulse would process")
    pulse_preview_parser.add_argument("--db", help="Database path")
    pulse_preview_parser.add_argument(
        "--limit", "-n", type=int, default=10,
        help="Max signals to preview (default: 10)"
    )

    # integrity command
    integrity_parser = subparsers.add_parser("integrity", help="Check system integrity")
    integrity_parser.add_argument("--db", help="Database path")
    integrity_parser.add_argument(
        "--features-dir", "-f",
        help="Directory containing feature files (default: tests/features)"
    )

    # rhythm command (Phase 2 - via protocol)
    rhythm_parser = subparsers.add_parser("rhythm", help="Sense kairotic phase and system rhythm")
    rhythm_parser.add_argument("--db", help="Database path")

    # prune command with subcommands
    prune_parser = subparsers.add_parser("prune", help="Prune lifecycle: detect, approve, reject")
    prune_subparsers = prune_parser.add_subparsers(dest="prune_action")

    # prune detect (default)
    prune_detect_parser = prune_subparsers.add_parser("detect", help="Detect prunable entities")
    prune_detect_parser.add_argument("--db", help="Database path")
    prune_detect_parser.add_argument(
        "--dry-run", action="store_true",
        help="Show prunable items without emitting signals/focuses"
    )
    prune_detect_parser.add_argument(
        "--propose", action="store_true",
        help="Create Focus entities for human approval instead of signals"
    )

    # prune approve
    prune_approve_parser = prune_subparsers.add_parser("approve", help="Approve a prune proposal")
    prune_approve_parser.add_argument("focus_id", help="ID of the Focus entity proposing the prune")
    prune_approve_parser.add_argument("--db", help="Database path")

    # prune reject
    prune_reject_parser = prune_subparsers.add_parser("reject", help="Reject a prune proposal")
    prune_reject_parser.add_argument("focus_id", help="ID of the Focus entity proposing the prune")
    prune_reject_parser.add_argument("reason", nargs="?", default=None, help="Reason for rejection")
    prune_reject_parser.add_argument("--db", help="Database path")

    # Legacy support: prune without subcommand defaults to detect
    prune_parser.add_argument("--db", help="Database path")
    prune_parser.add_argument(
        "--dry-run", action="store_true",
        help="Show prunable items without emitting signals/focuses"
    )
    prune_parser.add_argument(
        "--propose", action="store_true",
        help="Create Focus entities for human approval instead of signals"
    )
    prune_parser.add_argument(
        "--via-protocol", dest="via_protocol", action="store_true",
        help="Route detection through protocol-prune-detect (Phase 2 migration)"
    )

    # Also add --via-protocol to detect subcommand
    prune_detect_parser.add_argument(
        "--via-protocol", dest="via_protocol", action="store_true",
        help="Route detection through protocol-prune-detect (Phase 2 migration)"
    )

    # bond command
    bond_parser = subparsers.add_parser("bond", help="Create a bond between entities")
    bond_parser.add_argument("verb", help="Bond type (yields, surfaces, implements, etc.)")
    bond_parser.add_argument("from_id", help="Source entity ID")
    bond_parser.add_argument("to_id", help="Target entity ID")
    bond_parser.add_argument("--db", help="Database path")
    bond_parser.add_argument(
        "-c", "--confidence", type=float, default=1.0,
        help="Epistemic confidence (0.0-1.0, default 1.0)"
    )
    bond_parser.add_argument(
        "--no-physics", action="store_true",
        help="Disable physics type validation"
    )

    # entropy command
    entropy_parser = subparsers.add_parser("entropy", help="Report metabolic health")
    entropy_parser.add_argument("--db", help="Database path")

    # digest command
    digest_parser = subparsers.add_parser("digest", help="Transform entity into learning")
    digest_parser.add_argument("entity_id", help="Entity ID to digest")
    digest_parser.add_argument("--db", help="Database path")

    # compost command
    compost_parser = subparsers.add_parser("compost", help="Archive orphan entity")
    compost_parser.add_argument("entity_id", help="Entity ID to compost")
    compost_parser.add_argument("--db", help="Database path")
    compost_parser.add_argument(
        "--force", action="store_true",
        help="Force compost even if entity has active bonds"
    )

    # induce command
    induce_parser = subparsers.add_parser("induce", help="Propose pattern from learnings")
    induce_parser.add_argument(
        "learning_ids", nargs="+",
        help="Learning IDs to cluster (minimum 3)"
    )
    induce_parser.add_argument("--db", help="Database path")

    # reflex command group
    reflex_parser = subparsers.add_parser("reflex", help="Autonomic reflex arc commands")
    reflex_subparsers = reflex_parser.add_subparsers(dest="reflex_command", required=True)

    # reflex build command
    reflex_build_parser = reflex_subparsers.add_parser(
        "build",
        help="Detect build quality regressions and emit signals"
    )
    reflex_build_parser.add_argument("--db", help="Database path")
    reflex_build_parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't emit/resolve signals, just show what would happen"
    )
    reflex_build_parser.add_argument(
        "-p", "--package",
        help="Check specific package only (e.g., chora-cvm)"
    )
    reflex_build_parser.add_argument(
        "-c", "--check",
        choices=["lint", "typecheck", "test"],
        help="Run specific check only"
    )

    # reflex arc command
    reflex_arc_parser = reflex_subparsers.add_parser(
        "arc",
        help="Run full reflex arc: void detection and signal emission"
    )
    reflex_arc_parser.add_argument("--db", help="Database path")
    reflex_arc_parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't emit signals, just show what would happen"
    )

    # bootstrap command group
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Self-manifestation: bootstrap subsystems")
    bootstrap_subparsers = bootstrap_parser.add_subparsers(dest="bootstrap_command", required=True)

    # bootstrap build command
    bootstrap_build_parser = bootstrap_subparsers.add_parser(
        "build",
        help="Manifest build governance entities (principles, patterns, behaviors, tools)"
    )
    bootstrap_build_parser.add_argument("--db", help="Database path")

    # bootstrap physics command
    bootstrap_physics_parser = bootstrap_subparsers.add_parser(
        "physics",
        help="Genesis: crystallize the Laws of Nature as axiom entities"
    )
    bootstrap_physics_parser.add_argument("--db", help="Database path")

    # bootstrap circle-orient command
    bootstrap_circle_orient_parser = bootstrap_subparsers.add_parser(
        "circle-orient",
        help="Manifest protocol-circle-orient for circle-aware orientation"
    )
    bootstrap_circle_orient_parser.add_argument("--db", help="Database path")

    # build command group
    build_parser = subparsers.add_parser("build", help="Build integrity operations")
    build_subparsers = build_parser.add_subparsers(dest="build_command", required=True)

    # build check command
    build_check_parser = build_subparsers.add_parser(
        "check",
        help="Check build integrity across all packages (lint, typecheck, tests)"
    )
    build_check_parser.add_argument("--db", help="Database path for signal emission")
    build_check_parser.add_argument(
        "--workspace", "-w",
        help="Workspace root directory"
    )
    build_check_parser.add_argument(
        "--no-signals", action="store_true",
        help="Don't emit signals for failures"
    )

    # create command
    create_parser = subparsers.add_parser("create", help="Create an entity")
    create_parser.add_argument("type", help="Entity type (story, behavior, tool, etc.)")
    create_parser.add_argument("title", help="Entity title")
    create_parser.add_argument("data", nargs="?", default="{}", help="JSON data")
    create_parser.add_argument("--db", help="Database path")

    # garden command
    garden_parser = subparsers.add_parser("garden", help="Auto-gardener: propose bonds for voids")
    garden_parser.add_argument("--db", help="Database path")
    garden_parser.add_argument(
        "--confidence", type=float, default=0.85,
        help="Minimum similarity for proposals (default 0.85)"
    )
    garden_parser.add_argument(
        "--dry-run", action="store_true",
        help="Show proposals without creating signals"
    )
    garden_parser.add_argument(
        "--auto", action="store_true",
        help="Auto-create bonds above 95%% threshold"
    )

    # horizon command
    horizon_parser = subparsers.add_parser("horizon", help="What wants attention")
    horizon_parser.add_argument("--db", help="Database path")
    horizon_parser.add_argument(
        "--days", type=int, default=7,
        help="Look at learnings from last N days"
    )
    horizon_parser.add_argument(
        "--limit", type=int, default=10,
        help="Maximum recommendations"
    )

    # search command
    search_parser = subparsers.add_parser("search", help="Semantic search across the Loom")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("--db", help="Database path")
    search_parser.add_argument(
        "--type", dest="type",
        help="Filter by entity type"
    )
    search_parser.add_argument(
        "--limit", type=int, default=10,
        help="Maximum results"
    )

    # confidence command
    confidence_parser = subparsers.add_parser("confidence", help="Update bond confidence")
    confidence_parser.add_argument("bond_id", help="Bond ID to update")
    confidence_parser.add_argument("confidence", type=float, help="New confidence value (0.0-1.0)")
    confidence_parser.add_argument("--db", help="Database path")

    # harvest command group
    harvest_parser = subparsers.add_parser("harvest", help="Harvest content from legacy sources")
    harvest_subparsers = harvest_parser.add_subparsers(dest="harvest_command", required=True)

    # harvest entities
    harvest_entities_parser = harvest_subparsers.add_parser(
        "entities", help="Harvest entities from ~/.chora databases"
    )
    harvest_entities_parser.add_argument(
        "db_path", nargs="?", default="chora-legacy.db",
        help="Target database path (default: chora-legacy.db)"
    )
    harvest_entities_parser.add_argument(
        "--search", "-s", metavar="QUERY",
        help="Search legacy entities with FTS5 query"
    )
    harvest_entities_parser.add_argument(
        "--limit", "-n", type=int, default=20,
        help="Maximum search results (default: 20)"
    )
    harvest_entities_parser.add_argument(
        "--stats", action="store_true",
        help="Show legacy entity statistics"
    )

    # harvest legacy
    harvest_legacy_parser = harvest_subparsers.add_parser(
        "legacy", help="Harvest content from legacy repository packages"
    )
    harvest_legacy_parser.add_argument(
        "db_path", nargs="?", default="chora-legacy.db",
        help="Target database path (default: chora-legacy.db)"
    )
    harvest_legacy_parser.add_argument(
        "--search", "-s", metavar="QUERY",
        help="Search the database with FTS5 query"
    )
    harvest_legacy_parser.add_argument(
        "--limit", "-n", type=int, default=20,
        help="Maximum search results (default: 20)"
    )
    harvest_legacy_parser.add_argument(
        "--stats", action="store_true",
        help="Show database statistics"
    )
    harvest_legacy_parser.add_argument(
        "--workspace", "-w", default=str(Path(__file__).parent.parent.parent.parent.parent.parent),
        help="Workspace root directory"
    )
    harvest_legacy_parser.add_argument(
        "--archive", "-a", action="store_true",
        help="Include archive repositories (v4-store, v4-starship, v5)"
    )

    # harvest plans
    harvest_plans_parser = harvest_subparsers.add_parser(
        "plans", help="Harvest plans corpus from ~/.claude/plans"
    )
    harvest_plans_parser.add_argument(
        "db_path", nargs="?", default="chora-cvm-plans.db",
        help="Target database path (default: chora-cvm-plans.db)"
    )

    # harvest principles
    harvest_principles_parser = harvest_subparsers.add_parser(
        "principles", help="Harvest principles from legacy archive into Loom"
    )
    harvest_principles_parser.add_argument("--db", help="Loom database path")

    # harvest setup
    harvest_setup_parser = harvest_subparsers.add_parser(
        "setup", help="Bootstrap harvest primitives and protocol"
    )
    harvest_setup_parser.add_argument("--db", help="Database path")

    # harvest pattern
    harvest_pattern_parser = harvest_subparsers.add_parser(
        "pattern", help="Execute harvest protocol for a pattern"
    )
    harvest_pattern_parser.add_argument(
        "pattern_id", nargs="?",
        help="Pattern ID to harvest (default: pattern-self-extension-pattern)"
    )
    harvest_pattern_parser.add_argument("--db", help="Database path")

    # orient command
    orient_parser = subparsers.add_parser("orient", help="Summarize Loom entity landscape")
    orient_parser.add_argument("--db", help="Database path")

    # teach command
    teach_parser = subparsers.add_parser("teach", help="Explain an entity in Diataxis format")
    teach_parser.add_argument("entity_id", help="Entity ID to explain")
    teach_parser.add_argument("--db", help="Database path")

    # circle command group
    circle_parser = subparsers.add_parser("circle", help="Circle-aware commands")
    circle_subparsers = circle_parser.add_subparsers(dest="circle_command", required=True)

    # circle orient
    circle_orient_parser = circle_subparsers.add_parser(
        "orient", help="Resolve circle and show assets/tools"
    )
    circle_orient_parser.add_argument("--db", help="Database path")

    # manifest command group
    manifest_parser = subparsers.add_parser("manifest", help="Manifest entities into the Loom")
    manifest_subparsers = manifest_parser.add_subparsers(dest="manifest_command", required=True)

    # manifest circle
    manifest_circle_parser = manifest_subparsers.add_parser(
        "circle", help="Manifest a Circle and its local repo Asset"
    )
    manifest_circle_parser.add_argument("--db", help="Database path")

    # semantic command group
    semantic_parser = subparsers.add_parser("semantic", help="Semantic search and embedding operations")
    semantic_subparsers = semantic_parser.add_subparsers(dest="semantic_command", required=True)

    # semantic setup
    semantic_setup_parser = semantic_subparsers.add_parser(
        "setup", help="Setup semantic primitives and protocols"
    )
    semantic_setup_parser.add_argument("--db", help="Database path")

    # docs command group
    docs_parser = subparsers.add_parser("docs", help="Documentation operations")
    docs_subparsers = docs_parser.add_subparsers(dest="docs_command", required=True)

    # docs setup
    docs_setup_parser = docs_subparsers.add_parser(
        "setup", help="Setup docs/teach primitives and protocols"
    )
    docs_setup_parser.add_argument("--db", help="Database path")

    # docs check
    docs_check_parser = docs_subparsers.add_parser(
        "check", help="Check Diataxis completeness for tools"
    )
    docs_check_parser.add_argument("--db", help="Database path")

    # docs generate
    docs_generate_parser = docs_subparsers.add_parser(
        "generate", help="Generate browsable Markdown docs for Loom tools"
    )
    docs_generate_parser.add_argument("--db", help="Database path")
    docs_generate_parser.add_argument(
        "--output", "-o", default="docs/loom.md",
        help="Output file path (default: docs/loom.md)"
    )

    # docs core
    docs_core_parser = docs_subparsers.add_parser(
        "core", help="Manifest core Diataxis docs for key tools"
    )
    docs_core_parser.add_argument("--db", help="Database path")

    # whoami command
    whoami_parser = subparsers.add_parser("whoami", help="Show personas from the Loom")
    whoami_parser.add_argument("--db", help="Database path")
    whoami_parser.add_argument(
        "persona_id", nargs="?",
        help="Specific persona ID to show (default: show all)"
    )

    # provenance command group
    provenance_parser = subparsers.add_parser("provenance", help="Tool provenance verification")
    provenance_subparsers = provenance_parser.add_subparsers(dest="provenance_command", required=True)

    # provenance audit
    provenance_audit_parser = provenance_subparsers.add_parser(
        "audit", help="Audit all tools for provenance completeness"
    )
    provenance_audit_parser.add_argument("--db", help="Database path")
    provenance_audit_parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show detailed per-tool breakdown"
    )
    provenance_audit_parser.add_argument(
        "--csv", action="store_true",
        help="Output as CSV"
    )
    provenance_audit_parser.add_argument(
        "--gaps", action="store_true",
        help="Show tools with missing bonds"
    )

    # provenance check
    provenance_check_parser = provenance_subparsers.add_parser(
        "check", help="Check provenance for a specific tool"
    )
    provenance_check_parser.add_argument("tool_id", help="Tool ID to check")
    provenance_check_parser.add_argument("--db", help="Database path")

    # provenance heal
    provenance_heal_parser = provenance_subparsers.add_parser(
        "heal", help="Suggest provenance fixes"
    )
    provenance_heal_parser.add_argument("--db", help="Database path")
    provenance_heal_parser.add_argument(
        "--category", "-c",
        choices=["origin", "implements", "verifies"],
        help="Focus on specific gap category"
    )

    args = parser.parse_args()

    if args.command == "dispatch":
        return cmd_dispatch(args)
    elif args.command == "capabilities":
        return cmd_capabilities(args)
    elif args.command == "invoke":
        return cmd_invoke(args)
    elif args.command == "worker":
        return cmd_worker(args)
    elif args.command == "status":
        return cmd_status(args)
    elif args.command == "login":
        return cmd_login(args)
    elif args.command == "context":
        return cmd_context(args)
    elif args.command == "pulse-status":
        return cmd_pulse_status(args)
    elif args.command == "pulse-preview":
        return cmd_pulse_preview(args)
    elif args.command == "integrity":
        return cmd_integrity(args)
    elif args.command == "rhythm":
        return cmd_rhythm(args)
    elif args.command == "prune":
        # Handle prune subcommands
        prune_action = getattr(args, 'prune_action', None)
        if prune_action == "approve":
            return cmd_prune_approve(args)
        elif prune_action == "reject":
            return cmd_prune_reject(args)
        else:
            # Default to detect (including 'detect' subcommand or no subcommand)
            return cmd_prune(args)
    elif args.command == "bond":
        return cmd_bond(args)
    elif args.command == "entropy":
        return cmd_entropy(args)
    elif args.command == "digest":
        return cmd_digest(args)
    elif args.command == "compost":
        return cmd_compost(args)
    elif args.command == "induce":
        return cmd_induce(args)
    elif args.command == "reflex":
        if args.reflex_command == "build":
            return cmd_reflex_build(args)
        elif args.reflex_command == "arc":
            return cmd_reflex_arc(args)
        return 1
    elif args.command == "bootstrap":
        if args.bootstrap_command == "build":
            return cmd_bootstrap_build(args)
        elif args.bootstrap_command == "physics":
            return cmd_bootstrap_physics(args)
        elif args.bootstrap_command == "circle-orient":
            return cmd_bootstrap_circle_orient(args)
        return 1
    elif args.command == "build":
        if args.build_command == "check":
            return cmd_build_check(args)
        return 1
    elif args.command == "create":
        return cmd_create(args)
    elif args.command == "garden":
        return cmd_garden(args)
    elif args.command == "horizon":
        return cmd_horizon(args)
    elif args.command == "search":
        return cmd_search(args)
    elif args.command == "confidence":
        return cmd_confidence(args)
    elif args.command == "harvest":
        if args.harvest_command == "entities":
            return cmd_harvest_entities(args)
        elif args.harvest_command == "legacy":
            return cmd_harvest_legacy(args)
        elif args.harvest_command == "plans":
            return cmd_harvest_plans(args)
        elif args.harvest_command == "principles":
            return cmd_harvest_principles(args)
        elif args.harvest_command == "setup":
            return cmd_harvest_setup(args)
        elif args.harvest_command == "pattern":
            return cmd_harvest_pattern(args)
        return 1
    elif args.command == "orient":
        return cmd_orient(args)
    elif args.command == "teach":
        return cmd_teach(args)
    elif args.command == "circle":
        if args.circle_command == "orient":
            return cmd_circle_orient(args)
        return 1
    elif args.command == "manifest":
        if args.manifest_command == "circle":
            return cmd_manifest_circle(args)
        return 1
    elif args.command == "semantic":
        if args.semantic_command == "setup":
            return cmd_semantic_setup(args)
        return 1
    elif args.command == "docs":
        if args.docs_command == "setup":
            return cmd_docs_setup(args)
        elif args.docs_command == "check":
            return cmd_docs_check(args)
        elif args.docs_command == "generate":
            return cmd_docs_generate(args)
        elif args.docs_command == "core":
            return cmd_docs_core(args)
        return 1
    elif args.command == "whoami":
        return cmd_whoami(args)
    elif args.command == "provenance":
        if args.provenance_command == "audit":
            return cmd_provenance_audit(args)
        elif args.provenance_command == "check":
            return cmd_provenance_check(args)
        elif args.provenance_command == "heal":
            return cmd_provenance_heal(args)
        return 1

    return 1


if __name__ == "__main__":
    sys.exit(main())
