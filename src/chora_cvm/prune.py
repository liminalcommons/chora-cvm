"""
Prune — Physics-Driven Code Lifecycle.

Entity lifecycle drives code lifecycle. The graph is the source of truth;
code becomes prunable when its corresponding entities are marked deprecated,
orphaned, or unverified.

Detection queries axiom entities (homoiconic), signals are entities,
approvals flow through Focus, wisdom lives in Learnings.

Based on the principle: "Logic derives from the graph"
"""
from __future__ import annotations

import ast
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .store import EventStore
from .std import manifest_entity


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PrunableEntity:
    """An entity that may be prunable."""
    id: str
    type: str
    title: str | None = None
    handler: str | None = None
    reason: str = ""
    handler_exists: bool = True


@dataclass
class PruneReport:
    """Results of prune detection."""
    orphan_tools: list[PrunableEntity] = field(default_factory=list)
    deprecated_tools: list[PrunableEntity] = field(default_factory=list)
    broken_handlers: list[PrunableEntity] = field(default_factory=list)
    dark_matter: list[dict] = field(default_factory=list)  # code without entities
    signals_emitted: list[dict] = field(default_factory=list)


# =============================================================================
# Axiom Loading (homoiconic — same pattern as run_reflex.py)
# =============================================================================


def load_axioms(db_path: str) -> dict[str, dict]:
    """
    Load Axiom entities from the graph.

    Returns dict mapping verb -> {subject_type, object_type}
    This is the system's Laws of Nature made queryable.
    """
    store = EventStore(db_path)
    cur = store._conn.cursor()

    cur.execute("""
        SELECT
            json_extract(data_json, '$.verb') as verb,
            json_extract(data_json, '$.subject_type') as subject_type,
            json_extract(data_json, '$.object_type') as object_type
        FROM entities
        WHERE type = 'axiom'
        AND json_extract(data_json, '$.verb') IS NOT NULL
    """)

    axioms = {}
    for row in cur.fetchall():
        verb, subject_type, object_type = row
        if verb and subject_type:
            axioms[verb] = {
                "subject_type": subject_type,
                "object_type": object_type,
            }

    store.close()
    return axioms


# =============================================================================
# Detection (graph-derived)
# =============================================================================


def detect_orphan_tools(db_path: str, axioms: dict) -> list[PrunableEntity]:
    """
    Detect tools without 'implements' bonds from behaviors.

    A tool is orphan if no behavior implements it.
    Derived from axiom: behavior --implements--> tool
    """
    orphans = []
    store = EventStore(db_path)
    cur = store._conn.cursor()

    # The axiom tells us tools should be the target of 'implements' bonds
    # If no such bond exists, the tool is orphan (code may be dead)
    cur.execute("""
        SELECT
            e.id,
            e.type,
            json_extract(e.data_json, '$.title') as title,
            json_extract(e.data_json, '$.handler') as handler
        FROM entities e
        WHERE e.type = 'tool'
        AND COALESCE(json_extract(e.data_json, '$.status'), 'active') != 'deprecated'
        AND COALESCE(json_extract(e.data_json, '$.internal'), json('false')) != json('true')
        AND NOT EXISTS (
            SELECT 1 FROM bonds b
            WHERE b.to_id = e.id AND b.type = 'implements'
        )
    """)

    for row in cur.fetchall():
        orphans.append(PrunableEntity(
            id=row[0],
            type=row[1],
            title=row[2],
            handler=row[3],
            reason="No behavior implements this tool",
        ))

    store.close()
    return orphans


def detect_deprecated_tools(db_path: str) -> list[PrunableEntity]:
    """
    Detect tools marked as deprecated.

    These are explicit candidates for removal.
    """
    deprecated = []
    store = EventStore(db_path)
    cur = store._conn.cursor()

    cur.execute("""
        SELECT
            e.id,
            e.type,
            json_extract(e.data_json, '$.title') as title,
            json_extract(e.data_json, '$.handler') as handler,
            json_extract(e.data_json, '$.deprecated_at') as deprecated_at
        FROM entities e
        WHERE e.type = 'tool'
        AND json_extract(e.data_json, '$.status') = 'deprecated'
    """)

    for row in cur.fetchall():
        deprecated.append(PrunableEntity(
            id=row[0],
            type=row[1],
            title=row[2],
            handler=row[3],
            reason=f"Marked deprecated{': ' + row[4] if row[4] else ''}",
        ))

    store.close()
    return deprecated


def detect_broken_handlers(db_path: str, src_dir: Path | None = None) -> list[PrunableEntity]:
    """
    Detect tools whose handlers reference non-existent code.

    Cross-references tool.handler fields with actual Python code.
    """
    if src_dir is None:
        # Default to CVM source directory
        src_dir = Path(__file__).parent

    broken = []
    store = EventStore(db_path)
    cur = store._conn.cursor()

    # Get all tools with handlers
    cur.execute("""
        SELECT
            e.id,
            e.type,
            json_extract(e.data_json, '$.title') as title,
            json_extract(e.data_json, '$.handler') as handler
        FROM entities e
        WHERE e.type = 'tool'
        AND json_extract(e.data_json, '$.handler') IS NOT NULL
        AND COALESCE(json_extract(e.data_json, '$.status'), 'active') != 'deprecated'
    """)

    # Build set of existing functions from code
    existing_functions = _discover_functions(src_dir)

    for row in cur.fetchall():
        tool_id, tool_type, title, handler = row

        if not handler:
            continue

        # Parse handler to get function name
        # Handler format: "module.function" or "chora_cvm.module.function"
        func_name = handler.split(".")[-1] if handler else None

        if func_name and func_name not in existing_functions:
            broken.append(PrunableEntity(
                id=tool_id,
                type=tool_type,
                title=title,
                handler=handler,
                reason=f"Handler '{handler}' not found in codebase",
                handler_exists=False,
            ))

    store.close()
    return broken


def _discover_functions(src_dir: Path) -> set[str]:
    """Discover all function names in Python files."""
    functions = set()

    for py_file in src_dir.glob("**/*.py"):
        if py_file.name.startswith("_") and py_file.name != "__init__.py":
            continue

        try:
            source = py_file.read_text()
            tree = ast.parse(source)

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    functions.add(node.name)

        except (SyntaxError, OSError):
            continue

    return functions


def detect_dark_matter(db_path: str, src_dir: Path | None = None) -> list[dict]:
    """
    Detect code functions without corresponding entities.

    This is 'dark matter' - code that exists but is invisible to the system.
    """
    if src_dir is None:
        src_dir = Path(__file__).parent

    dark_matter = []
    store = EventStore(db_path)
    cur = store._conn.cursor()

    # Get all tool handlers and primitive refs
    cur.execute("""
        SELECT json_extract(data_json, '$.handler') FROM entities WHERE type = 'tool'
        UNION
        SELECT json_extract(data_json, '$.python_ref') FROM entities WHERE type = 'primitive'
    """)
    entity_refs = {row[0].split(".")[-1] for row in cur.fetchall() if row[0]}

    # Get functions from code
    for py_file in src_dir.glob("*.py"):
        if py_file.name.startswith("_") and py_file.name != "__init__.py":
            continue

        try:
            source = py_file.read_text()
            tree = ast.parse(source)

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # Skip private functions
                    if node.name.startswith("_"):
                        continue

                    # Check if function has corresponding entity
                    if node.name not in entity_refs:
                        dark_matter.append({
                            "name": node.name,
                            "file": py_file.name,
                            "line": node.lineno,
                        })

        except (SyntaxError, OSError):
            continue

    store.close()
    return dark_matter


# =============================================================================
# Full Detection
# =============================================================================


def detect_prunable(db_path: str, src_dir: Path | None = None) -> PruneReport:
    """
    Detect all prunable entities and code.

    Derives detection from axiom entities (homoiconic).

    Returns:
        PruneReport with categorized prunable items.
    """
    axioms = load_axioms(db_path)

    report = PruneReport(
        orphan_tools=detect_orphan_tools(db_path, axioms),
        deprecated_tools=detect_deprecated_tools(db_path),
        broken_handlers=detect_broken_handlers(db_path, src_dir),
        dark_matter=detect_dark_matter(db_path, src_dir),
    )

    return report


# =============================================================================
# Signal Emission
# =============================================================================


def emit_prune_signals(db_path: str, report: PruneReport, dry_run: bool = False) -> list[dict]:
    """
    Emit signals for prunable items that exceed thresholds.

    Thresholds:
    - orphan_tools: > 3 → signal
    - deprecated_tools: > 0 → signal (any deprecated should be reviewed)
    - broken_handlers: > 0 → signal (immediate attention needed)
    - dark_matter: > 10 → signal (significant uncovered code)
    """
    signals = []

    # Orphan tools signal
    if len(report.orphan_tools) > 3:
        signal_id = f"signal-prunable-orphan-tools-{uuid.uuid4().hex[:8]}"
        count = len(report.orphan_tools)

        if not dry_run:
            manifest_entity(
                db_path,
                "signal",
                signal_id,
                {
                    "title": f"Prunable: {count} orphan tools detected",
                    "status": "active",
                    "signal_type": "prune-candidate",
                    "category": "orphan-tools",
                    "count": count,
                    "tool_ids": [t.id for t in report.orphan_tools[:10]],
                    "handlers": [t.handler for t in report.orphan_tools[:10] if t.handler],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        signals.append({
            "id": signal_id,
            "category": "orphan-tools",
            "count": count,
        })

    # Deprecated tools signal
    if len(report.deprecated_tools) > 0:
        signal_id = f"signal-prunable-deprecated-{uuid.uuid4().hex[:8]}"
        count = len(report.deprecated_tools)

        if not dry_run:
            manifest_entity(
                db_path,
                "signal",
                signal_id,
                {
                    "title": f"Prunable: {count} deprecated tools awaiting removal",
                    "status": "active",
                    "signal_type": "prune-candidate",
                    "category": "deprecated-tools",
                    "count": count,
                    "tool_ids": [t.id for t in report.deprecated_tools],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        signals.append({
            "id": signal_id,
            "category": "deprecated-tools",
            "count": count,
        })

    # Broken handlers signal
    if len(report.broken_handlers) > 0:
        signal_id = f"signal-prunable-broken-handlers-{uuid.uuid4().hex[:8]}"
        count = len(report.broken_handlers)

        if not dry_run:
            manifest_entity(
                db_path,
                "signal",
                signal_id,
                {
                    "title": f"Prunable: {count} tools with broken handlers",
                    "status": "active",
                    "signal_type": "prune-candidate",
                    "category": "broken-handlers",
                    "count": count,
                    "tool_ids": [t.id for t in report.broken_handlers],
                    "broken_handlers": [t.handler for t in report.broken_handlers],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        signals.append({
            "id": signal_id,
            "category": "broken-handlers",
            "count": count,
        })

    # Dark matter signal
    if len(report.dark_matter) > 10:
        signal_id = f"signal-prunable-dark-matter-{uuid.uuid4().hex[:8]}"
        count = len(report.dark_matter)

        if not dry_run:
            manifest_entity(
                db_path,
                "signal",
                signal_id,
                {
                    "title": f"Dark matter: {count} functions without entities",
                    "status": "active",
                    "signal_type": "prune-candidate",
                    "category": "dark-matter",
                    "count": count,
                    "sample_functions": report.dark_matter[:10],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        signals.append({
            "id": signal_id,
            "category": "dark-matter",
            "count": count,
        })

    report.signals_emitted = signals
    return signals


# =============================================================================
# Focus Proposal (Phase B - human approval gate)
# =============================================================================


def propose_prune(
    db_path: str,
    report: PruneReport,
    dry_run: bool = False,
) -> list[dict]:
    """
    Create Focus entities for human approval of prune candidates.

    Each Focus contains the proposed action and pre-created learning
    to capture wisdom before removal.
    """
    focuses_created = []

    # Create focuses for deprecated tools (highest priority)
    for tool in report.deprecated_tools:
        focus_id = f"focus-prune-{tool.id[:30]}-{uuid.uuid4().hex[:8]}"

        focus_data = {
            "title": f"Prune: Remove deprecated tool '{tool.title or tool.id}'",
            "status": "pending",
            "category": "prune-approval",
            "tool_id": tool.id,
            "handler": tool.handler,
            "reason": tool.reason,
            "proposed_action": {
                "type": "compost",
                "entity_id": tool.id,
            },
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        if not dry_run:
            manifest_entity(db_path, "focus", focus_id, focus_data)

        focuses_created.append({
            "id": focus_id,
            "tool_id": tool.id,
            "category": "deprecated",
        })

    # Create focuses for broken handlers (needs immediate attention)
    for tool in report.broken_handlers:
        focus_id = f"focus-prune-{tool.id[:30]}-{uuid.uuid4().hex[:8]}"

        focus_data = {
            "title": f"Prune: Fix or remove tool with broken handler '{tool.handler}'",
            "status": "pending",
            "category": "prune-approval",
            "tool_id": tool.id,
            "handler": tool.handler,
            "reason": tool.reason,
            "proposed_action": {
                "type": "fix-or-compost",
                "entity_id": tool.id,
                "options": [
                    "Update handler to point to correct function",
                    "Compost the tool if no longer needed",
                ],
            },
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        if not dry_run:
            manifest_entity(db_path, "focus", focus_id, focus_data)

        focuses_created.append({
            "id": focus_id,
            "tool_id": tool.id,
            "category": "broken-handler",
        })

    return focuses_created


# =============================================================================
# Approval and Rejection (Phase C - human decision)
# =============================================================================


def prune_approve(db_path: str, focus_id: str) -> dict:
    """
    Approve a prune proposal — compost the entity and create a learning.

    Args:
        db_path: Path to the CVM database
        focus_id: ID of the Focus entity proposing the prune

    Returns:
        Dict with archived entity info, learning_id, and status
    """
    from .metabolic import compost

    store = EventStore(db_path)

    # Load the focus
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (focus_id,))
    row = cur.fetchone()

    if not row:
        store.close()
        return {"error": "Focus not found", "focus_id": focus_id}

    focus_data = json.loads(row[0])

    # Validate it's a prune proposal
    if focus_data.get("category") != "prune-approval":
        store.close()
        return {"error": "Focus is not a prune proposal", "focus_id": focus_id}

    # Check if already resolved
    if focus_data.get("status") == "resolved":
        store.close()
        return {"error": "Focus already resolved", "focus_id": focus_id}

    # Get the target entity
    tool_id = focus_data.get("tool_id")
    if not tool_id:
        store.close()
        return {"error": "Focus missing tool_id", "focus_id": focus_id}

    # Load the target entity to extract wisdom
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (tool_id,))
    tool_row = cur.fetchone()

    if not tool_row:
        store.close()
        return {"error": "Target entity not found", "entity_id": tool_id}

    tool_data = json.loads(tool_row[0])
    store.close()

    # Extract wisdom from the tool before composting
    phenomenology = tool_data.get("phenomenology", "")
    handler = tool_data.get("handler", "")
    title = tool_data.get("title", tool_id)

    # Compost the entity (this creates a learning and archives)
    compost_result = compost(db_path, tool_id, force=True)

    if compost_result.get("error"):
        return {"error": compost_result["error"], "entity_id": tool_id}

    # Create a prune-specific learning with extracted wisdom
    learning_id = f"learning-pruned-{tool_id[:30]}-{uuid.uuid4().hex[:8]}"
    learning_data = {
        "title": f"Pruned: {title}",
        "insight": f"Tool '{title}' was pruned. Handler: {handler}. Purpose was: {phenomenology}. Reason: {focus_data.get('reason', 'unspecified')}",
        "domain": "prune",
        "pruned_tool_id": tool_id,
        "pruned_handler": handler,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    manifest_entity(db_path, "learning", learning_id, learning_data)

    # Create crystallized-from bond to archived entity reference
    store = EventStore(db_path)
    store.save_bond(
        bond_id=f"rel-crystallized-from-{learning_id}-{tool_id}",
        bond_type="crystallized-from",
        from_id=learning_id,
        to_id=f"archive-{tool_id}",  # Reference to archived entity
        status="active",
    )
    store.close()

    # Resolve the focus
    from .std import resolve_focus
    resolve_focus(db_path, focus_id, outcome="completed")

    return {
        "archived": True,
        "archive_id": compost_result.get("archive_id"),
        "learning_id": learning_id,
        "focus_id": focus_id,
        "tool_id": tool_id,
    }


def prune_reject(db_path: str, focus_id: str, reason: str | None = None) -> dict:
    """
    Reject a prune proposal — capture the reason as a learning.

    Args:
        db_path: Path to the CVM database
        focus_id: ID of the Focus entity proposing the prune
        reason: Why the prune was rejected

    Returns:
        Dict with learning_id and status
    """
    store = EventStore(db_path)

    # Load the focus
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (focus_id,))
    row = cur.fetchone()

    if not row:
        store.close()
        return {"error": "Focus not found", "focus_id": focus_id}

    focus_data = json.loads(row[0])

    # Validate it's a prune proposal
    if focus_data.get("category") != "prune-approval":
        store.close()
        return {"error": "Focus is not a prune proposal", "focus_id": focus_id}

    # Check if already resolved
    if focus_data.get("status") == "resolved":
        store.close()
        return {"error": "Focus already resolved", "focus_id": focus_id}

    tool_id = focus_data.get("tool_id")
    tool_title = focus_data.get("title", tool_id)
    store.close()

    # Create a learning capturing why the prune was rejected
    rejection_reason = reason or "Rejected without specified reason"
    learning_id = f"learning-prune-rejected-{uuid.uuid4().hex[:8]}"
    learning_data = {
        "title": f"Prune rejected: {tool_title}",
        "insight": f"Prune proposal for '{tool_id}' was rejected. Reason: {rejection_reason}",
        "domain": "prune",
        "rejected_tool_id": tool_id,
        "rejection_reason": rejection_reason,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    manifest_entity(db_path, "learning", learning_id, learning_data)

    # Resolve the focus with abandoned outcome
    from .std import resolve_focus
    resolve_focus(db_path, focus_id, outcome="abandoned")

    return {
        "rejected": True,
        "learning_id": learning_id,
        "focus_id": focus_id,
        "tool_id": tool_id,
        "reason": rejection_reason,
    }
