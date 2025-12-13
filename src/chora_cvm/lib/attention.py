"""
Domain: Attention (Plasma Layer)
ID Prefix: attention.*

The attention layer tracks what the agent is focusing on and what demands
attention. Focus is plasma - the energy of attention. Signal is impulse -
something that demands attention.

Primitives:
  - attention.focus.create: Declare what is being attended to
  - attention.focus.resolve: Close the attention loop
  - attention.focus.list: List active (unresolved) focuses
  - attention.signal.emit: Emit a signal demanding attention
"""
from __future__ import annotations

import datetime
import json
import re
import sqlite3
from typing import Any, Dict, List, Optional

from ..schema import ExecutionContext, GenericEntity
from ..store import EventStore


# =============================================================================
# Focus Operations
# =============================================================================


def focus_create(
    title: str,
    _ctx: ExecutionContext,
    description: str | None = None,
    signal_id: str | None = None,
    persona_id: str | None = None,
    data: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Primitive: attention.focus.create

    Declare what the agent is attending to. Focus is plasma - the energy
    of attention in the system. Creating a focus makes attention visible.

    Args:
        title: What is being attended to (e.g., "Implementing the audit tool")
        _ctx: Execution context (MANDATORY in lib/)
        description: Optional elaboration on the focus
        signal_id: Optional signal that triggered this focus (creates triggers bond)
        persona_id: Which persona is focusing (defaults to resident-architect)
        data: Additional data to include in the focus entity

    Returns:
        {"status": "success", "id": focus_id, "focus_status": "active"}
    """
    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        # Generate focus ID from title
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
        focus_id = f"focus-{slug}"

        # Build focus data
        focus_data = {
            "title": title,
            "description": description or f"Attention on: {title}",
            "status": "active",
            "engaged_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "persona_id": persona_id or "persona-resident-architect",
        }

        if signal_id:
            focus_data["triggered_by"] = signal_id

        # Merge additional data if provided
        if data:
            focus_data.update(data)

        # Save the focus entity
        entity = GenericEntity(id=focus_id, type="focus", data=focus_data)
        store.save_entity(entity)

        # If triggered by a signal, create a triggers bond
        if signal_id:
            store.save_bond(
                bond_id=f"rel-triggers-{signal_id}-{focus_id}",
                bond_type="triggers",
                from_id=signal_id,
                to_id=focus_id,
                status="active",
                confidence=1.0,
                data={},
            )

        return {
            "status": "success",
            "id": focus_id,
            "focus_status": "active",
        }
    except Exception as e:
        return {"status": "error", "error": str(e), "id": None}
    finally:
        if should_close:
            store.close()


def focus_resolve(
    focus_id: str,
    _ctx: ExecutionContext,
    outcome: str | None = None,
    learning_title: str | None = None,
    learning_insight: str | None = None,
) -> Dict[str, Any]:
    """
    Primitive: attention.focus.resolve

    Close the attention loop. When attention completes, the focus is marked
    resolved. Optionally, resolution can yield a Learning entity capturing
    what was discovered.

    Args:
        focus_id: The focus to resolve
        _ctx: Execution context (MANDATORY in lib/)
        outcome: Brief description of what happened (e.g., "completed", "deferred")
        learning_title: If provided, create a learning entity with this title
        learning_insight: The insight to capture in the learning

    Returns:
        {"status": "success", "id": focus_id, "focus_status": "resolved", "learning_id": ...}
    """
    try:
        conn = sqlite3.connect(_ctx.db_path)
        conn.row_factory = sqlite3.Row

        # Load existing focus
        cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (focus_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return {
                "status": "error",
                "error": f"Focus not found: {focus_id}",
                "id": focus_id,
                "focus_status": "not_found",
                "learning_id": None,
            }

        data = json.loads(row["data_json"])
        data["status"] = "resolved"
        data["resolved_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data["outcome"] = outcome or "completed"

        # Update focus entity
        conn.execute(
            "UPDATE entities SET data_json = json(?) WHERE id = ?",
            (json.dumps(data), focus_id),
        )
        conn.commit()
        conn.close()

        learning_id = None

        # Create learning if requested
        if learning_title and learning_insight:
            store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
            should_close = _ctx.store is None

            try:
                # Generate learning ID
                learning_slug = re.sub(r"[^a-z0-9]+", "-", learning_title.lower()).strip("-")
                learning_id = f"learning-{learning_slug}"

                learning_data = {
                    "title": learning_title,
                    "insight": learning_insight,
                    "domain": "attention",
                    "surfaced_from": focus_id,
                }

                learning = GenericEntity(id=learning_id, type="learning", data=learning_data)
                store.save_entity(learning)

                # Create yields bond from focus to learning
                store.save_bond(
                    bond_id=f"rel-yields-{focus_id}-{learning_id}",
                    bond_type="yields",
                    from_id=focus_id,
                    to_id=learning_id,
                    status="active",
                    confidence=1.0,
                    data={},
                )
            finally:
                if should_close:
                    store.close()

        return {
            "status": "success",
            "id": focus_id,
            "focus_status": "resolved",
            "learning_id": learning_id,
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "id": focus_id,
            "focus_status": "error",
            "learning_id": None,
        }


def focus_list(
    _ctx: ExecutionContext,
    persona_id: str | None = None,
) -> Dict[str, Any]:
    """
    Primitive: attention.focus.list

    List all active (unresolved) focus entities.

    Args:
        _ctx: Execution context (MANDATORY in lib/)
        persona_id: Optional filter by persona

    Returns:
        {"status": "success", "focuses": [...], "count": n}
    """
    try:
        conn = sqlite3.connect(_ctx.db_path)
        conn.row_factory = sqlite3.Row

        sql = """
            SELECT id, data_json FROM entities
            WHERE type = 'focus'
            AND json_extract(data_json, '$.status') = 'active'
        """
        params: List[Any] = []

        if persona_id:
            sql += " AND json_extract(data_json, '$.persona_id') = ?"
            params.append(persona_id)

        sql += " ORDER BY json_extract(data_json, '$.engaged_at') DESC"

        cur = conn.execute(sql, params)
        focuses = []
        for row in cur.fetchall():
            data = json.loads(row["data_json"])
            focuses.append({
                "id": row["id"],
                "title": data.get("title"),
                "engaged_at": data.get("engaged_at"),
                "triggered_by": data.get("triggered_by"),
                "persona_id": data.get("persona_id"),
            })

        conn.close()

        return {
            "status": "success",
            "focuses": focuses,
            "count": len(focuses),
        }
    except Exception as e:
        return {"status": "error", "error": str(e), "focuses": [], "count": 0}


# =============================================================================
# Signal Operations
# =============================================================================


def signal_emit(
    title: str,
    _ctx: ExecutionContext,
    source_id: str | None = None,
    signal_type: str = "attention",
    urgency: str = "normal",
    description: str | None = None,
    data: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Primitive: attention.signal.emit

    Emit a Signal entity - something demands attention. Signals are impulses
    that interrupt the current flow. They can be emitted by tools (via emits
    bond), by void detection, or manually when something needs attention.

    Args:
        title: What demands attention
        _ctx: Execution context (MANDATORY in lib/)
        source_id: Entity that emitted this signal (for emits bond)
        signal_type: Category of signal (attention, void, interrupt, invitation)
        urgency: Priority level (low, normal, high, critical)
        description: Optional elaboration
        data: Additional structured data for the signal

    Returns:
        {"status": "success", "id": signal_id, "signal_status": "active"}
    """
    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        # Generate signal ID
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
        signal_id = f"signal-{slug}"

        signal_data = {
            "title": title,
            "description": description or title,
            "status": "active",
            "signal_type": signal_type,
            "urgency": urgency,
            "emitted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

        if source_id:
            signal_data["source_id"] = source_id

        # Merge additional data if provided
        if data:
            signal_data.update(data)

        # Save the signal entity
        entity = GenericEntity(id=signal_id, type="signal", data=signal_data)
        store.save_entity(entity)

        # If emitted from a source, create an emits bond
        if source_id:
            store.save_bond(
                bond_id=f"rel-emits-{source_id}-{signal_id}",
                bond_type="emits",
                from_id=source_id,
                to_id=signal_id,
                status="active",
                confidence=1.0,
                data={},
            )

        return {
            "status": "success",
            "id": signal_id,
            "signal_status": "active",
        }
    except Exception as e:
        return {"status": "error", "error": str(e), "id": None}
    finally:
        if should_close:
            store.close()
