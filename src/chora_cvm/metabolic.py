"""
Metabolic Operations — Autophagy for System Health.

Dead branches, stale signals, and deprecated entities are not waste — they are compost.
The system digests entropy and radiates learnings.
Every decomposition creates provenance via crystallized-from bonds.

Based on patterns from archive/v3/chora-store/src/chora_store/metabolism.py

NOTE: Several functions have been protocolized and archived:
- sense_entropy → protocol-sense-entropy (via cvm entropy)
- digest → protocol-digest (via cvm digest)
- induce → protocol-induce (via cvm induce)
See archive/v5/metabolic/protocolized.py for the original implementations.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

from .schema import GenericEntity
from .store import EventStore
from .std import manage_bond, manifest_entity


# =============================================================================
# Helper — TTL Thresholds
# =============================================================================


def _load_ttl_thresholds(store: EventStore) -> dict[str, int]:
    """Load TTL thresholds from principle entities."""
    thresholds = {}
    cur = store._conn.cursor()

    cur.execute(
        """
        SELECT id, data_json FROM entities
        WHERE type = 'principle'
        AND json_extract(data_json, '$.category') = 'metabolic-threshold'
        """
    )

    for row in cur.fetchall():
        data = json.loads(row["data_json"])
        entity_type = data.get("entity_type")
        ttl = data.get("ttl_days")
        if entity_type and ttl:
            thresholds[entity_type] = ttl

    return thresholds


# =============================================================================
# compost — Archive orphan entity
# =============================================================================


def compost(db_path: str, entity_id: str, force: bool = False) -> dict[str, Any]:
    """
    Archive an orphan entity, creating a learning about the decomposition.

    Handles dangling bonds by archiving them first.
    Refuses to compost entities with active bonds unless force=True.

    Args:
        db_path: Path to the database
        entity_id: ID of the entity to compost
        force: If True, compost even if entity has bonds (archive bonds first)

    Returns:
        Dict with archived=True/False, learning_id, bonds_archived count.
    """
    store = EventStore(db_path)

    entity = store.load_entity(entity_id, GenericEntity)
    if not entity:
        store.close()
        return {"error": f"Entity not found: {entity_id}"}

    # Check for bonds
    bonds_from = store.get_bonds_from(entity_id)
    bonds_to = store.get_bonds_to(entity_id)

    # Separate dangling bonds (target entity doesn't exist) from active bonds
    dangling_bonds = []
    active_bonds = []

    for bond in bonds_from:
        target_id = bond.get("to_id")
        target = store.load_entity(target_id, GenericEntity)
        if target is None:
            dangling_bonds.append(bond)
        else:
            active_bonds.append(bond)

    for bond in bonds_to:
        source_id = bond.get("from_id")
        source = store.load_entity(source_id, GenericEntity)
        if source is None:
            dangling_bonds.append(bond)
        else:
            active_bonds.append(bond)

    # If there are active (non-dangling) bonds and force is False, refuse
    if len(active_bonds) > 0 and not force:
        store.close()
        return {
            "error": "Entity has active bonds; use digest or force",
            "bond_count": len(active_bonds),
        }

    # Archive dangling bonds first
    bonds_archived = 0
    for bond in dangling_bonds:
        result = store.archive_bond(bond["id"], reason=f"dangling bond composted with {entity_id}")
        if result:
            bonds_archived += 1

    # Archive active bonds if force=True
    if force:
        for bond in active_bonds:
            result = store.archive_bond(bond["id"], reason=f"forced compost with {entity_id}")
            if result:
                bonds_archived += 1

    total_bonds = len(dangling_bonds) + len(active_bonds)

    # Create learning about the decomposition
    learning_id = f"learning-composted-{entity.type}-{uuid.uuid4().hex[:8]}"
    learning_data = {
        "title": f"Composted {entity.type}: {entity_id}",
        "insight": f"Entity '{entity.data.get('title', entity_id)}' was composted. It had {total_bonds} bonds that were archived.",
        "domain": "metabolism",
        "composted_type": entity.type,
        "composted_id": entity_id,
        "bonds_archived": bonds_archived,
        "composted_at": datetime.now(timezone.utc).isoformat(),
    }

    manifest_entity(db_path, "learning", learning_id, learning_data)

    # Archive the entity
    archive_result = store.archive_entity(
        entity_id,
        reason="composted",
        archived_by="metabolic.compost",
        learning_id=learning_id,
    )

    store.close()

    return {
        "archived": archive_result is not None,
        "archive_id": archive_result["id"] if archive_result else None,
        "learning_id": learning_id,
        "bonds_archived": bonds_archived,
    }


# =============================================================================
# detect_stagnation — Find stagnant entities and emit signals
# =============================================================================


def detect_stagnation(db_path: str) -> dict[str, Any]:
    """
    Detect stagnant entities based on TTL thresholds and emit signals.

    This is called by the pulse to detect stagnation conditions.

    Returns:
        Dict with signals_emitted list containing emitted signal info.
    """
    store = EventStore(db_path)
    thresholds = _load_ttl_thresholds(store)
    signals_emitted = []

    cur = store._conn.cursor()

    # Check each threshold
    for entity_type, ttl_days in thresholds.items():
        cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()

        # Find stagnant entities of this type
        cur.execute(
            """
            SELECT id, data_json FROM entities
            WHERE type = ?
            AND json_extract(data_json, '$.created_at') < ?
            AND json_extract(data_json, '$.status') != 'resolved'
            AND json_extract(data_json, '$.status') != 'completed'
            AND json_extract(data_json, '$.status') != 'digested'
            """,
            (entity_type, cutoff),
        )

        for row in cur.fetchall():
            entity_id = row[0]
            data = json.loads(row[1])

            # For signals that are stagnant, emit escalation
            # For other entity types, emit stagnation-detected
            if entity_type == "signal":
                signal_id = f"signal-escalation-{uuid.uuid4().hex[:8]}"
                manifest_entity(
                    db_path,
                    "signal",
                    signal_id,
                    {
                        "title": f"Escalation: {entity_id} is stuck",
                        "status": "active",
                        "signal_type": "escalation",
                        "category": "stagnation",
                        "escalates": entity_id,
                        "tracks_entity_id": entity_id,
                        "entity_type": entity_type,
                        "ttl_days": ttl_days,
                    },
                )
                signals_emitted.append({
                    "id": signal_id,
                    "signal_type": "escalation",
                    "escalates": entity_id,
                    "tracks_entity_id": entity_id,
                    "entity_type": entity_type,
                    "category": "stagnation",
                })
            else:
                signal_id = f"signal-stagnant-{entity_type}-{uuid.uuid4().hex[:8]}"
                manifest_entity(
                    db_path,
                    "signal",
                    signal_id,
                    {
                        "title": f"Stagnation detected: {entity_id}",
                        "status": "active",
                        "signal_type": "stagnation-detected",
                        "category": "stagnation",
                        "tracks_entity_id": entity_id,
                        "entity_type": entity_type,
                        "ttl_days": ttl_days,
                    },
                )
                signals_emitted.append({
                    "id": signal_id,
                    "tracks_entity_id": entity_id,
                    "entity_type": entity_type,
                    "category": "stagnation",
                })

    store.close()
    return {"signals_emitted": signals_emitted}


# =============================================================================
# check_void_resolution — Self-healing signal auto-resolution
# =============================================================================


def check_void_resolution(db_path: str) -> dict[str, Any]:
    """
    Check if void conditions have cleared and auto-resolve signals.

    Implements the self-healing pattern from v4 voids.py:
    - void_detected → signal_created (status: active)
    - void_persists → signal_escalates (status: escalated)
    - void_disappears → signal_auto_resolves (status: resolved)

    Returns:
        Dict with resolved_signals list.
    """
    store = EventStore(db_path)
    resolved_signals = []

    cur = store._conn.cursor()

    # Find signals that track voids
    cur.execute(
        """
        SELECT id, data_json FROM entities
        WHERE type = 'signal'
        AND json_extract(data_json, '$.status') = 'active'
        AND json_extract(data_json, '$.signal_type') IN ('orphan-detected', 'stagnation-detected')
        """
    )

    for row in cur.fetchall():
        signal_id = row[0]
        signal_data = json.loads(row[1])
        tracked_id = signal_data.get("tracks_entity_id")

        if not tracked_id:
            continue

        void_cleared = False

        # Check if orphan void cleared (entity now has bonds)
        if signal_data.get("signal_type") == "orphan-detected":
            bonds_from = store.get_bonds_from(tracked_id)
            bonds_to = store.get_bonds_to(tracked_id)
            if bonds_from or bonds_to:
                void_cleared = True

        # Check if stagnation void cleared (entity updated recently)
        if signal_data.get("signal_type") == "stagnation-detected":
            entity = store.load_entity(tracked_id, GenericEntity)
            if entity:
                updated_at = entity.data.get("updated_at")
                if updated_at:
                    ttl_days = signal_data.get("ttl_days", 30)
                    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()
                    if updated_at > cutoff:
                        void_cleared = True

        # Auto-resolve if void cleared
        if void_cleared:
            signal_entity = store.load_entity(signal_id, GenericEntity)
            if signal_entity:
                signal_entity.data["status"] = "resolved"
                signal_entity.data["resolution"] = "auto-resolved: void cleared"
                signal_entity.data["resolved_at"] = datetime.now(timezone.utc).isoformat()
                store.save_entity(signal_entity)
                resolved_signals.append(signal_id)

    store.close()
    return {"resolved_signals": resolved_signals}
