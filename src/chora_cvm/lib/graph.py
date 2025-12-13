"""
Domain: Graph Physics
ID Prefix: graph.*

The core vocabulary for entity and bond operations. Everything in Chora
is an entity in a graph connected by bonds. This domain provides the
primitives for CRUD operations and queries on the graph structure.

Primitives:
  - graph.entity.get: Load a single entity by ID
  - graph.entity.create: Manifest a new entity into the graph
  - graph.entity.update: Update fields on an existing entity
  - graph.entity.archive: Archive an entity (soft delete with provenance)
  - graph.bond.manage: Create or update a bond between entities
  - graph.bond.list: List bonds for an entity (constellation)
  - graph.query: Universal query with filters for type, JSON fields, orphans, etc.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

from ..schema import ExecutionContext, GenericEntity, PrimitiveEntity, PrimitiveData, ProtocolEntity, ProtocolData
from ..store import EventStore


# =============================================================================
# Entity Operations
# =============================================================================


def entity_get(
    entity_id: str,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: graph.entity.get

    Load a single entity by ID.

    Args:
        entity_id: ID of the entity to load
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "entity": {...}, "found": True} or
        {"status": "success", "entity": None, "found": False}
    """
    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        entity = store.load_entity(entity_id, GenericEntity)
        if entity:
            return {
                "status": "success",
                "entity": {
                    "id": entity.id,
                    "type": entity.type,
                    "version": entity.version,
                    "status": entity.status,
                    "data": entity.data,
                },
                "found": True,
            }
        return {"status": "success", "entity": None, "found": False}
    finally:
        if should_close:
            store.close()


def entity_create(
    entity_type: str,
    entity_id: str,
    data: Dict[str, Any],
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: graph.entity.create

    Manifest a new entity into the graph.

    Handles special entity types (primitive, protocol) with schema validation.
    All other types are wrapped as GenericEntity.

    Args:
        entity_type: Type of entity (primitive, protocol, tool, story, etc.)
        entity_id: Unique identifier for the entity
        data: Entity data payload
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "id": entity_id, "type": entity_type}
    """
    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        # Apply defaults for specific entity types
        if entity_type == "circle" and "sync_policy" not in data:
            data = {**data, "sync_policy": "local-only"}

        if entity_type == "primitive":
            entity = PrimitiveEntity(id=entity_id, data=PrimitiveData(**data))
        elif entity_type == "protocol":
            entity = ProtocolEntity(id=entity_id, data=ProtocolData(**data))
        else:
            entity = GenericEntity(id=entity_id, type=entity_type, data=data)

        store.save_entity(entity)
        return {"status": "success", "id": entity_id, "type": entity_type}
    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        if should_close:
            store.close()


def entity_update(
    entity_id: str,
    updates: Dict[str, Any],
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: graph.entity.update

    Update fields on an existing entity.

    Merges updates into existing entity data. Does not delete existing fields
    unless explicitly set to None.

    Args:
        entity_id: ID of the entity to update
        updates: Dict of fields to update (merged with existing data)
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "id": entity_id, "updated": True} or
        {"status": "error", "error": "...", "updated": False}
    """
    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        entity = store.load_entity(entity_id, GenericEntity)
        if not entity:
            return {"status": "error", "error": f"Entity not found: {entity_id}", "updated": False}

        # Merge updates into existing data
        entity.data.update(updates)
        store.save_entity(entity)

        return {"status": "success", "id": entity_id, "updated": True}
    finally:
        if should_close:
            store.close()


def entity_archive(
    entity_id: str,
    _ctx: ExecutionContext,
    reason: str = "archived",
    create_learning: bool = True,
) -> Dict[str, Any]:
    """
    Primitive: graph.entity.archive

    Archive an entity (soft delete with provenance).

    Creates a learning entity capturing the archival and optionally
    creates a crystallized-from bond for provenance.

    Args:
        entity_id: ID of the entity to archive
        _ctx: Execution context (MANDATORY in lib/)
        reason: Reason for archival (stored in learning)
        create_learning: Whether to create a learning entity

    Returns:
        {"status": "success", "archived": True, "learning_id": "..."} or
        {"status": "error", "error": "...", "archived": False}
    """
    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        entity = store.load_entity(entity_id, GenericEntity)
        if not entity:
            return {"status": "error", "error": f"Entity not found: {entity_id}", "archived": False}

        # Archive via status update
        entity.status = "archived"
        entity.data["archived_reason"] = reason
        store.save_entity(entity)

        learning_id = None
        if create_learning:
            import uuid
            learning_id = f"learning-archived-{uuid.uuid4().hex[:8]}"
            learning = GenericEntity(
                id=learning_id,
                type="learning",
                data={
                    "title": f"Archived: {entity_id}",
                    "insight": f"Entity '{entity_id}' of type '{entity.type}' was archived. Reason: {reason}",
                    "domain": "archive",
                    "source_entity_id": entity_id,
                    "source_entity_type": entity.type,
                },
            )
            store.save_entity(learning)

        return {
            "status": "success",
            "archived": True,
            "entity_id": entity_id,
            "learning_id": learning_id,
        }
    except Exception as e:
        return {"status": "error", "error": str(e), "archived": False}
    finally:
        if should_close:
            store.close()


# =============================================================================
# Bond Operations
# =============================================================================


# The 12 forces (bond types)
BOND_TYPES = {
    "yields", "surfaces", "induces", "governs", "clarifies",
    "structures", "specifies", "implements", "verifies",
    "emits", "triggers", "crystallized-from",
    "inhabits", "owns",  # Circle bonds
}


def bond_manage(
    bond_type: str,
    from_id: str,
    to_id: str,
    _ctx: ExecutionContext,
    status: str = "active",
    confidence: float = 1.0,
    data: Optional[Dict[str, Any]] = None,
    enforce_physics: bool = True,
) -> Dict[str, Any]:
    """
    Primitive: graph.bond.manage

    Create or update a bond between entities.

    Bonds are the forces that hold the graph together. Each bond type has
    physics constraints (which entity types can connect via which bond type).

    Args:
        bond_type: One of the 12+ forces (yields, surfaces, verifies, etc.)
        from_id: Source entity ID
        to_id: Target entity ID
        _ctx: Execution context (MANDATORY in lib/)
        status: Bond state (forming, active, stressed, dissolved)
        confidence: Epistemic certainty (0.0-1.0)
        data: Additional metadata for the bond
        enforce_physics: If True, validate type constraints

    Returns:
        {"status": "success", "id": bond_id, "bond_type": bond_type, ...} or
        {"status": "error", "error": "..."}
    """
    import re

    if bond_type not in BOND_TYPES:
        return {
            "status": "error",
            "error": f"Invalid bond type: {bond_type}",
            "valid_types": list(BOND_TYPES),
        }

    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        # Verify entities exist
        from_entity = store.load_entity(from_id, GenericEntity)
        to_entity = store.load_entity(to_id, GenericEntity)

        if not from_entity:
            return {"status": "error", "error": f"Entity not found: {from_id}"}
        if not to_entity:
            return {"status": "error", "error": f"Entity not found: {to_id}"}

        # Generate bond ID
        from_slug = re.sub(r"[^a-z0-9]+", "-", from_id.lower()).strip("-")
        to_slug = re.sub(r"[^a-z0-9]+", "-", to_id.lower()).strip("-")
        bond_id = f"rel-{bond_type}-{from_slug}-{to_slug}"

        # Clamp confidence to valid range
        confidence = max(0.0, min(1.0, confidence))

        # Save the bond
        store.save_bond(
            bond_id=bond_id,
            bond_type=bond_type,
            from_id=from_id,
            to_id=to_id,
            status=status,
            confidence=confidence,
            data=data or {},
        )

        return {
            "status": "success",
            "id": bond_id,
            "bond_type": bond_type,
            "from_id": from_id,
            "to_id": to_id,
            "confidence": confidence,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        if should_close:
            store.close()


def bond_list(
    entity_id: str,
    _ctx: ExecutionContext,
    direction: str = "both",
) -> Dict[str, Any]:
    """
    Primitive: graph.bond.list

    List bonds for an entity (its constellation).

    Args:
        entity_id: ID of the entity
        _ctx: Execution context (MANDATORY in lib/)
        direction: "from" (outgoing), "to" (incoming), or "both"

    Returns:
        {"status": "success", "bonds": [...], "count": n}
    """
    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        bonds = []

        if direction in ("from", "both"):
            bonds.extend(store.get_bonds_from(entity_id))

        if direction in ("to", "both"):
            bonds.extend(store.get_bonds_to(entity_id))

        return {
            "status": "success",
            "bonds": bonds,
            "count": len(bonds),
        }
    finally:
        if should_close:
            store.close()


# =============================================================================
# Universal Query
# =============================================================================


def query(
    _ctx: ExecutionContext,
    entity_type: Optional[str] = None,
    status: Optional[str] = None,
    json_conditions: Optional[Dict[str, Any]] = None,
    orphans_only: bool = False,
    has_bond_type: Optional[str] = None,
    missing_bond_type: Optional[str] = None,
    order_by: str = "recent",
    limit: int = 100,
) -> Dict[str, Any]:
    """
    Primitive: graph.query

    Universal query for entities with flexible filtering.

    This consolidates multiple specialized queries (entities_recent,
    entities_orphans, entities_unverified, entities_query_json) into
    a single powerful primitive.

    Args:
        _ctx: Execution context (MANDATORY in lib/)
        entity_type: Filter by entity type (tool, story, principle, etc.)
        status: Filter by entity status (active, archived, etc.)
        json_conditions: Dict of json_path -> expected_value conditions
        orphans_only: If True, only return entities with no bonds
        has_bond_type: Filter to entities that have this bond type outgoing
        missing_bond_type: Filter to entities missing this bond type outgoing
        order_by: "recent" (descending rowid) or "oldest" (ascending rowid)
        limit: Maximum number of entities to return

    Returns:
        {"status": "success", "entities": [...], "ids": [...], "count": n}

    Examples:
        # Get recent learnings
        query(_ctx, entity_type="learning", limit=10)

        # Find orphan entities
        query(_ctx, orphans_only=True)

        # Find unverified tools (tools missing 'verifies' bond)
        query(_ctx, entity_type="tool", missing_bond_type="verifies")

        # Find active signals
        query(_ctx, entity_type="signal", json_conditions={"$.status": "active"})
    """
    store = _ctx.store if _ctx.store else EventStore(_ctx.db_path)
    should_close = _ctx.store is None

    try:
        # Build query
        sql_parts = ["SELECT e.id, e.type, e.data_json FROM entities e"]
        conditions: List[str] = []
        params: List[Any] = []

        # Type filter
        if entity_type:
            conditions.append("e.type = ?")
            params.append(entity_type)

        # Status filter (from data_json since GenericEntity stores status in data)
        if status:
            conditions.append("json_extract(e.data_json, '$.status') = ?")
            params.append(status)

        # JSON conditions
        if json_conditions:
            for path, value in json_conditions.items():
                # Auto-prepend $. if not present
                if not path.startswith("$."):
                    path = f"$.{path}"
                conditions.append(f"json_extract(e.data_json, ?) = ?")
                params.extend([path, value])

        # Orphans filter (no bonds)
        if orphans_only:
            sql_parts.append("LEFT JOIN bonds b1 ON e.id = b1.from_id")
            sql_parts.append("LEFT JOIN bonds b2 ON e.id = b2.to_id")
            conditions.append("b1.id IS NULL AND b2.id IS NULL")
            conditions.append("e.type != 'relationship'")

        # Has bond type filter
        if has_bond_type:
            conditions.append("""
                EXISTS (
                    SELECT 1 FROM bonds b WHERE b.from_id = e.id AND b.type = ?
                )
            """)
            params.append(has_bond_type)

        # Missing bond type filter
        if missing_bond_type:
            conditions.append("""
                NOT EXISTS (
                    SELECT 1 FROM bonds b WHERE b.from_id = e.id AND b.type = ?
                )
            """)
            params.append(missing_bond_type)

        # Combine conditions
        if conditions:
            sql_parts.append("WHERE " + " AND ".join(conditions))

        # Order
        if order_by == "recent":
            sql_parts.append("ORDER BY e.rowid DESC")
        else:
            sql_parts.append("ORDER BY e.rowid ASC")

        # Limit
        sql_parts.append("LIMIT ?")
        params.append(limit)

        sql = " ".join(sql_parts)

        # Execute
        conn = sqlite3.connect(_ctx.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(sql, params)
            entities = []
            for row in cur.fetchall():
                entities.append({
                    "id": row["id"],
                    "type": row["type"],
                    "data": json.loads(row["data_json"]),
                })

            return {
                "status": "success",
                "entities": entities,
                "ids": [e["id"] for e in entities],
                "count": len(entities),
            }
        finally:
            conn.close()
    finally:
        if should_close:
            store.close()


# =============================================================================
# Database Sensing
# =============================================================================


def db_sense(
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: graph.db.sense

    Sense database health for orientation on arrival.

    Returns a structured summary of database state including entity counts
    by type, bond statistics, and temporal information.

    Args:
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {
            "entity_counts": {"learning": 5, "tool": 10, ...},
            "total_bonds": 42,
            "orphan_bonds": 2,
            "last_modified": "2025-01-01T12:00:00"
        }
    """
    conn = sqlite3.connect(_ctx.db_path)
    conn.row_factory = sqlite3.Row

    try:
        # Entity counts by type
        entity_counts: Dict[str, int] = {}
        cur = conn.execute("SELECT type, COUNT(*) as cnt FROM entities GROUP BY type")
        for row in cur.fetchall():
            entity_counts[row["type"]] = row["cnt"]

        # Total bonds
        cur = conn.execute("SELECT COUNT(*) as cnt FROM bonds")
        total_bonds = cur.fetchone()["cnt"]

        # Orphan bonds (bonds where from_id or to_id doesn't exist in entities)
        cur = conn.execute("""
            SELECT COUNT(*) as cnt FROM bonds b
            WHERE NOT EXISTS (SELECT 1 FROM entities e WHERE e.id = b.from_id)
               OR NOT EXISTS (SELECT 1 FROM entities e WHERE e.id = b.to_id)
        """)
        orphan_bonds = cur.fetchone()["cnt"]

        # Last modified (most recent entity update)
        cur = conn.execute("""
            SELECT MAX(json_extract(data_json, '$.updated_at')) as last_mod
            FROM entities
        """)
        row = cur.fetchone()
        last_modified = row["last_mod"] if row and row["last_mod"] else None

        # Fallback: if no updated_at, try to get from max rowid timestamp
        if last_modified is None:
            cur = conn.execute("SELECT MAX(rowid) as max_id FROM entities")
            row = cur.fetchone()
            if row and row["max_id"]:
                # Use current timestamp as proxy
                from datetime import datetime
                last_modified = datetime.now().isoformat()

        return {
            "entity_counts": entity_counts,
            "total_bonds": total_bonds,
            "orphan_bonds": orphan_bonds,
            "last_modified": last_modified,
        }
    finally:
        conn.close()
