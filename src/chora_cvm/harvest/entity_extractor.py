"""
Entity Extractor: Extract entities from legacy .chora databases.

Extracts entities and relationships from old Chora databases (v2, v3, etc.)
into the harvest database for searchable access to historical decisions and rationale.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Iterator


@dataclass
class ExtractedEntity:
    """An entity extracted from a legacy database."""

    source_db: str
    entity_type: str
    entity_id: str
    title: str
    content: str
    status: str | None
    created_at: str | None


@dataclass
class ExtractedRelationship:
    """A relationship extracted from a legacy database."""

    source_db: str
    from_entity_id: str
    to_entity_id: str
    bond_type: str
    metadata: dict


def format_entity_content(entity_type: str, data: dict, status: str) -> str:
    """Format entity data as searchable text content."""
    parts = []

    # Name/title
    name = data.get("name", "")
    if name:
        parts.append(f"# {name}")

    # Status
    if status:
        parts.append(f"Status: {status}")

    # Type-specific fields
    if entity_type == "story":
        if data.get("cares_for"):
            parts.append(f"\n## Cares For\n{data['cares_for']}")
        if data.get("acceptance_criteria"):
            parts.append(f"\n## Acceptance Criteria\n{data['acceptance_criteria']}")

    elif entity_type == "behavior":
        if data.get("given"):
            parts.append(f"\n## Given\n{data['given']}")
        if data.get("when"):
            parts.append(f"\n## When\n{data['when']}")
        if data.get("then"):
            parts.append(f"\n## Then\n{data['then']}")

    elif entity_type == "tool":
        if data.get("handler"):
            parts.append(f"\nHandler: {data['handler']}")
        if data.get("phenomenology"):
            parts.append(f"\n## Phenomenology\n{data['phenomenology']}")
        if data.get("cognition"):
            cog = data["cognition"]
            if isinstance(cog, dict):
                if cog.get("ready_at_hand"):
                    parts.append(f"\n## Ready-at-Hand\n{cog['ready_at_hand']}")

    elif entity_type == "principle":
        if data.get("statement"):
            parts.append(f"\n## Statement\n{data['statement']}")

    elif entity_type == "pattern":
        if data.get("target"):
            parts.append(f"\nTarget: {data['target']}")
        if data.get("template"):
            parts.append(f"\n## Template\n{data['template']}")

    elif entity_type == "learning":
        if data.get("insight"):
            parts.append(f"\n## Insight\n{data['insight']}")

    elif entity_type == "inquiry":
        if data.get("question"):
            parts.append(f"\n## Question\n{data['question']}")

    # Description (all types)
    description = data.get("description", "")
    if description:
        parts.append(f"\n## Description\n{description}")

    # Metadata
    if data.get("created_by"):
        parts.append(f"\nCreated by: {data['created_by']}")

    return "\n".join(parts)


def extract_entities(source_db_path: str) -> Iterator[ExtractedEntity]:
    """Extract all entities from a legacy database."""
    source_name = Path(source_db_path).name

    conn = sqlite3.connect(source_db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get all non-relationship entities
    cur.execute("""
        SELECT id, type, status, data, created_at
        FROM entities
        WHERE type != 'relationship'
        ORDER BY type, id
    """)

    for row in cur.fetchall():
        try:
            data = json.loads(row["data"]) if row["data"] else {}
        except json.JSONDecodeError:
            data = {}

        title = data.get("name", row["id"])
        content = format_entity_content(row["type"], data, row["status"])

        yield ExtractedEntity(
            source_db=source_name,
            entity_type=row["type"],
            entity_id=row["id"],
            title=title,
            content=content,
            status=row["status"],
            created_at=row["created_at"],
        )

    conn.close()


def extract_relationships(source_db_path: str) -> Iterator[ExtractedRelationship]:
    """Extract all relationships from a legacy database."""
    source_name = Path(source_db_path).name

    conn = sqlite3.connect(source_db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get relationship entities
    cur.execute("""
        SELECT id, data
        FROM entities
        WHERE type = 'relationship'
    """)

    for row in cur.fetchall():
        try:
            data = json.loads(row["data"]) if row["data"] else {}
        except json.JSONDecodeError:
            continue

        from_entity = data.get("from_entity")
        to_entity = data.get("to_entity")
        bond_type = data.get("relationship_type")

        if from_entity and to_entity and bond_type:
            yield ExtractedRelationship(
                source_db=source_name,
                from_entity_id=from_entity,
                to_entity_id=to_entity,
                bond_type=bond_type,
                metadata={
                    k: v
                    for k, v in data.items()
                    if k not in ("from_entity", "to_entity", "relationship_type")
                },
            )

    conn.close()


def harvest_entities_to_db(
    source_db_path: str, target_conn: sqlite3.Connection
) -> dict:
    """
    Harvest entities from a legacy database into the target harvest database.

    Returns statistics about what was harvested.
    """
    source_name = Path(source_db_path).name
    cur = target_conn.cursor()
    now = datetime.now().isoformat()

    # Clear existing entities from this source (re-harvest)
    cur.execute("DELETE FROM legacy_entities WHERE source_db = ?", (source_name,))
    cur.execute("DELETE FROM legacy_entities_fts WHERE id IN (SELECT id FROM legacy_entities WHERE source_db = ?)", (source_name,))
    cur.execute(
        "DELETE FROM legacy_relationships WHERE source_db = ?", (source_name,)
    )

    entity_count = 0
    by_type: dict[str, int] = {}

    # Extract and insert entities
    for entity in extract_entities(source_db_path):
        # Generate unique ID for harvest DB
        harvest_id = f"legacy-{entity.source_db}-{entity.entity_id}"

        cur.execute(
            """
            INSERT INTO legacy_entities
            (id, source_db, entity_type, entity_id, title, content, status, created_at, harvested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                harvest_id,
                entity.source_db,
                entity.entity_type,
                entity.entity_id,
                entity.title,
                entity.content,
                entity.status,
                entity.created_at,
                now,
            ),
        )

        # Index in FTS
        try:
            cur.execute(
                """
                INSERT INTO legacy_entities_fts (id, entity_type, title, content)
                VALUES (?, ?, ?, ?)
                """,
                (harvest_id, entity.entity_type, entity.title, entity.content),
            )
        except sqlite3.OperationalError:
            pass  # FTS not available

        entity_count += 1
        by_type[entity.entity_type] = by_type.get(entity.entity_type, 0) + 1

    # Extract and insert relationships
    rel_count = 0
    rel_by_type: dict[str, int] = {}

    for rel in extract_relationships(source_db_path):
        rel_id = f"legacy-rel-{sha256(f'{rel.source_db}-{rel.from_entity_id}-{rel.to_entity_id}-{rel.bond_type}'.encode()).hexdigest()[:12]}"

        cur.execute(
            """
            INSERT OR IGNORE INTO legacy_relationships
            (id, source_db, from_entity_id, to_entity_id, bond_type, metadata_json, harvested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rel_id,
                rel.source_db,
                rel.from_entity_id,
                rel.to_entity_id,
                rel.bond_type,
                json.dumps(rel.metadata),
                now,
            ),
        )

        rel_count += 1
        rel_by_type[rel.bond_type] = rel_by_type.get(rel.bond_type, 0) + 1

    target_conn.commit()

    return {
        "source_db": source_name,
        "entities": entity_count,
        "by_type": by_type,
        "relationships": rel_count,
        "rel_by_type": rel_by_type,
    }


def search_legacy_entities(
    conn: sqlite3.Connection, query: str, limit: int = 20
) -> list[dict]:
    """Search legacy entities using FTS5."""
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                le.id,
                le.source_db,
                le.entity_type,
                le.entity_id,
                le.title,
                le.status,
                snippet(legacy_entities_fts, 3, '>>>', '<<<', '...', 64) as snippet
            FROM legacy_entities_fts fts
            JOIN legacy_entities le ON le.id = fts.id
            WHERE legacy_entities_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, limit),
        )

        return [dict(row) for row in cur.fetchall()]

    except sqlite3.OperationalError:
        # FTS not available, fall back to LIKE
        cur.execute(
            """
            SELECT
                id, source_db, entity_type, entity_id, title, status,
                substr(content, 1, 200) as snippet
            FROM legacy_entities
            WHERE content LIKE ?
            LIMIT ?
            """,
            (f"%{query}%", limit),
        )

        return [dict(row) for row in cur.fetchall()]
