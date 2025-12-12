from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional, Type

import sqlite3

from .schema import EventRecord, StateEntity

# Type alias for entity save hooks
# Signature: (entity_id, entity_type, data) -> None
EntitySaveHook = Callable[[str, str, dict], None]


class EventStore:
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        # Enable foreign key constraints (required for CASCADE delete)
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._on_entity_saved: list[EntitySaveHook] = []
        self._ensure_schema()

    def add_entity_hook(self, callback: EntitySaveHook) -> None:
        """
        Register a callback to be invoked when an entity is saved.

        The callback receives (entity_id, entity_type, data) after the
        database commit succeeds. This is used by SyncBridge to emit
        changes to the cloud.

        Args:
            callback: Function taking (entity_id, entity_type, data)
        """
        self._on_entity_saved.append(callback)

    def remove_entity_hook(self, callback: EntitySaveHook) -> None:
        """Remove a previously registered entity hook."""
        self._on_entity_saved.remove(callback)

    def _fire_entity_hooks(self, entity_id: str, entity_type: str, data: dict) -> None:
        """Fire all registered entity hooks after a save."""
        for hook in self._on_entity_saved:
            hook(entity_id, entity_type, data)

    @property
    def path(self) -> str:
        return self._path

    def _ensure_schema(self) -> None:
        cur = self._conn.cursor()

        # Core event log
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                clock_actor TEXT NOT NULL,
                clock_seq INTEGER NOT NULL,
                type TEXT NOT NULL,
                op TEXT NOT NULL,
                persona_id TEXT,
                signature TEXT,
                payload_json TEXT NOT NULL
            )
            """
        )

        # State snapshots for running/fulfilled protocols
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS states (
                id TEXT PRIMARY KEY,
                protocol_id TEXT NOT NULL,
                status TEXT NOT NULL,
                data_json TEXT NOT NULL
            )
            """
        )

        # Entity graph (Decemvirate + primitives/protocols/circles/assets)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                data_json TEXT NOT NULL
            )
            """
        )

        # Helpful indexes for common JSON1 paths
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entities_type
            ON entities(type)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entities_circle_id
            ON entities(json_extract(data_json, '$.circle_id'))
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entities_tags
            ON entities(json_extract(data_json, '$.tags'))
            """
        )

        # FTS5 surface for narrative entities (stories, patterns, principles)
        # Columns: id, type, title, body
        try:
            cur.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS entity_fts
                USING fts5(id, type, title, body)
                """
            )
        except sqlite3.OperationalError:
            # FTS5 not available in this SQLite build; search primitives
            # should degrade gracefully when the table is missing.
            pass

        # Bonds projection table (Standing Waves)
        # Each bond is projected state from interaction events.
        # The bond itself is ALSO an entity (relationship-*) in the entities table.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bonds (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                from_id TEXT NOT NULL,
                to_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                confidence REAL NOT NULL DEFAULT 1.0,
                data_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        # Migration: Add confidence column if missing (for existing databases)
        try:
            cur.execute("ALTER TABLE bonds ADD COLUMN confidence REAL NOT NULL DEFAULT 1.0")
        except sqlite3.OperationalError:
            pass  # Column already exists
        # Indices for O(1) graph traversal (constellation queries)
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bonds_from
            ON bonds(from_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bonds_to
            ON bonds(to_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bonds_type
            ON bonds(type)
            """
        )

        # Embeddings table for semantic similarity
        # Each entity has one canonical embedding; invalidated on entity update.
        # Follows principle-embeddings-are-per-entity-truth.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS embeddings (
                entity_id TEXT PRIMARY KEY,
                model_name TEXT NOT NULL,
                vector BLOB NOT NULL,
                dimension INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_embeddings_model
            ON embeddings(model_name)
            """
        )

        # Archive table for composted/decomposed entities
        # Never delete. Always archive. Enables resurrection if needed.
        # Part of the Autophagy/Metabolic layer.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS archive (
                id TEXT PRIMARY KEY,
                original_id TEXT NOT NULL,
                original_type TEXT NOT NULL,
                data_json TEXT NOT NULL,
                archived_at TEXT DEFAULT CURRENT_TIMESTAMP,
                archived_by TEXT,
                reason TEXT,
                learning_id TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_archive_original_id
            ON archive(original_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_archive_original_type
            ON archive(original_type)
            """
        )

        self._conn.commit()

    def append(self, event: EventRecord) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO events (
                id,
                clock_actor,
                clock_seq,
                type,
                op,
                persona_id,
                signature,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, json(?))
            """,
            (
                event.id,
                event.clock.actor,
                event.clock.seq,
                event.type.value,
                event.op.value,
                event.persona_id,
                event.signature,
                json.dumps(event.payload),
            ),
        )
        self._conn.commit()

    def iter_events(self) -> Iterable[EventRecord]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM events ORDER BY clock_seq")
        for row in cur:
            yield EventRecord(
                id=row["id"],
                clock={"actor": row["clock_actor"], "seq": row["clock_seq"]},
                type=row["type"],
                op=row["op"],
                persona_id=row["persona_id"],
                signature=row["signature"],
                payload=json.loads(row["payload_json"]),
            )

    def save_state(self, state: StateEntity) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO states (id, protocol_id, status, data_json)
            VALUES (?, ?, ?, json(?))
            ON CONFLICT(id) DO UPDATE SET
                status=excluded.status,
                data_json=excluded.data_json
            """,
            (
                state.id,
                state.data.protocol_id,
                state.status.value,
                json.dumps(state.data.model_dump()),
            ),
        )
        self._conn.commit()

    def load_state(self, state_id: str) -> Optional[StateEntity]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM states WHERE id = ?", (state_id,))
        row = cur.fetchone()
        if not row:
            return None

        data_dict = json.loads(row["data_json"])
        return StateEntity(id=row["id"], status=row["status"], data=data_dict)

    def save_entity(self, entity: Any) -> None:
        cur = self._conn.cursor()
        data_obj = getattr(entity, "data", {})
        if hasattr(data_obj, "model_dump"):
            data_payload = data_obj.model_dump(by_alias=True)  # type: ignore[call-arg]
        else:
            data_payload = data_obj

        cur.execute(
            """
            INSERT INTO entities (id, type, data_json)
            VALUES (?, ?, json(?))
            ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json
            """,
            (
                entity.id,
                entity.type,
                json.dumps(data_payload),
            ),
        )
        self._conn.commit()

        # Invalidate any stale embedding when entity content changes
        # Follows principle-embeddings-are-per-entity-truth
        self.delete_embedding(entity.id)

        # Fire hooks after successful commit
        self._fire_entity_hooks(entity.id, entity.type, data_payload)

    def save_generic_entity(self, entity_id: str, entity_type: str, data: Dict[str, Any]) -> None:
        """Persist an arbitrary entity payload without imposing a schema."""
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO entities (id, type, data_json)
            VALUES (?, ?, json(?))
            ON CONFLICT(id) DO UPDATE SET
                type=excluded.type,
                data_json=excluded.data_json
            """,
            (
                entity_id,
                entity_type,
                json.dumps(data),
            ),
        )
        self._conn.commit()

        # Invalidate any stale embedding when entity content changes
        # Follows principle-embeddings-are-per-entity-truth
        self.delete_embedding(entity_id)

        # Fire hooks after successful commit
        self._fire_entity_hooks(entity_id, entity_type, data)

    def load_entity(self, entity_id: str, model_cls: Type[Any]) -> Optional[Any]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM entities WHERE id = ?", (entity_id,))
        row = cur.fetchone()
        if not row:
            return None

        data_dict = json.loads(row["data_json"])
        return model_cls(id=row["id"], type=row["type"], data=data_dict)  # type: ignore[arg-type]

    def save_bond(
        self,
        bond_id: str,
        bond_type: str,
        from_id: str,
        to_id: str,
        status: str = "active",
        confidence: float = 1.0,
        data: dict[str, Any] | None = None,
    ) -> None:
        """
        Project a bond into the bonds table.

        This is a Standing Wave - projected state from interaction events.
        The bond is ALSO saved as a relationship entity in the entities table.

        Args:
            bond_id: Unique identifier for the bond
            bond_type: Type of relationship (e.g., surfaces, verifies)
            from_id: Source entity ID
            to_id: Target entity ID
            status: Bond state (forming, active, stressed, dissolved)
            confidence: Epistemic certainty (0.0-1.0, default 1.0)
            data: Additional metadata
        """
        cur = self._conn.cursor()
        data = data or {}

        # Clamp confidence to valid range
        confidence = max(0.0, min(1.0, confidence))

        # Upsert into bonds projection table
        cur.execute(
            """
            INSERT INTO bonds (id, type, from_id, to_id, status, confidence, data_json)
            VALUES (?, ?, ?, ?, ?, ?, json(?))
            ON CONFLICT(id) DO UPDATE SET
                type=excluded.type,
                from_id=excluded.from_id,
                to_id=excluded.to_id,
                status=excluded.status,
                confidence=excluded.confidence,
                data_json=excluded.data_json
            """,
            (bond_id, bond_type, from_id, to_id, status, confidence, json.dumps(data)),
        )

        # Also save as a relationship entity (bonds can be subjects of other bonds)
        entity_data = {
            "title": f"{from_id} --{bond_type}--> {to_id}",
            "bond_type": bond_type,
            "from_id": from_id,
            "to_id": to_id,
            "status": status,
            "confidence": confidence,
            **data,
        }
        cur.execute(
            """
            INSERT INTO entities (id, type, data_json)
            VALUES (?, 'relationship', json(?))
            ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json
            """,
            (bond_id, json.dumps(entity_data)),
        )

        self._conn.commit()

    def get_bond(self, bond_id: str) -> dict[str, Any] | None:
        """Get a single bond by ID."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM bonds WHERE id = ?", (bond_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def update_bond_confidence(
        self,
        bond_id: str,
        confidence: float,
    ) -> dict[str, Any] | None:
        """
        Update the confidence of an existing bond.

        Returns the previous confidence value or None if bond not found.
        """
        cur = self._conn.cursor()

        # Get previous confidence
        cur.execute("SELECT confidence FROM bonds WHERE id = ?", (bond_id,))
        row = cur.fetchone()
        if not row:
            return None

        previous_confidence = row["confidence"]

        # Clamp confidence to valid range
        confidence = max(0.0, min(1.0, confidence))

        # Update bond table
        cur.execute(
            "UPDATE bonds SET confidence = ? WHERE id = ?",
            (confidence, bond_id),
        )

        # Update relationship entity
        cur.execute(
            """
            UPDATE entities
            SET data_json = json_set(data_json, '$.confidence', ?)
            WHERE id = ?
            """,
            (confidence, bond_id),
        )

        self._conn.commit()

        return {"previous_confidence": previous_confidence, "new_confidence": confidence}

    def get_bonds_from(self, entity_id: str) -> list[dict[str, Any]]:
        """Get all bonds originating from an entity."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT * FROM bonds WHERE from_id = ?",
            (entity_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    def get_bonds_to(self, entity_id: str) -> list[dict[str, Any]]:
        """Get all bonds pointing to an entity."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT * FROM bonds WHERE to_id = ?",
            (entity_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    def get_constellation(self, entity_id: str) -> dict[str, Any]:
        """Get the full tension network around an entity."""
        return {
            "entity_id": entity_id,
            "outgoing": self.get_bonds_from(entity_id),
            "incoming": self.get_bonds_to(entity_id),
        }

    # =========================================================================
    # Circle Physics: inhabits queries
    # =========================================================================

    def get_inhabited_circles(self, entity_id: str) -> list[str]:
        """
        Get all circles that an entity inhabits.

        Returns a list of circle IDs that the entity has an 'inhabits' bond to.
        This is used for sync routing decisions.
        """
        cur = self._conn.cursor()
        cur.execute(
            "SELECT to_id FROM bonds WHERE from_id = ? AND type = 'inhabits'",
            (entity_id,),
        )
        return [row["to_id"] for row in cur.fetchall()]

    def get_inhabitants(self, circle_id: str) -> list[dict[str, Any]]:
        """
        Get all entities that inhabit a circle.

        Returns entity records for all entities with 'inhabits' bonds to this circle.
        """
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT e.id, e.type, e.data_json
            FROM entities e
            JOIN bonds b ON e.id = b.from_id
            WHERE b.to_id = ? AND b.type = 'inhabits'
            """,
            (circle_id,),
        )
        return [
            {"id": row["id"], "type": row["type"], "data": json.loads(row["data_json"])}
            for row in cur.fetchall()
        ]

    # =========================================================================
    # Circle Physics: sync_policy queries
    # =========================================================================

    def get_entity(self, entity_id: str) -> dict[str, Any] | None:
        """
        Get an entity by ID.

        Returns dict with id, type, and data fields, or None if not found.
        """
        cur = self._conn.cursor()
        cur.execute("SELECT id, type, data_json FROM entities WHERE id = ?", (entity_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "type": row["type"],
            "data": json.loads(row["data_json"]),
        }

    def is_local_only(self, circle_id: str) -> bool:
        """
        Check if a circle has sync_policy = "local-only".

        Returns True if:
        - sync_policy is "local-only"
        - sync_policy is not specified (default to local-only)
        - circle doesn't exist (safe default)

        Returns False if sync_policy is "cloud".
        """
        entity = self.get_entity(circle_id)
        if not entity:
            return True  # Safe default for non-existent circles

        sync_policy = entity.get("data", {}).get("sync_policy", "local-only")
        return sync_policy != "cloud"

    def get_local_only_circles(self) -> list[str]:
        """
        Get all circles with sync_policy = "local-only" (or unspecified).

        Returns list of circle IDs that should NOT sync to cloud.
        """
        cur = self._conn.cursor()
        # Get all circle entities where sync_policy is NOT 'cloud'
        # This includes circles where sync_policy is 'local-only' or missing
        cur.execute(
            """
            SELECT id FROM entities
            WHERE type = 'circle'
            AND (
                json_extract(data_json, '$.sync_policy') IS NULL
                OR json_extract(data_json, '$.sync_policy') = 'local-only'
            )
            """
        )
        return [row["id"] for row in cur.fetchall()]

    def get_cloud_circles(self) -> list[str]:
        """
        Get all circles with sync_policy = "cloud".

        Returns list of circle IDs that SHOULD sync to cloud.
        """
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT id FROM entities
            WHERE type = 'circle'
            AND json_extract(data_json, '$.sync_policy') = 'cloud'
            """
        )
        return [row["id"] for row in cur.fetchall()]

    # =========================================================================
    # Circle Physics: asset ownership queries
    # =========================================================================

    def get_assets(self, circle_id: str) -> list[dict[str, Any]]:
        """
        Get all assets that belong to a circle.

        Returns entity records for all assets with 'belongs-to' bonds to this circle.
        """
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT e.id, e.type, e.data_json
            FROM entities e
            JOIN bonds b ON e.id = b.from_id
            WHERE b.to_id = ? AND b.type = 'belongs-to'
            """,
            (circle_id,),
        )
        return [
            {"id": row["id"], "type": row["type"], "data": json.loads(row["data_json"])}
            for row in cur.fetchall()
        ]

    def get_owner_circles(self, asset_id: str) -> list[str]:
        """
        Get all circles that own an asset.

        Returns a list of circle IDs that the asset has a 'belongs-to' bond to.
        """
        cur = self._conn.cursor()
        cur.execute(
            "SELECT to_id FROM bonds WHERE from_id = ? AND type = 'belongs-to'",
            (asset_id,),
        )
        return [row["to_id"] for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()

    # =========================================================================
    # Embedding Persistence: semantic similarity support
    # =========================================================================

    def save_embedding(
        self,
        entity_id: str,
        model_name: str,
        vector: bytes,
        dimension: int,
    ) -> None:
        """
        Store an embedding vector for an entity.

        The vector should be serialized as bytes (e.g., numpy.ndarray.tobytes()).
        Follows principle-embeddings-are-per-entity-truth: one embedding per entity.

        Args:
            entity_id: ID of the entity this embedding represents
            model_name: Name of the embedding model (e.g., "text-embedding-3-small")
            vector: Serialized embedding vector as bytes
            dimension: Number of dimensions in the vector
        """
        cur = self._conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cur.execute(
            """
            INSERT INTO embeddings (entity_id, model_name, vector, dimension, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_id) DO UPDATE SET
                model_name=excluded.model_name,
                vector=excluded.vector,
                dimension=excluded.dimension,
                updated_at=excluded.updated_at
            """,
            (entity_id, model_name, vector, dimension, now, now),
        )
        self._conn.commit()

    def get_embedding(self, entity_id: str) -> Dict[str, Any] | None:
        """
        Retrieve the embedding for an entity.

        Returns dict with entity_id, model_name, vector (bytes), dimension,
        created_at, updated_at, or None if no embedding exists.
        """
        cur = self._conn.cursor()
        cur.execute(
            "SELECT * FROM embeddings WHERE entity_id = ?",
            (entity_id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        return {
            "entity_id": row["entity_id"],
            "model_name": row["model_name"],
            "vector": row["vector"],
            "dimension": row["dimension"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def delete_embedding(self, entity_id: str) -> bool:
        """
        Delete the embedding for an entity.

        Called when an entity is updated to invalidate stale embeddings.
        Returns True if an embedding was deleted, False if none existed.
        """
        cur = self._conn.cursor()
        cur.execute("DELETE FROM embeddings WHERE entity_id = ?", (entity_id,))
        deleted = cur.rowcount > 0
        self._conn.commit()
        return deleted

    def has_embedding(self, entity_id: str) -> bool:
        """Check if an entity has a stored embedding."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT 1 FROM embeddings WHERE entity_id = ? LIMIT 1",
            (entity_id,),
        )
        return cur.fetchone() is not None

    def get_all_embeddings(
        self,
        model_name: str | None = None,
        limit: int | None = None,
    ) -> list[Dict[str, Any]]:
        """
        Get all stored embeddings, optionally filtered by model.

        Used for batch operations like clustering.
        """
        cur = self._conn.cursor()
        query = "SELECT * FROM embeddings"
        params: list[Any] = []

        if model_name:
            query += " WHERE model_name = ?"
            params.append(model_name)

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        cur.execute(query, params)
        return [
            {
                "entity_id": row["entity_id"],
                "model_name": row["model_name"],
                "vector": row["vector"],
                "dimension": row["dimension"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in cur.fetchall()
        ]

    # =========================================================================
    # Archive: Metabolic decomposition support (Autophagy)
    # =========================================================================

    def archive_entity(
        self,
        entity_id: str,
        reason: str = "composted",
        archived_by: str | None = None,
        learning_id: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Archive an entity - move it from entities table to archive table.

        Never delete. Always archive. This preserves audit trail and enables resurrection.

        Args:
            entity_id: ID of the entity to archive
            reason: Why this entity was archived (e.g., "orphan", "deprecated", "stale")
            archived_by: Persona or protocol that initiated the archive
            learning_id: ID of the learning created from this decomposition

        Returns:
            Archive record dict if successful, None if entity not found
        """
        cur = self._conn.cursor()

        # Load the entity to archive
        cur.execute("SELECT id, type, data_json FROM entities WHERE id = ?", (entity_id,))
        row = cur.fetchone()
        if not row:
            return None

        import uuid
        archive_id = f"archive-{uuid.uuid4().hex[:8]}"

        # Insert into archive
        cur.execute(
            """
            INSERT INTO archive (id, original_id, original_type, data_json, archived_by, reason, learning_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (archive_id, row["id"], row["type"], row["data_json"], archived_by, reason, learning_id),
        )

        # Remove from entities (the entity is now in archive)
        cur.execute("DELETE FROM entities WHERE id = ?", (entity_id,))

        self._conn.commit()

        return {
            "id": archive_id,
            "original_id": row["id"],
            "original_type": row["type"],
            "archived_by": archived_by,
            "reason": reason,
            "learning_id": learning_id,
        }

    def archive_bond(
        self,
        bond_id: str,
        reason: str = "composted",
        archived_by: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Archive a bond - move it from bonds table to archive table.

        Used when archiving entities that have dangling bonds.

        Args:
            bond_id: ID of the bond to archive
            reason: Why this bond was archived
            archived_by: Persona or protocol that initiated the archive

        Returns:
            Archive record dict if successful, None if bond not found
        """
        cur = self._conn.cursor()

        # Load the bond to archive
        cur.execute("SELECT * FROM bonds WHERE id = ?", (bond_id,))
        row = cur.fetchone()
        if not row:
            return None

        import uuid
        archive_id = f"archive-bond-{uuid.uuid4().hex[:8]}"

        # Insert into archive
        cur.execute(
            """
            INSERT INTO archive (id, original_id, original_type, data_json, archived_by, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (archive_id, row["id"], "bond", json.dumps(dict(row)), archived_by, reason),
        )

        # Remove from bonds
        cur.execute("DELETE FROM bonds WHERE id = ?", (bond_id,))

        self._conn.commit()

        return {
            "id": archive_id,
            "original_id": row["id"],
            "original_type": "bond",
            "archived_by": archived_by,
            "reason": reason,
        }

    def resurrect_entity(self, archive_id: str) -> dict[str, Any] | None:
        """
        Resurrect an archived entity - move it back from archive to entities.

        This restores the entity to active state. A learning about the resurrection
        can be created by the caller if desired.

        Args:
            archive_id: ID of the archive record (not the original entity ID)

        Returns:
            Resurrected entity dict if successful, None if archive record not found
        """
        cur = self._conn.cursor()

        # Load the archive record
        cur.execute(
            "SELECT id, original_id, original_type, data_json FROM archive WHERE id = ?",
            (archive_id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        # Restore to entities table
        cur.execute(
            """
            INSERT INTO entities (id, type, data_json)
            VALUES (?, ?, ?)
            """,
            (row["original_id"], row["original_type"], row["data_json"]),
        )

        # Remove from archive
        cur.execute("DELETE FROM archive WHERE id = ?", (archive_id,))

        self._conn.commit()

        return {
            "id": row["original_id"],
            "type": row["original_type"],
            "data": json.loads(row["data_json"]),
            "resurrected_from": archive_id,
        }

    def get_archived(self, original_id: str | None = None, original_type: str | None = None) -> list[dict[str, Any]]:
        """
        Get archived records, optionally filtered by original ID or type.

        Args:
            original_id: Filter by original entity ID
            original_type: Filter by original entity type

        Returns:
            List of archive records
        """
        cur = self._conn.cursor()
        query = "SELECT * FROM archive WHERE 1=1"
        params: list[Any] = []

        if original_id:
            query += " AND original_id = ?"
            params.append(original_id)

        if original_type:
            query += " AND original_type = ?"
            params.append(original_type)

        query += " ORDER BY archived_at DESC"

        cur.execute(query, params)
        return [
            {
                "id": row["id"],
                "original_id": row["original_id"],
                "original_type": row["original_type"],
                "data": json.loads(row["data_json"]),
                "archived_at": row["archived_at"],
                "archived_by": row["archived_by"],
                "reason": row["reason"],
                "learning_id": row["learning_id"],
            }
            for row in cur.fetchall()
        ]
