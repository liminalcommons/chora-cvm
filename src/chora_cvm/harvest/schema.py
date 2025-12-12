"""
Database schema for legacy content harvesting.

Tables:
- repositories: Source package tracking
- documents: One row per file
- chunks: Sections/functions/classes extracted from documents
- chunks_fts: FTS5 virtual table for full-text search
- tags: Flexible tagging (SAP references, feature IDs, etc.)
"""

from __future__ import annotations

import sqlite3
from datetime import datetime


def init_legacy_db(db_path: str) -> sqlite3.Connection:
    """Initialize the legacy content database with full schema."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Enable foreign keys
    cur.execute("PRAGMA foreign_keys = ON")

    # ===========================================================================
    # REPOSITORIES: Track source packages
    # ===========================================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repositories (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            description TEXT,
            priority INTEGER DEFAULT 0,
            total_files INTEGER DEFAULT 0,
            total_size_bytes INTEGER DEFAULT 0,
            last_harvested_at TEXT NOT NULL
        )
    """)

    # ===========================================================================
    # DOCUMENTS: One row per file
    # ===========================================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            repository_id TEXT NOT NULL,
            filename TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            absolute_path TEXT NOT NULL,
            content_type TEXT NOT NULL,
            title TEXT,
            size_bytes INTEGER,
            line_count INTEGER,
            content_hash TEXT,
            is_duplicate_of TEXT,
            metadata_json TEXT,
            harvested_at TEXT NOT NULL,
            FOREIGN KEY (repository_id) REFERENCES repositories(id)
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_repo ON documents(repository_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_type ON documents(content_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_hash ON documents(content_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_duplicate ON documents(is_duplicate_of)")

    # ===========================================================================
    # CHUNKS: Sections of documents
    # ===========================================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL,
            section_title TEXT,
            content TEXT NOT NULL,
            line_start INTEGER,
            line_end INTEGER,
            chunk_type TEXT DEFAULT 'section',
            heading_level INTEGER,
            parent_chunk_id TEXT,
            metadata_json TEXT,
            FOREIGN KEY (document_id) REFERENCES documents(id),
            FOREIGN KEY (parent_chunk_id) REFERENCES chunks(id)
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_document ON chunks(document_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_type ON chunks(chunk_type)")

    # ===========================================================================
    # FTS5: Full-text search
    # ===========================================================================
    try:
        cur.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts
            USING fts5(
                id,
                document_id,
                section_title,
                content,
                chunk_type,
                tokenize='porter unicode61'
            )
        """)
    except sqlite3.OperationalError:
        pass  # FTS5 not available

    # ===========================================================================
    # TAGS: Flexible tagging for content categorization
    # ===========================================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            chunk_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            value TEXT,
            PRIMARY KEY (chunk_id, tag),
            FOREIGN KEY (chunk_id) REFERENCES chunks(id)
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_tags_value ON tags(tag, value)")

    # ===========================================================================
    # CANDIDATES: Entities to potentially promote to CVM Loom
    # ===========================================================================
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

    # ===========================================================================
    # LEGACY ENTITIES: Extracted from old .chora databases
    # ===========================================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS legacy_entities (
            id TEXT PRIMARY KEY,
            source_db TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            title TEXT,
            content TEXT NOT NULL,
            status TEXT,
            created_at TEXT,
            harvested_at TEXT NOT NULL
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_legacy_entities_source ON legacy_entities(source_db)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_legacy_entities_type ON legacy_entities(entity_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_legacy_entities_original ON legacy_entities(entity_id)")

    # FTS for legacy entities
    try:
        cur.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS legacy_entities_fts
            USING fts5(
                id,
                entity_type,
                title,
                content,
                tokenize='porter unicode61'
            )
        """)
    except sqlite3.OperationalError:
        pass  # FTS5 not available

    # ===========================================================================
    # LEGACY RELATIONSHIPS: Bonds between legacy entities
    # ===========================================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS legacy_relationships (
            id TEXT PRIMARY KEY,
            source_db TEXT NOT NULL,
            from_entity_id TEXT NOT NULL,
            to_entity_id TEXT NOT NULL,
            bond_type TEXT NOT NULL,
            metadata_json TEXT,
            harvested_at TEXT NOT NULL
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_legacy_rels_from ON legacy_relationships(from_entity_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_legacy_rels_to ON legacy_relationships(to_entity_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_legacy_rels_type ON legacy_relationships(bond_type)")

    conn.commit()
    return conn


def get_db_stats(conn: sqlite3.Connection) -> dict:
    """Get statistics about the database."""
    cur = conn.cursor()

    stats = {}

    # Repository count
    cur.execute("SELECT COUNT(*) FROM repositories")
    stats["repositories"] = cur.fetchone()[0]

    # Document counts
    cur.execute("SELECT COUNT(*) FROM documents")
    stats["documents"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM documents WHERE is_duplicate_of IS NOT NULL")
    stats["duplicates"] = cur.fetchone()[0]

    # Documents by type
    cur.execute("""
        SELECT content_type, COUNT(*)
        FROM documents
        GROUP BY content_type
    """)
    stats["by_type"] = dict(cur.fetchall())

    # Chunk count
    cur.execute("SELECT COUNT(*) FROM chunks")
    stats["chunks"] = cur.fetchone()[0]

    # Tag count
    cur.execute("SELECT COUNT(DISTINCT tag) FROM tags")
    stats["tag_types"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM tags")
    stats["total_tags"] = cur.fetchone()[0]

    # Legacy entities
    try:
        cur.execute("SELECT COUNT(*) FROM legacy_entities")
        stats["legacy_entities"] = cur.fetchone()[0]

        cur.execute("""
            SELECT entity_type, COUNT(*)
            FROM legacy_entities
            GROUP BY entity_type
        """)
        stats["legacy_by_type"] = dict(cur.fetchall())

        cur.execute("SELECT COUNT(*) FROM legacy_relationships")
        stats["legacy_relationships"] = cur.fetchone()[0]
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        stats["legacy_entities"] = 0
        stats["legacy_by_type"] = {}
        stats["legacy_relationships"] = 0

    return stats
