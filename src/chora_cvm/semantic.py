"""
Semantic primitives for the Chora CVM.

These primitives provide embedding computation, similarity search, bond suggestion,
and clustering capabilities. All operations gracefully degrade when chora-inference
is unavailable.

Follows principle-semantic-capability-degrades-gracefully:
"Semantic features enhance but never gate core functionality."

Follows principle-inference-is-optional-dependency:
"chora-cvm MUST NOT import chora-inference at module level."
"""
from __future__ import annotations

import json
import math
import sqlite3
import struct
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .store import EventStore


# =============================================================================
# Helper Functions
# =============================================================================


def get_embedding_provider():
    """
    Dynamically import and return the embedding provider from chora-inference.

    Raises:
        ImportError: If chora-inference is not installed
    """
    from chora_inference.embeddings import get_embedding_provider as _get_provider
    return _get_provider()


def entity_to_semantic_text(entity_type: str, data: dict) -> str:
    """
    Extract semantic text from an entity based on its type.

    Different entity types have different relevant fields for semantic similarity.
    """
    parts = []

    # Title is always relevant
    if "title" in data:
        parts.append(data["title"])

    # Type-specific extraction
    if entity_type == "learning":
        if "insight" in data:
            parts.append(data["insight"])
    elif entity_type == "principle":
        if "statement" in data:
            parts.append(data["statement"])
    elif entity_type == "pattern":
        if "description" in data:
            parts.append(data["description"])
        if "template" in data:
            parts.append(data["template"])
    elif entity_type == "story":
        if "description" in data:
            parts.append(data["description"])
    elif entity_type == "behavior":
        # GWT format
        parts.extend([
            f"Given {data.get('given', '')}",
            f"When {data.get('when', '')}",
            f"Then {data.get('then', '')}",
        ])
    elif entity_type == "tool":
        if "phenomenology" in data:
            parts.append(data["phenomenology"])
        cognition = data.get("cognition", {})
        if cognition.get("ready_at_hand"):
            parts.append(cognition["ready_at_hand"])
    elif entity_type == "inquiry":
        if "question" in data:
            parts.append(data["question"])
        elif "title" in data:
            parts.append(data["title"])
    else:
        # Generic fallback
        if "description" in data:
            parts.append(data["description"])

    return " ".join(filter(None, parts))


def cosine_similarity_bytes(vec1: bytes, vec2: bytes, dimension: int) -> float:
    """
    Compute cosine similarity between two embedding vectors stored as bytes.

    Vectors are assumed to be normalized, so this is just the dot product.
    """
    v1 = struct.unpack(f'{dimension}f', vec1)
    v2 = struct.unpack(f'{dimension}f', vec2)

    dot = sum(a * b for a, b in zip(v1, v2))
    return max(0.0, min(1.0, dot))


# =============================================================================
# Bond Attractors - Which bonds make sense for which entity types
# =============================================================================

BOND_ATTRACTORS = {
    "inquiry": {
        "yields": ["learning"],
    },
    "learning": {
        "surfaces": ["principle"],
        "induces": ["pattern"],
    },
    "principle": {
        "governs": ["pattern"],
        "clarifies": ["story"],
    },
    "pattern": {
        "structures": ["story"],
    },
    "story": {
        "specifies": ["behavior"],
    },
    "behavior": {
        "implements": ["tool"],
    },
    "tool": {
        "verifies": ["behavior"],
        "emits": ["signal"],
    },
    "signal": {
        "triggers": ["focus"],
    },
}


# =============================================================================
# Semantic Primitives
# =============================================================================


def embed_entity(
    db_path: str,
    entity_id: str,
) -> Dict[str, Any]:
    """
    Compute and store an embedding for an entity.

    Follows principle-semantic-capability-degrades-gracefully:
    Returns None embedding if inference unavailable, but never fails.

    Args:
        db_path: Path to the CVM database
        entity_id: ID of the entity to embed

    Returns:
        {
            "entity_id": str,
            "embedding": bytes | None,
            "dimension": int | None,
            "method": "semantic" | "fallback",
            "error": str | None,
        }
    """
    store = EventStore(db_path)

    # Load entity
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT type, data_json FROM entities WHERE id = ?", (entity_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        store.close()
        return {
            "entity_id": entity_id,
            "embedding": None,
            "dimension": None,
            "method": "fallback",
            "error": f"Entity not found: {entity_id}",
        }

    entity_type = row["type"]
    data = json.loads(row["data_json"])

    # Extract semantic text
    text = entity_to_semantic_text(entity_type, data)

    if not text.strip():
        store.close()
        return {
            "entity_id": entity_id,
            "embedding": None,
            "dimension": None,
            "method": "fallback",
            "error": "No semantic text extracted from entity",
        }

    # Try to compute embedding via chora-inference
    try:
        provider = get_embedding_provider()
        embedding_np = provider.embed_text(text)

        # Convert to bytes for storage
        dimension = len(embedding_np)
        embedding_bytes = struct.pack(f'{dimension}f', *embedding_np.tolist())

        # Store in database
        store.save_embedding(
            entity_id=entity_id,
            model_name=provider.model_name,
            vector=embedding_bytes,
            dimension=dimension,
        )
        store.close()

        return {
            "entity_id": entity_id,
            "embedding": embedding_bytes,
            "dimension": dimension,
            "method": "semantic",
        }

    except ImportError:
        # Graceful degradation - inference not available
        store.close()
        return {
            "entity_id": entity_id,
            "embedding": None,
            "dimension": None,
            "method": "fallback",
            "note": "chora-inference not available",
        }


def embed_text(
    db_path: str,
    text: str,
) -> Dict[str, Any]:
    """
    Compute embedding for arbitrary text (not stored).

    Args:
        db_path: Path to the CVM database (for consistency with other primitives)
        text: Text to embed

    Returns:
        {
            "vector": bytes | None,
            "dimension": int | None,
            "method": "semantic" | "fallback",
        }
    """
    try:
        provider = get_embedding_provider()
        embedding_np = provider.embed_text(text)

        dimension = len(embedding_np)
        vector_bytes = struct.pack(f'{dimension}f', *embedding_np.tolist())

        return {
            "vector": vector_bytes,
            "dimension": dimension,
            "method": "semantic",
        }

    except ImportError:
        return {
            "vector": None,
            "dimension": None,
            "method": "fallback",
            "note": "chora-inference not available",
        }


def semantic_similarity(
    db_path: str,
    entity_id_1: str,
    entity_id_2: str,
) -> Dict[str, Any]:
    """
    Compute semantic similarity between two entities.

    Uses stored embeddings if available, otherwise returns 0.0.

    Args:
        db_path: Path to the CVM database
        entity_id_1: First entity ID
        entity_id_2: Second entity ID

    Returns:
        {
            "similarity": float (0.0-1.0),
            "method": "semantic" | "fallback",
        }
    """
    # Same entity - always 1.0
    if entity_id_1 == entity_id_2:
        return {
            "similarity": 1.0,
            "method": "semantic",
        }

    store = EventStore(db_path)

    # Get embeddings
    emb1 = store.get_embedding(entity_id_1)
    emb2 = store.get_embedding(entity_id_2)
    store.close()

    if not emb1 or not emb2:
        return {
            "similarity": 0.0,
            "method": "fallback",
            "note": "Embeddings not found for one or both entities",
        }

    # Compute cosine similarity
    dimension = emb1["dimension"]
    if emb2["dimension"] != dimension:
        return {
            "similarity": 0.0,
            "method": "fallback",
            "note": "Dimension mismatch between embeddings",
        }

    similarity = cosine_similarity_bytes(emb1["vector"], emb2["vector"], dimension)

    return {
        "similarity": similarity,
        "method": "semantic",
    }


def semantic_search(
    db_path: str,
    query: str,
    entity_type: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Search entities by semantic similarity to query.

    Falls back to FTS5 search when inference unavailable.

    Args:
        db_path: Path to the CVM database
        query: Search query
        entity_type: Optional type filter
        limit: Maximum results

    Returns:
        {
            "results": [{"id": str, "type": str, "similarity": float, ...}, ...],
            "method": "semantic" | "fts5",
        }
    """
    store = EventStore(db_path)

    # Try semantic search
    use_fts5 = False
    try:
        provider = get_embedding_provider()
        query_embedding = provider.embed_text(query)
        dimension = len(query_embedding)
        query_bytes = struct.pack(f'{dimension}f', *query_embedding.tolist())

        # Get all embeddings and compute similarity
        all_embeddings = store.get_all_embeddings()

        # If no embeddings exist, fall back to FTS5
        if not all_embeddings:
            store.close()
            use_fts5 = True
        else:
            store.close()
            results = []
            for emb in all_embeddings:
                if emb["dimension"] != dimension:
                    continue

                similarity = cosine_similarity_bytes(query_bytes, emb["vector"], dimension)

                # Get entity info
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.execute("SELECT type, data_json FROM entities WHERE id = ?", (emb["entity_id"],))
                row = cur.fetchone()
                conn.close()

                if row:
                    etype = row["type"]
                    if entity_type and etype != entity_type:
                        continue

                    data = json.loads(row["data_json"])
                    results.append({
                        "id": emb["entity_id"],
                        "type": etype,
                        "title": data.get("title", emb["entity_id"]),
                        "similarity": similarity,
                    })

            # Sort by similarity descending
            results.sort(key=lambda x: x["similarity"], reverse=True)

            return {
                "results": results[:limit],
                "method": "semantic",
            }

    except ImportError:
        store.close()
        use_fts5 = True

    if use_fts5:
        # Fall back to FTS5
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        try:
            if entity_type:
                sql = """
                    SELECT id, type, snippet(entity_fts, 3, '[', ']', '...', 64) AS snippet
                    FROM entity_fts
                    WHERE entity_fts MATCH ? AND type = ?
                    LIMIT ?
                """
                cur = conn.execute(sql, (query, entity_type, limit))
            else:
                sql = """
                    SELECT id, type, snippet(entity_fts, 3, '[', ']', '...', 64) AS snippet
                    FROM entity_fts
                    WHERE entity_fts MATCH ?
                    LIMIT ?
                """
                cur = conn.execute(sql, (query, limit))

            results = [dict(row) for row in cur.fetchall()]
            conn.close()

            return {
                "results": results,
                "method": "fts5",
            }
        except sqlite3.OperationalError:
            conn.close()
            return {
                "results": [],
                "method": "fts5",
                "note": "FTS5 table not available",
            }


def suggest_bonds(
    db_path: str,
    entity_id: str,
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Suggest potential bonds for an entity based on semantic similarity.

    Falls back to type-based suggestions when inference unavailable.

    Args:
        db_path: Path to the CVM database
        entity_id: Entity to find bond suggestions for
        limit: Maximum suggestions

    Returns:
        {
            "candidates": [{"to_id": str, "to_type": str, "bond_type": str, "similarity": float}, ...],
            "method": "semantic" | "type-based",
        }
    """
    store = EventStore(db_path)

    # Get entity info
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT type FROM entities WHERE id = ?", (entity_id,))
    row = cur.fetchone()

    if not row:
        conn.close()
        store.close()
        return {
            "candidates": [],
            "method": "fallback",
            "error": f"Entity not found: {entity_id}",
        }

    entity_type = row["type"]
    conn.close()

    # Get valid bond types for this entity type
    valid_bonds = BOND_ATTRACTORS.get(entity_type, {})

    # Try semantic suggestions
    try:
        provider = get_embedding_provider()

        # Get source embedding
        source_emb = store.get_embedding(entity_id)
        if not source_emb:
            raise ValueError("No embedding for source entity")

        dimension = source_emb["dimension"]

        # Get all embeddings
        all_embeddings = store.get_all_embeddings()
        store.close()

        candidates = []
        for emb in all_embeddings:
            if emb["entity_id"] == entity_id:
                continue
            if emb["dimension"] != dimension:
                continue

            # Get target entity info
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.execute("SELECT type, data_json FROM entities WHERE id = ?", (emb["entity_id"],))
            target_row = cur.fetchone()
            conn.close()

            if not target_row:
                continue

            target_type = target_row["type"]

            # Find valid bond type for this target
            bond_type = None
            for bt, target_types in valid_bonds.items():
                if target_type in target_types:
                    bond_type = bt
                    break

            if not bond_type:
                continue

            similarity = cosine_similarity_bytes(source_emb["vector"], emb["vector"], dimension)

            data = json.loads(target_row["data_json"])
            candidates.append({
                "to_id": emb["entity_id"],
                "to_type": target_type,
                "bond_type": bond_type,
                "similarity": similarity,
                "title": data.get("title", emb["entity_id"]),
            })

        # Sort by similarity
        candidates.sort(key=lambda x: x["similarity"], reverse=True)

        return {
            "candidates": candidates[:limit],
            "method": "semantic",
        }

    except (ImportError, ValueError):
        # Fall back to type-based suggestions
        store.close()

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        candidates = []
        for bond_type, target_types in valid_bonds.items():
            for target_type in target_types:
                cur = conn.execute(
                    "SELECT id, data_json FROM entities WHERE type = ? LIMIT ?",
                    (target_type, limit)
                )
                for target_row in cur.fetchall():
                    data = json.loads(target_row["data_json"])
                    candidates.append({
                        "to_id": target_row["id"],
                        "to_type": target_type,
                        "bond_type": bond_type,
                        "title": data.get("title", target_row["id"]),
                    })

        conn.close()

        return {
            "candidates": candidates[:limit],
            "method": "type-based",
        }


def detect_clusters(
    db_path: str,
    entity_type: str,
    threshold: float = 0.8,
    limit: int = 100,
) -> Dict[str, Any]:
    """
    Detect clusters of similar entities.

    Falls back to keyword-based clustering when inference unavailable.

    Args:
        db_path: Path to the CVM database
        entity_type: Type of entities to cluster
        threshold: Similarity threshold for clustering
        limit: Maximum entities to consider

    Returns:
        {
            "clusters": [{"entities": [str, ...], "centroid": str}, ...],
            "method": "semantic" | "keyword",
        }
    """
    store = EventStore(db_path)

    # Get entities of type
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, data_json FROM entities WHERE type = ? LIMIT ?",
        (entity_type, limit)
    )
    entities = [(row["id"], json.loads(row["data_json"])) for row in cur.fetchall()]
    conn.close()

    if len(entities) <= 1:
        store.close()
        clusters = []
        if entities:
            clusters.append({
                "entities": [entities[0][0]],
                "centroid": entities[0][0],
            })
        return {
            "clusters": clusters,
            "method": "semantic",
        }

    # Try semantic clustering
    try:
        provider = get_embedding_provider()

        # Get embeddings for all entities
        embeddings = {}
        for entity_id, _ in entities:
            emb = store.get_embedding(entity_id)
            if emb:
                embeddings[entity_id] = emb

        store.close()

        if len(embeddings) < 2:
            raise ValueError("Not enough embeddings")

        # Simple greedy clustering
        dimension = next(iter(embeddings.values()))["dimension"]
        remaining = set(embeddings.keys())
        clusters = []

        while remaining:
            # Start new cluster with first remaining entity
            centroid_id = next(iter(remaining))
            remaining.remove(centroid_id)

            cluster = [centroid_id]
            centroid_emb = embeddings[centroid_id]["vector"]

            # Find similar entities
            to_remove = []
            for entity_id in remaining:
                emb = embeddings[entity_id]
                if emb["dimension"] != dimension:
                    continue

                similarity = cosine_similarity_bytes(centroid_emb, emb["vector"], dimension)
                if similarity >= threshold:
                    cluster.append(entity_id)
                    to_remove.append(entity_id)

            for entity_id in to_remove:
                remaining.remove(entity_id)

            clusters.append({
                "entities": cluster,
                "centroid": centroid_id,
            })

        return {
            "clusters": clusters,
            "method": "semantic",
        }

    except (ImportError, ValueError):
        # Fall back to keyword clustering
        store.close()

        # Extract keywords from entities
        entity_keywords = {}
        for entity_id, data in entities:
            text = entity_to_semantic_text(entity_type, data)
            # Simple keyword extraction: split and lowercase
            words = set(w.lower() for w in text.split() if len(w) > 3)
            entity_keywords[entity_id] = words

        # Group by shared keywords
        remaining = set(entity_keywords.keys())
        clusters = []

        while remaining:
            centroid_id = next(iter(remaining))
            remaining.remove(centroid_id)

            centroid_words = entity_keywords[centroid_id]
            cluster = [centroid_id]

            to_remove = []
            for entity_id in remaining:
                entity_words = entity_keywords[entity_id]
                # Check overlap
                overlap = len(centroid_words & entity_words)
                total = len(centroid_words | entity_words)
                if total > 0 and overlap / total >= 0.3:  # Jaccard threshold
                    cluster.append(entity_id)
                    to_remove.append(entity_id)

            for entity_id in to_remove:
                remaining.remove(entity_id)

            clusters.append({
                "entities": cluster,
                "centroid": centroid_id,
            })

        return {
            "clusters": clusters,
            "method": "keyword",
        }


def horizon(
    db_path: str,
    days: int = 7,
    limit: int = 10,
) -> dict:
    """
    Look forward: find unverified tools ranked by semantic proximity to recent learnings.

    The horizon shows what naturally wants attention based on recent insight.
    "We just learned about X, so we should verify tools related to X."

    Args:
        db_path: Path to the database
        days: Look at learnings from the last N days (default 7)
        limit: Maximum number of tools to return

    Returns:
        {
            "recommendations": [
                {"tool_id": str, "similarity": float, "learning_id": str, "reasoning": str},
                ...
            ],
            "recent_learnings": [str, ...],
            "unverified_tools": [str, ...],
            "method": "semantic" | "fallback",
        }
    """
    import sqlite3

    store = EventStore(db_path)

    # Get recent learnings (use rowid for recency since no timestamp column)
    # days parameter becomes "limit to last N*5 learnings" heuristic
    recent_limit = days * 5  # Assume ~5 learnings per day average

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    cur = conn.execute("""
        SELECT id, data_json FROM entities
        WHERE type = 'learning'
        ORDER BY rowid DESC
        LIMIT ?
    """, (recent_limit,))
    recent_learnings = []
    for row in cur.fetchall():
        recent_learnings.append({
            "id": row["id"],
            "data": json.loads(row["data_json"]),
        })

    # Get unverified tools (tools without verifies bonds)
    cur = conn.execute("""
        SELECT e.id, e.data_json FROM entities e
        WHERE e.type = 'tool'
        AND NOT EXISTS (
            SELECT 1 FROM bonds b
            WHERE b.from_id = e.id AND b.type = 'verifies'
        )
    """)
    unverified_tools = []
    for row in cur.fetchall():
        unverified_tools.append({
            "id": row["id"],
            "data": json.loads(row["data_json"]),
        })

    conn.close()

    if not recent_learnings or not unverified_tools:
        store.close()
        return {
            "recommendations": [],
            "recent_learnings": [l["id"] for l in recent_learnings],
            "unverified_tools": [t["id"] for t in unverified_tools],
            "method": "fallback",
            "note": "No recent learnings or no unverified tools",
        }

    # Try semantic ranking
    try:
        provider = get_embedding_provider()

        # Compute average embedding of recent learnings
        learning_embeddings = []
        for learning in recent_learnings:
            emb = store.get_embedding(learning["id"])
            if emb:
                vec = struct.unpack(f'{emb["dimension"]}f', emb["vector"])
                learning_embeddings.append(vec)

        store.close()

        if not learning_embeddings:
            # No embeddings stored, compute them on the fly
            for learning in recent_learnings:
                text = entity_to_semantic_text("learning", learning["data"])
                vec = provider.embed_text(text)
                learning_embeddings.append(vec.tolist())

        # Average the learning embeddings to get "context"
        import numpy as np
        context_vec = np.mean(learning_embeddings, axis=0)
        dimension = len(context_vec)
        context_bytes = struct.pack(f'{dimension}f', *context_vec.tolist())

        # Rank unverified tools by similarity to learning context
        recommendations = []
        for tool in unverified_tools:
            # Get or compute tool embedding
            tool_emb = None
            store2 = EventStore(db_path)
            stored_emb = store2.get_embedding(tool["id"])
            store2.close()

            if stored_emb and stored_emb["dimension"] == dimension:
                tool_bytes = stored_emb["vector"]
            else:
                # Compute on the fly
                text = entity_to_semantic_text("tool", tool["data"])
                tool_vec = provider.embed_text(text)
                tool_bytes = struct.pack(f'{dimension}f', *tool_vec.tolist())

            similarity = cosine_similarity_bytes(context_bytes, tool_bytes, dimension)

            # Find the closest learning for reasoning
            closest_learning = recent_learnings[0]["id"]
            closest_sim = 0
            for i, learning in enumerate(recent_learnings):
                if i < len(learning_embeddings):
                    learn_bytes = struct.pack(f'{dimension}f', *learning_embeddings[i])
                    learn_sim = cosine_similarity_bytes(tool_bytes, learn_bytes, dimension)
                    if learn_sim > closest_sim:
                        closest_sim = learn_sim
                        closest_learning = learning["id"]

            recommendations.append({
                "tool_id": tool["id"],
                "similarity": similarity,
                "learning_id": closest_learning,
                "reasoning": f"Semantically close to '{closest_learning}' ({closest_sim:.0%})",
            })

        # Sort by similarity
        recommendations.sort(key=lambda x: x["similarity"], reverse=True)

        return {
            "recommendations": recommendations[:limit],
            "recent_learnings": [l["id"] for l in recent_learnings],
            "unverified_tools": [t["id"] for t in unverified_tools],
            "method": "semantic",
        }

    except ImportError:
        store.close()
        # Fall back to simple listing
        return {
            "recommendations": [{"tool_id": t["id"], "similarity": 0, "learning_id": "", "reasoning": "Semantic unavailable"} for t in unverified_tools[:limit]],
            "recent_learnings": [l["id"] for l in recent_learnings],
            "unverified_tools": [t["id"] for t in unverified_tools],
            "method": "fallback",
        }
