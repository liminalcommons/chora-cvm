"""
Domain: Cognition (Semantic Operations)
ID Prefix: cognition.*

Vector embedding and similarity operations for semantic reasoning.
These enable the CVM to work with meaning-space operations: finding
similar entities, ranking by relevance, and clustering related concepts.

Primitives:
  - cognition.embed.text: Generate embedding vector from text
  - cognition.vector.sim: Cosine similarity between two vectors
  - cognition.vector.rank: Rank candidates by similarity to query
  - cognition.cluster: Greedy clustering of embeddings
"""
from __future__ import annotations

import struct
from typing import Any, Dict, List

from ..schema import ExecutionContext


def embed_text(
    text: str,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: cognition.embed.text

    Generate an embedding vector for arbitrary text.

    Uses lazy import of chora-inference. Gracefully degrades if unavailable.
    The embedding enables semantic similarity operations across the graph.

    Args:
        text: The text to embed
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "vector": bytes, "dimension": int}
        {"status": "error", "error": str, "vector": None, "dimension": 0}
    """
    try:
        # Lazy import - chora-inference may not be installed
        from chora_inference.embeddings import get_embedding_provider

        provider = get_embedding_provider()
        embedding_np = provider.embed_text(text)
        dimension = len(embedding_np)
        vector_bytes = struct.pack(f'{dimension}f', *embedding_np.tolist())
        return {
            "status": "success",
            "vector": vector_bytes,
            "dimension": dimension,
        }
    except ImportError:
        return {
            "status": "error",
            "error": "inference_unavailable",
            "vector": None,
            "dimension": 0,
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "vector": None,
            "dimension": 0,
        }


def vector_sim(
    vector_a: bytes,
    vector_b: bytes,
    dimension: int,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: cognition.vector.sim

    Compute cosine similarity between two binary vectors.

    Assumes vectors are normalized (L2 norm = 1.0), so cosine similarity
    equals dot product. Pure Python implementation - no external dependencies.

    Args:
        vector_a: First vector as bytes
        vector_b: Second vector as bytes
        dimension: Number of float elements in each vector
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "similarity": float} in range [0.0, 1.0]
        {"status": "error", "error": str} on failure
    """
    try:
        v1 = struct.unpack(f'{dimension}f', vector_a)
        v2 = struct.unpack(f'{dimension}f', vector_b)

        dot = sum(a * b for a, b in zip(v1, v2))
        # Clamp to [0, 1] to handle floating-point errors
        similarity = max(0.0, min(1.0, dot))
        return {"status": "success", "similarity": similarity}
    except struct.error as e:
        return {"status": "error", "error": f"Vector unpack error: {e}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def vector_rank(
    query_vector: bytes,
    candidates: List[Dict[str, Any]],
    dimension: int,
    _ctx: ExecutionContext,
    threshold: float = 0.0,
) -> Dict[str, Any]:
    """
    Primitive: cognition.vector.rank

    Core similarity ranking loop (GPU operation under GPU Doctrine).

    This is a "fat primitive" - keeping matrix operations in Python (the GPU)
    while protocols invoke it as a single step. Ranks candidates by cosine
    similarity to the query vector.

    Args:
        query_vector: Query embedding as binary bytes
        candidates: List of dicts with "id", "vector" (bytes), and optional metadata
        dimension: Vector dimension
        _ctx: Execution context (MANDATORY in lib/)
        threshold: Minimum similarity threshold (default 0.0 = all)

    Returns:
        {"status": "success", "ranked": [...sorted by similarity...], "count": int}
        {"status": "error", "error": str} on failure

    Example:
        vector_rank(query_vec, [
            {"id": "entity-1", "vector": b"...", "title": "Foo"},
            {"id": "entity-2", "vector": b"...", "title": "Bar"},
        ], 1536, _ctx, threshold=0.7)
    """
    try:
        results = []

        for candidate in candidates:
            vec_bytes = candidate.get("vector")
            if not vec_bytes:
                continue

            # Compute cosine similarity (vectors assumed normalized = dot product)
            try:
                vec1 = struct.unpack(f'{dimension}f', query_vector)
                vec2 = struct.unpack(f'{dimension}f', vec_bytes)
                similarity = sum(a * b for a, b in zip(vec1, vec2))
            except struct.error:
                continue

            if similarity >= threshold:
                result = {
                    "id": candidate.get("id", "unknown"),
                    "similarity": similarity,
                }
                # Copy through any additional metadata
                for key, value in candidate.items():
                    if key not in ("id", "vector", "similarity"):
                        result[key] = value
                results.append(result)

        # Sort by similarity descending
        results.sort(key=lambda x: x["similarity"], reverse=True)

        return {
            "status": "success",
            "ranked": results,
            "count": len(results),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def cluster(
    embeddings: Dict[str, Dict[str, Any]],
    dimension: int,
    _ctx: ExecutionContext,
    threshold: float = 0.8,
) -> Dict[str, Any]:
    """
    Primitive: cognition.cluster

    Greedy clustering algorithm (GPU operation under GPU Doctrine).

    Assigns items to clusters based on similarity to cluster centroid.
    This is a "fat primitive" - O(n²) similarity computation stays in Python.

    Args:
        embeddings: Dict mapping entity_id to {"vector": bytes, ...metadata}
        dimension: Vector dimension
        _ctx: Execution context (MANDATORY in lib/)
        threshold: Similarity threshold for cluster membership

    Returns:
        {"status": "success", "clusters": [...], "count": int}
        {"status": "error", "error": str} on failure

    Example:
        cluster({
            "e1": {"vector": b"..."},
            "e2": {"vector": b"..."},
        }, 1536, _ctx, threshold=0.8)
    """
    try:
        if len(embeddings) == 0:
            return {"status": "success", "clusters": [], "count": 0}

        if len(embeddings) == 1:
            only_id = next(iter(embeddings.keys()))
            return {
                "status": "success",
                "clusters": [{"entities": [only_id], "centroid": only_id}],
                "count": 1,
            }

        remaining = set(embeddings.keys())
        clusters = []

        while remaining:
            # Start new cluster with first remaining entity
            centroid_id = next(iter(remaining))
            remaining.remove(centroid_id)

            cluster_entities = [centroid_id]
            centroid_vec = embeddings[centroid_id].get("vector")

            if not centroid_vec:
                clusters.append({"entities": cluster_entities, "centroid": centroid_id})
                continue

            # Find all entities similar enough to join this cluster
            to_add = []
            for entity_id in list(remaining):
                entity_vec = embeddings[entity_id].get("vector")
                if not entity_vec:
                    continue

                # Compute similarity to centroid
                try:
                    v1 = struct.unpack(f'{dimension}f', centroid_vec)
                    v2 = struct.unpack(f'{dimension}f', entity_vec)
                    similarity = sum(a * b for a, b in zip(v1, v2))
                except struct.error:
                    continue

                if similarity >= threshold:
                    to_add.append(entity_id)

            # Add similar entities to cluster
            for entity_id in to_add:
                remaining.remove(entity_id)
                cluster_entities.append(entity_id)

            clusters.append({"entities": cluster_entities, "centroid": centroid_id})

        return {
            "status": "success",
            "clusters": clusters,
            "count": len(clusters),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
