"""
Genesis Protocols: Wave 2 Protocolization.

This module defines the Graph Protocols that replace Python business logic
in metabolic.py and api.py. The goal is to "hollow out" Python and express
behavior as inspectable, evolvable protocol entities.

Protocols defined:
- protocol-orient: Dashboard showing system state
- protocol-digest: Transform entity into learning
- protocol-induce: Propose pattern from clustered learnings
- protocol-sense-entropy: Report metabolic health
"""

from __future__ import annotations

import sys
from pathlib import Path

from chora_cvm.schema import (
    EdgeCondition,
    ConditionOp,
    PrimitiveData,
    PrimitiveEntity,
    ProtocolData,
    ProtocolEntity,
    ProtocolGraph,
    ProtocolInterface,
    ProtocolNode,
    ProtocolNodeKind,
    ProtocolEdge,
)
from chora_cvm.store import EventStore


def bootstrap_protocol_orient(store: EventStore, verbose: bool = True) -> str:
    """Bootstrap protocol-orient: Dashboard showing system state."""
    if verbose:
        print("[*] Bootstrapping protocol-orient...")

    # protocol-orient: The simplest protocol
    # Flow: count_by_type → query_focuses → recent_signals → recent_learnings → RETURN
    proto_orient = ProtocolEntity(
        id="protocol-orient",
        data=ProtocolData(
            title="Orient",
            description="Dashboard showing system state: entity counts, active focuses, recent signals/learnings",
            interface=ProtocolInterface(
                inputs={
                    "db_path": {"type": "string"},
                },
                outputs={
                    "entity_counts": {"type": "object"},
                    "total_entities": {"type": "integer"},
                    "active_focuses": {"type": "array"},
                    "recent_signals": {"type": "array"},
                    "recent_learnings": {"type": "array"},
                },
            ),
            graph=ProtocolGraph(
                start="node_count_by_type",
                nodes={
                    # Step 1: Count entities by type
                    "node_count_by_type": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.count_by_type",
                        inputs={
                            "db_path": "$.inputs.db_path",
                        },
                    ),
                    # Step 2: Query active focuses
                    "node_query_focuses": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.json",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_type": "focus",
                            "json_conditions": {"status": "active"},
                            "limit": 10,
                        },
                    ),
                    # Step 3: Get recent signals
                    "node_recent_signals": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.recent",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_type": "signal",
                            "limit": 5,
                        },
                    ),
                    # Step 4: Get recent learnings
                    "node_recent_learnings": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.recent",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_type": "learning",
                            "limit": 5,
                        },
                    ),
                    # Return assembled result
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "entity_counts": "$.node_count_by_type.counts",
                            "total_entities": "$.node_count_by_type.total",
                            "active_focuses": "$.node_query_focuses.entities",
                            "recent_signals": "$.node_recent_signals.entities",
                            "recent_learnings": "$.node_recent_learnings.entities",
                        },
                    ),
                },
                edges=[
                    ProtocolEdge(**{"from": "node_count_by_type", "to": "node_query_focuses"}),
                    ProtocolEdge(**{"from": "node_query_focuses", "to": "node_recent_signals"}),
                    ProtocolEdge(**{"from": "node_recent_signals", "to": "node_recent_learnings"}),
                    ProtocolEdge(**{"from": "node_recent_learnings", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto_orient)
    if verbose:
        print(f"    + Protocol: {proto_orient.id}")
    return proto_orient.id


def bootstrap_protocol_digest(store: EventStore, verbose: bool = True) -> str:
    """Bootstrap protocol-digest: Transform entity into learning."""
    if verbose:
        print("[*] Bootstrapping protocol-digest...")

    # protocol-digest: Moderate complexity
    # Flow: load_entity → check_found → extract_wisdom → gen_uuid → format_id →
    #       manifest_learning → create_bond → update_source → RETURN
    proto_digest = ProtocolEntity(
        id="protocol-digest",
        data=ProtocolData(
            title="Digest",
            description="Transform an entity into a learning, preserving wisdom",
            interface=ProtocolInterface(
                inputs={
                    "db_path": {"type": "string"},
                    "entity_id": {"type": "string"},
                },
                outputs={
                    "learning_id": {"type": "string"},
                    "bond_id": {"type": "string"},
                    "wisdom": {"type": "object"},
                    "error": {"type": "string"},
                },
            ),
            graph=ProtocolGraph(
                start="node_load_entity",
                nodes={
                    # Step 1: Load the entity
                    "node_load_entity": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.get",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_id": "$.inputs.entity_id",
                        },
                    ),
                    # Step 2a: Return error if not found
                    "node_error_not_found": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "error": "Entity not found",
                        },
                    ),
                    # Step 2b: Extract wisdom from entity
                    "node_extract_wisdom": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="cognition.wisdom.extract",
                        inputs={
                            "entity": "$.node_load_entity.entity",
                        },
                    ),
                    # Step 3: Generate UUID for learning
                    "node_gen_uuid": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="sys.uuid.short",
                        inputs={},
                    ),
                    # Step 4: Get entity type for ID formatting
                    "node_get_type": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.json.get",
                        inputs={
                            "data": "$.node_load_entity.entity",
                            "path": "type",
                            "default": "entity",
                        },
                    ),
                    # Step 5: Format learning ID
                    "node_format_id": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.string.format",
                        inputs={
                            "template": "learning-digested-{type}-{uuid}",
                            "values": {
                                "type": "$.node_get_type.value",
                                "uuid": "$.node_gen_uuid.uuid",
                            },
                        },
                    ),
                    # Step 6: Get current timestamp
                    "node_timestamp": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="chronos.now",
                        inputs={},
                    ),
                    # Step 7: Manifest learning entity
                    "node_manifest_learning": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.create",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_type": "learning",
                            "entity_id": "$.node_format_id.result",
                            "data": {
                                "title": "Digested from {$.inputs.entity_id}",
                                "insight": "$.node_extract_wisdom.insight",
                                "domain": "$.node_extract_wisdom.domain",
                                "source_type": "$.node_get_type.value",
                                "source_id": "$.inputs.entity_id",
                                "digested_at": "$.node_timestamp.timestamp",
                            },
                        },
                    ),
                    # Step 8: Create crystallized-from bond
                    "node_create_bond": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.bond.manage",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "bond_type": "crystallized-from",
                            "from_id": "$.node_format_id.result",
                            "to_id": "$.inputs.entity_id",
                        },
                    ),
                    # Step 9: Update source entity status
                    "node_update_source": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.update",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_id": "$.inputs.entity_id",
                            "updates": {
                                "status": "digested",
                                "digested_at": "$.node_timestamp.timestamp",
                            },
                        },
                    ),
                    # Return success
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "learning_id": "$.node_manifest_learning.id",
                            "bond_id": "$.node_create_bond.id",
                            "wisdom": {
                                "insight": "$.node_extract_wisdom.insight",
                                "domain": "$.node_extract_wisdom.domain",
                            },
                        },
                    ),
                },
                edges=[
                    # Conditional branch: found or not found
                    ProtocolEdge(
                        **{"from": "node_load_entity", "to": "node_error_not_found"},
                        condition=EdgeCondition(
                            op=ConditionOp.EQ,
                            path="$.node_load_entity.found",
                            value=False,
                        ),
                    ),
                    ProtocolEdge(
                        **{"from": "node_load_entity", "to": "node_extract_wisdom"},
                        default=True,
                    ),
                    # Linear flow after extraction
                    ProtocolEdge(**{"from": "node_extract_wisdom", "to": "node_gen_uuid"}),
                    ProtocolEdge(**{"from": "node_gen_uuid", "to": "node_get_type"}),
                    ProtocolEdge(**{"from": "node_get_type", "to": "node_format_id"}),
                    ProtocolEdge(**{"from": "node_format_id", "to": "node_timestamp"}),
                    ProtocolEdge(**{"from": "node_timestamp", "to": "node_manifest_learning"}),
                    ProtocolEdge(**{"from": "node_manifest_learning", "to": "node_create_bond"}),
                    ProtocolEdge(**{"from": "node_create_bond", "to": "node_update_source"}),
                    ProtocolEdge(**{"from": "node_update_source", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto_digest)
    if verbose:
        print(f"    + Protocol: {proto_digest.id}")
    return proto_digest.id


def bootstrap_protocol_induce(store: EventStore, verbose: bool = True) -> str:
    """Bootstrap protocol-induce: Propose pattern from clustered learnings."""
    if verbose:
        print("[*] Bootstrapping protocol-induce...")

    # protocol-induce: Complex - involves iteration
    # Flow: count → check_min → load_batch → extract_domains → find_mode →
    #       gen_uuid → format_id → manifest_pattern → create_bonds → emit_signal → RETURN
    proto_induce = ProtocolEntity(
        id="protocol-induce",
        data=ProtocolData(
            title="Induce",
            description="Propose a pattern from clustered learnings via pattern induction",
            interface=ProtocolInterface(
                inputs={
                    "db_path": {"type": "string"},
                    "learning_ids": {"type": "array", "items": {"type": "string"}},
                },
                outputs={
                    "pattern_id": {"type": "string"},
                    "crystallized_from_count": {"type": "integer"},
                    "review_signal_id": {"type": "string"},
                    "domain": {"type": "string"},
                    "error": {"type": "string"},
                },
            ),
            graph=ProtocolGraph(
                start="node_count_learnings",
                nodes={
                    # Step 1: Count input learnings
                    "node_count_learnings": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.list.length",
                        inputs={
                            "items": "$.inputs.learning_ids",
                        },
                    ),
                    # Step 2a: Error if less than 3
                    "node_error_min": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "error": "Minimum 3 learnings required for pattern induction",
                        },
                    ),
                    # Step 2b: Load all learnings
                    "node_load_batch": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.get_batch",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_ids": "$.inputs.learning_ids",
                        },
                    ),
                    # Step 3: Extract domains from learnings (nested in data.domain)
                    "node_extract_domains": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.list.map",
                        inputs={
                            "items": "$.node_load_batch.entities",
                            "key": "data.domain",
                        },
                    ),
                    # Step 4: Find common domain (mode)
                    "node_find_mode": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.list.mode",
                        inputs={
                            "items": "$.node_extract_domains.values",
                        },
                    ),
                    # Step 5: Generate UUID
                    "node_gen_uuid": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="sys.uuid.short",
                        inputs={},
                    ),
                    # Step 6: Format pattern ID
                    "node_format_id": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.string.format",
                        inputs={
                            "template": "pattern-induced-{domain}-{uuid}",
                            "values": {
                                "domain": "$.node_find_mode.value",
                                "uuid": "$.node_gen_uuid.uuid",
                            },
                        },
                    ),
                    # Step 7: Get timestamp
                    "node_timestamp": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="chronos.now",
                        inputs={},
                    ),
                    # Step 8: Extract first 3 insights for context
                    "node_slice_insights": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.list.slice",
                        inputs={
                            "items": "$.node_load_batch.entities",
                            "start": 0,
                            "end": 3,
                        },
                    ),
                    # Step 9: Extract insight field from sliced (nested in data.insight)
                    "node_extract_insights": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.list.map",
                        inputs={
                            "items": "$.node_slice_insights.items",
                            "key": "data.insight",
                        },
                    ),
                    # Step 10: Join insights for context
                    "node_join_insights": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.string.join",
                        inputs={
                            "items": "$.node_extract_insights.values",
                            "separator": "; ",
                        },
                    ),
                    # Step 11: Manifest pattern entity
                    "node_manifest_pattern": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.create",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_type": "pattern",
                            "entity_id": "$.node_format_id.result",
                            "data": {
                                "title": "Induced pattern from {$.node_count_learnings.length} learnings ({$.node_find_mode.value})",
                                "status": "proposed",
                                "domain": "$.node_find_mode.value",
                                "source_learnings": "$.inputs.learning_ids",
                                "problem": "Common pattern emerged from {$.node_count_learnings.length} learnings",
                                "solution": "Pattern to be refined by human review",
                                "context": "$.node_join_insights.result",
                                "induced_at": "$.node_timestamp.timestamp",
                            },
                        },
                    ),
                    # Step 12: Create bonds to all source learnings
                    "node_create_bonds": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.bond.for_each",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "bond_type": "crystallized-from",
                            "from_id": "$.node_format_id.result",
                            "to_ids": "$.inputs.learning_ids",
                        },
                    ),
                    # Step 13: Emit review signal
                    "node_emit_signal": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="attention.signal.emit",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "title": "Pattern proposed for review: {$.node_format_id.result}",
                            "signal_type": "review-request",
                            "description": "Pattern induction completed - review needed",
                            "data": {
                                "pattern_id": "$.node_format_id.result",
                                "learning_count": "$.node_count_learnings.length",
                                "domain": "$.node_find_mode.value",
                            },
                        },
                    ),
                    # Return success
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "pattern_id": "$.node_manifest_pattern.id",
                            "crystallized_from_count": "$.node_create_bonds.bonds_created",
                            "review_signal_id": "$.node_emit_signal.id",
                            "domain": "$.node_find_mode.value",
                        },
                    ),
                },
                edges=[
                    # Conditional: min 3 learnings
                    ProtocolEdge(
                        **{"from": "node_count_learnings", "to": "node_error_min"},
                        condition=EdgeCondition(
                            op=ConditionOp.LT,
                            path="$.node_count_learnings.length",
                            value=3,
                        ),
                    ),
                    ProtocolEdge(
                        **{"from": "node_count_learnings", "to": "node_load_batch"},
                        default=True,
                    ),
                    # Linear flow
                    ProtocolEdge(**{"from": "node_load_batch", "to": "node_extract_domains"}),
                    ProtocolEdge(**{"from": "node_extract_domains", "to": "node_find_mode"}),
                    ProtocolEdge(**{"from": "node_find_mode", "to": "node_gen_uuid"}),
                    ProtocolEdge(**{"from": "node_gen_uuid", "to": "node_format_id"}),
                    ProtocolEdge(**{"from": "node_format_id", "to": "node_timestamp"}),
                    ProtocolEdge(**{"from": "node_timestamp", "to": "node_slice_insights"}),
                    ProtocolEdge(**{"from": "node_slice_insights", "to": "node_extract_insights"}),
                    ProtocolEdge(**{"from": "node_extract_insights", "to": "node_join_insights"}),
                    ProtocolEdge(**{"from": "node_join_insights", "to": "node_manifest_pattern"}),
                    ProtocolEdge(**{"from": "node_manifest_pattern", "to": "node_create_bonds"}),
                    ProtocolEdge(**{"from": "node_create_bonds", "to": "node_emit_signal"}),
                    ProtocolEdge(**{"from": "node_emit_signal", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto_induce)
    if verbose:
        print(f"    + Protocol: {proto_induce.id}")
    return proto_induce.id


def bootstrap_protocol_sense_entropy(store: EventStore, verbose: bool = True) -> str:
    """Bootstrap protocol-sense-entropy: Report metabolic health."""
    if verbose:
        print("[*] Bootstrapping protocol-sense-entropy...")

    # protocol-sense-entropy: Most complex - multiple conditionals
    # Flow: count_entities → count_bonds → find_orphans → calc_rate →
    #       check_orphans → emit? → check_deprecated → emit? → RETURN
    proto_sense_entropy = ProtocolEntity(
        id="protocol-sense-entropy",
        data=ProtocolData(
            title="Sense Entropy",
            description="Report system metabolic health and emit signals for threshold breaches",
            interface=ProtocolInterface(
                inputs={
                    "db_path": {"type": "string"},
                },
                outputs={
                    "health": {"type": "object"},
                    "signals_emitted": {"type": "array"},
                },
            ),
            graph=ProtocolGraph(
                start="node_count_entities",
                nodes={
                    # Step 1: Count entities by type
                    "node_count_entities": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.count_by_type",
                        inputs={
                            "db_path": "$.inputs.db_path",
                        },
                    ),
                    # Step 2: Count bonds
                    "node_count_bonds": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.bond.count",
                        inputs={
                            "db_path": "$.inputs.db_path",
                        },
                    ),
                    # Step 3: Find orphans
                    "node_find_orphans": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.orphans",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "limit": 100,
                        },
                    ),
                    # Step 4: Query deprecated entities
                    "node_query_deprecated": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.json",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "json_conditions": {"status": "deprecated"},
                            "limit": 100,
                        },
                    ),
                    # Step 5: Count learnings for digestion rate
                    "node_count_learnings": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.json",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_type": "learning",
                            "json_conditions": {},
                            "limit": 1000,
                        },
                    ),
                    # Step 6: Calculate digestion rate (learnings / total * 100)
                    # For now, simplify - we'll return raw counts
                    # Step 7: Check orphans threshold and maybe emit
                    "node_check_orphans": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="io.sys.log",
                        inputs={
                            "message": "Checking orphan threshold: {$.node_find_orphans.count} orphans",
                        },
                    ),
                    # Step 8a: Emit orphan signal (if count > 5)
                    "node_emit_orphan_signal": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="attention.signal.emit",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "title": "Orphans detected: {$.node_find_orphans.count} entities have no bonds",
                            "signal_type": "orphans",
                            "description": "Metabolic health alert: orphan entities detected",
                            "data": {
                                "count": "$.node_find_orphans.count",
                            },
                        },
                    ),
                    # Step 8b: Skip signal (placeholder node)
                    "node_skip_orphan_signal": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="io.sys.log",
                        inputs={
                            "message": "Orphan count below threshold, skipping signal",
                        },
                    ),
                    # Step 9: Return health report
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "health": {
                                "total_entities": "$.node_count_entities.total",
                                "total_bonds": "$.node_count_bonds.total",
                                "orphan_count": "$.node_find_orphans.count",
                                "deprecated_count": "$.node_query_deprecated.count",
                                "learning_count": "$.node_count_learnings.count",
                            },
                            "signals_emitted": [],
                        },
                    ),
                    # Return with signal emitted
                    "node_return_with_signal": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "health": {
                                "total_entities": "$.node_count_entities.total",
                                "total_bonds": "$.node_count_bonds.total",
                                "orphan_count": "$.node_find_orphans.count",
                                "deprecated_count": "$.node_query_deprecated.count",
                                "learning_count": "$.node_count_learnings.count",
                            },
                            "signals_emitted": [
                                {
                                    "id": "$.node_emit_orphan_signal.id",
                                    "type": "orphans",
                                    "count": "$.node_find_orphans.count",
                                },
                            ],
                        },
                    ),
                },
                edges=[
                    # Linear setup
                    ProtocolEdge(**{"from": "node_count_entities", "to": "node_count_bonds"}),
                    ProtocolEdge(**{"from": "node_count_bonds", "to": "node_find_orphans"}),
                    ProtocolEdge(**{"from": "node_find_orphans", "to": "node_query_deprecated"}),
                    ProtocolEdge(**{"from": "node_query_deprecated", "to": "node_count_learnings"}),
                    ProtocolEdge(**{"from": "node_count_learnings", "to": "node_check_orphans"}),
                    # Conditional: emit signal if orphans > 5
                    ProtocolEdge(
                        **{"from": "node_check_orphans", "to": "node_emit_orphan_signal"},
                        condition=EdgeCondition(
                            op=ConditionOp.GT,
                            path="$.node_find_orphans.count",
                            value=5,
                        ),
                    ),
                    ProtocolEdge(
                        **{"from": "node_check_orphans", "to": "node_skip_orphan_signal"},
                        default=True,
                    ),
                    # Routes to return
                    ProtocolEdge(**{"from": "node_emit_orphan_signal", "to": "node_return_with_signal"}),
                    ProtocolEdge(**{"from": "node_skip_orphan_signal", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto_sense_entropy)
    if verbose:
        print(f"    + Protocol: {proto_sense_entropy.id}")
    return proto_sense_entropy.id


def bootstrap_protocol_horizon(store: EventStore, verbose: bool = True) -> str:
    """
    Bootstrap protocol-horizon: semantic ranking of tools based on learnings.

    This protocol implements the "What wants attention?" query by:
    1. Getting recent learnings to establish context
    2. Loading embeddings for learnings and unverified tools
    3. Computing semantic similarity to rank tools by relevance

    Returns the protocol entity ID.
    """
    proto_horizon = ProtocolEntity(
        id="protocol-horizon",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={
                    "db_path": {"type": "string"},
                    "days": {"type": "integer", "default": 7},
                    "limit": {"type": "integer", "default": 10},
                },
                outputs={
                    "recommendations": {"type": "array"},
                    "recent_learnings": {"type": "array"},
                    "unverified_tools": {"type": "array"},
                    "method": {"type": "string"},
                },
            ),
            graph=ProtocolGraph(
                start="node_get_learnings",
                nodes={
                    "node_get_learnings": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.recent",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_type": "learning",
                            "limit": "$.inputs.limit",
                        },
                    ),
                    "node_get_unverified": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.unverified",
                        inputs={
                            "db_path": "$.inputs.db_path",
                        },
                    ),
                    "node_load_learning_embeddings": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="cognition.embed.batch_load",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_ids": "$.node_get_learnings.ids",
                        },
                    ),
                    "node_convert_to_vectors": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="cognition.embed.to_vectors",
                        inputs={
                            "embeddings": "$.node_load_learning_embeddings.embeddings",
                        },
                    ),
                    "node_compute_context": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="cognition.vector.mean",
                        inputs={
                            "vectors": "$.node_convert_to_vectors.vectors",
                            "dimension": 1536,
                        },
                    ),
                    "node_load_tool_embeddings": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="cognition.embed.batch_load",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_ids": "$.node_get_unverified.ids",
                        },
                    ),
                    "node_convert_to_candidates": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="cognition.embed.to_candidates",
                        inputs={
                            "embeddings": "$.node_load_tool_embeddings.embeddings",
                        },
                    ),
                    "node_rank_tools": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="cognition.semantic.rank_loop",
                        inputs={
                            "query_vector": "$.node_compute_context.vector",
                            "candidates": "$.node_convert_to_candidates.candidates",
                            "dimension": 1536,
                            "threshold": 0.0,
                        },
                    ),
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "recommendations": "$.node_rank_tools.ranked",
                            "recent_learnings": "$.node_get_learnings.ids",
                            "unverified_tools": "$.node_get_unverified.ids",
                            "method": "semantic",
                        },
                    ),
                    # Cold start branch: return early when no learnings exist
                    "node_return_empty": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "recommendations": [],
                            "recent_learnings": [],
                            "unverified_tools": "$.node_get_unverified.ids",
                            "method": "cold_start",
                            "note": "No recent learnings found - semantic ranking not possible",
                        },
                    ),
                },
                edges=[
                    # Always: get learnings first, then get unverified tools
                    ProtocolEdge(**{"from": "node_get_learnings", "to": "node_get_unverified"}),
                    # Branch point: check if learnings.ids is empty
                    ProtocolEdge(
                        **{"from": "node_get_unverified", "to": "node_return_empty"},
                        condition=EdgeCondition(op=ConditionOp.EMPTY, path="$.node_get_learnings.ids"),
                    ),
                    ProtocolEdge(
                        **{"from": "node_get_unverified", "to": "node_load_learning_embeddings"},
                        default=True,  # Continue semantic path when learnings exist
                    ),
                    # Rest of semantic flow with conversion steps
                    ProtocolEdge(**{"from": "node_load_learning_embeddings", "to": "node_convert_to_vectors"}),
                    ProtocolEdge(**{"from": "node_convert_to_vectors", "to": "node_compute_context"}),
                    ProtocolEdge(**{"from": "node_compute_context", "to": "node_load_tool_embeddings"}),
                    ProtocolEdge(**{"from": "node_load_tool_embeddings", "to": "node_convert_to_candidates"}),
                    ProtocolEdge(**{"from": "node_convert_to_candidates", "to": "node_rank_tools"}),
                    ProtocolEdge(**{"from": "node_rank_tools", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto_horizon)
    if verbose:
        print(f"    + Protocol: {proto_horizon.id}")
    return proto_horizon.id


def bootstrap_protocol_compost(store: EventStore, verbose: bool = True) -> str:
    """
    Bootstrap protocol-compost: transform any entity into a learning.

    This protocol implements composting by:
    1. Getting the source entity
    2. Extracting its text representation
    3. Creating a new learning entity
    4. Bonding the learning to the source
    5. Marking the source as composted

    Returns the protocol entity ID.
    """
    proto_compost = ProtocolEntity(
        id="protocol-compost",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={
                    "db_path": {"type": "string"},
                    "entity_id": {"type": "string"},
                },
                outputs={
                    "learning_id": {"type": "string"},
                    "composted": {"type": "boolean"},
                },
            ),
            graph=ProtocolGraph(
                start="node_get_entity",
                nodes={
                    "node_get_entity": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.get",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_id": "$.inputs.entity_id",
                        },
                    ),
                    "node_extract_text": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.to_text",
                        inputs={
                            "entity_type": "$.node_get_entity.entity.type",
                            "data": "$.node_get_entity.entity.data",
                        },
                    ),
                    "node_create_learning": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.create_batch",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entities": [{
                                "type": "learning",
                                "data": {
                                    "title": "Composted from $.inputs.entity_id",
                                    "insight": "$.node_extract_text.text",
                                    "domain": "composting",
                                },
                            }],
                        },
                    ),
                    "node_create_bond": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.bond.manage",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "bond_type": "crystallized-from",
                            "from_id": "$.node_create_learning.created[0]",
                            "to_id": "$.inputs.entity_id",
                        },
                    ),
                    "node_update_status": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.entity.update",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "entity_id": "$.inputs.entity_id",
                            "updates": {"status": "composted"},
                        },
                    ),
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "learning_id": "$.node_create_learning.created[0]",
                            "composted": True,
                        },
                    ),
                },
                edges=[
                    ProtocolEdge(**{"from": "node_get_entity", "to": "node_extract_text"}),
                    ProtocolEdge(**{"from": "node_extract_text", "to": "node_create_learning"}),
                    ProtocolEdge(**{"from": "node_create_learning", "to": "node_create_bond"}),
                    ProtocolEdge(**{"from": "node_create_bond", "to": "node_update_status"}),
                    ProtocolEdge(**{"from": "node_update_status", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto_compost)
    if verbose:
        print(f"    + Protocol: {proto_compost.id}")
    return proto_compost.id


def bootstrap_protocol_detect_stagnation(store: EventStore, verbose: bool = True) -> str:
    """
    Bootstrap protocol-detect-stagnation: find stagnant entities.

    This protocol identifies entities that haven't been touched recently:
    1. Calculate a timestamp cutoff
    2. Query for entities older than cutoff
    3. Count them for reporting

    Returns the protocol entity ID.
    """
    proto_detect_stagnation = ProtocolEntity(
        id="protocol-detect-stagnation",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={
                    "db_path": {"type": "string"},
                    "stale_days": {"type": "integer", "default": 30},
                },
                outputs={
                    "stagnant_count": {"type": "integer"},
                    "signals_emitted": {"type": "integer"},
                },
            ),
            graph=ProtocolGraph(
                start="node_calc_cutoff",
                nodes={
                    "node_calc_cutoff": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="chronos.offset",
                        inputs={
                            "days": "$.inputs.stale_days",
                            "negate": True,
                        },
                    ),
                    "node_query_stale": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="graph.query.json",
                        inputs={
                            "db_path": "$.inputs.db_path",
                            "json_conditions": {
                                "$.status": "active",
                            },
                        },
                    ),
                    "node_extract_ids": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.list.map",
                        inputs={
                            "items": "$.node_query_stale.entities",
                            "key": "id",
                        },
                    ),
                    "node_count": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="logic.list.length",
                        inputs={
                            "items": "$.node_extract_ids.values",
                        },
                    ),
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "stagnant_count": "$.node_count.length",
                            "signals_emitted": 0,
                        },
                    ),
                },
                edges=[
                    ProtocolEdge(**{"from": "node_calc_cutoff", "to": "node_query_stale"}),
                    ProtocolEdge(**{"from": "node_query_stale", "to": "node_extract_ids"}),
                    ProtocolEdge(**{"from": "node_extract_ids", "to": "node_count"}),
                    ProtocolEdge(**{"from": "node_count", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto_detect_stagnation)
    if verbose:
        print(f"    + Protocol: {proto_detect_stagnation.id}")
    return proto_detect_stagnation.id


def bootstrap_protocols(store: EventStore, verbose: bool = True) -> list[str]:
    """
    Bootstrap all protocol entities.

    Returns list of created protocol IDs.
    """
    if verbose:
        print("[*] Bootstrapping Protocols...")

    created = []

    # Wave 2 protocols (metabolic)
    created.append(bootstrap_protocol_orient(store, verbose))
    created.append(bootstrap_protocol_digest(store, verbose))
    created.append(bootstrap_protocol_induce(store, verbose))
    created.append(bootstrap_protocol_sense_entropy(store, verbose))

    # Wave 3 protocols (semantic)
    created.append(bootstrap_protocol_horizon(store, verbose))
    created.append(bootstrap_protocol_compost(store, verbose))
    created.append(bootstrap_protocol_detect_stagnation(store, verbose))

    if verbose:
        print(f"[*] Protocols complete: {len(created)} protocols")

    return created


def main(db_path: str = "chora-cvm-manifest.db") -> None:
    """Bootstrap all protocols (standalone mode)."""
    store = EventStore(db_path)
    print(f"[*] Bootstrapping Protocols in {db_path}...")

    # Register all protocols
    created = bootstrap_protocols(store, verbose=True)

    print(f"[*] Protocol bootstrap complete: {len(created)} protocols")
    store.close()


if __name__ == "__main__":
    import sys
    db_arg = sys.argv[1] if len(sys.argv) > 1 else "chora-cvm-manifest.db"
    main(db_arg)
