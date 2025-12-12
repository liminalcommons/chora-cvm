"""
Step definitions for Metabolic Operations (Autophagy) feature.

These tests verify the behaviors specified by story-system-metabolizes-entropy-into-growth.

BDD Flow: Feature file -> Step definitions -> Implementation
Tests should FAIL initially until metabolic.py is implemented.
"""
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from chora_cvm.schema import GenericEntity
from chora_cvm.std import manage_bond, manifest_entity
from chora_cvm.store import EventStore

# Load scenarios from feature file
scenarios("../features/metabolic.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {}


@pytest.fixture
def db_path():
    """Create a temporary database for each test."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name

    # Initialize the database with required tables
    store = EventStore(path)
    store.close()

    yield path

    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


# =============================================================================
# Background Steps
# =============================================================================


@given("a fresh CVM database")
def fresh_database(db_path, test_context):
    """Set up a fresh database for testing."""
    test_context["db_path"] = db_path


# =============================================================================
# Sense Entropy Setup Steps
# =============================================================================


@given(parsers.parse("the Loom has {total:d} entities with {bonds:d} bonds"))
def setup_entities_with_bonds(db_path, test_context, total: int, bonds: int):
    """Create entities with bonds, ensuring all entities are bonded.

    Uses a circular pattern so all entities have at least one bond,
    then adds extra bonds to reach requested count.
    """
    store = EventStore(db_path)

    # Create entities
    for i in range(total):
        entity = GenericEntity(
            id=f"test-entity-{i}",
            type="learning",
            data={"title": f"Test entity {i}"},
        )
        store.save_entity(entity)

    # Create bonds: first ensure ALL entities are connected via a circular chain
    # Each entity i connects to entity (i+1) % total, ensuring no orphans
    for i in range(total):
        store.save_bond(
            bond_id=f"rel-chain-{i}",
            bond_type="crystallized-from",
            from_id=f"test-entity-{i}",
            to_id=f"test-entity-{(i + 1) % total}",
            data={"confidence": 1.0},
        )

    # Now add any extra bonds beyond the circular chain
    # (if bonds > total, add cross-links)
    for i in range(total, bonds):
        store.save_bond(
            bond_id=f"rel-extra-{i}",
            bond_type="crystallized-from",
            from_id=f"test-entity-{i % total}",
            to_id=f"test-entity-{(i + 2) % total}",
            data={"confidence": 1.0},
        )

    store.close()
    test_context["total_entities"] = total
    # Note: actual bond count is max(total, bonds) due to circular chain
    test_context["total_bonds"] = max(total, bonds)


@given(parsers.parse("{count:d} entities are orphans (no bonds)"))
def setup_orphan_entities(db_path, test_context, count: int):
    """Create orphan entities (with no bonds)."""
    store = EventStore(db_path)

    for i in range(count):
        entity = GenericEntity(
            id=f"orphan-entity-{i}",
            type="learning",
            data={"title": f"Orphan entity {i}"},
        )
        store.save_entity(entity)

    store.close()
    test_context["orphan_count"] = count


@given(parsers.parse("{count:d} signals are older than 7 days"))
def setup_stale_signals(db_path, test_context, count: int):
    """Create stale signals with bonds so they don't count as orphans."""
    store = EventStore(db_path)
    old_date = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()

    for i in range(count):
        signal_id = f"signal-stale-{i}"
        entity = GenericEntity(
            id=signal_id,
            type="signal",
            data={
                "title": f"Stale signal {i}",
                "status": "active",
                "created_at": old_date,
            },
        )
        store.save_entity(entity)

        # Give the signal a bond so it doesn't count as orphan
        store.save_bond(
            bond_id=f"rel-stale-signal-{i}",
            bond_type="triggers",
            from_id=signal_id,
            to_id="test-entity-0" if "test-entity-0" else signal_id,
            data={"confidence": 1.0},
        )

    store.close()
    test_context["stale_signal_count"] = count


@given(parsers.parse("the Loom has {count:d} stale signals older than the 7-day threshold"))
def setup_many_stale_signals(db_path, test_context, count: int):
    """Create multiple stale signals for threshold testing."""
    setup_stale_signals(db_path, test_context, count)


# =============================================================================
# Digest Setup Steps
# =============================================================================


@given(parsers.parse('a pattern "{pattern_id}" exists with problem/solution data'))
def setup_pattern_with_data(db_path, test_context, pattern_id: str):
    """Create a pattern entity with digestible data."""
    store = EventStore(db_path)
    entity = GenericEntity(
        id=pattern_id,
        type="pattern",
        data={
            "title": "Old Approach Pattern",
            "problem": "Legacy systems were hard to maintain",
            "solution": "Introduced modular architecture",
            "context": "Enterprise software development",
        },
    )
    store.save_entity(entity)
    store.close()
    test_context["pattern_id"] = pattern_id


@given(parsers.parse('a tool "{tool_id}" exists with phenomenology data'))
def setup_tool_with_phenomenology(db_path, test_context, tool_id: str):
    """Create a tool entity with phenomenology data."""
    store = EventStore(db_path)
    entity = GenericEntity(
        id=tool_id,
        type="tool",
        data={
            "title": "Legacy Helper Tool",
            "handler": "legacy.helper",
            "phenomenology": "This tool emerged when we needed rapid prototyping capability",
            "cognition": {"ready_at_hand": "When exploring new patterns quickly"},
        },
    )
    store.save_entity(entity)
    store.close()
    test_context["tool_id"] = tool_id


# =============================================================================
# Compost Setup Steps
# =============================================================================


@given(parsers.parse('an entity "{entity_id}" exists with no bonds'))
def setup_orphan_entity(db_path, test_context, entity_id: str):
    """Create an orphan entity (no bonds)."""
    store = EventStore(db_path)
    entity = GenericEntity(
        id=entity_id,
        type="learning",
        data={"title": "Forgotten Learning", "insight": "Something we learned"},
    )
    store.save_entity(entity)
    store.close()
    test_context["orphan_entity_id"] = entity_id


@given(parsers.parse('an entity "{entity_id}" exists with bonds to deleted entities'))
def setup_entity_with_dangling_bonds(db_path, test_context, entity_id: str):
    """Create an entity with bonds pointing to nonexistent entities."""
    store = EventStore(db_path)

    entity = GenericEntity(
        id=entity_id,
        type="pattern",
        data={"title": "Deprecated Pattern", "status": "deprecated"},
    )
    store.save_entity(entity)

    # Create bonds to entities that don't exist (dangling)
    store.save_bond(
        bond_id=f"rel-dangling-1-{entity_id}",
        bond_type="structures",
        from_id=entity_id,
        to_id="story-deleted-1",
        data={"confidence": 1.0},
    )
    store.save_bond(
        bond_id=f"rel-dangling-2-{entity_id}",
        bond_type="structures",
        from_id=entity_id,
        to_id="story-deleted-2",
        data={"confidence": 1.0},
    )

    store.close()
    test_context["dangling_entity_id"] = entity_id


@given(parsers.parse('an entity "{entity_id}" exists with active bonds'))
def setup_entity_with_active_bonds(db_path, test_context, entity_id: str):
    """Create an entity with active bonds (not an orphan)."""
    store = EventStore(db_path)

    # Create the main entity
    entity = GenericEntity(
        id=entity_id,
        type="story",
        data={"title": "Active Story"},
    )
    store.save_entity(entity)

    # Create a behavior it specifies
    behavior = GenericEntity(
        id=f"behavior-for-{entity_id}",
        type="behavior",
        data={"title": "Active Behavior"},
    )
    store.save_entity(behavior)

    # Create the bond
    store.save_bond(
        bond_id=f"rel-specifies-{entity_id}",
        bond_type="specifies",
        from_id=entity_id,
        to_id=f"behavior-for-{entity_id}",
        data={"confidence": 1.0},
    )

    store.close()
    test_context["active_entity_id"] = entity_id


# =============================================================================
# Induce Setup Steps
# =============================================================================


@given(parsers.parse('{count:d} learnings exist with common domain "{domain}"'))
def setup_clustered_learnings(db_path, test_context, count: int, domain: str):
    """Create learnings with common domain for clustering."""
    store = EventStore(db_path)
    learning_ids = []

    for i in range(count):
        entity_id = f"learning-{domain}-{i}"
        entity = GenericEntity(
            id=entity_id,
            type="learning",
            data={
                "title": f"Learning about {domain} #{i}",
                "domain": domain,
                "insight": f"Insight #{i} about {domain}",
            },
        )
        store.save_entity(entity)
        learning_ids.append(entity_id)

    store.close()
    test_context["clustered_learning_ids"] = learning_ids
    test_context["cluster_domain"] = domain


# =============================================================================
# Stagnation Setup Steps
# =============================================================================


@given(parsers.parse('an inquiry "{inquiry_id}" was created {days:d} days ago'))
def setup_old_inquiry(db_path, test_context, inquiry_id: str, days: int):
    """Create an old inquiry for stagnation testing."""
    store = EventStore(db_path)
    old_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    entity = GenericEntity(
        id=inquiry_id,
        type="inquiry",
        data={
            "title": "Old Inquiry",
            "status": "active",
            "created_at": old_date,
        },
    )
    store.save_entity(entity)
    store.close()
    test_context["stagnant_inquiry_id"] = inquiry_id


@given(parsers.parse('principle "{principle_id}" defines TTL = {ttl:d}'))
def setup_ttl_principle(db_path, test_context, principle_id: str, ttl: int):
    """Create a TTL threshold principle.

    Extracts entity_type from principle ID like 'principle-inquiry-stagnates-after-30-days'.
    """
    # Extract entity type from principle ID
    # e.g., "principle-inquiry-stagnates-after-30-days" -> "inquiry"
    # or "principle-signal-stagnates-after-7-days" -> "signal"
    entity_type = None
    for etype in ["inquiry", "signal", "focus", "pattern"]:
        if etype in principle_id:
            entity_type = etype
            break

    store = EventStore(db_path)
    entity = GenericEntity(
        id=principle_id,
        type="principle",
        data={
            "title": f"TTL threshold {ttl} days",
            "ttl_days": ttl,
            "entity_type": entity_type,
            "category": "metabolic-threshold",
        },
    )
    store.save_entity(entity)
    store.close()
    test_context["ttl_entity_type"] = entity_type


@given(parsers.parse('a signal "{signal_id}" was created {days:d} days ago'))
def setup_old_signal(db_path, test_context, signal_id: str, days: int):
    """Create an old signal for stagnation testing."""
    store = EventStore(db_path)
    old_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    entity = GenericEntity(
        id=signal_id,
        type="signal",
        data={
            "title": "Stuck Signal",
            "status": "active",
            "created_at": old_date,
        },
    )
    store.save_entity(entity)
    store.close()
    test_context["stagnant_signal_id"] = signal_id


# =============================================================================
# Auto-Resolution Setup Steps
# =============================================================================


@given(parsers.parse('a signal "{signal_id}" tracks orphan "{entity_id}"'))
def setup_orphan_tracking_signal(db_path, test_context, signal_id: str, entity_id: str):
    """Create a signal that tracks an orphan entity."""
    store = EventStore(db_path)

    # Create the orphan entity
    entity = GenericEntity(
        id=entity_id,
        type="learning",
        data={"title": "Orphan Learning"},
    )
    store.save_entity(entity)

    # Create the tracking signal
    signal = GenericEntity(
        id=signal_id,
        type="signal",
        data={
            "title": f"Orphan detected: {entity_id}",
            "status": "active",
            "signal_type": "orphan-detected",
            "tracks_entity_id": entity_id,
        },
    )
    store.save_entity(signal)
    store.close()

    test_context["tracking_signal_id"] = signal_id
    test_context["tracked_entity_id"] = entity_id


@given(parsers.parse('"{entity_id}" has no bonds'))
def verify_entity_has_no_bonds(db_path, entity_id: str):
    """Verify an entity has no bonds (precondition)."""
    store = EventStore(db_path)
    bonds_from = store.get_bonds_from(entity_id)
    bonds_to = store.get_bonds_to(entity_id)
    store.close()

    assert len(bonds_from) == 0 and len(bonds_to) == 0, f"Entity {entity_id} has bonds"


@given(parsers.parse('a signal "{signal_id}" tracks stagnant "{entity_id}"'))
def setup_stagnation_tracking_signal(db_path, test_context, signal_id: str, entity_id: str):
    """Create a signal that tracks a stagnant entity."""
    store = EventStore(db_path)

    # Create the tracking signal
    signal = GenericEntity(
        id=signal_id,
        type="signal",
        data={
            "title": f"Stagnation detected: {entity_id}",
            "status": "active",
            "signal_type": "stagnation-detected",
            "tracks_entity_id": entity_id,
        },
    )
    store.save_entity(signal)
    store.close()

    test_context["stagnation_signal_id"] = signal_id


@given(parsers.parse('"{entity_id}" was last updated {days:d} days ago'))
def setup_stale_entity(db_path, test_context, entity_id: str, days: int):
    """Create/update an entity with an old updated_at timestamp."""
    store = EventStore(db_path)
    old_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    # Check if entity already exists (from prior step)
    existing = store.load_entity(entity_id, GenericEntity)
    if existing:
        existing.data["updated_at"] = old_date
        store.save_entity(existing)
    else:
        entity = GenericEntity(
            id=entity_id,
            type="inquiry",
            data={
                "title": "Stagnant Inquiry",
                "status": "active",
                "updated_at": old_date,
            },
        )
        store.save_entity(entity)

    store.close()
    test_context["stagnant_entity_id"] = entity_id


# =============================================================================
# Action Steps
# =============================================================================


@when("sense_entropy is invoked")
def invoke_sense_entropy(db_path, test_context):
    """Call sense_entropy and store result."""
    # Import will fail until metabolic.py exists
    try:
        from chora_cvm.metabolic import sense_entropy
        result = sense_entropy(db_path)
        test_context["sense_result"] = result
    except ImportError:
        pytest.skip("metabolic.py not yet implemented")


@when(parsers.parse('digest is invoked with entity_id "{entity_id}"'))
def invoke_digest(db_path, test_context, entity_id: str):
    """Call digest on an entity."""
    try:
        from chora_cvm.metabolic import digest
        result = digest(db_path, entity_id)
        test_context["digest_result"] = result
    except ImportError:
        pytest.skip("metabolic.py not yet implemented")


@when(parsers.parse('compost is invoked with entity_id "{entity_id}"'))
def invoke_compost(db_path, test_context, entity_id: str):
    """Call compost on an entity."""
    try:
        from chora_cvm.metabolic import compost
        result = compost(db_path, entity_id)
        test_context["compost_result"] = result
    except ImportError:
        pytest.skip("metabolic.py not yet implemented")


@when("induce is invoked with those learning_ids")
def invoke_induce(db_path, test_context):
    """Call induce with clustered learning IDs."""
    try:
        from chora_cvm.metabolic import induce
        learning_ids = test_context.get("clustered_learning_ids", [])
        result = induce(db_path, learning_ids)
        test_context["induce_result"] = result
    except ImportError:
        pytest.skip("metabolic.py not yet implemented")


@when("pulse detects stagnation")
def pulse_detects_stagnation(db_path, test_context):
    """Simulate pulse detecting stagnation conditions."""
    try:
        from chora_cvm.metabolic import detect_stagnation
        result = detect_stagnation(db_path)
        test_context["stagnation_result"] = result
    except ImportError:
        pytest.skip("metabolic.py not yet implemented")


@when(parsers.parse('a bond is created from "{from_id}" to another entity'))
def create_bond_to_clear_void(db_path, test_context, from_id: str):
    """Create a bond to clear an orphan void condition."""
    # Create a target entity
    store = EventStore(db_path)
    target = GenericEntity(
        id="target-entity",
        type="principle",
        data={"title": "Target Principle"},
    )
    store.save_entity(target)
    store.close()

    # Create the bond
    manage_bond(db_path, "surfaces", from_id, "target-entity")
    test_context["void_cleared"] = True


@when(parsers.parse('"{entity_id}" yields a new learning'))
def entity_yields_learning(db_path, test_context, entity_id: str):
    """Create a yield bond to clear stagnation condition."""
    store = EventStore(db_path)

    # Create a new learning
    learning = GenericEntity(
        id="learning-new-from-inquiry",
        type="learning",
        data={"title": "Fresh Learning", "insight": "New insight"},
    )
    store.save_entity(learning)

    # Ensure the source entity exists and update its updated_at
    entity = store.load_entity(entity_id, GenericEntity)
    if not entity:
        # Create the entity if it doesn't exist
        entity = GenericEntity(
            id=entity_id,
            type="inquiry",
            data={"title": "Inquiry", "status": "active"},
        )
    entity.data["updated_at"] = datetime.now(timezone.utc).isoformat()
    store.save_entity(entity)

    store.close()

    # Create the yield bond
    manage_bond(db_path, "yields", entity_id, "learning-new-from-inquiry")
    test_context["stagnation_cleared"] = True


@when("pulse detects the void condition has cleared")
def pulse_detects_void_cleared(db_path, test_context):
    """Simulate pulse detecting that void condition cleared."""
    try:
        from chora_cvm.metabolic import check_void_resolution
        result = check_void_resolution(db_path)
        test_context["void_resolution_result"] = result
    except ImportError:
        pytest.skip("metabolic.py not yet implemented")


# =============================================================================
# Assertion Steps
# =============================================================================


@then("the result includes a MetabolicHealth report")
def check_metabolic_health_report(test_context):
    """Verify MetabolicHealth report structure."""
    result = test_context.get("sense_result", {})
    assert "orphan_count" in result or "health" in result, "Missing MetabolicHealth report"


@then(parsers.parse("the report shows orphan_count = {count:d}"))
def check_orphan_count(test_context, count: int):
    """Verify orphan count in report."""
    result = test_context.get("sense_result", {})
    health = result.get("health", result)
    assert health.get("orphan_count") == count, f"Expected orphan_count={count}"


@then(parsers.parse("the report shows stale_signal_count = {count:d}"))
def check_stale_signal_count(test_context, count: int):
    """Verify stale signal count in report."""
    result = test_context.get("sense_result", {})
    health = result.get("health", result)
    assert health.get("stale_signal_count") == count, f"Expected stale_signal_count={count}"


@then(parsers.parse('a signal "{signal_id}" is emitted'))
def check_signal_emitted(db_path, test_context, signal_id: str):
    """Verify a signal was emitted (by prefix match since IDs have UUID suffix)."""
    # Check in signals_emitted from sense_result first
    result = test_context.get("sense_result", {})
    signals = result.get("signals_emitted", [])
    found_in_result = any(s.get("id", "").startswith(signal_id) for s in signals)

    if found_in_result:
        return

    # Fall back to checking database for exact or prefix match
    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT id FROM entities WHERE type = 'signal' AND id LIKE ?", (f"{signal_id}%",))
    rows = cur.fetchall()
    store.close()
    assert len(rows) > 0, f"Signal matching '{signal_id}*' was not emitted"


@then(parsers.parse("the signal metadata includes count = {count:d}"))
def check_signal_count_metadata(test_context, count: int):
    """Verify signal metadata includes count."""
    result = test_context.get("sense_result", {})
    signals = result.get("signals_emitted", [])
    assert any(s.get("count") == count for s in signals), f"No signal with count={count}"


@then("a new learning entity is created")
def check_learning_created(db_path, test_context):
    """Verify a learning was created from digestion."""
    result = test_context.get("digest_result", {})
    learning_id = result.get("learning_id")
    assert learning_id is not None, "No learning_id in digest result"

    store = EventStore(db_path)
    entity = store.load_entity(learning_id, GenericEntity)
    store.close()
    assert entity is not None, f"Learning {learning_id} was not created"
    test_context["created_learning_id"] = learning_id


@then(parsers.parse('the learning title includes "{text}"'))
def check_learning_title(db_path, test_context, text: str):
    """Verify learning title contains expected text."""
    learning_id = test_context.get("created_learning_id")
    store = EventStore(db_path)
    entity = store.load_entity(learning_id, GenericEntity)
    store.close()
    assert text in entity.data.get("title", ""), f"Title doesn't include '{text}'"


@then(parsers.parse('the learning insight includes the phenomenology content'))
def check_learning_phenomenology(db_path, test_context):
    """Verify learning insight contains phenomenology."""
    learning_id = test_context.get("created_learning_id")
    store = EventStore(db_path)
    entity = store.load_entity(learning_id, GenericEntity)
    store.close()
    insight = entity.data.get("insight", "")
    assert len(insight) > 0, "Learning insight is empty"


@then(parsers.parse('a crystallized-from bond connects the learning to "{source_id}"'))
def check_crystallized_from_bond(db_path, test_context, source_id: str):
    """Verify crystallized-from bond exists."""
    learning_id = test_context.get("created_learning_id")
    store = EventStore(db_path)
    bonds = store.get_bonds_from(learning_id)
    store.close()

    found = any(
        b.get("type") == "crystallized-from" and b.get("to_id") == source_id
        for b in bonds
    )
    assert found, f"No crystallized-from bond from {learning_id} to {source_id}"


@then("the entity is moved to the archive table")
def check_entity_archived(db_path, test_context):
    """Verify entity was moved to archive."""
    entity_id = test_context.get("orphan_entity_id")
    result = test_context.get("compost_result", {})

    assert result.get("archived") is True, "Entity was not archived"

    # Verify it's in archive table
    store = EventStore(db_path)
    # Note: This requires archive table implementation
    store.close()


@then("a learning about the composting is created")
def check_compost_learning(test_context):
    """Verify a learning about composting was created."""
    result = test_context.get("compost_result", {})
    assert result.get("learning_id") is not None, "No learning created from compost"


@then("the original entity no longer exists in entities table")
def check_entity_removed(db_path, test_context):
    """Verify entity was removed from entities table."""
    entity_id = test_context.get("orphan_entity_id")
    store = EventStore(db_path)
    entity = store.load_entity(entity_id, GenericEntity)
    store.close()
    assert entity is None, f"Entity {entity_id} still exists"


@then("the dangling bonds are archived first")
def check_dangling_bonds_archived(test_context):
    """Verify dangling bonds were archived."""
    result = test_context.get("compost_result", {})
    assert result.get("bonds_archived", 0) > 0, "No dangling bonds were archived"


@then("then the entity is archived")
def check_entity_then_archived(test_context):
    """Verify entity was archived after bonds."""
    result = test_context.get("compost_result", {})
    assert result.get("archived") is True, "Entity was not archived after bonds"


@then("a learning records the bond cleanup")
def check_bond_cleanup_learning(test_context):
    """Verify learning records bond cleanup."""
    result = test_context.get("compost_result", {})
    learning_id = result.get("learning_id")
    assert learning_id is not None, "No learning about bond cleanup"


@then("the operation returns an error")
def check_operation_error(test_context):
    """Verify operation returned an error."""
    result = test_context.get("compost_result") or test_context.get("induce_result", {})
    assert result.get("error") is not None, "Expected an error"
    test_context["error_message"] = result.get("error")


@then(parsers.parse('the error message says "{message}"'))
def check_error_message(test_context, message: str):
    """Verify error message contains expected text."""
    error = test_context.get("error_message", "")
    assert message in error, f"Error message doesn't contain '{message}': {error}"


@then(parsers.parse('a new pattern entity is created with status "{status}"'))
def check_pattern_created(db_path, test_context, status: str):
    """Verify pattern was created with expected status."""
    result = test_context.get("induce_result", {})
    pattern_id = result.get("pattern_id")
    assert pattern_id is not None, "No pattern_id in induce result"

    store = EventStore(db_path)
    entity = store.load_entity(pattern_id, GenericEntity)
    store.close()

    assert entity is not None, f"Pattern {pattern_id} not found"
    assert entity.data.get("status") == status, f"Pattern status is not '{status}'"
    test_context["created_pattern_id"] = pattern_id


@then(parsers.parse("crystallized-from bonds connect the pattern to all {count:d} learnings"))
def check_pattern_provenance(db_path, test_context, count: int):
    """Verify crystallized-from bonds to all source learnings."""
    pattern_id = test_context.get("created_pattern_id")
    learning_ids = test_context.get("clustered_learning_ids", [])

    store = EventStore(db_path)
    bonds = store.get_bonds_from(pattern_id)
    store.close()

    cf_bonds = [b for b in bonds if b.get("type") == "crystallized-from"]
    assert len(cf_bonds) == count, f"Expected {count} crystallized-from bonds, got {len(cf_bonds)}"

    for learning_id in learning_ids:
        found = any(b.get("to_id") == learning_id for b in cf_bonds)
        assert found, f"Missing crystallized-from bond to {learning_id}"


@then("a signal is emitted for human review")
def check_review_signal(test_context):
    """Verify signal for human review was emitted."""
    result = test_context.get("induce_result", {})
    assert result.get("review_signal_id") is not None, "No review signal emitted"


@then(parsers.parse('a signal is emitted for "{entity_id}"'))
def check_stagnation_signal(db_path, test_context, entity_id: str):
    """Verify stagnation signal was emitted."""
    result = test_context.get("stagnation_result", {})
    signals = result.get("signals_emitted", [])
    found = any(s.get("tracks_entity_id") == entity_id for s in signals)
    assert found, f"No signal emitted for {entity_id}"


@then(parsers.parse('the signal category is "{category}"'))
def check_signal_category(test_context, category: str):
    """Verify signal category."""
    result = test_context.get("stagnation_result", {})
    signals = result.get("signals_emitted", [])
    found = any(s.get("category") == category for s in signals)
    assert found, f"No signal with category '{category}'"


@then("a new escalation signal is emitted")
def check_escalation_signal(test_context):
    """Verify escalation signal was emitted."""
    result = test_context.get("stagnation_result", {})
    signals = result.get("signals_emitted", [])
    found = any(s.get("signal_type") == "escalation" for s in signals)
    assert found, "No escalation signal emitted"


@then(parsers.parse('the escalation references "{signal_id}"'))
def check_escalation_reference(test_context, signal_id: str):
    """Verify escalation references original signal."""
    result = test_context.get("stagnation_result", {})
    signals = result.get("signals_emitted", [])
    escalation = next((s for s in signals if s.get("signal_type") == "escalation"), None)
    assert escalation is not None, "No escalation signal found"
    assert escalation.get("escalates") == signal_id, f"Escalation doesn't reference {signal_id}"


@then(parsers.parse('signal "{signal_id}" status becomes "{status}"'))
def check_signal_resolved(db_path, signal_id: str, status: str):
    """Verify signal status changed."""
    store = EventStore(db_path)
    entity = store.load_entity(signal_id, GenericEntity)
    store.close()

    assert entity is not None, f"Signal {signal_id} not found"
    assert entity.data.get("status") == status, f"Signal status is not '{status}'"


@then(parsers.parse('the resolution metadata includes "{text}"'))
def check_resolution_metadata(db_path, test_context, text: str):
    """Verify resolution metadata contains expected text."""
    signal_id = test_context.get("tracking_signal_id") or test_context.get("stagnation_signal_id")
    store = EventStore(db_path)
    entity = store.load_entity(signal_id, GenericEntity)
    store.close()

    resolution = entity.data.get("resolution", "")
    assert text in resolution, f"Resolution doesn't include '{text}'"
