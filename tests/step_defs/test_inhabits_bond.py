"""
Step definitions for the inhabits Bond feature.

These tests verify the behaviors specified by story-entities-inhabit-circles.
Entities belong to circles via the inhabits bond.
"""
import json
import os
import tempfile

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import manifest_entity, manage_bond

# Load scenarios from feature file
scenarios("../features/inhabits_bond.feature")


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
    test_context["circles"] = []
    test_context["learnings"] = []


@given(parsers.parse('a circle "{circle_id}" exists'))
def create_circle(db_path, test_context, circle_id: str):
    """Create a circle entity."""
    manifest_entity(
        db_path,
        "circle",
        circle_id,
        {"title": f"Test circle {circle_id}", "sync_policy": "local-only"},
    )
    test_context["circles"].append(circle_id)


@given(parsers.parse('a learning "{learning_id}" exists'))
def create_learning(db_path, test_context, learning_id: str):
    """Create a learning entity."""
    manifest_entity(
        db_path,
        "learning",
        learning_id,
        {"title": f"Test learning {learning_id}"},
    )
    test_context["learnings"].append(learning_id)
    test_context["last_learning"] = learning_id


# =============================================================================
# Multiple Entity Steps
# =============================================================================


@given(parsers.parse("{count:d} learnings exist"))
def create_multiple_learnings(db_path, test_context, count: int):
    """Create multiple learning entities."""
    for i in range(count):
        learning_id = f"learning-test-{i}"
        manifest_entity(
            db_path,
            "learning",
            learning_id,
            {"title": f"Test learning {i}"},
        )
        test_context["learnings"].append(learning_id)


@given(parsers.parse('all {count:d} inhabit "{circle_id}"'))
def all_inhabit_circle(db_path, test_context, count: int, circle_id: str):
    """Bond all learnings to a circle."""
    for learning_id in test_context["learnings"][-count:]:
        manage_bond(db_path, "inhabits", learning_id, circle_id)


@given(parsers.parse('a learning that inhabits "{circle1}" and "{circle2}"'))
def learning_inhabits_two_circles(db_path, test_context, circle1: str, circle2: str):
    """Create a learning that inhabits two circles."""
    learning_id = "learning-multi-circle"
    manifest_entity(
        db_path,
        "learning",
        learning_id,
        {"title": "Multi-circle learning"},
    )
    manage_bond(db_path, "inhabits", learning_id, circle1)
    manage_bond(db_path, "inhabits", learning_id, circle2)
    test_context["multi_circle_learning"] = learning_id


@given(parsers.parse('a learning "{learning_id}" exists with no inhabits bonds'))
def create_orphan_learning(db_path, test_context, learning_id: str):
    """Create a learning with no circle membership."""
    manifest_entity(
        db_path,
        "learning",
        learning_id,
        {"title": f"Orphan learning {learning_id}"},
    )
    test_context["orphan_learning"] = learning_id


# =============================================================================
# Action Steps
# =============================================================================


@when(parsers.parse('I bond inhabits from "{from_id}" to "{to_id}"'))
def bond_inhabits(db_path, test_context, from_id: str, to_id: str):
    """Create an inhabits bond."""
    result = manage_bond(db_path, "inhabits", from_id, to_id)
    test_context["last_bond"] = result


@when(parsers.parse('I query get_inhabitants for "{circle_id}"'))
def query_inhabitants(db_path, test_context, circle_id: str):
    """Query inhabitants of a circle."""
    store = EventStore(db_path)
    inhabitants = store.get_inhabitants(circle_id)
    store.close()
    test_context["query_result"] = inhabitants


@when(parsers.parse('I query get_inhabited_circles for "{entity_id}"'))
def query_inhabited_circles(db_path, test_context, entity_id: str):
    """Query circles an entity inhabits."""
    store = EventStore(db_path)
    circles = store.get_inhabited_circles(entity_id)
    store.close()
    test_context["query_result"] = circles


@when("I query get_inhabited_circles for the learning")
def query_learning_circles(db_path, test_context):
    """Query circles for the multi-circle learning."""
    learning_id = test_context.get("multi_circle_learning")
    store = EventStore(db_path)
    circles = store.get_inhabited_circles(learning_id)
    store.close()
    test_context["query_result"] = circles


# =============================================================================
# Assertion Steps - Inhabitants
# =============================================================================


@then(parsers.parse("the learning appears in {circle_id}'s inhabitants"))
def check_learning_in_inhabitants(db_path, test_context, circle_id: str):
    """Verify learning appears in circle's inhabitants."""
    learning_id = test_context.get("last_learning")
    store = EventStore(db_path)
    inhabitants = store.get_inhabitants(circle_id)
    store.close()

    inhabitant_ids = [i["id"] for i in inhabitants]
    assert learning_id in inhabitant_ids, \
        f"Learning {learning_id} not in {circle_id} inhabitants: {inhabitant_ids}"


@then(parsers.parse('get_inhabited_circles for "{entity_id}" returns "{circle_id}"'))
def check_inhabited_circles_single(db_path, entity_id: str, circle_id: str):
    """Verify entity inhabits expected circle."""
    store = EventStore(db_path)
    circles = store.get_inhabited_circles(entity_id)
    store.close()

    assert circle_id in circles, f"Entity {entity_id} doesn't inhabit {circle_id}: {circles}"


@then("get_inhabited_circles for the learning returns both circles")
def check_both_circles(test_context):
    """Verify learning inhabits both circles."""
    circles = test_context.get("query_result", [])
    assert len(circles) >= 2, f"Expected at least 2 circles, got {circles}"


@then(parsers.parse('get_inhabited_circles for "{entity_id}" returns both circles'))
def check_both_circles_for_entity(db_path, entity_id: str):
    """Verify entity inhabits both circles."""
    store = EventStore(db_path)
    circles = store.get_inhabited_circles(entity_id)
    store.close()
    assert len(circles) >= 2, f"Expected at least 2 circles for {entity_id}, got {circles}"


@then("the learning appears in both circles' inhabitants")
def check_in_both_circles(db_path, test_context):
    """Verify learning appears in both circles."""
    learning_id = test_context.get("learnings", ["learning-shared-insight"])[-1]

    for circle_id in test_context.get("circles", []):
        store = EventStore(db_path)
        inhabitants = store.get_inhabitants(circle_id)
        store.close()

        inhabitant_ids = [i["id"] for i in inhabitants]
        assert learning_id in inhabitant_ids, \
            f"Learning not in {circle_id}: {inhabitant_ids}"


@then(parsers.parse("all {count:d} learnings are returned"))
def check_all_learnings_returned(test_context, count: int):
    """Verify all learnings are returned."""
    result = test_context.get("query_result", [])
    assert len(result) == count, f"Expected {count} learnings, got {len(result)}"


@then("each result includes entity id, type, and data")
def check_result_structure(test_context):
    """Verify result structure."""
    result = test_context.get("query_result", [])
    for entity in result:
        assert "id" in entity, f"Missing id in {entity}"
        assert "type" in entity, f"Missing type in {entity}"
        assert "data" in entity, f"Missing data in {entity}"


@then("an empty list is returned")
def check_empty_result(test_context):
    """Verify empty result."""
    result = test_context.get("query_result", [])
    assert len(result) == 0, f"Expected empty list, got {result}"


@then(parsers.parse('both "{circle1}" and "{circle2}" are returned'))
def check_both_circles_returned(test_context, circle1: str, circle2: str):
    """Verify both circles are in result."""
    circles = test_context.get("query_result", [])
    assert circle1 in circles, f"{circle1} not in {circles}"
    assert circle2 in circles, f"{circle2} not in {circles}"


@then("the result can be used for sync routing decisions")
def check_routing_usable(test_context):
    """Verify result is usable for routing (non-empty list of circle IDs)."""
    circles = test_context.get("query_result", [])
    assert isinstance(circles, list), f"Expected list, got {type(circles)}"
    for circle in circles:
        assert isinstance(circle, str), f"Expected string circle ID, got {type(circle)}"
