"""
Step definitions for the Sync Router feature.

These tests verify the behaviors specified by story-system-decides-what-to-sync.
Routes entity changes based on inhabits bonds and keyring policies.
"""
import os
import tempfile

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import manifest_entity, manage_bond
from chora_cvm.keyring import Keyring, CircleBinding, Identity, create_keyring
from chora_cvm.sync_router import SyncRouter

# Load scenarios from feature file
scenarios("../features/sync_router.feature")


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


@given("a keyring with test bindings")
def keyring_with_bindings(test_context):
    """Create a keyring for testing."""
    test_context["keyring"] = create_keyring("testuser")
    test_context["bindings"] = []


# =============================================================================
# Circle Setup Steps
# =============================================================================


@given(parsers.parse('a circle "{circle_id}" with sync_policy "{policy}" in keyring'))
def circle_with_policy_in_keyring(db_path, test_context, circle_id: str, policy: str):
    """Create a circle and add binding to keyring."""
    # Create circle entity in database
    manifest_entity(
        db_path,
        "circle",
        circle_id,
        {"title": f"Test circle {circle_id}", "sync_policy": policy},
    )
    test_context["circles"].append(circle_id)

    # Add binding to keyring
    binding = CircleBinding(circle_id=circle_id, sync_policy=policy)
    test_context["keyring"].add_binding(binding)


# =============================================================================
# Entity Setup Steps
# =============================================================================


@given(parsers.parse('a learning "{learning_id}" that inhabits "{circle_id}"'))
def learning_inhabits_circle(db_path, test_context, learning_id: str, circle_id: str):
    """Create a learning that inhabits a circle."""
    manifest_entity(
        db_path,
        "learning",
        learning_id,
        {"title": f"Test learning {learning_id}"},
    )
    manage_bond(db_path, "inhabits", learning_id, circle_id)
    test_context["learnings"].append(learning_id)
    test_context["last_learning"] = learning_id


@given(parsers.parse('a learning "{learning_id}" that inhabits both circles'))
def learning_inhabits_both(db_path, test_context, learning_id: str):
    """Create a learning that inhabits both circles."""
    manifest_entity(
        db_path,
        "learning",
        learning_id,
        {"title": f"Test learning {learning_id}"},
    )
    # Inhabit all circles in context
    for circle_id in test_context["circles"]:
        manage_bond(db_path, "inhabits", learning_id, circle_id)
    test_context["learnings"].append(learning_id)
    test_context["last_learning"] = learning_id


@given(parsers.parse('a learning "{learning_id}" with no inhabits bonds'))
def orphan_learning(db_path, test_context, learning_id: str):
    """Create a learning with no circle membership."""
    manifest_entity(
        db_path,
        "learning",
        learning_id,
        {"title": f"Orphan learning {learning_id}"},
    )
    test_context["learnings"].append(learning_id)
    test_context["last_learning"] = learning_id


# =============================================================================
# When Steps
# =============================================================================


@when(parsers.parse('I call should_emit for "{entity_id}"'))
def call_should_emit(db_path, test_context, entity_id: str):
    """Call should_emit on the sync router."""
    store = EventStore(db_path)
    router = SyncRouter(store, test_context["keyring"])
    result = router.should_emit(entity_id)
    store.close()
    test_context["result"] = result


@when(parsers.parse('I call get_cloud_circle_ids for "{entity_id}"'))
def call_get_cloud_circle_ids(db_path, test_context, entity_id: str):
    """Call get_cloud_circle_ids on the sync router."""
    store = EventStore(db_path)
    router = SyncRouter(store, test_context["keyring"])
    result = router.get_cloud_circle_ids(entity_id)
    store.close()
    test_context["result"] = result


@when(parsers.parse('I call get_target_circles for "{entity_id}"'))
def call_get_target_circles(db_path, test_context, entity_id: str):
    """Call get_target_circles on the sync router."""
    store = EventStore(db_path)
    router = SyncRouter(store, test_context["keyring"])
    result = router.get_target_circles(entity_id)
    store.close()
    test_context["result"] = result


# =============================================================================
# Then Steps
# =============================================================================


@then("the result is true")
def check_result_true(test_context):
    """Verify result is True."""
    assert test_context["result"] is True, f"Expected True, got {test_context['result']}"


@then("the result is false")
def check_result_false(test_context):
    """Verify result is False."""
    assert test_context["result"] is False, f"Expected False, got {test_context['result']}"


@then("the result is empty")
def check_result_empty(test_context):
    """Verify result list is empty."""
    result = test_context["result"]
    assert len(result) == 0, f"Expected empty list, got {result}"


@then(parsers.parse('the result contains "{circle_id}"'))
def check_result_contains(test_context, circle_id: str):
    """Verify result contains circle."""
    result = test_context["result"]
    assert circle_id in result, f"Expected {circle_id} in {result}"


@then(parsers.parse('the result does not contain "{circle_id}"'))
def check_result_not_contains(test_context, circle_id: str):
    """Verify result does not contain circle."""
    result = test_context["result"]
    assert circle_id not in result, f"Expected {circle_id} not in {result}"
