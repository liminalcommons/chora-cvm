"""
Step definitions for the Circle Sync Policy feature.

These tests verify the behaviors specified by story-circles-declare-their-sync-boundary.
Circles declare sync_policy (local-only vs cloud) as the foundation for routing.
"""
import json
import os
import tempfile

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import manifest_entity

# Load scenarios from feature file
scenarios("../features/circle_sync_policy.feature")


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


# =============================================================================
# Circle Creation Steps
# =============================================================================


@given(parsers.parse('I create a circle "{circle_name}" with sync_policy "{sync_policy}"'))
def create_circle_with_policy(db_path, test_context, circle_name: str, sync_policy: str):
    """Create a circle with specified sync_policy."""
    circle_id = f"circle-{circle_name}"
    manifest_entity(
        db_path,
        "circle",
        circle_id,
        {"title": f"Test circle {circle_name}", "sync_policy": sync_policy},
    )
    test_context["circles"].append(circle_id)
    test_context["last_circle"] = circle_id


@given(parsers.parse('I create a circle "{circle_name}" without specifying sync_policy'))
def create_circle_without_policy(db_path, test_context, circle_name: str):
    """Create a circle without specifying sync_policy (should default to local-only)."""
    circle_id = f"circle-{circle_name}"
    manifest_entity(
        db_path,
        "circle",
        circle_id,
        {"title": f"Test circle {circle_name}"},
    )
    test_context["circles"].append(circle_id)
    test_context["last_circle"] = circle_id


# =============================================================================
# Query Steps
# =============================================================================


@when(parsers.parse('I query is_local_only for "{circle_id}"'))
def query_is_local_only(db_path, test_context, circle_id: str):
    """Query if a circle is local-only."""
    full_id = f"circle-{circle_id}" if not circle_id.startswith("circle-") else circle_id
    store = EventStore(db_path)
    result = store.is_local_only(full_id)
    store.close()
    test_context["query_result"] = result


@when(parsers.parse('I query the circle data for "{circle_name}"'))
def query_circle_data(db_path, test_context, circle_name: str):
    """Query the data for a circle."""
    circle_id = f"circle-{circle_name}"
    store = EventStore(db_path)
    entity = store.get_entity(circle_id)
    store.close()
    test_context["circle_data"] = entity.get("data", {}) if entity else {}


@when("I query get_local_only_circles")
def query_local_only_circles(db_path, test_context):
    """Query all local-only circles."""
    store = EventStore(db_path)
    circles = store.get_local_only_circles()
    store.close()
    test_context["query_result"] = circles


@when("I query get_cloud_circles")
def query_cloud_circles(db_path, test_context):
    """Query all cloud circles."""
    store = EventStore(db_path)
    circles = store.get_cloud_circles()
    store.close()
    test_context["query_result"] = circles


# =============================================================================
# Assertion Steps
# =============================================================================


@then("the result is true")
def check_result_true(test_context):
    """Verify result is True."""
    result = test_context.get("query_result")
    assert result is True, f"Expected True, got {result}"


@then("the result is false")
def check_result_false(test_context):
    """Verify result is False."""
    result = test_context.get("query_result")
    assert result is False, f"Expected False, got {result}"


@then(parsers.parse('the sync_policy field is "{expected_policy}"'))
def check_sync_policy(test_context, expected_policy: str):
    """Verify sync_policy field value."""
    data = test_context.get("circle_data", {})
    actual = data.get("sync_policy")
    assert actual == expected_policy, f"Expected sync_policy '{expected_policy}', got '{actual}'"


@then(parsers.parse('the result contains "{circle_id}"'))
def check_result_contains(test_context, circle_id: str):
    """Verify result list contains the circle."""
    result = test_context.get("query_result", [])
    assert circle_id in result, f"Expected {circle_id} in {result}"


@then(parsers.parse('the result does not contain "{circle_id}"'))
def check_result_not_contains(test_context, circle_id: str):
    """Verify result list does not contain the circle."""
    result = test_context.get("query_result", [])
    assert circle_id not in result, f"Expected {circle_id} not in {result}"
