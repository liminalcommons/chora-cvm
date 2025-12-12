"""
Step definitions for the Sync Bridge feature.

Verifies that SyncBridge correctly routes entity changes to the sync layer
based on circle membership and sync policies.
"""
import json
import tempfile
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.keyring import create_keyring, CircleBinding
from chora_cvm.sync_bridge import SyncBridge

# Load scenarios from feature file
scenarios("../features/sync_bridge.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {
        "callback_changes": [],
        "flushed_changes": [],
    }


@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        yield f.name
    Path(f.name).unlink(missing_ok=True)


# =============================================================================
# Given Steps
# =============================================================================


@given("a fresh EventStore with keyring")
def fresh_store_with_keyring(test_context, temp_db):
    """Create a fresh EventStore and Keyring."""
    store = EventStore(temp_db)

    # Create keyring with no circles yet
    keyring = create_keyring(user_id="test-user")

    test_context["store"] = store
    test_context["keyring"] = keyring
    test_context["db_path"] = temp_db


@given(parsers.parse('an entity "{entity_id}" that inhabits a cloud circle'))
def entity_inhabits_cloud_circle(test_context, entity_id: str):
    """Set up an entity that belongs to a cloud-synced circle."""
    store = test_context["store"]
    keyring = test_context["keyring"]

    circle_id = f"circle-cloud-{entity_id}"

    # Create the circle entity with cloud sync policy
    store.save_generic_entity(circle_id, "circle", {
        "name": "Cloud Circle",
        "sync_policy": "cloud",
    })

    # Create the entity
    store.save_generic_entity(entity_id, "note", {"title": "Initial"})

    # Create inhabits bond (use save_bond to populate bonds table)
    bond_id = f"bond-inhabits-{entity_id}"
    store.save_bond(
        bond_id=bond_id,
        bond_type="inhabits",
        from_id=entity_id,
        to_id=circle_id,
    )

    # Add circle binding to keyring
    keyring.add_binding(CircleBinding(
        circle_id=circle_id,
        sync_policy="cloud",
    ))

    test_context["circle_id"] = circle_id
    test_context["entity_id"] = entity_id


@given(parsers.parse('an entity "{entity_id}" that inhabits a local-only circle'))
def entity_inhabits_local_circle(test_context, entity_id: str):
    """Set up an entity that belongs to a local-only circle."""
    store = test_context["store"]
    keyring = test_context["keyring"]

    circle_id = f"circle-local-{entity_id}"

    # Create the circle entity with local-only sync policy
    store.save_generic_entity(circle_id, "circle", {
        "name": "Local Circle",
        "sync_policy": "local-only",
    })

    # Create the entity
    store.save_generic_entity(entity_id, "note", {"title": "Initial"})

    # Create inhabits bond (use save_bond to populate bonds table)
    bond_id = f"bond-inhabits-{entity_id}"
    store.save_bond(
        bond_id=bond_id,
        bond_type="inhabits",
        from_id=entity_id,
        to_id=circle_id,
    )

    # Add circle binding to keyring
    keyring.add_binding(CircleBinding(
        circle_id=circle_id,
        sync_policy="local-only",
    ))

    test_context["circle_id"] = circle_id
    test_context["entity_id"] = entity_id


@given("a SyncBridge connected to the store")
def sync_bridge_connected(test_context):
    """Create and connect a SyncBridge to the store."""
    store = test_context["store"]
    keyring = test_context["keyring"]

    bridge = SyncBridge(store, keyring, site_id="test-site")
    test_context["bridge"] = bridge


@given("a change callback is registered")
def register_change_callback(test_context):
    """Register a callback for change notifications."""
    bridge = test_context["bridge"]

    def callback(changes):
        test_context["callback_changes"].extend(changes)

    bridge.set_change_callback(callback)


# =============================================================================
# When Steps
# =============================================================================


@when(parsers.parse("I save entity \"{entity_id}\" with data '{data}'"))
def save_entity_with_data(test_context, entity_id: str, data: str):
    """Save an entity with the given data."""
    store = test_context["store"]
    data_dict = json.loads(data)
    store.save_generic_entity(entity_id, "note", data_dict)


@when("I flush the changes")
def flush_changes(test_context):
    """Flush pending changes from the bridge."""
    bridge = test_context["bridge"]
    test_context["flushed_changes"] = bridge.flush_changes()


@when("I close the bridge")
def close_bridge(test_context):
    """Close the bridge (unhook from store)."""
    bridge = test_context["bridge"]
    bridge.close()


# =============================================================================
# Then Steps
# =============================================================================


@then(parsers.parse("the bridge should have {count:d} pending change"))
@then(parsers.parse("the bridge should have {count:d} pending changes"))
def bridge_has_pending_changes(test_context, count: int):
    """Verify the number of pending changes."""
    bridge = test_context["bridge"]
    actual = len(bridge.pending_changes)
    assert actual == count, f"Expected {count} pending changes, got {actual}"


@then(parsers.parse('the pending change should have entity_id "{expected}"'))
def pending_change_has_entity_id(test_context, expected: str):
    """Verify the pending change has the correct entity_id."""
    bridge = test_context["bridge"]
    changes = bridge.pending_changes
    assert len(changes) > 0, "No pending changes"
    actual = changes[0]["entity_id"]
    assert actual == expected, f"Expected entity_id '{expected}', got '{actual}'"


@then("the pending change should have the circle_id")
def pending_change_has_circle_id(test_context):
    """Verify the pending change has the circle_id."""
    bridge = test_context["bridge"]
    changes = bridge.pending_changes
    assert len(changes) > 0, "No pending changes"
    circle_ids = changes[0].get("circle_ids", [])
    expected = test_context["circle_id"]
    assert expected in circle_ids, f"Expected circle_id '{expected}' in {circle_ids}"


@then(parsers.parse("the callback should have been invoked with {count:d} change"))
@then(parsers.parse("the callback should have been invoked with {count:d} changes"))
def callback_invoked_with_changes(test_context, count: int):
    """Verify the callback received the expected number of changes."""
    actual = len(test_context["callback_changes"])
    assert actual == count, f"Expected callback with {count} changes, got {actual}"


@then(parsers.parse("flush should return {count:d} change"))
@then(parsers.parse("flush should return {count:d} changes"))
def flush_returned_changes(test_context, count: int):
    """Verify flush returned the expected number of changes."""
    actual = len(test_context["flushed_changes"])
    assert actual == count, f"Expected flush to return {count} changes, got {actual}"
