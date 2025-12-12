"""
Step definitions for the Entity Save Hooks feature.

Verifies that the EventStore fires hooks when entities are saved,
enabling the sync layer to observe changes.
"""
import json
import tempfile
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.schema import PrimitiveEntity, PrimitiveData

# Load scenarios from feature file
scenarios("../features/entity_hooks.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {
        "hook_calls": [],
        "hook_calls_2": [],
        "hook_removed": False,
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


@given("a fresh EventStore database")
def fresh_store(test_context, temp_db):
    """Create a fresh EventStore."""
    test_context["store"] = EventStore(temp_db)
    test_context["db_path"] = temp_db


@given("a registered entity save hook")
def register_hook(test_context):
    """Register an entity save hook that records calls."""
    store = test_context["store"]

    def hook(entity_id: str, entity_type: str, data: dict) -> None:
        # Verify entity exists in database at hook time
        entity = store.get_entity(entity_id)
        test_context["entity_exists_at_hook_time"] = entity is not None
        test_context["hook_calls"].append({
            "entity_id": entity_id,
            "entity_type": entity_type,
            "data": data,
        })

    test_context["hook"] = hook
    store.add_entity_hook(hook)


@given("two registered entity save hooks")
def register_two_hooks(test_context):
    """Register two entity save hooks."""
    store = test_context["store"]

    def hook1(entity_id: str, entity_type: str, data: dict) -> None:
        test_context["hook_calls"].append({"hook": 1})

    def hook2(entity_id: str, entity_type: str, data: dict) -> None:
        test_context["hook_calls_2"].append({"hook": 2})

    test_context["hook1"] = hook1
    test_context["hook2"] = hook2
    store.add_entity_hook(hook1)
    store.add_entity_hook(hook2)


@given("the hook is removed")
def remove_hook(test_context):
    """Remove the registered hook."""
    store = test_context["store"]
    hook = test_context["hook"]
    store.remove_entity_hook(hook)
    test_context["hook_removed"] = True


# =============================================================================
# When Steps
# =============================================================================


@when(parsers.parse("I save an entity with id \"{entity_id}\" type \"{entity_type}\" and data '{data}'"))
def save_entity(test_context, entity_id: str, entity_type: str, data: str):
    """Save an entity using save_generic_entity."""
    store = test_context["store"]
    data_dict = json.loads(data)
    store.save_generic_entity(entity_id, entity_type, data_dict)


@when("I save a PrimitiveEntity via save_entity")
def save_pydantic_entity(test_context):
    """Save a Pydantic entity using save_entity."""
    store = test_context["store"]
    entity = PrimitiveEntity(
        id="primitive-test",
        data=PrimitiveData(
            python_ref="test.module.function",
            description="Test primitive",
            interface={},
        ),
    )
    store.save_entity(entity)


# =============================================================================
# Then Steps
# =============================================================================


@then("the hook should be called once")
def hook_called_once(test_context):
    """Verify hook was called exactly once."""
    assert len(test_context["hook_calls"]) == 1, (
        f"Expected 1 hook call, got {len(test_context['hook_calls'])}"
    )


@then(parsers.parse('the hook should receive entity_id "{expected}"'))
def hook_receives_entity_id(test_context, expected: str):
    """Verify hook received correct entity_id."""
    actual = test_context["hook_calls"][0]["entity_id"]
    assert actual == expected, f"Expected entity_id '{expected}', got '{actual}'"


@then(parsers.parse('the hook should receive entity_type "{expected}"'))
def hook_receives_entity_type(test_context, expected: str):
    """Verify hook received correct entity_type."""
    actual = test_context["hook_calls"][0]["entity_type"]
    assert actual == expected, f"Expected entity_type '{expected}', got '{actual}'"


@then(parsers.parse('the hook should receive data containing title "{expected}"'))
def hook_receives_data_title(test_context, expected: str):
    """Verify hook received data with correct title."""
    actual = test_context["hook_calls"][0]["data"].get("title")
    assert actual == expected, f"Expected title '{expected}', got '{actual}'"


@then("the entity should exist in the database before hook completes")
def entity_exists_at_hook_time(test_context):
    """Verify entity was committed before hook was called."""
    assert test_context.get("entity_exists_at_hook_time", False), (
        "Entity did not exist in database when hook was called"
    )


@then("both hooks should be called")
def both_hooks_called(test_context):
    """Verify both hooks were called."""
    assert len(test_context["hook_calls"]) == 1, "First hook not called"
    assert len(test_context["hook_calls_2"]) == 1, "Second hook not called"


@then("the hook should not be called")
def hook_not_called(test_context):
    """Verify hook was not called."""
    assert len(test_context["hook_calls"]) == 0, (
        f"Hook was called {len(test_context['hook_calls'])} times"
    )
