"""
Step definitions for the Autopoietic Tools feature.

These tests verify tool entity creation via API for the Command Palette.
Behavior: behavior-voice-command-manifests-tool-entity-that-appears-in-palette
"""
import json
import os
import tempfile
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when, parsers
from fastapi.testclient import TestClient

from chora_cvm.api import app
from chora_cvm.store import EventStore

# Load scenarios from feature file
scenarios("../features/autopoietic_tools.feature")


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


@pytest.fixture
def api_client(db_path, monkeypatch):
    """Create a test client with the database path set."""
    monkeypatch.setenv("CHORA_DB", db_path)
    # Also patch the module-level variable
    import chora_cvm.api as api_module
    monkeypatch.setattr(api_module, "DEFAULT_DB_PATH", db_path)
    return TestClient(app)


# =============================================================================
# Background Steps
# =============================================================================


@given("a fresh CVM database")
def fresh_database(db_path, test_context):
    """Set up a fresh database for testing."""
    test_context["db_path"] = db_path


# =============================================================================
# When Steps - Tool Creation
# =============================================================================


@when(parsers.parse('I create a tool entity with title "{title}" and handler "{handler}"'))
def create_tool_simple(api_client, test_context, title: str, handler: str):
    """Create a tool entity with minimal data."""
    response = api_client.post(
        "/entities",
        json={
            "type": "tool",
            "data": {
                "title": title,
                "handler": handler,
            }
        }
    )
    test_context["response"] = response
    test_context["response_data"] = response.json() if response.status_code == 200 else None
    test_context["created_title"] = title


@given(parsers.parse('I create a tool entity with title "{title}" and handler "{handler}"'))
def given_create_tool_simple(api_client, test_context, title: str, handler: str):
    """Create a tool entity with minimal data (as a Given step)."""
    create_tool_simple(api_client, test_context, title, handler)


@when("I create a tool entity with:")
def create_tool_with_data(api_client, test_context, datatable):
    """Create a tool entity with data from a table."""
    data = {}
    for row in datatable:
        key = row[0]
        value = row[1]
        # Parse boolean for internal flag
        if key == "internal":
            value = value.lower() == "true"
        data[key] = value

    response = api_client.post(
        "/entities",
        json={
            "type": "tool",
            "data": data,
        }
    )
    test_context["response"] = response
    test_context["response_data"] = response.json() if response.status_code == 200 else None
    test_context["created_title"] = data.get("title")


@when("I fetch the tools list")
def fetch_tools_list(api_client, test_context):
    """Fetch tools from the API."""
    response = api_client.get("/tools")
    test_context["tools_response"] = response
    test_context["tools_data"] = response.json()


# =============================================================================
# Then Steps - Assertions
# =============================================================================


@then("the response should be successful")
def check_response_successful(test_context):
    """Verify response was successful."""
    response = test_context.get("response")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"


@then(parsers.parse('a tool entity "{entity_id}" should exist in the database'))
def check_tool_exists(db_path, entity_id: str):
    """Verify tool entity exists in database."""
    store = EventStore(db_path)
    entity = store.get_entity(entity_id)
    store.close()
    assert entity is not None, f"Tool entity {entity_id} not found"


@then(parsers.parse('the tool should have handler "{handler}"'))
def check_tool_handler(test_context, handler: str):
    """Verify tool has expected handler."""
    response_data = test_context.get("response_data")
    assert response_data is not None, "No response data"

    # Get the entity ID from response and check in database
    entity_id = response_data.get("id")
    assert entity_id is not None, "No entity ID in response"

    db_path = test_context.get("db_path")
    store = EventStore(db_path)
    entity = store.get_entity(entity_id)
    store.close()

    assert entity is not None, f"Entity {entity_id} not found"
    data = entity["data"]  # EventStore.get_entity() returns parsed JSON as 'data'
    assert data.get("handler") == handler, f"Expected handler '{handler}', got '{data.get('handler')}'"


@then(parsers.parse('the response should contain a tool with title "{title}"'))
def check_tools_contains_title(test_context, title: str):
    """Verify tools list contains tool with given title."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])

    found = any(t.get("title") == title for t in tools)
    assert found, f"Tool with title '{title}' not found. Got: {[t.get('title') for t in tools]}"


@then(parsers.parse('the response should not contain a tool with title "{title}"'))
def check_tools_not_contains_title(test_context, title: str):
    """Verify tools list does not contain tool with given title."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])

    found = any(t.get("title") == title for t in tools)
    assert not found, f"Tool with title '{title}' should not be in response"


@then(parsers.parse('the tool should have group "{group}"'))
def check_tool_group(test_context, group: str):
    """Verify tool has expected group."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])
    title = test_context.get("created_title")

    tool = next((t for t in tools if t.get("title") == title), None)
    assert tool is not None, f"Tool with title '{title}' not found"
    assert tool.get("group") == group, f"Expected group '{group}', got '{tool.get('group')}'"


@then(parsers.parse('the tool should appear in tools list with description "{description}"'))
def check_tool_description(api_client, test_context, description: str):
    """Verify tool appears in list with expected description."""
    # Fetch tools list
    response = api_client.get("/tools")
    tools_data = response.json()
    tools = tools_data.get("tools", [])
    title = test_context.get("created_title")

    tool = next((t for t in tools if t.get("title") == title), None)
    assert tool is not None, f"Tool with title '{title}' not found"
    assert tool.get("description") == description, f"Expected description '{description}', got '{tool.get('description')}'"


@then(parsers.parse('the tool should appear in tools list with group "{group}"'))
def check_tool_in_list_group(api_client, test_context, group: str):
    """Verify tool appears in list with expected group."""
    response = api_client.get("/tools")
    tools_data = response.json()
    tools = tools_data.get("tools", [])
    title = test_context.get("created_title")

    tool = next((t for t in tools if t.get("title") == title), None)
    assert tool is not None, f"Tool with title '{title}' not found"
    assert tool.get("group") == group, f"Expected group '{group}', got '{tool.get('group')}'"


@then(parsers.parse('the tool should appear in tools list with shortcut "{shortcut}"'))
def check_tool_shortcut(api_client, test_context, shortcut: str):
    """Verify tool appears in list with expected shortcut."""
    response = api_client.get("/tools")
    tools_data = response.json()
    tools = tools_data.get("tools", [])
    title = test_context.get("created_title")

    tool = next((t for t in tools if t.get("title") == title), None)
    assert tool is not None, f"Tool with title '{title}' not found"
    assert tool.get("shortcut") == shortcut, f"Expected shortcut '{shortcut}', got '{tool.get('shortcut')}'"
