"""
Step definitions for the Tools API feature.

These tests verify the /tools endpoint for the Homoiconic Command Palette.
Behavior: behavior-command-palette-shows-cvm-tools-dynamically
"""
import json
import os
import tempfile
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when, parsers
from httpx import Client
from fastapi.testclient import TestClient

from chora_cvm.api import app
from chora_cvm.store import EventStore
from chora_cvm.std import manifest_entity

# Load scenarios from feature file
scenarios("../features/tools_api.feature")


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
# Tool Creation Steps (Given)
# =============================================================================


@given(parsers.parse('a tool entity "{tool_id}" exists with title "{title}"'))
def create_tool_with_title(db_path, test_context, tool_id: str, title: str):
    """Create a tool entity with a title."""
    manifest_entity(
        db_path,
        entity_type="tool",
        entity_id=tool_id,
        data={"title": title},
    )
    test_context[f"actual_{tool_id}"] = tool_id


@given(parsers.parse('a tool entity "{tool_id}" exists with:'))
def create_tool_with_data(db_path, test_context, tool_id: str, datatable):
    """Create a tool entity with data from a table."""
    data = {}
    for row in datatable:
        key = row[0]
        value = row[1]
        data[key] = value

    manifest_entity(
        db_path,
        entity_type="tool",
        entity_id=tool_id,
        data=data,
    )
    test_context[f"actual_{tool_id}"] = tool_id


@given(parsers.parse('a tool entity "{tool_id}" exists with internal flag set'))
def create_internal_tool(db_path, test_context, tool_id: str):
    """Create an internal tool entity."""
    manifest_entity(
        db_path,
        entity_type="tool",
        entity_id=tool_id,
        data={
            "title": tool_id.replace("tool-", "").replace("-", " ").title(),
            "internal": True,
        },
    )
    test_context[f"actual_{tool_id}"] = tool_id


@given(parsers.parse('a tool entity "{tool_id}" exists with status "{status}"'))
def create_tool_with_status(db_path, test_context, tool_id: str, status: str):
    """Create a tool entity with a specific status."""
    manifest_entity(
        db_path,
        entity_type="tool",
        entity_id=tool_id,
        data={
            "title": tool_id.replace("tool-", "").replace("-", " ").title(),
            "status": status,
        },
    )
    test_context[f"actual_{tool_id}"] = tool_id


@given(parsers.parse('a tool entity "{tool_id}" exists with cognition ready_at_hand "{description}"'))
def create_tool_with_cognition(db_path, test_context, tool_id: str, description: str):
    """Create a tool entity with cognition.ready_at_hand."""
    manifest_entity(
        db_path,
        entity_type="tool",
        entity_id=tool_id,
        data={
            "title": tool_id.replace("tool-", "").replace("-", " ").title(),
            "cognition": {"ready_at_hand": description},
        },
    )
    test_context[f"actual_{tool_id}"] = tool_id


# =============================================================================
# When Steps
# =============================================================================


@when("I fetch the tools list")
def fetch_tools_list(api_client, test_context):
    """Fetch tools from the API."""
    response = api_client.get("/tools")
    test_context["response"] = response
    test_context["tools_data"] = response.json()


@when("I fetch the tools list with active_only true")
def fetch_tools_list_active(api_client, test_context):
    """Fetch only active tools from the API."""
    response = api_client.get("/tools?active_only=true")
    test_context["response"] = response
    test_context["tools_data"] = response.json()


# =============================================================================
# Then Steps
# =============================================================================


@then(parsers.parse('the response should contain a tool with id "{tool_id}"'))
def check_response_contains_tool(test_context, tool_id: str):
    """Verify response contains a tool with given ID."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])

    found = any(t.get("id") == tool_id for t in tools)
    assert found, f"Tool {tool_id} not found in response. Got: {[t.get('id') for t in tools]}"


@then(parsers.parse('the response should not contain a tool with id "{tool_id}"'))
def check_response_not_contains_tool(test_context, tool_id: str):
    """Verify response does not contain a tool with given ID."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])

    found = any(t.get("id") == tool_id for t in tools)
    assert not found, f"Tool {tool_id} should not be in response"


@then(parsers.parse('the tool should have title "{title}"'))
def check_tool_has_title(test_context, title: str):
    """Verify the first tool has the expected title."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])

    assert len(tools) > 0, "No tools in response"
    found = any(t.get("title") == title for t in tools)
    assert found, f"Tool with title '{title}' not found. Got: {[t.get('title') for t in tools]}"


@then(parsers.parse('the tool "{tool_id}" should have handler "{handler}"'))
def check_tool_handler(test_context, tool_id: str, handler: str):
    """Verify tool has expected handler."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])

    tool = next((t for t in tools if t.get("id") == tool_id), None)
    assert tool is not None, f"Tool {tool_id} not found"
    assert tool.get("handler") == handler, f"Expected handler {handler}, got {tool.get('handler')}"


@then(parsers.parse('the tool "{tool_id}" should have description "{description}"'))
def check_tool_description(test_context, tool_id: str, description: str):
    """Verify tool has expected description."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])

    tool = next((t for t in tools if t.get("id") == tool_id), None)
    assert tool is not None, f"Tool {tool_id} not found"
    assert tool.get("description") == description, f"Expected description '{description}', got '{tool.get('description')}'"


@then(parsers.parse('the tool "{tool_id}" should have group "{group}"'))
def check_tool_group(test_context, tool_id: str, group: str):
    """Verify tool has expected group."""
    tools_data = test_context.get("tools_data", {})
    tools = tools_data.get("tools", [])

    tool = next((t for t in tools if t.get("id") == tool_id), None)
    assert tool is not None, f"Tool {tool_id} not found"
    assert tool.get("group") == group, f"Expected group '{group}', got '{tool.get('group')}'"
