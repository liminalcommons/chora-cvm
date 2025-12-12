"""
Step definitions for the Stigmergic Layout feature.

These tests verify layout entity management and signal-triggered mutations.
Behavior: behavior-layout-entity-responds-to-signal-urgency
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
from chora_cvm.std import manifest_entity, emit_signal

# Load scenarios from feature file
scenarios("../features/stigmergic_layout.feature")


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
# Layout Entity Creation Steps
# =============================================================================


@when("I create a layout entity with:")
def create_layout_entity(api_client, test_context, datatable):
    """Create a layout entity with data from a table."""
    data = {
        "mode": "split",
        "panels": {
            "context": True,
            "events": True,
            "signals": True,
            "artifacts": True,
            "workflows": True,
        }
    }

    for row in datatable:
        key = row[0]
        value = row[1]

        # Parse boolean values
        if value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False

        # Handle nested keys like "panels.context"
        if "." in key:
            parts = key.split(".")
            if parts[0] == "panels":
                data["panels"][parts[1]] = value
        else:
            data[key] = value

    response = api_client.post(
        "/entities",
        json={
            "type": "pattern",
            "id": "pattern-hud-layout-default",
            "data": data,
        }
    )
    test_context["response"] = response
    test_context["response_data"] = response.json() if response.status_code == 200 else None


@given("a layout entity exists with signals panel closed")
def layout_with_signals_closed(db_path, test_context):
    """Create a layout entity with signals panel closed."""
    data = {
        "mode": "split",
        "panels": {
            "context": True,
            "events": True,
            "signals": False,
            "artifacts": True,
            "workflows": True,
        }
    }
    manifest_entity(
        db_path,
        entity_type="pattern",
        entity_id="pattern-hud-layout-default",
        data=data,
    )
    test_context["layout_id"] = "pattern-hud-layout-default"


@given(parsers.parse('a layout entity exists with mode "{mode}"'))
def layout_with_mode(db_path, test_context, mode: str):
    """Create a layout entity with specified mode."""
    data = {
        "mode": mode,
        "panels": {
            "context": True,
            "events": True,
            "signals": False,
            "artifacts": True,
            "workflows": True,
        }
    }
    manifest_entity(
        db_path,
        entity_type="pattern",
        entity_id="pattern-hud-layout-default",
        data=data,
    )
    test_context["layout_id"] = "pattern-hud-layout-default"


# =============================================================================
# When Steps - Layout Operations
# =============================================================================


@when("I fetch the layout endpoint")
def fetch_layout(api_client, test_context):
    """Fetch the layout from the API."""
    response = api_client.get("/layout")
    test_context["layout_response"] = response
    if response.status_code == 200:
        test_context["layout_data"] = response.json()


@when(parsers.parse('a signal with urgency "{urgency}" is emitted'))
def emit_signal_with_urgency(db_path, test_context, urgency: str):
    """Emit a signal with specified urgency."""
    result = emit_signal(
        db_path=db_path,
        title="Test signal",
        urgency=urgency,
    )
    test_context["signal_result"] = result


@when(parsers.parse('I update the layout to mode "{mode}"'))
def update_layout_mode(api_client, test_context, mode: str):
    """Update the layout mode."""
    response = api_client.patch(
        "/layout",
        json={"mode": mode}
    )
    test_context["update_response"] = response


# =============================================================================
# Then Steps - Assertions
# =============================================================================


@then("the response should be successful")
def check_response_successful(test_context):
    """Verify response was successful."""
    response = test_context.get("response")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"


@then(parsers.parse('a layout entity "{entity_id}" should exist'))
def check_layout_exists(db_path, entity_id: str):
    """Verify layout entity exists in database."""
    store = EventStore(db_path)
    entity = store.get_entity(entity_id)
    store.close()
    assert entity is not None, f"Layout entity {entity_id} not found"


@then("the response should include panel configuration")
def check_panel_config(test_context):
    """Verify response includes panel configuration."""
    layout_data = test_context.get("layout_data")
    assert layout_data is not None, "No layout data in response"
    assert "panels" in layout_data, "No panels in layout data"


@then("the signals panel should be closed")
def check_signals_panel_closed(test_context):
    """Verify signals panel is closed."""
    layout_data = test_context.get("layout_data")
    assert layout_data is not None, "No layout data"
    panels = layout_data.get("panels", {})
    assert panels.get("signals") is False, f"Expected signals panel closed, got {panels.get('signals')}"


@then("the signals panel should be open")
def check_signals_panel_open(test_context):
    """Verify signals panel is open."""
    layout_data = test_context.get("layout_data")
    assert layout_data is not None, "No layout data"
    panels = layout_data.get("panels", {})
    assert panels.get("signals") is True, f"Expected signals panel open, got {panels.get('signals')}"


@then(parsers.parse('the layout mode should be "{mode}"'))
def check_layout_mode(test_context, mode: str):
    """Verify layout mode."""
    layout_data = test_context.get("layout_data")
    assert layout_data is not None, "No layout data"
    assert layout_data.get("mode") == mode, f"Expected mode '{mode}', got '{layout_data.get('mode')}'"
