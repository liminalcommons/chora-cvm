"""
Step definitions for the Focus Lifecycle feature.

These tests verify the behaviors specified by story-attention-declares-what-matters.
Focus is plasma - the energy of attention in the system.
"""
import json
import os
import tempfile
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import (
    create_focus,
    resolve_focus,
    list_active_focuses,
    emit_signal,
    manifest_entity,
)

# Load scenarios from feature file
scenarios("../features/focus.feature")


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
# Focus Creation Steps
# =============================================================================


@when(parsers.parse('I create a focus with title "{title}"'))
def create_focus_with_title(db_path, test_context, title: str):
    """Create a focus entity."""
    result = create_focus(db_path, title=title)
    test_context["focus_result"] = result
    test_context["focus_id"] = result.get("id")


@when(parsers.parse('I create a focus with title "{title}" triggered by "{signal_id}"'))
def create_focus_with_signal(db_path, test_context, title: str, signal_id: str):
    """Create a focus triggered by a signal."""
    result = create_focus(db_path, title=title, signal_id=signal_id)
    test_context["focus_result"] = result
    test_context["focus_id"] = result.get("id")


@then(parsers.parse('a focus entity should exist with title "{title}"'))
def check_focus_exists(db_path, title: str):
    """Verify focus entity exists."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE type = 'focus'"
    )
    rows = cur.fetchall()
    conn.close()

    found = False
    for row in rows:
        data = json.loads(row["data_json"])
        if data.get("title") == title:
            found = True
            break

    assert found, f"Focus with title '{title}' not found"


@then(parsers.parse('the focus status should be "{status}"'))
def check_focus_status(db_path, test_context, status: str):
    """Verify focus has expected status."""
    import sqlite3
    focus_id = test_context.get("focus_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"Focus {focus_id} not found"
    data = json.loads(row["data_json"])
    assert data.get("status") == status, f"Expected status {status}, got {data.get('status')}"


@then("the focus should have an engaged_at timestamp")
def check_focus_engaged_at(db_path, test_context):
    """Verify focus has engaged_at timestamp."""
    import sqlite3
    focus_id = test_context.get("focus_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert "engaged_at" in data, "Focus missing engaged_at timestamp"


@then(parsers.parse('the focus should have triggered_by "{signal_id}"'))
def check_focus_triggered_by(db_path, test_context, signal_id: str):
    """Verify focus has triggered_by field."""
    import sqlite3
    focus_id = test_context.get("focus_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert data.get("triggered_by") == signal_id, f"Expected triggered_by {signal_id}"


# =============================================================================
# Focus Resolution Steps
# =============================================================================


@given(parsers.parse('an active focus "{focus_id}" exists'))
def create_active_focus(db_path, test_context, focus_id: str):
    """Create an active focus for testing."""
    result = create_focus(db_path, title=focus_id.replace("focus-", "").replace("-", " ").title())
    # Store the actual ID that was created
    test_context[f"actual_{focus_id}"] = result.get("id")
    test_context["focus_id"] = result.get("id")


@given(parsers.parse('a resolved focus "{focus_id}" exists'))
def create_resolved_focus(db_path, test_context, focus_id: str):
    """Create a resolved focus for testing."""
    result = create_focus(db_path, title=focus_id.replace("focus-", "").replace("-", " ").title())
    actual_id = result.get("id")
    resolve_focus(db_path, actual_id, outcome="completed")
    test_context[f"actual_{focus_id}"] = actual_id


@given(parsers.parse('an active focus "{focus_id}" exists for persona "{persona_id}"'))
def create_focus_for_persona(db_path, test_context, focus_id: str, persona_id: str):
    """Create an active focus for a specific persona."""
    result = create_focus(
        db_path,
        title=focus_id.replace("focus-", "").replace("-", " ").title(),
        persona_id=persona_id,
    )
    test_context[f"actual_{focus_id}"] = result.get("id")


@when(parsers.parse('I resolve focus "{focus_id}" with outcome "{outcome}"'))
def resolve_focus_with_outcome(db_path, test_context, focus_id: str, outcome: str):
    """Resolve a focus with an outcome."""
    actual_id = test_context.get(f"actual_{focus_id}", focus_id)
    result = resolve_focus(db_path, actual_id, outcome=outcome)
    test_context["resolve_result"] = result
    test_context["focus_id"] = actual_id


@when(parsers.parse('I resolve focus "{focus_id}" with learning "{learning_title}"'))
def resolve_focus_with_learning(db_path, test_context, focus_id: str, learning_title: str):
    """Resolve a focus and create a learning."""
    actual_id = test_context.get(f"actual_{focus_id}", focus_id)
    result = resolve_focus(
        db_path,
        actual_id,
        outcome="completed",
        learning_title=learning_title,
        learning_insight=f"Insight from: {learning_title}",
    )
    test_context["resolve_result"] = result
    test_context["focus_id"] = actual_id
    test_context["learning_id"] = result.get("learning_id")


@then("the focus should have a resolved_at timestamp")
def check_focus_resolved_at(db_path, test_context):
    """Verify focus has resolved_at timestamp."""
    import sqlite3
    focus_id = test_context.get("focus_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert "resolved_at" in data, "Focus missing resolved_at timestamp"


@then("a learning entity should be created")
def check_learning_created(db_path, test_context):
    """Verify learning entity was created."""
    learning_id = test_context.get("learning_id")
    assert learning_id is not None, "No learning_id in result"

    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, type FROM entities WHERE id = ?",
        (learning_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"Learning {learning_id} not found"
    assert row["type"] == "learning", f"Expected type 'learning', got {row['type']}"


@then("the learning should reference the focus")
def check_learning_references_focus(db_path, test_context):
    """Verify learning references the focus."""
    learning_id = test_context.get("learning_id")

    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (learning_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert "surfaced_from" in data, "Learning should reference focus via surfaced_from"


# =============================================================================
# Focus Listing Steps
# =============================================================================


@when("I list active focuses")
def list_all_focuses(db_path, test_context):
    """List all active focuses."""
    result = list_active_focuses(db_path)
    test_context["list_result"] = result


@when(parsers.parse('I list active focuses for persona "{persona_id}"'))
def list_focuses_for_persona(db_path, test_context, persona_id: str):
    """List active focuses for a persona."""
    result = list_active_focuses(db_path, persona_id=persona_id)
    test_context["list_result"] = result


@then(parsers.parse('the result should contain "{focus_id}"'))
def check_result_contains(test_context, focus_id: str):
    """Verify result contains expected focus."""
    result = test_context.get("list_result", {})
    focuses = result.get("focuses", [])
    actual_id = test_context.get(f"actual_{focus_id}", focus_id)

    found = any(f.get("id") == actual_id for f in focuses)
    assert found, f"Focus {actual_id} not found in result"


@then(parsers.parse('the result should not contain "{focus_id}"'))
def check_result_not_contains(test_context, focus_id: str):
    """Verify result does not contain focus."""
    result = test_context.get("list_result", {})
    focuses = result.get("focuses", [])
    actual_id = test_context.get(f"actual_{focus_id}", focus_id)

    found = any(f.get("id") == actual_id for f in focuses)
    assert not found, f"Focus {actual_id} should not be in result"


# =============================================================================
# Signal Emission Steps
# =============================================================================


@given(parsers.parse('an active signal "{signal_id}" exists'))
def create_active_signal(db_path, test_context, signal_id: str):
    """Create an active signal for testing."""
    result = emit_signal(
        db_path,
        title=signal_id.replace("signal-", "").replace("-", " ").title(),
        signal_type="test",
    )
    test_context[f"actual_{signal_id}"] = result.get("id")


@given(parsers.parse('a tool entity "{tool_id}" exists'))
def create_tool_entity(db_path, test_context, tool_id: str):
    """Create a tool entity for testing."""
    manifest_entity(
        db_path,
        entity_type="tool",
        entity_id=tool_id,
        data={
            "title": tool_id.replace("tool-", "").replace("-", " ").title(),
            "handler": f"chora_cvm.std.{tool_id}",
        },
    )
    test_context[f"actual_{tool_id}"] = tool_id


@when(parsers.parse('I emit a signal with title "{title}"'))
def emit_signal_with_title(db_path, test_context, title: str):
    """Emit a signal."""
    result = emit_signal(db_path, title=title)
    test_context["signal_result"] = result
    test_context["signal_id"] = result.get("id")


@when(parsers.parse('I emit a signal with title "{title}" and urgency "{urgency}"'))
def emit_signal_with_urgency(db_path, test_context, title: str, urgency: str):
    """Emit a signal with urgency."""
    result = emit_signal(db_path, title=title, urgency=urgency)
    test_context["signal_result"] = result
    test_context["signal_id"] = result.get("id")


@when(parsers.parse('I emit a signal with title "{title}" from source "{source_id}"'))
def emit_signal_from_source(db_path, test_context, title: str, source_id: str):
    """Emit a signal from a source entity."""
    actual_source = test_context.get(f"actual_{source_id}", source_id)
    result = emit_signal(db_path, title=title, source_id=actual_source)
    test_context["signal_result"] = result
    test_context["signal_id"] = result.get("id")


@then(parsers.parse('a signal entity should exist with title "{title}"'))
def check_signal_exists(db_path, title: str):
    """Verify signal entity exists."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE type = 'signal'"
    )
    rows = cur.fetchall()
    conn.close()

    found = False
    for row in rows:
        data = json.loads(row["data_json"])
        if data.get("title") == title:
            found = True
            break

    assert found, f"Signal with title '{title}' not found"


@then(parsers.parse('the signal status should be "{status}"'))
def check_signal_status(db_path, test_context, status: str):
    """Verify signal has expected status."""
    import sqlite3
    signal_id = test_context.get("signal_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (signal_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert data.get("status") == status, f"Expected status {status}, got {data.get('status')}"


@then("the signal should have an emitted_at timestamp")
def check_signal_emitted_at(db_path, test_context):
    """Verify signal has emitted_at timestamp."""
    import sqlite3
    signal_id = test_context.get("signal_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (signal_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert "emitted_at" in data, "Signal missing emitted_at timestamp"


@then(parsers.parse('the signal urgency should be "{urgency}"'))
def check_signal_urgency(db_path, test_context, urgency: str):
    """Verify signal has expected urgency."""
    import sqlite3
    signal_id = test_context.get("signal_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (signal_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert data.get("urgency") == urgency, f"Expected urgency {urgency}, got {data.get('urgency')}"


@then(parsers.parse('the signal source_id should be "{source_id}"'))
def check_signal_source(db_path, test_context, source_id: str):
    """Verify signal has expected source."""
    import sqlite3
    signal_id = test_context.get("signal_id")
    actual_source = test_context.get(f"actual_{source_id}", source_id)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (signal_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert data.get("source_id") == actual_source, f"Expected source {actual_source}"
