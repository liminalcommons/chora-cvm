"""
Step definitions for the Convergence Scanner feature.

These tests verify the behaviors specified by story-system-suggests-convergences.
Convergence is docs as a converging force — not just finding what's wrong,
but suggesting what wants to connect.
"""
import json
import os
import tempfile
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import (
    manifest_entity,
    manage_bond,
    scan_convergences,
)

# Load scenarios from feature file
scenarios("../features/convergence.feature")


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
# Entity Creation Steps
# =============================================================================


@given(parsers.parse('a learning entity "{learning_id}" with no bonds'))
def create_orphan_learning(db_path, test_context, learning_id: str):
    """Create a learning with no bonds."""
    manifest_entity(
        db_path,
        entity_type="learning",
        entity_id=learning_id,
        data={"title": f"Orphan learning: {learning_id}", "insight": "No connections"},
    )
    test_context["learning_id"] = learning_id


@given(parsers.parse('a learning entity "{learning_id}" that surfaces to a principle'))
def create_surfaced_learning(db_path, test_context, learning_id: str):
    """Create a learning that already surfaces to a principle."""
    # Create principle first
    principle_id = f"principle-for-{learning_id}"
    manifest_entity(
        db_path,
        entity_type="principle",
        entity_id=principle_id,
        data={"statement": "A principle for testing"},
    )

    # Create learning
    manifest_entity(
        db_path,
        entity_type="learning",
        entity_id=learning_id,
        data={"title": f"Connected learning: {learning_id}", "insight": "Has a surfaces bond"},
    )

    # Create surfaces bond
    manage_bond(db_path, "surfaces", learning_id, principle_id)
    test_context["learning_id"] = learning_id


@given(parsers.parse('a learning "{learning_id}" with title "{title}"'))
def create_learning_with_title(db_path, test_context, learning_id: str, title: str):
    """Create a learning with specific title."""
    manifest_entity(
        db_path,
        entity_type="learning",
        entity_id=learning_id,
        data={"title": title, "insight": f"Insight from {title}"},
    )
    test_context["learning_id"] = learning_id


@given(parsers.parse('a principle "{principle_id}" with statement "{statement}"'))
def create_principle_with_statement(db_path, test_context, principle_id: str, statement: str):
    """Create a principle with specific statement."""
    manifest_entity(
        db_path,
        entity_type="principle",
        entity_id=principle_id,
        data={"statement": statement},
    )
    test_context["principle_id"] = principle_id


@given(parsers.parse('a learning "{learning_id}" that surfaces to "{principle_id}"'))
def create_learning_surfacing_to_principle(db_path, test_context, learning_id: str, principle_id: str):
    """Create a learning that already surfaces to a specific principle."""
    # Create principle
    manifest_entity(
        db_path,
        entity_type="principle",
        entity_id=principle_id,
        data={"statement": f"Principle {principle_id}"},
    )

    # Create learning
    manifest_entity(
        db_path,
        entity_type="learning",
        entity_id=learning_id,
        data={"title": f"Learning {learning_id}"},
    )

    # Create surfaces bond
    manage_bond(db_path, "surfaces", learning_id, principle_id)
    test_context["learning_id"] = learning_id


@given(parsers.parse('a behavior "{behavior_id}" with title "{title}"'))
def create_behavior_with_title(db_path, test_context, behavior_id: str, title: str):
    """Create a behavior with specific title."""
    manifest_entity(
        db_path,
        entity_type="behavior",
        entity_id=behavior_id,
        data={"title": title, "given": "context", "when": "trigger", "then": "effect"},
    )
    test_context["behavior_id"] = behavior_id


@given(parsers.parse('a tool "{tool_id}" with title "{title}"'))
def create_tool_with_title(db_path, test_context, tool_id: str, title: str):
    """Create a tool with specific title."""
    manifest_entity(
        db_path,
        entity_type="tool",
        entity_id=tool_id,
        data={"title": title, "handler": f"chora_cvm.std.{tool_id}"},
    )
    test_context["tool_id"] = tool_id


@given(parsers.parse('a behavior "{behavior_id}" that is verified by "{tool_id}"'))
def create_verified_behavior(db_path, test_context, behavior_id: str, tool_id: str):
    """Create a behavior that is already verified by a tool."""
    # Create tool
    manifest_entity(
        db_path,
        entity_type="tool",
        entity_id=tool_id,
        data={"title": f"Tool {tool_id}"},
    )

    # Create behavior
    manifest_entity(
        db_path,
        entity_type="behavior",
        entity_id=behavior_id,
        data={"title": f"Behavior {behavior_id}"},
    )

    # Create verifies bond
    manage_bond(db_path, "verifies", tool_id, behavior_id)
    test_context["behavior_id"] = behavior_id


@given(parsers.parse("an entity with {count:d} outgoing bonds"))
def create_entity_with_bonds(db_path, test_context, count: int):
    """Create an entity with specific number of outgoing bonds."""
    entity_id = "entity-with-bonds"
    manifest_entity(
        db_path,
        entity_type="learning",
        entity_id=entity_id,
        data={"title": "Well-connected entity"},
    )

    # Create targets and bonds
    for i in range(count):
        target_id = f"principle-target-{i}"
        manifest_entity(
            db_path,
            entity_type="principle",
            entity_id=target_id,
            data={"statement": f"Target principle {i}"},
        )
        manage_bond(db_path, "surfaces", entity_id, target_id)

    test_context["entity_id"] = entity_id


@given("an entity with no bonds")
def create_isolated_entity(db_path, test_context):
    """Create an entity with no bonds."""
    entity_id = "entity-isolated"
    manifest_entity(
        db_path,
        entity_type="learning",
        entity_id=entity_id,
        data={"title": "Isolated entity"},
    )
    test_context["entity_id"] = entity_id


@given(parsers.parse('a principle "{principle_id}" with matching keywords'))
def create_matching_principle(db_path, test_context, principle_id: str):
    """Create a principle with keywords that match the learning."""
    # Use same keywords as the lonely learning
    manifest_entity(
        db_path,
        entity_type="principle",
        entity_id=principle_id,
        data={"statement": "Orphan entities need connections in the system"},
    )
    test_context["principle_id"] = principle_id


# =============================================================================
# Action Steps
# =============================================================================


@when("I scan for convergence opportunities")
def scan_convergences_action(db_path, test_context):
    """Run the convergence scanner."""
    result = scan_convergences(db_path, emit_signals=False)
    test_context["scan_result"] = result


@when("I scan for convergence opportunities with emit_signals enabled")
def scan_convergences_with_signals(db_path, test_context):
    """Run the convergence scanner with signal emission."""
    result = scan_convergences(db_path, emit_signals=True)
    test_context["scan_result"] = result


@when("I compute its coherence score")
def compute_coherence_score(db_path, test_context):
    """Compute coherence score for an entity."""
    import sqlite3
    entity_id = test_context.get("entity_id")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Count bonds
    cur = conn.execute(
        "SELECT COUNT(*) as count FROM bonds WHERE from_id = ? OR to_id = ?",
        (entity_id, entity_id)
    )
    row = cur.fetchone()
    conn.close()

    # Simple coherence score based on bond count
    test_context["coherence_score"] = row["count"] if row else 0


# =============================================================================
# Assertion Steps
# =============================================================================


@then(parsers.parse("the result should report {count:d} unsurfaced learning"))
@then(parsers.parse("the result should report {count:d} unsurfaced learnings"))
def check_unsurfaced_count(test_context, count: int):
    """Verify unsurfaced learning count."""
    result = test_context.get("scan_result", {})
    actual = result.get("unsurfaced_learnings", 0)
    assert actual == count, f"Expected {count} unsurfaced learnings, got {actual}"


@then(parsers.parse('a suggestion should exist for "{from_id}" to surface to "{to_id}"'))
def check_surfaces_suggestion(test_context, from_id: str, to_id: str):
    """Verify a surfaces suggestion exists."""
    result = test_context.get("scan_result", {})
    suggestions = result.get("suggestions", [])

    found = any(
        s["from_id"] == from_id and s["to_id"] == to_id and s["bond_type"] == "surfaces"
        for s in suggestions
    )
    assert found, f"No surfaces suggestion from {from_id} to {to_id} found in {suggestions}"


@then(parsers.parse('no suggestion should exist for "{entity_id}"'))
def check_no_suggestion(test_context, entity_id: str):
    """Verify no suggestion exists for an entity."""
    result = test_context.get("scan_result", {})
    suggestions = result.get("suggestions", [])

    found = any(
        s["from_id"] == entity_id or s["to_id"] == entity_id
        for s in suggestions
    )
    assert not found, f"Unexpected suggestion found for {entity_id}"


@then(parsers.parse('a suggestion should exist for "{tool_id}" to verify "{behavior_id}"'))
def check_verifies_suggestion(test_context, tool_id: str, behavior_id: str):
    """Verify a verifies suggestion exists."""
    result = test_context.get("scan_result", {})
    suggestions = result.get("suggestions", [])

    found = any(
        s["from_id"] == tool_id and s["to_id"] == behavior_id and s["bond_type"] == "verifies"
        for s in suggestions
    )
    assert found, f"No verifies suggestion from {tool_id} to {behavior_id} found"


@then("the score should be greater than 0")
def check_score_positive(test_context):
    """Verify coherence score is positive."""
    score = test_context.get("coherence_score", 0)
    assert score > 0, f"Expected positive score, got {score}"


@then("the score should be 0")
def check_score_zero(test_context):
    """Verify coherence score is zero."""
    score = test_context.get("coherence_score", 0)
    assert score == 0, f"Expected score 0, got {score}"


@then(parsers.parse('a signal of type "{signal_type}" should be emitted'))
def check_signal_emitted(db_path, test_context, signal_type: str):
    """Verify a signal was emitted."""
    result = test_context.get("scan_result", {})
    signals = result.get("signals", [])
    assert len(signals) > 0, f"No signals emitted. Result: {result}"

    # Verify signal exists in database with correct type
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
        if data.get("signal_type") == signal_type:
            found = True
            break

    assert found, f"No signal of type {signal_type} found in database"


@then("the emitted signal should contain from_id and to_id")
def check_signal_contains_bond_details(db_path, test_context):
    """Verify emitted signal contains bond suggestion details."""
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
        signal_data = data.get("data", {})
        if "from_id" in signal_data and "to_id" in signal_data:
            found = True
            break

    assert found, "No signal with from_id and to_id found"
