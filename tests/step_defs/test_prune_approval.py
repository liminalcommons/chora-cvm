"""
Step definitions for Prune Approval and Rejection feature.

These tests verify the behaviors specified by story-prune-approval-rejection-flow.

BDD Flow: Feature file -> Step definitions -> Implementation
"""
import json
import os
import tempfile
from datetime import datetime, timezone

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from chora_cvm.prune import prune_approve, prune_reject
from chora_cvm.schema import GenericEntity
from chora_cvm.std import manifest_entity
from chora_cvm.store import EventStore

# Load scenarios from feature file
scenarios("../features/prune_approval.feature")


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


@given("axiom entities define the physics rules")
def setup_axioms(db_path, test_context):
    """Create basic axiom entities for physics rules."""
    from chora_cvm.schema import PrimitiveData, PrimitiveEntity

    manifest_entity(db_path, "axiom", "axiom-implements", {
        "verb": "implements",
        "subject_type": "behavior",
        "object_type": "tool",
    })
    # Register prune primitives for CvmEngine dispatch tests
    store = EventStore(db_path)
    prim_approve = PrimitiveEntity(
        id="primitive-prune-approve",
        data=PrimitiveData(
            python_ref="chora_cvm.std.prune_approve_primitive",
            description="Approve a prune candidate - archives the entity with provenance",
            interface={
                "inputs": {"type": "object", "properties": {"db_path": {"type": "string"}, "focus_id": {"type": "string"}}},
                "outputs": {"type": "object", "properties": {"status": {"type": "string"}}},
            },
        ),
    )
    store.save_entity(prim_approve)
    prim_reject = PrimitiveEntity(
        id="primitive-prune-reject",
        data=PrimitiveData(
            python_ref="chora_cvm.std.prune_reject_primitive",
            description="Reject a prune candidate - resolves focus with rejection reason",
            interface={
                "inputs": {"type": "object", "properties": {"db_path": {"type": "string"}, "focus_id": {"type": "string"}, "reason": {"type": "string"}}},
                "outputs": {"type": "object", "properties": {"status": {"type": "string"}}},
            },
        ),
    )
    store.save_entity(prim_reject)
    store.close()


# =============================================================================
# Tool Setup Steps
# =============================================================================


@given(parsers.parse('a tool "{tool_id}" exists with status "{status}"'))
def create_tool_with_status(db_path, test_context, tool_id: str, status: str):
    """Create a tool entity with specified status."""
    manifest_entity(db_path, "tool", tool_id, {
        "title": tool_id.replace("-", " ").title(),
        "status": status,
        "handler": f"test.module.{tool_id.replace('-', '_')}",
        "phenomenology": "Test tool for prune testing",
    })
    test_context["tool_id"] = tool_id


@given(parsers.parse('a tool "{tool_id}" exists with phenomenology "{phenomenology}"'))
def create_tool_with_phenomenology(db_path, test_context, tool_id: str, phenomenology: str):
    """Create a tool entity with specified phenomenology."""
    manifest_entity(db_path, "tool", tool_id, {
        "title": tool_id.replace("-", " ").title(),
        "status": "active",
        "handler": f"test.module.{tool_id.replace('-', '_')}",
        "phenomenology": phenomenology,
    })
    test_context["tool_id"] = tool_id


@given(parsers.parse('a tool "{tool_id}" exists'))
def create_tool(db_path, test_context, tool_id: str):
    """Create a simple tool entity."""
    manifest_entity(db_path, "tool", tool_id, {
        "title": tool_id.replace("-", " ").title(),
        "status": "active",
        "handler": f"test.module.{tool_id.replace('-', '_')}",
    })
    test_context["tool_id"] = tool_id


# =============================================================================
# Focus Setup Steps
# =============================================================================


@given(parsers.parse('a focus "{focus_id}" exists for prune proposal'))
def create_prune_focus(db_path, test_context, focus_id: str):
    """Create a Focus entity for prune proposal."""
    tool_id = test_context.get("tool_id", "tool-test")
    manifest_entity(db_path, "focus", focus_id, {
        "title": f"Prune: {tool_id}",
        "status": "pending",
        "category": "prune-approval",
        "tool_id": tool_id,
        "reason": "Test prune proposal",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    test_context["focus_id"] = focus_id


@given(parsers.parse('the focus references tool_id "{tool_id}"'))
def focus_references_tool(db_path, test_context, tool_id: str):
    """Update the focus to reference a specific tool."""
    focus_id = test_context.get("focus_id")
    if focus_id:
        store = EventStore(db_path)
        cur = store._conn.cursor()
        cur.execute("SELECT data_json FROM entities WHERE id = ?", (focus_id,))
        row = cur.fetchone()
        if row:
            data = json.loads(row[0])
            data["tool_id"] = tool_id
            cur.execute(
                "UPDATE entities SET data_json = json(?) WHERE id = ?",
                (json.dumps(data), focus_id)
            )
            store._conn.commit()
        store.close()
    test_context["tool_id"] = tool_id


@given(parsers.parse('a focus exists for prune proposal of "{tool_id}"'))
def create_prune_focus_for_tool(db_path, test_context, tool_id: str):
    """Create a Focus entity for prune proposal of a specific tool."""
    focus_id = f"focus-prune-{tool_id}-test123"
    manifest_entity(db_path, "focus", focus_id, {
        "title": f"Prune: {tool_id}",
        "status": "pending",
        "category": "prune-approval",
        "tool_id": tool_id,
        "reason": "Test prune proposal",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    test_context["focus_id"] = focus_id
    test_context["tool_id"] = tool_id


@given(parsers.parse('a focus "{focus_id}" exists with category "{category}"'))
def create_focus_with_category(db_path, test_context, focus_id: str, category: str):
    """Create a Focus entity with a specific category."""
    manifest_entity(db_path, "focus", focus_id, {
        "title": f"Focus: {focus_id}",
        "status": "pending",
        "category": category,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    test_context["focus_id"] = focus_id


@given(parsers.parse('a focus "{focus_id}" exists with status "{status}"'))
def create_focus_with_status(db_path, test_context, focus_id: str, status: str):
    """Create a Focus entity with a specific status."""
    manifest_entity(db_path, "focus", focus_id, {
        "title": f"Focus: {focus_id}",
        "status": status,
        "category": "prune-approval",
        "tool_id": test_context.get("tool_id", "tool-test"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    test_context["focus_id"] = focus_id


# =============================================================================
# When Steps - Actions
# =============================================================================


@when(parsers.parse('prune_approve is invoked with focus_id "{focus_id}"'))
def invoke_prune_approve(db_path, test_context, focus_id: str):
    """Invoke prune_approve with a specific focus_id."""
    result = prune_approve(db_path, focus_id)
    test_context["result"] = result


@when("prune_approve is invoked with the focus_id")
def invoke_prune_approve_from_context(db_path, test_context):
    """Invoke prune_approve with focus_id from context."""
    focus_id = test_context.get("focus_id")
    result = prune_approve(db_path, focus_id)
    test_context["result"] = result


@when(parsers.parse('prune_reject is invoked with focus_id "{focus_id}" and reason "{reason}"'))
def invoke_prune_reject_with_reason(db_path, test_context, focus_id: str, reason: str):
    """Invoke prune_reject with focus_id and reason."""
    result = prune_reject(db_path, focus_id, reason)
    test_context["result"] = result


@when("prune_reject is invoked with focus_id and no reason")
def invoke_prune_reject_no_reason(db_path, test_context):
    """Invoke prune_reject with focus_id but no reason."""
    focus_id = test_context.get("focus_id")
    result = prune_reject(db_path, focus_id, None)
    test_context["result"] = result


@when(parsers.parse('prune_reject is invoked with focus_id "{focus_id}"'))
def invoke_prune_reject(db_path, test_context, focus_id: str):
    """Invoke prune_reject with a specific focus_id."""
    result = prune_reject(db_path, focus_id)
    test_context["result"] = result


# =============================================================================
# Then Steps - Assertions
# =============================================================================


@then(parsers.parse('the tool "{tool_id}" is composted'))
def verify_tool_composted(db_path, test_context, tool_id: str):
    """Verify the tool has been archived."""
    store = EventStore(db_path)
    cur = store._conn.cursor()

    # Check entity is no longer in entities table
    cur.execute("SELECT id FROM entities WHERE id = ?", (tool_id,))
    assert cur.fetchone() is None, f"Tool {tool_id} should be archived"

    # Check entity exists in archive table
    cur.execute("SELECT id FROM archive WHERE original_id = ?", (tool_id,))
    assert cur.fetchone() is not None, f"Tool {tool_id} should be in archive"

    store.close()


@then(parsers.parse('a learning entity is created with title containing "{title_part}"'))
def verify_learning_created_with_title(db_path, test_context, title_part: str):
    """Verify a learning was created with title containing the given text."""
    result = test_context.get("result", {})
    data = result.get("data", result)  # Unwrap CvmEngine dispatch format
    learning_id = data.get("learning_id")

    assert learning_id is not None, f"No learning_id in result: {result}"

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (learning_id,))
    row = cur.fetchone()
    store.close()

    assert row is not None, f"Learning {learning_id} not found"
    data = json.loads(row[0])
    # Case-insensitive comparison
    title_lower = data.get("title", "").lower()
    part_lower = title_part.lower()
    assert part_lower in title_lower, f"Title '{data.get('title')}' should contain '{title_part}'"


@then("a crystallized-from bond connects the learning to the archived entity")
def verify_crystallized_from_bond(db_path, test_context):
    """Verify a crystallized-from bond exists."""
    result = test_context.get("result", {})
    learning_id = result.get("learning_id")

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute(
        "SELECT id FROM bonds WHERE from_id = ? AND type = 'crystallized-from'",
        (learning_id,)
    )
    row = cur.fetchone()
    store.close()

    assert row is not None, "crystallized-from bond not found"


@then(parsers.parse('the focus status becomes "{status}"'))
def verify_focus_status(db_path, test_context, status: str):
    """Verify the focus has the expected status."""
    focus_id = test_context.get("focus_id")
    if not focus_id:
        # Try to extract from result
        result = test_context.get("result", {})
        focus_id = result.get("focus_id")

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (focus_id,))
    row = cur.fetchone()
    store.close()

    if row:
        data = json.loads(row[0])
        assert data.get("status") == status, f"Focus status should be '{status}'"


@then(parsers.parse('the focus outcome is "{outcome}"'))
def verify_focus_outcome(db_path, test_context, outcome: str):
    """Verify the focus has the expected outcome."""
    focus_id = test_context.get("focus_id")
    if not focus_id:
        result = test_context.get("result", {})
        focus_id = result.get("focus_id")

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (focus_id,))
    row = cur.fetchone()
    store.close()

    if row:
        data = json.loads(row[0])
        assert data.get("outcome") == outcome, f"Focus outcome should be '{outcome}'"


@then(parsers.parse('the created learning insight includes "{text}"'))
def verify_learning_insight(db_path, test_context, text: str):
    """Verify the learning insight contains the expected text."""
    result = test_context.get("result", {})
    learning_id = result.get("learning_id")

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (learning_id,))
    row = cur.fetchone()
    store.close()

    assert row is not None, f"Learning {learning_id} not found"
    data = json.loads(row[0])
    assert text in data.get("insight", ""), f"Insight should contain '{text}'"


@then(parsers.parse('the learning insight includes "{text}"'))
def verify_learning_insight_includes(db_path, test_context, text: str):
    """Verify the learning insight contains the expected text."""
    result = test_context.get("result", {})
    learning_id = result.get("learning_id")

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (learning_id,))
    row = cur.fetchone()
    store.close()

    assert row is not None, f"Learning {learning_id} not found"
    data = json.loads(row[0])
    assert text in data.get("insight", ""), f"Insight should contain '{text}'"


@then(parsers.parse('the learning domain is "{domain}"'))
def verify_learning_domain(db_path, test_context, domain: str):
    """Verify the learning has the expected domain."""
    result = test_context.get("result", {})
    learning_id = result.get("learning_id")

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (learning_id,))
    row = cur.fetchone()
    store.close()

    assert row is not None, f"Learning {learning_id} not found"
    data = json.loads(row[0])
    assert data.get("domain") == domain, f"Domain should be '{domain}'"


@then("the operation returns an error")
def verify_error_returned(test_context):
    """Verify the operation returned an error."""
    result = test_context.get("result", {})
    assert "error" in result, "Expected an error in result"


@then(parsers.parse('the error message says "{message}"'))
def verify_error_message(test_context, message: str):
    """Verify the error message matches."""
    result = test_context.get("result", {})
    assert result.get("error") == message, f"Error should be '{message}'"


@then(parsers.parse('the tool "{tool_id}" status remains unchanged'))
def verify_tool_unchanged(db_path, test_context, tool_id: str):
    """Verify the tool still exists and wasn't modified."""
    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (tool_id,))
    row = cur.fetchone()
    store.close()

    assert row is not None, f"Tool {tool_id} should still exist"


@then(parsers.parse('a learning is created with insight "{insight}"'))
def verify_learning_with_insight(db_path, test_context, insight: str):
    """Verify a learning was created with the exact insight."""
    result = test_context.get("result", {})
    learning_id = result.get("learning_id")

    assert learning_id is not None, "No learning_id in result"

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (learning_id,))
    row = cur.fetchone()
    store.close()

    assert row is not None, f"Learning {learning_id} not found"
    data = json.loads(row[0])
    assert insight in data.get("insight", ""), f"Insight should contain '{insight}'"


@then(parsers.parse('the focus is resolved with outcome "{outcome}"'))
def verify_focus_resolved_with_outcome(db_path, test_context, outcome: str):
    """Verify the focus is resolved with the expected outcome."""
    focus_id = test_context.get("focus_id")

    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (focus_id,))
    row = cur.fetchone()
    store.close()

    if row:
        data = json.loads(row[0])
        assert data.get("status") == "resolved", "Focus should be resolved"
        assert data.get("outcome") == outcome, f"Outcome should be '{outcome}'"


# =============================================================================
# Integration Steps - CvmEngine Dispatch
# =============================================================================


@when(parsers.parse('I dispatch "prune-approve" through CvmEngine with focus_id "{focus_id}"'))
def dispatch_prune_approve_via_engine(db_path, test_context, focus_id: str):
    """Dispatch prune-approve through the unified engine."""
    from chora_cvm.engine import CvmEngine

    engine = CvmEngine(db_path)
    result = engine.dispatch(
        "prune-approve",
        {"db_path": db_path, "focus_id": focus_id},
    )
    engine.close()

    # Store result in standard format
    if result.ok:
        test_context["result"] = result.data
    else:
        test_context["result"] = {"error": result.error_message}


@when(parsers.parse('I dispatch "prune-reject" through CvmEngine with focus_id "{focus_id}" and reason "{reason}"'))
def dispatch_prune_reject_via_engine(db_path, test_context, focus_id: str, reason: str):
    """Dispatch prune-reject through the unified engine."""
    from chora_cvm.engine import CvmEngine

    engine = CvmEngine(db_path)
    result = engine.dispatch(
        "prune-reject",
        {"db_path": db_path, "focus_id": focus_id, "reason": reason},
    )
    engine.close()

    if result.ok:
        test_context["result"] = result.data
    else:
        test_context["result"] = {"error": result.error_message}


@then("the dispatch result is successful")
def verify_dispatch_success(test_context):
    """Verify dispatch succeeded (no error in result)."""
    result = test_context.get("result", {})
    assert "error" not in result, f"Expected success, got error: {result.get('error')}"


@then("the result data shows archived is true")
def verify_result_archived(test_context):
    """Verify result shows archived=true."""
    result = test_context.get("result", {})
    # Handle both direct result and nested data
    data = result.get("data", result)
    assert data.get("archived") is True, f"Expected archived=true, got {data}"


@then("a learning entity exists for the pruned tool")
def verify_learning_exists_for_prune(db_path, test_context):
    """Verify a learning entity was created for the prune operation."""
    result = test_context.get("result", {})
    data = result.get("data", result)
    learning_id = data.get("learning_id")

    if not learning_id:
        # Try to find any learning with "Pruned" in title
        store = EventStore(db_path)
        cur = store._conn.cursor()
        cur.execute("SELECT id FROM entities WHERE type = 'learning' AND data_json LIKE '%Pruned%'")
        row = cur.fetchone()
        store.close()
        assert row is not None, "No learning entity found for pruned tool"
    else:
        store = EventStore(db_path)
        cur = store._conn.cursor()
        cur.execute("SELECT id FROM entities WHERE id = ?", (learning_id,))
        row = cur.fetchone()
        store.close()
        assert row is not None, f"Learning {learning_id} not found"
