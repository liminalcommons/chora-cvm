"""
Step definitions for Prune Detection feature.

These tests verify the behaviors specified by:
- behavior-prune-detects-orphan-and-deprecated-tools
- behavior-prune-emits-signals-for-threshold-breaches
- behavior-prune-proposes-focus-for-human-approval

Pattern: pattern-tool-creation (pilot validation)
"""
import json
import os
import tempfile
from pathlib import Path

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from chora_cvm.prune import (
    PruneReport,
    detect_prunable,
    emit_prune_signals,
    propose_prune,
)
from chora_cvm.std import manage_bond, manifest_entity
from chora_cvm.store import EventStore

# Load scenarios from feature file
scenarios("../features/prune.feature")


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
def src_dir():
    """Return the chora_cvm source directory for handler validation."""
    return Path(__file__).parent.parent.parent / "src" / "chora_cvm"


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
    # The implements axiom: behavior -> tool
    manifest_entity(db_path, "axiom", "axiom-implements", {
        "verb": "implements",
        "subject_type": "behavior",
        "object_type": "tool",
    })


# =============================================================================
# Tool Setup Steps
# =============================================================================


@given(parsers.parse('a tool "{tool_id}" exists with no implements bond'))
def create_orphan_tool(db_path, test_context, tool_id: str):
    """Create a tool entity with no behavior implementing it."""
    manifest_entity(db_path, "tool", tool_id, {
        "title": tool_id.replace("-", " ").title(),
        "status": "active",
        "handler": f"test.module.{tool_id.replace('-', '_')}",
    })
    test_context.setdefault("tools", []).append(tool_id)


@given(parsers.parse('a tool "{tool_id}" exists'))
def create_tool(db_path, test_context, tool_id: str):
    """Create a simple tool entity."""
    manifest_entity(db_path, "tool", tool_id, {
        "title": tool_id.replace("-", " ").title(),
        "status": "active",
        "handler": f"test.module.{tool_id.replace('-', '_')}",
    })
    test_context["tool_id"] = tool_id
    test_context.setdefault("tools", []).append(tool_id)


@given(parsers.parse('a tool "{tool_id}" exists with status "{status}"'))
def create_tool_with_status(db_path, test_context, tool_id: str, status: str):
    """Create a tool entity with specified status."""
    manifest_entity(db_path, "tool", tool_id, {
        "title": tool_id.replace("-", " ").title(),
        "status": status,
        "handler": f"test.module.{tool_id.replace('-', '_')}",
    })
    test_context["tool_id"] = tool_id
    test_context.setdefault("tools", []).append(tool_id)


@given(parsers.parse('the tool has deprecated_at "{deprecated_at}"'))
def set_deprecated_at(db_path, test_context, deprecated_at: str):
    """Update tool with deprecated_at timestamp."""
    tool_id = test_context.get("tool_id")
    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT data_json FROM entities WHERE id = ?", (tool_id,))
    row = cur.fetchone()
    if row:
        data = json.loads(row[0])
        data["deprecated_at"] = deprecated_at
        cur.execute(
            "UPDATE entities SET data_json = json(?) WHERE id = ?",
            (json.dumps(data), tool_id)
        )
        store._conn.commit()
    store.close()
    test_context["deprecated_at"] = deprecated_at


@given(parsers.parse('a behavior "{behavior_id}" implements "{tool_id}"'))
def create_implements_bond(db_path, test_context, behavior_id: str, tool_id: str):
    """Create a behavior and wire it to a tool via implements bond."""
    # Create the behavior
    manifest_entity(db_path, "behavior", behavior_id, {
        "title": behavior_id.replace("-", " ").title(),
        "given": "a context",
        "when": "an action",
        "then": "an outcome",
    })
    # Create the implements bond (behavior -> tool)
    manage_bond(db_path, "implements", behavior_id, tool_id)
    test_context.setdefault("behaviors", []).append(behavior_id)


@given(parsers.parse("{count:d} orphan tools exist"))
def create_multiple_orphan_tools(db_path, test_context, count: int):
    """Create multiple orphan tools."""
    for i in range(count):
        tool_id = f"tool-orphan-{i}"
        manifest_entity(db_path, "tool", tool_id, {
            "title": f"Orphan Tool {i}",
            "status": "active",
            "handler": f"test.module.orphan_{i}",
        })
        test_context.setdefault("tools", []).append(tool_id)


# =============================================================================
# When Steps - Actions
# =============================================================================


@when("detect_prunable is invoked")
def invoke_detect_prunable(db_path, test_context, src_dir):
    """Invoke detect_prunable and store the report."""
    report = detect_prunable(db_path, src_dir)
    test_context["report"] = report


@when("emit_prune_signals is invoked")
def invoke_emit_signals(db_path, test_context, src_dir):
    """Invoke emit_prune_signals after detection."""
    # First detect
    report = detect_prunable(db_path, src_dir)
    test_context["report"] = report
    # Then emit signals
    signals = emit_prune_signals(db_path, report, dry_run=False)
    test_context["signals"] = signals


@when("emit_prune_signals is invoked with dry_run")
def invoke_emit_signals_dry_run(db_path, test_context, src_dir):
    """Invoke emit_prune_signals in dry_run mode."""
    report = detect_prunable(db_path, src_dir)
    test_context["report"] = report
    signals = emit_prune_signals(db_path, report, dry_run=True)
    test_context["signals"] = signals


@when("propose_prune is invoked")
def invoke_propose_prune(db_path, test_context, src_dir):
    """Invoke propose_prune after detection."""
    report = detect_prunable(db_path, src_dir)
    test_context["report"] = report
    focuses = propose_prune(db_path, report, dry_run=False)
    test_context["focuses"] = focuses


@when("propose_prune is invoked with dry_run")
def invoke_propose_prune_dry_run(db_path, test_context, src_dir):
    """Invoke propose_prune in dry_run mode."""
    report = detect_prunable(db_path, src_dir)
    test_context["report"] = report
    focuses = propose_prune(db_path, report, dry_run=True)
    test_context["focuses"] = focuses


# =============================================================================
# Then Steps - Assertions
# =============================================================================


@then(parsers.parse('the report includes "{tool_id}" in orphan_tools'))
def verify_in_orphan_tools(test_context, tool_id: str):
    """Verify a tool appears in orphan_tools."""
    report: PruneReport = test_context.get("report")
    orphan_ids = [t.id for t in report.orphan_tools]
    assert tool_id in orphan_ids, f"{tool_id} should be in orphan_tools: {orphan_ids}"


@then(parsers.parse('the report does not include "{tool_id}" in orphan_tools'))
def verify_not_in_orphan_tools(test_context, tool_id: str):
    """Verify a tool does not appear in orphan_tools."""
    report: PruneReport = test_context.get("report")
    orphan_ids = [t.id for t in report.orphan_tools]
    assert tool_id not in orphan_ids, f"{tool_id} should not be in orphan_tools"


@then(parsers.parse('the report includes "{tool_id}" in deprecated_tools'))
def verify_in_deprecated_tools(test_context, tool_id: str):
    """Verify a tool appears in deprecated_tools."""
    report: PruneReport = test_context.get("report")
    deprecated_ids = [t.id for t in report.deprecated_tools]
    assert tool_id in deprecated_ids, f"{tool_id} should be in deprecated_tools"


@then(parsers.parse('the report does not include "{tool_id}" in deprecated_tools'))
def verify_not_in_deprecated_tools(test_context, tool_id: str):
    """Verify a tool does not appear in deprecated_tools."""
    report: PruneReport = test_context.get("report")
    deprecated_ids = [t.id for t in report.deprecated_tools]
    assert tool_id not in deprecated_ids, f"{tool_id} should not be in deprecated_tools"


@then(parsers.parse('the deprecation reason is "{reason}"'))
def verify_deprecation_reason(test_context, reason: str):
    """Verify the deprecation reason matches."""
    report: PruneReport = test_context.get("report")
    tool_id = test_context.get("tool_id")
    for tool in report.deprecated_tools:
        if tool.id == tool_id:
            assert tool.reason == reason, f"Reason should be '{reason}'"
            return
    pytest.fail(f"Tool {tool_id} not found in deprecated_tools")


@then(parsers.parse('the deprecation reason starts with "{prefix}"'))
def verify_deprecation_reason_starts_with(test_context, prefix: str):
    """Verify the deprecation reason starts with expected prefix."""
    report: PruneReport = test_context.get("report")
    tool_id = test_context.get("tool_id")
    for tool in report.deprecated_tools:
        if tool.id == tool_id:
            assert tool.reason.startswith(prefix), f"Reason should start with '{prefix}', got '{tool.reason}'"
            return
    pytest.fail(f"Tool {tool_id} not found in deprecated_tools")


@then(parsers.parse('a signal entity is created with category "{category}"'))
def verify_signal_created(db_path, test_context, category: str):
    """Verify a signal was created with the given category."""
    signals = test_context.get("signals", [])
    categories = [s.get("category") for s in signals]
    assert category in categories, f"Expected signal with category '{category}', got {categories}"


@then(parsers.parse("the signal count is {count:d}"))
def verify_signal_count(test_context, count: int):
    """Verify the signal count field."""
    signals = test_context.get("signals", [])
    for sig in signals:
        if sig.get("count") == count:
            return
    pytest.fail(f"Expected signal with count {count}, got {[s.get('count') for s in signals]}")


@then(parsers.parse('no signal is emitted for "{category}"'))
def verify_no_signal_for_category(test_context, category: str):
    """Verify no signal was emitted for the given category."""
    signals = test_context.get("signals", [])
    categories = [s.get("category") for s in signals]
    assert category not in categories, f"Should not emit signal for '{category}'"


@then("no signal entities are created")
def verify_no_signals(db_path, test_context):
    """Verify no signal entities exist in the database."""
    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT COUNT(*) FROM entities WHERE type = 'signal'")
    count = cur.fetchone()[0]
    store.close()
    assert count == 0, f"Expected 0 signals, found {count}"


@then(parsers.parse('a focus entity is created for "{tool_id}"'))
def verify_focus_created_for_tool(db_path, test_context, tool_id: str):
    """Verify a focus was created for the given tool."""
    focuses = test_context.get("focuses", [])
    tool_ids = [f.get("tool_id") for f in focuses]
    assert tool_id in tool_ids, f"Expected focus for '{tool_id}', got {tool_ids}"


@then(parsers.parse('the focus category is "{category}"'))
def verify_focus_category(test_context, category: str):
    """Verify the focus has the expected category."""
    focuses = test_context.get("focuses", [])
    assert len(focuses) > 0, "No focuses created"
    assert focuses[0].get("category") == category, f"Category should be '{category}'"


@then("the focus references the tool_id")
def verify_focus_references_tool(test_context):
    """Verify the focus has a tool_id reference."""
    focuses = test_context.get("focuses", [])
    assert len(focuses) > 0, "No focuses created"
    assert "tool_id" in focuses[0], "Focus should have tool_id"


@then("no focus entities are created")
def verify_no_focuses(db_path, test_context):
    """Verify no focus entities exist in the database."""
    store = EventStore(db_path)
    cur = store._conn.cursor()
    cur.execute("SELECT COUNT(*) FROM entities WHERE type = 'focus'")
    count = cur.fetchone()[0]
    store.close()
    assert count == 0, f"Expected 0 focuses, found {count}"
