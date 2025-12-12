"""
Step definitions for the System Integrity Truth feature.

These tests verify the behaviors specified by story-system-integrity-truth.
The system tells the truth about its own verification status.
"""
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.schema import GenericEntity
from chora_cvm.std import manifest_entity, manage_bond

# Load scenarios from feature file
scenarios("../features/integrity.feature")


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

    store = EventStore(path)
    store.close()

    yield path

    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def temp_features_dir():
    """Create a temporary directory for test feature files."""
    import tempfile
    import shutil

    temp_dir = tempfile.mkdtemp(prefix="test_features_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


# =============================================================================
# Background Steps
# =============================================================================


@given("a fresh CVM database")
def fresh_database(db_path, test_context):
    """Set up a fresh database for testing."""
    test_context["db_path"] = db_path
    test_context["behaviors"] = []
    test_context["feature_files"] = []


# =============================================================================
# Behavior Setup Steps
# =============================================================================


@given(parsers.parse('behavior "{behavior_id}" exists'))
def create_behavior(db_path, test_context, behavior_id: str):
    """Create a behavior entity."""
    manifest_entity(
        db_path,
        "behavior",
        behavior_id,
        {
            "title": f"Test behavior {behavior_id}",
            "status": "active",
            "given": "some context",
            "when": "some action",
            "then": "some outcome",
        },
    )
    test_context.setdefault("behaviors", []).append(behavior_id)


@given(parsers.parse('a feature file exists with @behavior:{tag} tag'))
def create_feature_with_tag(temp_features_dir, test_context, tag: str):
    """Create a feature file with a behavior tag."""
    feature_content = f'''@behavior:{tag}
Feature: Test Feature
  Scenario: Test scenario
    Given something
    Then something else
'''
    feature_path = Path(temp_features_dir) / f"test_{tag}.feature"
    feature_path.write_text(feature_content)
    test_context["features_dir"] = temp_features_dir
    test_context.setdefault("feature_files", []).append(str(feature_path))


@given(parsers.parse('the pulse.feature file has @behavior:pulse-processes-signals tag'))
def pulse_feature_has_tag(test_context):
    """Reference the existing pulse.feature file."""
    # The actual pulse.feature already has this tag
    test_context["real_features_dir"] = str(Path(__file__).parent.parent / "features")


@given("these behaviors exist:")
def create_behaviors_from_table(db_path, test_context, datatable):
    """Create multiple behaviors from a data table."""
    # pytest-bdd passes datatable as list of lists: [[headers], [row1], [row2], ...]
    # Convert to list of dicts
    headers = datatable[0]
    rows = [dict(zip(headers, row)) for row in datatable[1:]]

    for row in rows:
        behavior_id = row["behavior_id"]
        manifest_entity(
            db_path,
            "behavior",
            behavior_id,
            {
                "title": f"Test behavior {behavior_id}",
                "status": "active",
            },
        )
        test_context.setdefault("behaviors", []).append(behavior_id)
        # Store test expectations
        test_context.setdefault("expectations", {})[behavior_id] = {
            "has_scenarios": row.get("has_scenarios") == "true",
            "test_result": row.get("test_result"),
        }


@given(parsers.parse("{count:d} behaviors exist"))
def create_n_behaviors(db_path, test_context, count: int):
    """Create N behavior entities."""
    for i in range(count):
        behavior_id = f"behavior-test-{i}"
        manifest_entity(
            db_path,
            "behavior",
            behavior_id,
            {"title": f"Test behavior {i}", "status": "active"},
        )
        test_context.setdefault("behaviors", []).append(behavior_id)


@given(parsers.parse("{count:d} have passing tests"))
def mark_passing_tests(test_context, count: int):
    """Mark some behaviors as having passing tests."""
    test_context["passing_count"] = count


@given(parsers.parse("{count:d} have failing tests"))
def mark_failing_tests(test_context, count: int):
    """Mark some behaviors as having failing tests."""
    test_context["failing_count"] = count


@given(parsers.parse("{count:d} have no tests"))
def mark_no_tests(test_context, count: int):
    """Mark some behaviors as having no tests."""
    test_context["no_tests_count"] = count


# =============================================================================
# Tool and Bond Setup Steps
# =============================================================================


@given(parsers.parse('tool "{tool_id}" exists'))
def create_tool(db_path, test_context, tool_id: str):
    """Create a tool entity."""
    store = EventStore(db_path)
    entity = GenericEntity(
        id=tool_id,
        type="tool",
        data={
            "title": f"Test tool {tool_id}",
            "handler": "test.handler",
        },
    )
    store.save_entity(entity)
    store.close()


@given(parsers.parse('a verifies bond from {tool_id} to {behavior_id}'))
def create_verifies_bond(db_path, tool_id: str, behavior_id: str):
    """Create a verifies bond from tool to behavior."""
    manage_bond(
        db_path,
        "verifies",
        tool_id,
        behavior_id,
        enforce_physics=False,
    )


@given("a verifies bond exists")
def create_generic_verifies_bond(db_path, test_context):
    """Create a generic verifies bond for testing."""
    test_context["verifies_bond_tool"] = "tool-test"
    test_context["verifies_bond_behavior"] = "behavior-test"

    # Create the entities first
    create_tool(db_path, test_context, "tool-test")
    create_behavior(db_path, test_context, "behavior-test")
    create_verifies_bond(db_path, "tool-test", "behavior-test")


@given(parsers.parse('a feature file with passing scenario for that behavior'))
def create_passing_feature(temp_features_dir, test_context):
    """Create a feature file with a passing test."""
    feature_content = '''@behavior:with-passing-test
Feature: Passing Test
  Scenario: Always passes
    Given a true condition
    Then it passes
'''
    feature_path = Path(temp_features_dir) / "passing.feature"
    feature_path.write_text(feature_content)
    test_context["features_dir"] = temp_features_dir


@given(parsers.parse('a feature file with failing scenario for that behavior'))
def create_failing_feature(temp_features_dir, test_context):
    """Create a feature file with a failing test."""
    feature_content = '''@behavior:with-failing-test
Feature: Failing Test
  Scenario: Always fails
    Given a false condition
    Then it fails
'''
    feature_path = Path(temp_features_dir) / "failing.feature"
    feature_path.write_text(feature_content)
    test_context["features_dir"] = temp_features_dir


# =============================================================================
# Action Steps
# =============================================================================


@when("I run integrity discovery")
def run_integrity_discovery(db_path, test_context):
    """Run integrity discovery to map behaviors to scenarios."""
    from chora_cvm.std import integrity_discover_scenarios

    features_dir = test_context.get("features_dir") or test_context.get("real_features_dir")
    result = integrity_discover_scenarios(db_path, features_dir)
    test_context["discovery_result"] = result


@when(parsers.parse("I run integrity check with execute={execute}"))
def run_integrity_check_with_execute(db_path, test_context, execute: str):
    """Run integrity check, optionally executing tests."""
    from chora_cvm.std import integrity_check

    features_dir = test_context.get("features_dir")
    result = integrity_check(db_path, features_dir, execute=(execute == "true"))
    test_context["integrity_result"] = result


@when("I view the integrity report")
def view_integrity_report(db_path, test_context):
    """Generate and view the integrity report."""
    from chora_cvm.std import integrity_report

    # Use mock data for table-based tests
    if "expectations" in test_context:
        result = integrity_report(db_path, mock_results=test_context["expectations"])
    else:
        # Use counts for summary tests
        result = integrity_report(
            db_path,
            mock_counts={
                "passing": test_context.get("passing_count", 0),
                "failing": test_context.get("failing_count", 0),
                "no_tests": test_context.get("no_tests_count", 0),
            }
        )
    test_context["report"] = result


@when(parsers.parse("tests for {behavior_id:S} pass", extra_types={"S": str}))
def when_tests_pass_for_behavior(db_path, test_context, behavior_id: str):
    """Simulate tests passing for a behavior."""
    from chora_cvm.std import update_verifies_bond_metadata

    update_verifies_bond_metadata(db_path, behavior_id, "passed")
    test_context["last_verified_behavior"] = behavior_id


@when("the associated tests fail")
def associated_tests_fail(db_path, test_context):
    """Simulate associated tests failing."""
    from chora_cvm.std import update_verifies_bond_metadata

    behavior_id = test_context.get("verifies_bond_behavior", "behavior-test")
    update_verifies_bond_metadata(db_path, behavior_id, "failed", failure_summary="Test assertion failed")
    test_context["last_verified_behavior"] = behavior_id


# =============================================================================
# Assertion Steps
# =============================================================================


@then(parsers.parse('behavior "{behavior_id}" should show "has_scenarios: {value}"'))
def check_behavior_has_scenarios(test_context, behavior_id: str, value: str):
    """Verify behavior shows correct has_scenarios value."""
    result = test_context.get("discovery_result", {})
    behaviors = result.get("behaviors", {})

    expected = value == "true"
    actual = behaviors.get(behavior_id, {}).get("has_scenarios", False)

    assert actual == expected, f"Expected has_scenarios={expected} for {behavior_id}, got {actual}"


@then(parsers.parse('the mapping shows {behavior_id} -> {feature_path}'))
def check_behavior_feature_mapping(test_context, behavior_id: str, feature_path: str):
    """Verify behavior maps to expected feature file."""
    result = test_context.get("discovery_result", {})
    behaviors = result.get("behaviors", {})

    actual_path = behaviors.get(behavior_id, {}).get("feature_file", "")
    assert feature_path in actual_path, f"Expected {behavior_id} -> {feature_path}, got {actual_path}"


@then(parsers.parse('the result shows behavior "{behavior_id}" passed'))
def check_behavior_passed(test_context, behavior_id: str):
    """Verify behavior test result is passed."""
    result = test_context.get("integrity_result", {})
    behaviors = result.get("behaviors", {})

    actual = behaviors.get(behavior_id, {}).get("test_result")
    assert actual == "passed", f"Expected {behavior_id} to pass, got {actual}"


@then(parsers.parse('the result shows behavior "{behavior_id}" failed'))
def check_behavior_failed(test_context, behavior_id: str):
    """Verify behavior test result is failed."""
    result = test_context.get("integrity_result", {})
    behaviors = result.get("behaviors", {})

    actual = behaviors.get(behavior_id, {}).get("test_result")
    assert actual == "failed", f"Expected {behavior_id} to fail, got {actual}"


@then("the failure reason is captured")
def check_failure_captured(test_context):
    """Verify failure reason is captured."""
    result = test_context.get("integrity_result", {})
    behaviors = result.get("behaviors", {})

    # Find the failed behavior
    for bid, bdata in behaviors.items():
        if bdata.get("test_result") == "failed":
            assert "failure_reason" in bdata, f"Failure reason not captured for {bid}"
            return

    pytest.fail("No failed behavior found to check failure reason")


@then(parsers.parse('behavior "{behavior_id}" shows status "{status}"'))
def check_behavior_report_status(test_context, behavior_id: str, status: str):
    """Verify behavior shows expected status in report."""
    report = test_context.get("report", {})
    behaviors = report.get("behaviors", {})

    actual = behaviors.get(behavior_id, {}).get("status")
    assert actual == status, f"Expected {behavior_id} status={status}, got {actual}"


@then(parsers.parse('the summary shows "{summary}"'))
def check_summary(test_context, summary: str):
    """Verify the summary line matches."""
    report = test_context.get("report", {})
    actual = report.get("summary", "")

    assert summary in actual, f"Expected summary to contain '{summary}', got '{actual}'"


@then("the verifies bond has last_verified_at timestamp")
def check_verifies_bond_timestamp(db_path, test_context):
    """Verify the verifies bond has a timestamp."""
    import sqlite3

    behavior_id = test_context.get("last_verified_behavior")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM bonds WHERE type = 'verifies' AND to_id = ?",
        (behavior_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"No verifies bond found for {behavior_id}"
    bond_data = json.loads(row["data_json"])
    assert "last_verified_at" in bond_data, "verifies bond missing last_verified_at"


@then(parsers.parse('the verifies bond has verification_result "{result}"'))
def check_verifies_bond_result(db_path, test_context, result: str):
    """Verify the verifies bond has expected result."""
    import sqlite3

    behavior_id = test_context.get("last_verified_behavior")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM bonds WHERE type = 'verifies' AND to_id = ?",
        (behavior_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"No verifies bond found for {behavior_id}"
    bond_data = json.loads(row["data_json"])
    assert bond_data.get("verification_result") == result, f"Expected {result}, got {bond_data.get('verification_result')}"


@then("the verifies bond has failure_summary")
def check_verifies_bond_failure_summary(db_path, test_context):
    """Verify the verifies bond has failure summary."""
    import sqlite3

    behavior_id = test_context.get("last_verified_behavior")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM bonds WHERE type = 'verifies' AND to_id = ?",
        (behavior_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"No verifies bond found for {behavior_id}"
    bond_data = json.loads(row["data_json"])
    assert "failure_summary" in bond_data, "verifies bond missing failure_summary"
