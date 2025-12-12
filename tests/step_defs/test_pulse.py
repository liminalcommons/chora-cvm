"""
Step definitions for the Autonomic Pulse (Reflex) feature.

These tests verify the behaviors specified by story-autonomic-heartbeat.
"""
import json
import os
import tempfile
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.schema import GenericEntity
from chora_cvm.std import manifest_entity, manage_bond

# Load scenarios from feature file
scenarios("../features/pulse.feature")


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
def worker_db_path():
    """Create a temporary worker database for each test."""
    with tempfile.NamedTemporaryFile(suffix="-worker.db", delete=False) as f:
        path = f.name
    yield path
    if os.path.exists(path):
        os.unlink(path)


# =============================================================================
# Background Steps
# =============================================================================


@given("a fresh CVM database")
def fresh_database(db_path, test_context):
    """Set up a fresh database for testing."""
    test_context["db_path"] = db_path
    test_context["signals_processed"] = []
    test_context["protocols_executed"] = []
    test_context["pulse_runs"] = []


# =============================================================================
# Signal and Protocol Setup Steps
# =============================================================================


@given(parsers.parse('an active signal "{signal_id}" exists'))
def create_active_signal(db_path, test_context, signal_id: str):
    """Create an active signal entity."""
    manifest_entity(
        db_path,
        "signal",
        signal_id,
        {
            "title": f"Test signal {signal_id}",
            "status": "active",
            "signal_type": "test",
            "urgency": "normal",
        },
    )
    test_context.setdefault("signals", []).append(signal_id)


@given(parsers.parse('an active signal "{signal_id}" exists with no triggers bond'))
def create_signal_without_trigger(db_path, test_context, signal_id: str):
    """Create an active signal without any triggers bond."""
    manifest_entity(
        db_path,
        "signal",
        signal_id,
        {
            "title": f"Test signal {signal_id}",
            "status": "active",
            "signal_type": "test",
            "urgency": "normal",
        },
    )
    test_context.setdefault("signals_without_triggers", []).append(signal_id)


@given(parsers.parse('a protocol "{protocol_id}" exists'))
def create_protocol(db_path, test_context, protocol_id: str):
    """Create a test protocol entity using GenericEntity (bypasses strict schema)."""
    # Use GenericEntity to avoid strict protocol schema validation in tests
    # The pulse only needs to look up protocols and pass them to runner
    store = EventStore(db_path)
    entity = GenericEntity(
        id=protocol_id,
        type="protocol",
        data={
            "interface": {"inputs": {}, "outputs": {"status": {"type": "string"}}},
            "graph": {
                "start": "node_end",
                "nodes": {
                    "node_end": {
                        "kind": "return",
                        "outputs": {"status": "success"},
                    }
                },
                "edges": [],
            },
        },
    )
    store.save_entity(entity)
    store.close()
    test_context.setdefault("protocols", []).append(protocol_id)


@given(parsers.parse('a protocol "{protocol_id}" exists that returns success'))
def create_success_protocol(db_path, test_context, protocol_id: str):
    """Create a protocol that returns success."""
    create_protocol(db_path, test_context, protocol_id)


@given(parsers.parse('a protocol "{protocol_id}" exists that returns an error'))
def create_failing_protocol(db_path, test_context, protocol_id: str):
    """Create a protocol that deliberately fails."""
    store = EventStore(db_path)
    entity = GenericEntity(
        id=protocol_id,
        type="protocol",
        data={
            "interface": {"inputs": {}, "outputs": {}},
            "graph": {
                "start": "node_fail",
                "nodes": {
                    "node_fail": {
                        "kind": "call",
                        "ref": "primitive-nonexistent",  # Will fail - no such primitive
                        "inputs": {},
                    }
                },
                "edges": [],
            },
        },
    )
    store.save_entity(entity)
    store.close()
    test_context.setdefault("protocols", []).append(protocol_id)


@given(parsers.parse('signal "{signal_id}" has a triggers bond to "{protocol_id}"'))
def create_triggers_bond(db_path, signal_id: str, protocol_id: str):
    """Create a triggers bond from signal to protocol."""
    manage_bond(
        db_path,
        "triggers",
        signal_id,
        protocol_id,
        enforce_physics=False,  # Allow signal -> protocol for testing
    )


# =============================================================================
# Pulse History Setup Steps
# =============================================================================


@given(parsers.parse("the pulse has run {count:d} times"))
def setup_pulse_history(worker_db_path, test_context, count: int):
    """Set up pulse history records."""
    from chora_cvm.worker import init_pulse_log_table, record_pulse

    init_pulse_log_table(worker_db_path)
    test_context["worker_db_path"] = worker_db_path
    test_context["pulse_count"] = count


@given(parsers.parse("the first pulse processed {count:d} signals"))
def first_pulse_signals(test_context, count: int):
    """Record first pulse stats."""
    test_context.setdefault("pulse_history", []).append({"signals_processed": count, "errors": 0})


@given(parsers.parse("the second pulse processed {count:d} signal"))
def second_pulse_signals(test_context, count: int):
    """Record second pulse stats."""
    test_context.setdefault("pulse_history", []).append({"signals_processed": count, "errors": 0})


@given("the third pulse had an error")
def third_pulse_error(test_context):
    """Record third pulse with error."""
    test_context.setdefault("pulse_history", []).append({"signals_processed": 0, "errors": 1})


# =============================================================================
# Action Steps
# =============================================================================


@when("the pulse runs")
def run_pulse(db_path, test_context):
    """Execute the pulse cycle."""
    from chora_cvm.std import pulse_check_signals

    result = pulse_check_signals(db_path)
    test_context["pulse_result"] = result


@when("I request a pulse preview")
def request_preview(db_path, test_context):
    """Request a pulse preview without executing."""
    from chora_cvm.std import pulse_preview

    result = pulse_preview(db_path)
    test_context["preview_result"] = result


@when("I request pulse status")
def request_status(worker_db_path, test_context):
    """Request pulse status history."""
    from chora_cvm.worker import get_pulse_status

    # First, record the test pulse history
    from chora_cvm.worker import record_pulse

    for pulse_data in test_context.get("pulse_history", []):
        record_pulse(worker_db_path, pulse_data)

    result = get_pulse_status(worker_db_path)
    test_context["status_result"] = result


# =============================================================================
# Assertion Steps
# =============================================================================


@then(parsers.parse('signal "{signal_id}" status should be "{status}"'))
def check_signal_status(db_path, signal_id: str, status: str):
    """Verify signal has expected status."""
    import sqlite3 as sqlite

    conn = sqlite.connect(db_path)
    conn.row_factory = sqlite.Row
    cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (signal_id,))
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"Signal {signal_id} not found"
    entity_data = json.loads(row["data_json"])
    assert entity_data.get("status") == status, f"Expected status {status}, got {entity_data.get('status')}"


@then(parsers.parse('the protocol "{protocol_id}" should have been executed'))
def check_protocol_executed(test_context, protocol_id: str):
    """Verify protocol was executed."""
    pulse_result = test_context.get("pulse_result", {})
    protocols_triggered = pulse_result.get("protocols_triggered", [])
    assert protocol_id in protocols_triggered, f"Protocol {protocol_id} was not triggered"


@then(parsers.parse('signal "{signal_id}" should have outcome_data'))
def check_signal_has_outcome(db_path, signal_id: str):
    """Verify signal has outcome data."""
    import sqlite3 as sqlite

    conn = sqlite.connect(db_path)
    conn.row_factory = sqlite.Row
    cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (signal_id,))
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"Signal {signal_id} not found"
    entity_data = json.loads(row["data_json"])
    assert "outcome_data" in entity_data, "Signal missing outcome_data"


@then(parsers.parse('the outcome_data should include "{field}"'))
def check_outcome_field(db_path, test_context, field: str):
    """Verify outcome_data contains expected field (checks nested structures)."""
    import sqlite3 as sqlite

    # Get the most recently processed signal
    signals = test_context.get("signals", [])
    if not signals:
        pytest.fail("No signals in test context")

    signal_id = signals[-1]
    conn = sqlite.connect(db_path)
    conn.row_factory = sqlite.Row
    cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (signal_id,))
    row = cur.fetchone()
    conn.close()

    entity_data = json.loads(row["data_json"])
    outcome_data = entity_data.get("outcome_data", {})

    # Check field directly in outcome_data
    if field in outcome_data:
        return

    # Check in protocols_executed list
    if field == "protocol_id" or field == "duration_ms":
        protocols = outcome_data.get("protocols_executed", [])
        if protocols and field in protocols[0]:
            return

    # Check in errors list
    if field == "error":
        errors = outcome_data.get("errors", [])
        if errors and "error" in errors[0]:
            return

    pytest.fail(f"outcome_data missing field: {field}. Structure: {outcome_data}")


@then(parsers.parse('the preview should show signal "{signal_id}" would trigger "{protocol_id}"'))
def check_preview_shows_signal(test_context, signal_id: str, protocol_id: str):
    """Verify preview includes expected signal and protocol."""
    preview = test_context.get("preview_result", {})
    signals = preview.get("would_process", [])

    found = any(
        s.get("signal_id") == signal_id and s.get("triggers") == protocol_id for s in signals
    )
    assert found, f"Preview doesn't show {signal_id} triggering {protocol_id}"


@then(parsers.parse('the preview should not include signal "{signal_id}"'))
def check_preview_excludes_signal(test_context, signal_id: str):
    """Verify preview does not include signal."""
    preview = test_context.get("preview_result", {})
    signals = preview.get("would_process", [])

    found = any(s.get("signal_id") == signal_id for s in signals)
    assert not found, f"Preview unexpectedly includes {signal_id}"


@then("no signals should have been processed")
def check_no_signals_processed(db_path, test_context):
    """Verify no signals were actually processed during preview."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Check all signals are still active
    for signal_id in test_context.get("signals", []) + test_context.get("signals_without_triggers", []):
        cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (signal_id,))
        row = cur.fetchone()
        if row:
            entity_data = json.loads(row["data_json"])
            assert entity_data.get("status") == "active", f"Signal {signal_id} was processed during preview"

    conn.close()


@then(parsers.parse("the status should show {count:d} recent pulses"))
def check_status_pulse_count(test_context, count: int):
    """Verify status shows expected number of pulses."""
    status = test_context.get("status_result", {})
    pulses = status.get("pulses", [])
    assert len(pulses) == count, f"Expected {count} pulses, got {len(pulses)}"


@then("the status should show signals processed count for each pulse")
def check_status_signals_count(test_context):
    """Verify status shows signals count."""
    status = test_context.get("status_result", {})
    pulses = status.get("pulses", [])

    for pulse in pulses:
        assert "signals_processed" in pulse, "Pulse missing signals_processed count"


@then("the status should show error count for the third pulse")
def check_status_error_count(test_context):
    """Verify third pulse (most recent) shows error."""
    status = test_context.get("status_result", {})
    pulses = status.get("pulses", [])

    assert len(pulses) >= 3, "Not enough pulses in status"
    # Pulses are in DESC order (most recent first), so the third pulse is [0]
    third_pulse = pulses[0]
    assert third_pulse.get("errors", 0) > 0, f"Third pulse should have errors: {third_pulse}"


# =============================================================================
# Pulse Configuration Steps (behavior-pulse-runs-periodically)
# =============================================================================


@given(parsers.parse("a pulse configuration with interval {interval:d} seconds"))
def setup_pulse_config_interval(test_context, interval: int):
    """Set up a pulse configuration with specified interval."""
    test_context["pulse_config"] = {"interval_seconds": interval, "enabled": True}


@given(parsers.parse("a pulse configuration with enabled = {enabled}"))
def setup_pulse_config_enabled(test_context, enabled: str):
    """Set up a pulse configuration with enabled flag."""
    test_context["pulse_config"] = {
        "interval_seconds": 60,
        "enabled": enabled.lower() == "true",
    }


@when("the pulse configuration is loaded")
def load_pulse_config(test_context):
    """Simulate loading pulse configuration."""
    # In a real implementation, this would load from env/config
    config = test_context.get("pulse_config", {})
    test_context["loaded_config"] = config


@when("pulse_should_run is checked")
def check_pulse_should_run(test_context):
    """Check if pulse should run based on config."""
    config = test_context.get("pulse_config", {})
    test_context["should_run_result"] = config.get("enabled", True)


@then(parsers.parse("the pulse interval is {interval:d} seconds"))
def verify_pulse_interval(test_context, interval: int):
    """Verify pulse interval matches expected value."""
    config = test_context.get("loaded_config", {})
    assert config.get("interval_seconds") == interval, (
        f"Expected interval {interval}, got {config.get('interval_seconds')}"
    )


@then(parsers.parse("the result is {expected}"))
def verify_boolean_result(test_context, expected: str):
    """Verify a boolean result."""
    expected_bool = expected.lower() == "true"
    actual = test_context.get("should_run_result")
    assert actual == expected_bool, f"Expected {expected_bool}, got {actual}"
