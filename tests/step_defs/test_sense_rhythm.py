"""
Step definitions for Protocol Sense Rhythm feature.

These tests verify the behaviors for rhythm sensing primitives:
- behavior-sense-rhythm-detects-kairotic-phase
- behavior-sense-rhythm-returns-standardized-shape
- behavior-sense-rhythm-computes-satiation
- behavior-sense-rhythm-computes-temporal-health

BDD Flow: Feature file -> Step definitions -> Implementation
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from chora_cvm.engine import CvmEngine
from chora_cvm.schema import (
    GenericEntity,
    PrimitiveData,
    PrimitiveEntity,
    ProtocolData,
    ProtocolEntity,
    ProtocolGraph,
    ProtocolInterface,
    ProtocolNode,
    ProtocolNodeKind,
    ProtocolEdge,
)
from chora_cvm.store import EventStore

# Load scenarios from feature file
scenarios("../features/sense_rhythm.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {
        "db_path": None,
        "store": None,
        "engine": None,
        "result": None,
    }


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


# =============================================================================
# Background Steps
# =============================================================================


@given("a bootstrapped CVM database with rhythm sensing primitives")
def bootstrap_database(test_context, temp_db):
    """Bootstrap a fresh database with rhythm sensing primitives."""
    store = EventStore(temp_db)

    # Bootstrap primitives for rhythm sensing
    primitives = [
        ("primitive-sense-kairotic-state", "chora_cvm.std.sense_kairotic_state_primitive"),
        ("primitive-sense-temporal-health", "chora_cvm.std.sense_temporal_health_primitive"),
        ("primitive-sense-satiation", "chora_cvm.std.sense_satiation_primitive"),
        ("primitive-get-rhythm-summary", "chora_cvm.std.get_rhythm_summary_primitive"),
    ]

    for prim_id, python_ref in primitives:
        prim = PrimitiveEntity(
            id=prim_id,
            data=PrimitiveData(
                python_ref=python_ref,
                description=f"Rhythm sensing primitive {prim_id}",
                interface={},
            ),
        )
        store.save_entity(prim)

    # Bootstrap the sense-rhythm protocol
    proto = ProtocolEntity(
        id="protocol-sense-rhythm",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={"db_path": {"type": "string"}},
                outputs={
                    "status": {"type": "string"},
                    "data": {"type": "object"},
                },
            ),
            graph=ProtocolGraph(
                start="node_sense_kairotic",
                nodes={
                    "node_sense_kairotic": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="primitive-sense-kairotic-state",
                        inputs={"db_path": "$.inputs.db_path"},
                    ),
                    "node_sense_satiation": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="primitive-sense-satiation",
                        inputs={"db_path": "$.inputs.db_path"},
                    ),
                    "node_sense_health": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="primitive-sense-temporal-health",
                        inputs={"db_path": "$.inputs.db_path", "window_days": 7},
                    ),
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "status": "success",
                            "data": {
                                "kairotic": "$.node_sense_kairotic.data",
                                "satiation": "$.node_sense_satiation.data",
                                "health": "$.node_sense_health.data",
                            },
                        },
                    ),
                },
                edges=[
                    ProtocolEdge(**{"from": "node_sense_kairotic", "to": "node_sense_satiation"}),
                    ProtocolEdge(**{"from": "node_sense_satiation", "to": "node_sense_health"}),
                    ProtocolEdge(**{"from": "node_sense_health", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto)

    # Create some base behaviors (needed for rhythm calculations)
    behavior = GenericEntity(
        id="behavior-test-base",
        type="behavior",
        status="verified",
        data={"title": "Test behavior", "created_at": datetime.now(timezone.utc).isoformat()},
    )
    store.save_entity(behavior)

    test_context["db_path"] = temp_db
    test_context["store"] = store
    test_context["engine"] = CvmEngine(temp_db)


# =============================================================================
# Given Steps - Data Setup
# =============================================================================


@given("the database has recent inquiry activity")
def add_recent_inquiries(test_context):
    """Add recent inquiry entities to signal Pioneer phase."""
    store = test_context["store"]

    for i in range(3):
        inquiry = GenericEntity(
            id=f"inquiry-test-{i}",
            type="inquiry",
            status="active",
            data={
                "title": f"Test inquiry {i}",
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        store.save_entity(inquiry)


# =============================================================================
# When Steps - Primitive Execution
# =============================================================================


@when("the sense-kairotic-state primitive is executed")
def execute_sense_kairotic_state(test_context):
    """Execute the sense-kairotic-state primitive directly."""
    from chora_cvm.std import sense_kairotic_state_primitive

    db_path = test_context["db_path"]
    result = sense_kairotic_state_primitive(db_path)
    test_context["result"] = result


@when("the sense-satiation primitive is executed")
def execute_sense_satiation(test_context):
    """Execute the sense-satiation primitive directly."""
    from chora_cvm.std import sense_satiation_primitive

    db_path = test_context["db_path"]
    result = sense_satiation_primitive(db_path)
    test_context["result"] = result


@when(parsers.parse("the sense-temporal-health primitive is executed with window_days {window_days:d}"))
def execute_sense_temporal_health(test_context, window_days: int):
    """Execute the sense-temporal-health primitive with window parameter."""
    from chora_cvm.std import sense_temporal_health_primitive

    db_path = test_context["db_path"]
    result = sense_temporal_health_primitive(db_path, window_days=window_days)
    test_context["result"] = result


@when('I dispatch "primitive-sense-kairotic-state" through CvmEngine')
def dispatch_sense_kairotic_state(test_context):
    """Dispatch sense-kairotic-state primitive through the unified engine."""
    engine = test_context["engine"]
    result = engine.dispatch(
        "primitive-sense-kairotic-state",
        {"db_path": test_context["db_path"]},
    )
    test_context["result"] = result.data if result.ok else {"status": "error", "error": result.error_message}


@when('I dispatch "protocol-sense-rhythm" through CvmEngine')
def dispatch_sense_rhythm_protocol(test_context):
    """Dispatch sense-rhythm protocol through the unified engine."""
    engine = test_context["engine"]
    result = engine.dispatch(
        "protocol-sense-rhythm",
        {"db_path": test_context["db_path"]},
    )
    test_context["result"] = result.data if result.ok else {"status": "error", "error": result.error_message}


# =============================================================================
# Then Steps - Assertions
# =============================================================================


@then(parsers.parse('the result status is "{expected_status}"'))
def check_status(test_context, expected_status: str):
    """Verify the result status matches expected."""
    result = test_context["result"]
    assert result.get("status") == expected_status, (
        f"Expected status '{expected_status}', got '{result.get('status')}'"
    )


@then("the result data contains phases")
def check_has_phases(test_context):
    """Verify result data contains phases."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "phases" in data, f"Expected phases in data, got: {data.keys()}"


@then("the phases include all six archetypes")
def check_all_phases(test_context):
    """Verify all six kairotic phases are present."""
    result = test_context["result"]
    data = result.get("data", result)
    phases = data.get("phases", {})

    expected = ["pioneer", "cultivator", "regulator", "steward", "curator", "scout"]
    for phase in expected:
        assert phase in phases, f"Expected phase '{phase}' in phases, got: {phases.keys()}"


@then("the data contains a dominant phase")
def check_has_dominant(test_context):
    """Verify data contains dominant phase."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "dominant" in data, f"Expected dominant in data, got: {data.keys()}"


@then("the data contains a side value")
def check_has_side(test_context):
    """Verify data contains side value (orange/purple)."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "side" in data, f"Expected side in data, got: {data.keys()}"
    assert data["side"] in ("orange", "purple"), f"Expected side to be orange/purple, got: {data['side']}"


@then(parsers.parse('the result has key "{key}"'))
def check_result_has_key(test_context, key: str):
    """Verify result has a specific key."""
    result = test_context["result"]
    assert key in result, f"Expected key '{key}' in result, got: {result.keys()}"


@then(parsers.parse('the data has key "{key}"'))
def check_data_has_key(test_context, key: str):
    """Verify result data has a specific key."""
    result = test_context["result"]
    data = result.get("data", result)
    assert key in data, f"Expected key '{key}' in data, got: {data.keys()}"


@then("the result data contains score")
def check_has_score(test_context):
    """Verify result data contains score."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "score" in data, f"Expected score in data, got: {data.keys()}"


@then("the score is between 0.0 and 1.0")
def check_score_range(test_context):
    """Verify score is in valid range."""
    result = test_context["result"]
    data = result.get("data", result)
    score = data.get("score")
    assert 0.0 <= score <= 1.0, f"Expected score between 0.0 and 1.0, got: {score}"


@then("the result data contains label")
def check_has_label(test_context):
    """Verify result data contains label."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "label" in data, f"Expected label in data, got: {data.keys()}"


@then("the label is a valid satiation label")
def check_label_valid(test_context):
    """Verify label is one of the valid satiation labels."""
    result = test_context["result"]
    data = result.get("data", result)
    label = data.get("label")
    valid_labels = ["hungry", "digesting", "content", "satiated"]
    assert label in valid_labels, f"Expected label in {valid_labels}, got: {label}"


@then("the result data contains metrics")
def check_has_metrics(test_context):
    """Verify result data contains metrics."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "metrics" in data, f"Expected metrics in data, got: {data.keys()}"


@then("the metrics include creation and decomposition rates")
def check_metrics_content(test_context):
    """Verify metrics include expected fields."""
    result = test_context["result"]
    data = result.get("data", result)
    metrics = data.get("metrics", {})

    expected = ["entities_created", "bonds_created", "entities_composted"]
    for field in expected:
        assert field in metrics, f"Expected '{field}' in metrics, got: {metrics.keys()}"


@then("the data contains growth_rate")
def check_has_growth_rate(test_context):
    """Verify data contains growth_rate."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "growth_rate" in data, f"Expected growth_rate in data, got: {data.keys()}"


@then("the data contains metabolic_balance")
def check_has_metabolic_balance(test_context):
    """Verify data contains metabolic_balance."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "metabolic_balance" in data, f"Expected metabolic_balance in data, got: {data.keys()}"


@then("the dispatch result is successful")
def check_dispatch_success(test_context):
    """Verify dispatch succeeded."""
    result = test_context["result"]
    assert result.get("status") != "error", f"Expected success, got error: {result}"


@then("the result data contains phase information")
def check_has_phase_info(test_context):
    """Verify result contains phase information."""
    result = test_context["result"]
    data = result.get("data", result)
    # Phase info is in the result field for primitive dispatch
    result_data = data.get("result", data)
    if "data" in result_data:
        inner = result_data["data"]
        assert "phases" in inner or "dominant" in inner, f"Expected phase info, got: {result_data}"
    else:
        assert "phases" in result_data or "dominant" in result_data, f"Expected phase info, got: {result_data}"


@then("the result contains kairotic data")
def check_has_kairotic_data(test_context):
    """Verify protocol result contains kairotic data."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "kairotic" in data, f"Expected kairotic in data, got: {data.keys()}"


@then("the result contains satiation data")
def check_has_satiation_data(test_context):
    """Verify protocol result contains satiation data."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "satiation" in data, f"Expected satiation in data, got: {data.keys()}"


@then("the result contains health data")
def check_has_health_data(test_context):
    """Verify protocol result contains health data."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "health" in data, f"Expected health in data, got: {data.keys()}"
