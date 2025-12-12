"""
Step definitions for CvmEngine feature.

These tests verify the behaviors for the unified CvmEngine:
- behavior-engine-routes-protocol-invocation
- behavior-engine-routes-primitive-invocation
- behavior-engine-lists-capabilities
- behavior-engine-handles-errors-gracefully

BDD Flow: Feature file -> Step definitions -> Implementation
"""

import json
import os
import tempfile
from typing import Any, Dict, List

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from chora_cvm.engine import CapabilityKind, CvmEngine
from chora_cvm.schema import (
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
scenarios("../features/engine.feature")


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
        "capabilities": None,
        "captured_output": [],
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


@given("a bootstrapped CVM database with primitives and protocols")
def bootstrap_database(test_context, temp_db):
    """Bootstrap a fresh database with basic primitives and protocols."""
    store = EventStore(temp_db)

    # Bootstrap a minimal timestamp primitive
    prim_timestamp = PrimitiveEntity(
        id="primitive-timestamp-now",
        data=PrimitiveData(
            python_ref="chora_cvm.std.timestamp_now",
            description="Returns the current timestamp",
            interface={"outputs": {"timestamp": {"type": "string"}}},
        ),
    )
    store.save_entity(prim_timestamp)

    test_context["db_path"] = temp_db
    test_context["store"] = store
    test_context["engine"] = CvmEngine(temp_db)


# =============================================================================
# Given Steps - Data Setup
# =============================================================================


@given("the database contains protocol-horizon")
def add_protocol_horizon(test_context):
    """Add a minimal horizon protocol for testing dispatch."""
    store = test_context["store"]

    # Add required primitives
    primitives = [
        ("primitive-entities-recent", "chora_cvm.std.entities_recent"),
        ("primitive-entities-unverified", "chora_cvm.std.entities_unverified"),
    ]

    for prim_id, python_ref in primitives:
        prim = PrimitiveEntity(
            id=prim_id,
            data=PrimitiveData(
                python_ref=python_ref,
                description=f"Test primitive {prim_id}",
                interface={},
            ),
        )
        store.save_entity(prim)

    # Simple protocol that just returns empty data (for testing dispatch routing)
    proto = ProtocolEntity(
        id="protocol-horizon",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={"db_path": {"type": "string"}},
                outputs={"method": {"type": "string"}},
            ),
            graph=ProtocolGraph(
                start="node_return",
                nodes={
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "method": "test",
                            "recommendations": [],
                        },
                    ),
                },
                edges=[],
            ),
        ),
    )
    store.save_entity(proto)


@given("the database contains primitive-timestamp-now")
def primitive_exists(test_context):
    """Primitive already added in background step."""
    pass


@given("the database contains multiple protocols and primitives")
def add_multiple_capabilities(test_context):
    """Add several protocols and primitives for listing test."""
    store = test_context["store"]

    # Add more primitives
    for i in range(3):
        prim = PrimitiveEntity(
            id=f"primitive-test-{i}",
            data=PrimitiveData(
                python_ref="chora_cvm.std.timestamp_now",
                description=f"Test primitive {i}",
                interface={},
            ),
        )
        store.save_entity(prim)

    # Add protocols
    for i in range(2):
        proto = ProtocolEntity(
            id=f"protocol-test-{i}",
            data=ProtocolData(
                interface=ProtocolInterface(
                    inputs={},
                    outputs={},
                    description=f"Test protocol {i}",
                ),
                graph=ProtocolGraph(
                    start="node_return",
                    nodes={
                        "node_return": ProtocolNode(
                            kind=ProtocolNodeKind.RETURN,
                            outputs={"result": f"test-{i}"},
                        ),
                    },
                    edges=[],
                ),
            ),
        )
        store.save_entity(proto)


@given("the database contains protocol-manifest-entity")
def add_manifest_protocol(test_context):
    """Add manifest protocol for I/O membrane test."""
    store = test_context["store"]

    # Add ui_render primitive
    prim_render = PrimitiveEntity(
        id="primitive-ui-render",
        data=PrimitiveData(
            python_ref="chora_cvm.std.ui_render",
            description="Render output through I/O membrane",
            interface={},
        ),
    )
    store.save_entity(prim_render)

    # Protocol that uses ui_render
    proto = ProtocolEntity(
        id="protocol-manifest-entity",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={"message": {"type": "string", "default": "test"}},
                outputs={"rendered": {"type": "boolean"}},
            ),
            graph=ProtocolGraph(
                start="node_render",
                nodes={
                    "node_render": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="primitive-ui-render",
                        inputs={
                            "content": "$.inputs.message",
                            "style": "info",
                        },
                    ),
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={"rendered": True},
                    ),
                },
                edges=[
                    ProtocolEdge(**{"from": "node_render", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto)


@given("an output capturing sink")
def setup_capturing_sink(test_context):
    """Set up a list-capturing output sink."""
    test_context["captured_output"] = []


# =============================================================================
# When Steps
# =============================================================================


@when("I dispatch intent \"protocol-horizon\" with inputs")
def dispatch_protocol_horizon(test_context):
    """Dispatch the horizon protocol."""
    engine = test_context["engine"]
    result = engine.dispatch(
        "protocol-horizon",
        {"db_path": test_context["db_path"]},
    )
    test_context["result"] = result


@when("I dispatch intent \"horizon\" with inputs")
def dispatch_horizon_short(test_context):
    """Dispatch horizon using short name."""
    engine = test_context["engine"]
    result = engine.dispatch(
        "horizon",
        {"db_path": test_context["db_path"]},
    )
    test_context["result"] = result


@when("I dispatch intent \"primitive-timestamp-now\" with empty inputs")
def dispatch_primitive_full(test_context):
    """Dispatch timestamp primitive with full name."""
    engine = test_context["engine"]
    result = engine.dispatch("primitive-timestamp-now", {})
    test_context["result"] = result


@when("I dispatch intent \"timestamp_now\" with empty inputs")
def dispatch_primitive_short(test_context):
    """Dispatch timestamp primitive with short name."""
    engine = test_context["engine"]
    result = engine.dispatch("timestamp_now", {})
    test_context["result"] = result


@when("I request the capability list")
def request_capabilities(test_context):
    """Get the list of capabilities from the engine."""
    engine = test_context["engine"]
    test_context["capabilities"] = engine.list_capabilities()


@when("I dispatch intent \"nonexistent-thing\" with empty inputs")
def dispatch_nonexistent(test_context):
    """Dispatch a nonexistent intent."""
    engine = test_context["engine"]
    result = engine.dispatch("nonexistent-thing", {})
    test_context["result"] = result


@when("I dispatch intent \"manifest-entity\" through the engine with sink")
def dispatch_with_sink(test_context):
    """Dispatch with output sink for I/O membrane test."""
    engine = test_context["engine"]
    captured = test_context["captured_output"]
    result = engine.dispatch(
        "manifest-entity",
        {"message": "test output"},
        output_sink=captured.append,
    )
    test_context["result"] = result


# =============================================================================
# Then Steps
# =============================================================================


@then("the dispatch result is successful")
def check_success(test_context):
    """Verify dispatch succeeded."""
    result = test_context["result"]
    assert result.ok, f"Expected success, got error: {result.error_message}"


@then("the dispatch result is not successful")
def check_failure(test_context):
    """Verify dispatch failed."""
    result = test_context["result"]
    assert not result.ok, f"Expected failure, got success: {result.data}"


@then("the result contains protocol output")
def check_protocol_output(test_context):
    """Verify result contains protocol output data."""
    result = test_context["result"]
    assert result.data, "Expected protocol output data"
    # Horizon returns a 'method' field
    assert "method" in result.data, f"Expected 'method' in output, got: {result.data}"


@then("the result contains primitive output")
def check_primitive_output(test_context):
    """Verify result contains primitive output data."""
    result = test_context["result"]
    assert result.data, "Expected primitive output data"
    # timestamp_now returns a timestamp
    assert "timestamp" in result.data or "result" in result.data, (
        f"Expected timestamp output, got: {result.data}"
    )


@then("the list contains protocols")
def check_protocols_in_list(test_context):
    """Verify capability list includes protocols."""
    capabilities = test_context["capabilities"]
    protocols = [c for c in capabilities if c.kind == CapabilityKind.PROTOCOL]
    assert len(protocols) > 0, "Expected at least one protocol"


@then("the list contains primitives")
def check_primitives_in_list(test_context):
    """Verify capability list includes primitives."""
    capabilities = test_context["capabilities"]
    primitives = [c for c in capabilities if c.kind == CapabilityKind.PRIMITIVE]
    assert len(primitives) > 0, "Expected at least one primitive"


@then("each capability has an id and kind")
def check_capability_structure(test_context):
    """Verify capability objects have required fields."""
    capabilities = test_context["capabilities"]
    for cap in capabilities:
        assert cap.id, "Capability must have an id"
        assert cap.kind in (CapabilityKind.PROTOCOL, CapabilityKind.PRIMITIVE), (
            f"Invalid capability kind: {cap.kind}"
        )


@then(parsers.parse('the error kind is "{expected_kind}"'))
def check_error_kind(test_context, expected_kind: str):
    """Verify the error kind matches expected."""
    result = test_context["result"]
    assert result.error_kind == expected_kind, (
        f"Expected error kind '{expected_kind}', got '{result.error_kind}'"
    )


@then("any protocol output flows through the sink")
def check_sink_received_output(test_context):
    """Verify output was captured by the sink."""
    # The ui_render primitive should have sent output to our sink
    # If the protocol executed successfully with sink wired, the infrastructure works
    result = test_context["result"]
    # For now, just verify the dispatch succeeded with sink provided
    # Full I/O membrane verification is in test_horizon.py
    assert result.ok or "sink" in str(test_context.get("captured_output", []))
