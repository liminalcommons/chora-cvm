"""
Step definitions for Protocol Prune Detect feature.

These tests verify the behaviors for prune detection protocol:
- behavior-prune-detect-finds-orphan-tools
- behavior-prune-detect-finds-deprecated-tools
- behavior-prune-detect-returns-standardized-shape
- behavior-prune-detect-respects-internal-flag

BDD Flow: Feature file -> Step definitions -> Implementation
"""

import json
import os
import tempfile
from typing import Any, Dict, List

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from chora_cvm.engine import CvmEngine
from chora_cvm.runner import execute_protocol
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
scenarios("../features/prune_detect.feature")


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


@given("a bootstrapped CVM database with prune detection primitives")
def bootstrap_database(test_context, temp_db):
    """Bootstrap a fresh database with prune detection primitives."""
    store = EventStore(temp_db)

    # Bootstrap primitives for prune detection
    primitives = [
        ("primitive-detect-orphan-tools", "chora_cvm.std.detect_orphan_tools"),
        ("primitive-detect-deprecated-tools", "chora_cvm.std.detect_deprecated_tools"),
    ]

    for prim_id, python_ref in primitives:
        prim = PrimitiveEntity(
            id=prim_id,
            data=PrimitiveData(
                python_ref=python_ref,
                description=f"Prune detection primitive {prim_id}",
                interface={},
            ),
        )
        store.save_entity(prim)

    # Bootstrap the prune-detect protocol
    proto = ProtocolEntity(
        id="protocol-prune-detect",
        data=ProtocolData(
            interface=ProtocolInterface(
                inputs={"db_path": {"type": "string"}},
                outputs={
                    "status": {"type": "string"},
                    "data": {"type": "object"},
                },
            ),
            graph=ProtocolGraph(
                start="node_detect_orphans",
                nodes={
                    "node_detect_orphans": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="primitive-detect-orphan-tools",
                        inputs={"db_path": "$.inputs.db_path"},
                    ),
                    "node_detect_deprecated": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="primitive-detect-deprecated-tools",
                        inputs={"db_path": "$.inputs.db_path"},
                    ),
                    "node_return": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "status": "success",
                            "data": {
                                "orphan_tools": "$.node_detect_orphans.tools",
                                "deprecated_tools": "$.node_detect_deprecated.tools",
                                "summary": {
                                    "orphan_count": "$.node_detect_orphans.count",
                                    "deprecated_count": "$.node_detect_deprecated.count",
                                },
                            },
                        },
                    ),
                },
                edges=[
                    ProtocolEdge(**{"from": "node_detect_orphans", "to": "node_detect_deprecated"}),
                    ProtocolEdge(**{"from": "node_detect_deprecated", "to": "node_return"}),
                ],
            ),
        ),
    )
    store.save_entity(proto)

    # Create a behavior entity for testing implements bond
    behavior = GenericEntity(
        id="behavior-test-healthy",
        type="behavior",
        status="active",
        data={"title": "Test behavior for healthy tool"},
    )
    store.save_entity(behavior)

    test_context["db_path"] = temp_db
    test_context["store"] = store
    test_context["engine"] = CvmEngine(temp_db)


# =============================================================================
# Given Steps - Data Setup
# =============================================================================


@given(parsers.parse('the database contains a tool "{tool_id}" with no implements bond'))
def add_orphan_tool(test_context, tool_id: str):
    """Add a tool without an implements bond."""
    store = test_context["store"]
    tool = GenericEntity(
        id=tool_id,
        type="tool",
        status="active",
        data={"title": f"Tool {tool_id}", "handler": "test.handler"},
    )
    store.save_entity(tool)


@given(parsers.parse('the database contains a tool "{tool_id}" with an implements bond'))
def add_healthy_tool(test_context, tool_id: str):
    """Add a tool with an implements bond from a behavior."""
    store = test_context["store"]

    # Create the tool
    tool = GenericEntity(
        id=tool_id,
        type="tool",
        status="active",
        data={"title": f"Tool {tool_id}", "handler": "test.handler"},
    )
    store.save_entity(tool)

    # Create the implements bond
    store.save_bond(
        bond_id=f"rel-implements-behavior-test-healthy-{tool_id}",
        bond_type="implements",
        from_id="behavior-test-healthy",
        to_id=tool_id,
        status="active",
    )


@given(parsers.parse('the database contains a tool "{tool_id}" with status "{status}"'))
def add_tool_with_status(test_context, tool_id: str, status: str):
    """Add a tool with a specific status."""
    store = test_context["store"]
    tool = GenericEntity(
        id=tool_id,
        type="tool",
        status=status,
        data={"title": f"Tool {tool_id}", "handler": "test.handler", "status": status},
    )
    store.save_entity(tool)


@given(parsers.parse('the database contains an internal tool "{tool_id}"'))
def add_internal_tool(test_context, tool_id: str):
    """Add a tool marked as internal (should be excluded from orphan detection)."""
    store = test_context["store"]
    tool = GenericEntity(
        id=tool_id,
        type="tool",
        status="active",
        data={"title": f"Tool {tool_id}", "handler": "test.handler", "internal": True},
    )
    store.save_entity(tool)


# =============================================================================
# When Steps
# =============================================================================


@when("the prune-detect protocol is executed")
def execute_prune_detect(test_context):
    """Execute the prune-detect protocol."""
    db_path = test_context["db_path"]

    result = execute_protocol(
        db_path=db_path,
        protocol_id="protocol-prune-detect",
        inputs={"db_path": db_path},
    )

    test_context["result"] = result


@when('I dispatch "prune-detect" through CvmEngine')
def dispatch_prune_detect(test_context):
    """Dispatch prune-detect through the unified engine."""
    engine = test_context["engine"]
    result = engine.dispatch(
        "prune-detect",
        {"db_path": test_context["db_path"]},
    )
    test_context["result"] = result.data if result.ok else {"status": "error", "error": result.error_message}


# =============================================================================
# Then Steps
# =============================================================================


@then(parsers.parse('the result status is "{expected_status}"'))
def check_status(test_context, expected_status: str):
    """Verify the result status matches expected."""
    result = test_context["result"]
    assert result.get("status") == expected_status, (
        f"Expected status '{expected_status}', got '{result.get('status')}'"
    )


@then("the result data contains orphan_tools")
def check_has_orphan_tools(test_context):
    """Verify result data contains orphan_tools."""
    result = test_context["result"]
    data = result.get("data", result)  # Handle both protocol result and direct data
    assert "orphan_tools" in data, f"Expected orphan_tools in data, got: {data.keys()}"


@then("the result data contains deprecated_tools")
def check_has_deprecated_tools(test_context):
    """Verify result data contains deprecated_tools."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "deprecated_tools" in data, f"Expected deprecated_tools in data, got: {data.keys()}"


@then(parsers.parse('orphan_tools includes "{tool_id}"'))
def check_orphan_includes(test_context, tool_id: str):
    """Verify orphan_tools includes the specified tool."""
    result = test_context["result"]
    data = result.get("data", result)
    orphans = data.get("orphan_tools", [])

    # Handle both list of dicts and list of IDs
    orphan_ids = [t["id"] if isinstance(t, dict) else t for t in orphans]
    assert tool_id in orphan_ids, (
        f"Expected '{tool_id}' in orphan_tools, got: {orphan_ids}"
    )


@then(parsers.parse('orphan_tools does not include "{tool_id}"'))
def check_orphan_excludes(test_context, tool_id: str):
    """Verify orphan_tools does not include the specified tool."""
    result = test_context["result"]
    data = result.get("data", result)
    orphans = data.get("orphan_tools", [])

    orphan_ids = [t["id"] if isinstance(t, dict) else t for t in orphans]
    assert tool_id not in orphan_ids, (
        f"Expected '{tool_id}' NOT in orphan_tools, got: {orphan_ids}"
    )


@then(parsers.parse('deprecated_tools includes "{tool_id}"'))
def check_deprecated_includes(test_context, tool_id: str):
    """Verify deprecated_tools includes the specified tool."""
    result = test_context["result"]
    data = result.get("data", result)
    deprecated = data.get("deprecated_tools", [])

    deprecated_ids = [t["id"] if isinstance(t, dict) else t for t in deprecated]
    assert tool_id in deprecated_ids, (
        f"Expected '{tool_id}' in deprecated_tools, got: {deprecated_ids}"
    )


@then(parsers.parse('deprecated_tools does not include "{tool_id}"'))
def check_deprecated_excludes(test_context, tool_id: str):
    """Verify deprecated_tools does not include the specified tool."""
    result = test_context["result"]
    data = result.get("data", result)
    deprecated = data.get("deprecated_tools", [])

    deprecated_ids = [t["id"] if isinstance(t, dict) else t for t in deprecated]
    assert tool_id not in deprecated_ids, (
        f"Expected '{tool_id}' NOT in deprecated_tools, got: {deprecated_ids}"
    )


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


@then("the dispatch result is successful")
def check_dispatch_success(test_context):
    """Verify dispatch succeeded."""
    result = test_context["result"]
    assert result.get("status") != "error", f"Expected success, got error: {result}"


@then("the result data contains a summary")
def check_has_summary(test_context):
    """Verify result contains summary."""
    result = test_context["result"]
    data = result.get("data", result)
    assert "summary" in data, f"Expected summary in data, got: {data.keys()}"
