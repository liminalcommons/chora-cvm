"""
Step definitions for the Exit Node Recording feature.

Verifies that the VM records which RETURN node was executed,
making branching protocols debuggable.
"""
import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.schema import (
    ProtocolEntity,
    ProtocolData,
    ProtocolGraph,
    ProtocolNode,
    ProtocolEdge,
    ProtocolNodeKind,
    ProtocolInterface,
    StateEntity,
    StateData,
    StateStatus,
    EdgeCondition,
    ConditionOp,
)
from chora_cvm.registry import PrimitiveRegistry
from chora_cvm.vm import ProtocolVM

# Load scenarios from feature file
scenarios("../features/exit_node.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {}


@pytest.fixture
def registry():
    """Create a primitive registry with a simple test primitive."""
    reg = PrimitiveRegistry()
    return reg


# =============================================================================
# Protocol Builders
# =============================================================================


def make_simple_protocol(return_node_id: str = "return-success") -> ProtocolEntity:
    """Create a protocol with a single return node."""
    return ProtocolEntity(
        id="protocol-test-simple",
        data=ProtocolData(
            interface=ProtocolInterface(),
            graph=ProtocolGraph(
                start=return_node_id,
                nodes={
                    return_node_id: ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={"result": "success"},
                    )
                },
                edges=[],
            ),
        ),
    )


def make_branching_protocol() -> ProtocolEntity:
    """Create a protocol with two conditional branches."""
    return ProtocolEntity(
        id="protocol-test-branching",
        data=ProtocolData(
            interface=ProtocolInterface(inputs={"branch": "string"}),
            graph=ProtocolGraph(
                start="check-condition",
                nodes={
                    "check-condition": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="primitive-identity",
                        inputs={"value": "$.inputs.branch"},
                        outputs={},
                    ),
                    "return-a": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={"result": "path-a"},
                    ),
                    "return-b": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={"result": "path-b"},
                    ),
                },
                edges=[
                    ProtocolEdge.model_validate({
                        "from": "check-condition",
                        "to": "return-a",
                        "condition": {
                            "path": "$.inputs.branch",
                            "op": "eq",
                            "value": "a",
                        },
                    }),
                    ProtocolEdge.model_validate({
                        "from": "check-condition",
                        "to": "return-b",
                        "default": True,
                    }),
                ],
            ),
        ),
    )


def make_multi_return_protocol() -> ProtocolEntity:
    """Create a protocol with multiple return nodes for extraction test."""
    return ProtocolEntity(
        id="protocol-test-multi-return",
        data=ProtocolData(
            interface=ProtocolInterface(),
            graph=ProtocolGraph(
                start="return-default",
                nodes={
                    "return-default": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={"from": "default"},
                    ),
                    "return-special": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={"from": "special"},
                    ),
                },
                edges=[],
            ),
        ),
    )


# =============================================================================
# Given Steps
# =============================================================================


@given(parsers.parse('a protocol with a single RETURN node "{node_id}"'))
def simple_protocol(test_context, node_id: str):
    """Create a simple protocol with one return node."""
    test_context["protocol"] = make_simple_protocol(node_id)


@given('a protocol with two paths leading to "return-a" and "return-b"')
def branching_protocol(test_context, registry):
    """Create a branching protocol."""
    test_context["protocol"] = make_branching_protocol()

    # Register identity primitive for the condition check
    from chora_cvm.schema import PrimitiveEntity, PrimitiveData

    identity_primitive = PrimitiveEntity(
        id="primitive-identity",
        data=PrimitiveData(
            python_ref="chora_cvm.std.identity_primitive",
            description="Identity primitive for testing",
            interface={},
        ),
    )
    registry.register_from_entity(identity_primitive)
    test_context["registry"] = registry


@given('the input triggers the path to "return-b"')
def set_branch_b(test_context):
    """Set inputs to trigger branch b (default path)."""
    test_context["inputs"] = {"branch": "b"}


@given(parsers.parse('a fulfilled state with exit_node "{node_id}"'))
def fulfilled_state_with_exit(test_context, node_id: str):
    """Create a fulfilled state with specified exit node."""
    state = StateEntity(
        id="state-test",
        status=StateStatus.FULFILLED,
        data=StateData(
            protocol_id="protocol-test-multi-return",
            protocol_version=1,
            exit_node=node_id,
            memory={"inputs": {}},
        ),
    )
    test_context["state"] = state


@given("a protocol with multiple RETURN nodes with different outputs")
def multi_return_protocol(test_context):
    """Create a protocol with multiple return nodes."""
    test_context["protocol"] = make_multi_return_protocol()


# =============================================================================
# When Steps
# =============================================================================


@when("I execute the protocol")
def execute_protocol(test_context, registry):
    """Execute the protocol to completion."""
    protocol = test_context["protocol"]
    inputs = test_context.get("inputs", {})
    reg = test_context.get("registry", registry)

    vm = ProtocolVM(reg)
    state = vm.spawn(protocol, inputs)
    state.id = "state-test"
    state.status = StateStatus.RUNNING

    # Run to completion
    while state.status not in (StateStatus.FULFILLED, StateStatus.STRESSED):
        state, child = vm.step(protocol, state)
        if child:
            # For this test, we don't expect nested protocols
            break

    test_context["state"] = state


@when("I extract output from the state")
def extract_output(test_context, registry):
    """Extract output from the fulfilled state."""
    protocol = test_context["protocol"]
    state = test_context["state"]

    vm = ProtocolVM(registry)
    output = vm.extract_output(protocol, state)
    test_context["output"] = output


# =============================================================================
# Then Steps
# =============================================================================


@then(parsers.parse('the state exit_node is "{expected}"'))
def check_exit_node(test_context, expected: str):
    """Verify the exit node was recorded correctly."""
    state = test_context["state"]
    actual = state.data.exit_node
    assert actual == expected, f"Expected exit_node '{expected}', got '{actual}'"


@then(parsers.parse('the output comes from "{node_id}" node'))
def check_output_source(test_context, node_id: str):
    """Verify output came from the correct node."""
    output = test_context["output"]
    # The multi-return protocol has outputs that identify the source
    assert output.get("from") == "special", f"Expected output from special, got {output}"
