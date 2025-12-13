"""
Protocol Virtual Machine: The Brain of the CVM.
"""

from __future__ import annotations

import inspect
import re
from typing import Any, Callable, Dict, Optional, Tuple

from .registry import PrimitiveRegistry
from .schema import (
    ConditionOp,
    EdgeCondition,
    ExecutionContext,
    ProtocolEntity,
    ProtocolGraph,
    ProtocolNodeKind,
    StateData,
    StateEntity,
    StateError,
    StateStatus,
)


class ProtocolVM:
    def __init__(
        self,
        primitives: PrimitiveRegistry,
        protocol_loader: Optional[Callable[[str], Optional[ProtocolEntity]]] = None,
        context: Optional[ExecutionContext] = None,
    ) -> None:
        self._primitives = primitives
        self._loader = protocol_loader
        self._context = context

    def spawn(self, protocol: ProtocolEntity, inputs: Dict[str, Any]) -> StateEntity:
        state_data = StateData(
            protocol_id=protocol.id,
            protocol_version=protocol.version,
            cursor=protocol.data.graph.start,
            memory={"inputs": inputs},
        )
        # TODO: state id generation will move to the store/event layer
        return StateEntity(id="state-tbd", data=state_data)

    def extract_output(self, protocol: ProtocolEntity, state: StateEntity) -> Dict[str, Any]:
        """Extract outputs from a fulfilled state using the recorded exit node."""
        graph = protocol.data.graph

        # If we recorded which return node was hit, use it directly
        exit_node = state.data.exit_node
        if exit_node and exit_node in graph.nodes:
            node = graph.nodes[exit_node]
            if node.kind == ProtocolNodeKind.RETURN:
                return self._map_inputs(node.outputs, state.data.memory)

        # Fallback to scan (for backwards compatibility with existing states)
        for node_id, node in graph.nodes.items():
            if node.kind == ProtocolNodeKind.RETURN:
                return self._map_inputs(node.outputs, state.data.memory)
        return {}

    def step(
        self,
        protocol: ProtocolEntity,
        state: StateEntity,
        child_result: Optional[Dict[str, Any]] = None,
    ) -> Tuple[StateEntity, Optional[StateEntity]]:
        """
        Execute one step of the protocol.

        Returns:
            (updated_state, child_state)
            If child_state is returned, the runner should push it to the stack
            and execute it until fulfillment.
        """
        # Handle resumption from suspension
        if state.status == StateStatus.SUSPENDED:
            if child_result is not None:
                cursor = state.data.cursor
                if cursor:
                    state.data.memory[cursor] = child_result
                    return self._advance_cursor(protocol.data.graph, state, cursor), None
            return state, None

        if state.status not in (StateStatus.PENDING, StateStatus.RUNNING):
            return state, None

        graph = protocol.data.graph
        cursor = state.data.cursor

        if cursor is None or cursor not in graph.nodes:
            state.status = StateStatus.FULFILLED
            state.data.cursor = None
            return state, None

        node = graph.nodes[cursor]

        if node.kind == ProtocolNodeKind.CALL:
            ref = node.ref
            if not ref:
                return self._stress_state(state, "config_error", f"Node {cursor} missing ref"), None

            # Protocol call (recursion) - check first since protocol refs are more specific
            if ref.startswith("protocol-"):
                if not self._loader:
                    return self._stress_state(state, "config_error", "No protocol loader configured"), None

                child_protocol = self._loader(ref)
                if not child_protocol:
                    return self._stress_state(state, "protocol_error", f"Protocol {ref} not found"), None

                try:
                    child_inputs = self._map_inputs(node.inputs, state.data.memory)
                except Exception as exc:
                    return self._stress_state(state, "mapping_error", str(exc)), None

                child_state = self.spawn(child_protocol, child_inputs)
                child_state.data.parent_state_id = state.id
                state.status = StateStatus.SUSPENDED
                return state, child_state

            # Primitive call (supports both legacy "primitive-*" and Crystal Palace "domain.noun.verb")
            primitive = self._primitives.get(ref)
            if primitive is None or primitive.handler is None:
                return self._stress_state(state, "primitive_error", f"Primitive {ref} not found"), None

            try:
                handler_kwargs = self._map_inputs(node.inputs, state.data.memory)
            except Exception as exc:
                return self._stress_state(state, "mapping_error", str(exc)), None

            # Inject execution context if available and handler accepts it
            if self._context and primitive.handler:
                sig = inspect.signature(primitive.handler)
                if "_ctx" in sig.parameters or any(
                    p.kind == inspect.Parameter.VAR_KEYWORD
                    for p in sig.parameters.values()
                ):
                    handler_kwargs["_ctx"] = self._context

            try:
                result = primitive.handler(**handler_kwargs)
            except Exception as exc:
                return self._stress_state(state, "runtime_error", str(exc)), None

            state.data.memory[cursor] = result
            return self._advance_cursor(graph, state, cursor), None

        if node.kind == ProtocolNodeKind.RETURN:
            state.status = StateStatus.FULFILLED
            state.data.exit_node = cursor  # Record which return node was hit
            state.data.cursor = None
            return state, None

        return self._stress_state(state, "config_error", f"Unknown node kind: {node.kind}"), None

    def _stress_state(self, state: StateEntity, kind: str, msg: str) -> StateEntity:
        state.status = StateStatus.STRESSED
        state.data.error = StateError(kind=kind, message=msg, details={})
        return state

    def _resolve_value(self, pointer: Any, memory: Dict[str, Any]) -> Any:
        # Recursively resolve nested dicts
        if isinstance(pointer, dict):
            return {k: self._resolve_value(v, memory) for k, v in pointer.items()}

        # Recursively resolve nested lists
        if isinstance(pointer, list):
            return [self._resolve_value(item, memory) for item in pointer]

        if not isinstance(pointer, str):
            return pointer

        if pointer.startswith("$."):
            path = pointer[2:].split(".")
            value: Any = memory
            for segment in path:
                if isinstance(value, dict) and segment in value:
                    value = value[segment]
                elif isinstance(value, list) and segment.isdigit():
                    idx = int(segment)
                    if 0 <= idx < len(value):
                        value = value[idx]
                    else:
                        return None
                else:
                    return None
            return value

        pattern = re.compile(r"{(\$\.[^}]+)}")

        def replacer(match: re.Match[str]) -> str:
            expr = match.group(1)
            resolved = self._resolve_value(expr, memory)
            return "" if resolved is None else str(resolved)

        if "{" in pointer and "$." in pointer:
            return pattern.sub(replacer, pointer)

        return pointer

    def _map_inputs(self, node_inputs: Dict[str, Any], memory: Dict[str, Any]) -> Dict[str, Any]:
        mapped: Dict[str, Any] = {}
        for key, value_ref in node_inputs.items():
            mapped[key] = self._resolve_value(value_ref, memory)
        return mapped

    def _evaluate_condition(self, condition: EdgeCondition, memory: Dict[str, Any]) -> bool:
        actual = self._resolve_value(condition.path, memory)
        expected = condition.value

        if condition.op == ConditionOp.EQ:
            return actual == expected
        if condition.op == ConditionOp.NEQ:
            return actual != expected
        if condition.op == ConditionOp.GT:
            return actual > expected
        if condition.op == ConditionOp.LT:
            return actual < expected
        if condition.op == ConditionOp.EMPTY:
            return not actual
        if condition.op == ConditionOp.CONTAINS:
            try:
                return expected in actual
            except TypeError:
                return False
        return False

    def _advance_cursor(
        self,
        graph: ProtocolGraph,
        state: StateEntity,
        current_node_id: str,
    ) -> StateEntity:
        candidates = [edge for edge in graph.edges if edge.from_node == current_node_id]

        # 1. First, try edges with conditions that evaluate to true
        for edge in candidates:
            if edge.condition and self._evaluate_condition(edge.condition, state.data.memory):
                state.data.cursor = edge.to_node
                state.status = StateStatus.RUNNING
                return state

        # 2. Then, try the default edge (for conditional branches)
        default_edge = next((edge for edge in candidates if edge.default), None)
        if default_edge:
            state.data.cursor = default_edge.to_node
            state.status = StateStatus.RUNNING
            return state

        # 3. Finally, try unconditional edges (no condition, not default - simple sequential flow)
        unconditional_edge = next(
            (edge for edge in candidates if not edge.condition and not edge.default),
            None,
        )
        if unconditional_edge:
            state.data.cursor = unconditional_edge.to_node
            state.status = StateStatus.RUNNING
            return state

        # No valid edge found - end of flow
        state.status = StateStatus.FULFILLED
        state.data.cursor = None
        return state
