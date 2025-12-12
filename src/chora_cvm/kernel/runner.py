"""
Protocol Runner: Shared execution logic for CLI and Worker.

This module contains the core protocol execution machinery that can be used
by both synchronous (CLI) and asynchronous (Worker) execution paths.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .registry import PrimitiveRegistry
from .schema import ExecutionContext, PrimitiveEntity, ProtocolEntity, StateStatus
from .store import EventStore
from .vm import ProtocolVM


def hydrate_primitives(store: EventStore, registry: PrimitiveRegistry) -> None:
    """Load all primitives from the database into the registry."""
    conn = sqlite3.connect(store.path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT id, data_json FROM entities WHERE type = 'primitive'")

    for row in cur.fetchall():
        data = json.loads(row["data_json"])
        entity = PrimitiveEntity(id=row["id"], data=data)
        registry.register_from_entity(entity)

    conn.close()


def load_protocol(store: EventStore, protocol_id: str) -> Optional[ProtocolEntity]:
    """Load a protocol entity from the database."""
    conn = sqlite3.connect(store.path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, data_json FROM entities WHERE id = ? AND type = 'protocol'",
        (protocol_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    data = json.loads(row["data_json"])
    return ProtocolEntity(id=row["id"], data=data)


def run_protocol(
    store: EventStore,
    registry: PrimitiveRegistry,
    protocol: ProtocolEntity,
    inputs: Dict[str, Any],
    state_id: Optional[str] = None,
    persona_id: Optional[str] = None,
    output_sink: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Execute a protocol to completion.

    Returns the outputs from the fulfilled state.
    """
    # Create protocol loader for recursive calls
    def protocol_loader(pid: str) -> Optional[ProtocolEntity]:
        return load_protocol(store, pid)

    # Register protocol invoker so primitives can invoke sub-protocols
    # without importing runner (prevents circular dependencies)
    def invoke_nested(pid: str, pinputs: Dict[str, Any]) -> Dict[str, Any]:
        nested_protocol = load_protocol(store, pid)
        if not nested_protocol:
            return {"error": f"Protocol {pid} not found"}
        return run_protocol(
            store, registry, nested_protocol, pinputs,
            persona_id=persona_id,
        )

    registry.set_protocol_invoker(invoke_nested)

    # Create execution context for dependency injection
    # The output_sink enables I/O Membrane: Nucleus (logic) decoupled from Membrane (display)
    context = ExecutionContext(
        db_path=store.path,
        store=store,
        persona_id=persona_id,
        state_id=state_id,
        output_sink=output_sink,
    )

    vm = ProtocolVM(registry, protocol_loader=protocol_loader, context=context)

    # Spawn or resume state
    state = vm.spawn(protocol, inputs)
    state.id = state_id or f"state-{uuid.uuid4()}"
    state.status = StateStatus.RUNNING

    # Execution stack for recursive protocol calls
    stack = [(protocol, state)]

    while stack:
        current_protocol, current_state = stack[-1]

        if current_state.status == StateStatus.FULFILLED:
            # Pop and propagate result to parent
            stack.pop()
            if stack:
                parent_protocol, parent_state = stack[-1]
                result = vm.extract_output(current_protocol, current_state)
                vm.step(parent_protocol, parent_state, child_result=result)
            continue

        if current_state.status == StateStatus.STRESSED:
            # Error - propagate up
            error = current_state.data.error
            return {
                "status": "error",
                "error_kind": error.kind if error else "unknown",
                "error_message": error.message if error else "Unknown error",
            }

        # Step the current state
        updated_state, child_state = vm.step(current_protocol, current_state)
        stack[-1] = (current_protocol, updated_state)

        if child_state:
            # Push child onto stack
            child_protocol = protocol_loader(child_state.data.protocol_id)
            if child_protocol:
                child_state.status = StateStatus.RUNNING
                stack.append((child_protocol, child_state))

    # Extract final output
    return vm.extract_output(protocol, state)


def execute_protocol(
    db_path: str,
    protocol_id: str,
    inputs: Optional[Dict[str, Any]] = None,
    persona_id: Optional[str] = None,
    state_id: Optional[str] = None,
    output_sink: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    High-level protocol execution entry point.

    This is the primary interface for executing protocols, used by both
    the CLI (synchronous) and Worker (asynchronous) paths.

    Args:
        db_path: Path to the database
        protocol_id: ID of the protocol to execute
        inputs: Optional dictionary of input parameters
        persona_id: Optional persona context
        state_id: Optional state ID for tracking/resumption

    Returns:
        Dictionary containing protocol outputs or error information
    """
    if not Path(db_path).exists():
        return {
            "status": "error",
            "error_kind": "database_not_found",
            "error_message": f"Database not found: {db_path}",
        }

    store = EventStore(db_path)
    registry = PrimitiveRegistry()

    # Hydrate primitives
    hydrate_primitives(store, registry)

    # Load protocol
    protocol = load_protocol(store, protocol_id)
    if not protocol:
        store.close()
        return {
            "status": "error",
            "error_kind": "protocol_not_found",
            "error_message": f"Protocol not found: {protocol_id}",
        }

    # Build full inputs
    full_inputs: Dict[str, Any] = {"db_path": db_path}
    if inputs:
        full_inputs.update(inputs)
    if persona_id:
        full_inputs["persona_id"] = persona_id

    # Execute
    try:
        result = run_protocol(
            store,
            registry,
            protocol,
            full_inputs,
            state_id=state_id,
            persona_id=persona_id,
            output_sink=output_sink,
        )
    finally:
        store.close()

    return result
