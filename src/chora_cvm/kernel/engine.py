"""
CvmEngine: The Unified Entry Point for All Interfaces.

This module implements the Event Horizon — the single point where all external
interfaces (CLI, API, MCP) converge into the CVM physics. Everything outside
is "The Void" (Shell, Network, Mind); everything inside is "The Physics."

Architecture:
    CLI ────┐
    API ────┼──> CvmEngine.dispatch() ──> Protocol/Primitive Execution
    MCP ────┘

The Litmus Test:
    "I create a new Protocol in the database, and it instantly becomes available
    as a CLI command, an API endpoint, and an Agent tool, without writing a
    single line of Python."
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from .registry import PrimitiveRegistry
from .runner import execute_protocol, hydrate_primitives, load_protocol
from .store import EventStore


class CapabilityKind(Enum):
    """The two kinds of invocable capabilities in the CVM."""
    PROTOCOL = "protocol"
    PRIMITIVE = "primitive"


@dataclass
class Capability:
    """A discoverable capability in the CVM."""
    id: str
    kind: CapabilityKind
    description: str
    interface: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DispatchResult:
    """Result of a dispatch operation."""
    ok: bool
    data: Dict[str, Any] = field(default_factory=dict)
    error_kind: Optional[str] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {"ok": self.ok, "data": self.data}
        if not self.ok:
            result["error_kind"] = self.error_kind
            result["error_message"] = self.error_message
        return result


class CvmEngine:
    """
    The Single Point of Entry — all interfaces converge here.

    This is the Event Horizon of the CVM. External interfaces (CLI, API, MCP)
    translate user intent into dispatch calls. The engine resolves the intent
    to a protocol or primitive, validates inputs, and executes.

    Example:
        engine = CvmEngine("path/to/db")
        result = engine.dispatch("horizon", {"days": 7}, output_sink=print)
    """

    def __init__(self, db_path: str):
        """
        Initialize the engine with a database path.

        Args:
            db_path: Path to the CVM SQLite database.
        """
        self.db_path = db_path
        self._store: Optional[EventStore] = None
        self._registry: Optional[PrimitiveRegistry] = None
        self._hydrated = False

    def _ensure_hydrated(self) -> None:
        """Lazily initialize store and registry."""
        if self._hydrated:
            return

        if not Path(self.db_path).exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        self._store = EventStore(self.db_path)
        self._registry = PrimitiveRegistry()
        hydrate_primitives(self._store, self._registry)
        self._hydrated = True

    @property
    def store(self) -> EventStore:
        """Get the event store (lazily initialized)."""
        self._ensure_hydrated()
        assert self._store is not None
        return self._store

    @property
    def registry(self) -> PrimitiveRegistry:
        """Get the primitive registry (lazily initialized)."""
        self._ensure_hydrated()
        assert self._registry is not None
        return self._registry

    def list_capabilities(self) -> List[Capability]:
        """
        Return all available protocols and primitives.

        This enables dynamic discovery — interfaces can enumerate
        what the CVM can do without hardcoding.

        Returns:
            List of Capability objects describing available operations.
        """
        self._ensure_hydrated()
        capabilities: List[Capability] = []

        # Query protocols
        import sqlite3
        import json

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # Get protocols
        cur = conn.execute(
            "SELECT id, data_json FROM entities WHERE type = 'protocol'"
        )
        for row in cur.fetchall():
            data = json.loads(row["data_json"])
            interface = data.get("interface", {})
            description = interface.get("description", f"Protocol {row['id']}")

            capabilities.append(Capability(
                id=row["id"],
                kind=CapabilityKind.PROTOCOL,
                description=description,
                interface=interface,
            ))

        # Get primitives
        cur = conn.execute(
            "SELECT id, data_json FROM entities WHERE type = 'primitive'"
        )
        for row in cur.fetchall():
            data = json.loads(row["data_json"])
            description = data.get("description", f"Primitive {row['id']}")
            interface = data.get("interface", {})

            capabilities.append(Capability(
                id=row["id"],
                kind=CapabilityKind.PRIMITIVE,
                description=description,
                interface=interface,
            ))

        conn.close()
        return capabilities

    def resolve_intent(self, intent: str) -> Optional[Capability]:
        """
        Resolve an intent string to a capability.

        Intent resolution follows these rules:
        1. Exact match on protocol ID (e.g., "protocol-horizon")
        2. Exact match on primitive ID (e.g., "primitive-entity-get")
        3. Short name match for protocols (e.g., "horizon" -> "protocol-horizon")
        4. Short name match for primitives (e.g., "entity_get" -> "primitive-entity-get")

        Args:
            intent: The intent string to resolve.

        Returns:
            Capability if found, None otherwise.
        """
        capabilities = self.list_capabilities()

        # Build lookup maps
        by_id = {cap.id: cap for cap in capabilities}
        by_short_name: Dict[str, Capability] = {}

        for cap in capabilities:
            # Extract short name
            if cap.kind == CapabilityKind.PROTOCOL and cap.id.startswith("protocol-"):
                short = cap.id[9:]  # Remove "protocol-" prefix
                by_short_name[short] = cap
            elif cap.kind == CapabilityKind.PRIMITIVE and cap.id.startswith("primitive-"):
                short = cap.id[10:]  # Remove "primitive-" prefix
                by_short_name[short] = cap
                # Also support underscore variant
                by_short_name[short.replace("-", "_")] = cap

        # Try exact match first
        if intent in by_id:
            return by_id[intent]

        # Try short name
        if intent in by_short_name:
            return by_short_name[intent]

        return None

    def dispatch(
        self,
        intent: str,
        inputs: Optional[Dict[str, Any]] = None,
        output_sink: Optional[Callable[[str], None]] = None,
        persona_id: Optional[str] = None,
        state_id: Optional[str] = None,
    ) -> DispatchResult:
        """
        Resolve intent to protocol/primitive, validate inputs, execute.

        This is the ONLY entry point for all interfaces.

        Args:
            intent: What to execute (protocol or primitive name/ID).
            inputs: Input parameters for the execution.
            output_sink: Callback for output (I/O Membrane pattern).
            persona_id: Optional persona context.
            state_id: Optional state ID for tracking.

        Returns:
            DispatchResult containing success/failure and data.
        """
        inputs = inputs or {}

        # Resolve intent to capability
        capability = self.resolve_intent(intent)
        if capability is None:
            return DispatchResult(
                ok=False,
                error_kind="intent_not_found",
                error_message=f"Could not resolve intent: {intent}",
            )

        # Execute based on capability kind
        if capability.kind == CapabilityKind.PROTOCOL:
            return self._dispatch_protocol(
                capability.id, inputs, output_sink, persona_id, state_id
            )
        else:
            return self._dispatch_primitive(capability.id, inputs, output_sink)

    def _dispatch_protocol(
        self,
        protocol_id: str,
        inputs: Dict[str, Any],
        output_sink: Optional[Callable[[str], None]],
        persona_id: Optional[str],
        state_id: Optional[str],
    ) -> DispatchResult:
        """Execute a protocol through the existing runner."""
        result = execute_protocol(
            db_path=self.db_path,
            protocol_id=protocol_id,
            inputs=inputs,
            persona_id=persona_id,
            state_id=state_id,
            output_sink=output_sink,
        )

        # Normalize result to DispatchResult
        if result.get("status") == "error":
            return DispatchResult(
                ok=False,
                error_kind=result.get("error_kind", "execution_error"),
                error_message=result.get("error_message", "Unknown error"),
            )

        return DispatchResult(ok=True, data=result)

    def _dispatch_primitive(
        self,
        primitive_id: str,
        inputs: Dict[str, Any],
        output_sink: Optional[Callable[[str], None]],
    ) -> DispatchResult:
        """Execute a primitive directly."""
        self._ensure_hydrated()

        # Get the primitive record from registry
        try:
            record = self.registry.get(primitive_id)
        except KeyError:
            return DispatchResult(
                ok=False,
                error_kind="primitive_not_found",
                error_message=f"Primitive not registered: {primitive_id}",
            )

        if record.handler is None:
            return DispatchResult(
                ok=False,
                error_kind="primitive_not_loaded",
                error_message=f"Primitive handler could not be loaded: {primitive_id}",
            )

        func = record.handler

        # Build context for primitives that need it
        from .schema import ExecutionContext

        context = ExecutionContext(
            db_path=self.db_path,
            store=self.store,
            output_sink=output_sink,
        )

        # Execute the primitive
        try:
            # Inject context if the primitive accepts it
            import inspect
            sig = inspect.signature(func)
            if "ctx" in sig.parameters:
                result = func(ctx=context, **inputs)
            else:
                result = func(**inputs)

            # Normalize result
            if isinstance(result, dict):
                return DispatchResult(ok=True, data=result)
            else:
                return DispatchResult(ok=True, data={"result": result})

        except Exception as e:
            return DispatchResult(
                ok=False,
                error_kind="primitive_execution_error",
                error_message=str(e),
            )

    def close(self) -> None:
        """Close the engine and release resources."""
        if self._store:
            self._store.close()
            self._store = None
        self._registry = None
        self._hydrated = False
