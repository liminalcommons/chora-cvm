from __future__ import annotations

from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class PrimitiveInterfaceSchema(BaseModel):
    type: str = "object"
    properties: Dict[str, Any] = Field(default_factory=dict)
    required: List[str] = Field(default_factory=list)


class PrimitiveData(BaseModel):
    python_ref: str
    description: Optional[str] = None
    interface: Dict[str, Any]


class PrimitiveEntity(BaseModel):
    id: str
    type: str = "primitive"
    version: int = 1
    status: str = "active"
    data: PrimitiveData


class ConditionOp(str, Enum):
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    LT = "lt"
    CONTAINS = "contains"
    EMPTY = "empty"


class EdgeCondition(BaseModel):
    op: ConditionOp
    path: str
    value: Optional[Any] = None


class ProtocolNodeKind(str, Enum):
    CALL = "call"
    RETURN = "return"


class ProtocolNode(BaseModel):
    kind: ProtocolNodeKind
    ref: Optional[str] = None
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)


class ProtocolEdge(BaseModel):
    from_node: str = Field(alias="from")
    to_node: str = Field(alias="to")
    condition: Optional[EdgeCondition] = None
    default: bool = False


class ProtocolGraph(BaseModel):
    start: str
    nodes: Dict[str, ProtocolNode]
    edges: List[ProtocolEdge]


class ProtocolInterface(BaseModel):
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)


class ProtocolData(BaseModel):
    interface: ProtocolInterface
    graph: ProtocolGraph
    # Optional metadata for command palette integration
    title: Optional[str] = None
    description: Optional[str] = None
    group: Optional[str] = None
    internal: Optional[bool] = None
    inputs_schema: Optional[Dict[str, Any]] = None


class ProtocolEntity(BaseModel):
    id: str
    type: str = "protocol"
    version: int = 1
    status: str = "active"
    data: ProtocolData


class GenericEntity(BaseModel):
    """
    Container for any entity type not strictly modeled by the CVM kernel
    (for example: Decemvirate story, tool, persona).
    """

    id: str
    type: str
    version: int = 1
    status: str = "active"
    data: Dict[str, Any] = Field(default_factory=dict)


class CircleEntity(BaseModel):
    """
    Circle: container for a working context (people, assets, rhythms).
    """

    id: str
    type: str = "circle"
    version: int = 1
    status: str = "active"
    data: Dict[str, Any] = Field(default_factory=dict)


class AssetEntity(BaseModel):
    """
    Asset: external resource (repo, DB, bucket, transcript, etc.) bound to a circle.
    """

    id: str
    type: str = "asset"
    version: int = 1
    status: str = "active"
    data: Dict[str, Any] = Field(default_factory=dict)


class StateStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    FULFILLED = "fulfilled"
    STRESSED = "stressed"
    SUSPENDED = "suspended"
    CANCELLED = "cancelled"


class StateError(BaseModel):
    kind: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class StateData(BaseModel):
    protocol_id: str
    protocol_version: int
    parent_state_id: Optional[str] = None
    cursor: Optional[str] = None
    exit_node: Optional[str] = None  # Records which RETURN node was hit
    memory: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[StateError] = None


class StateEntity(BaseModel):
    id: str
    type: str = "state"
    status: StateStatus = StateStatus.PENDING
    data: StateData


class ExecutionContext(BaseModel):
    """Context passed to primitives during execution.

    Enables dependency injection of shared resources like database connections,
    avoiding per-primitive connection instantiation.

    The output_sink enables I/O Membrane pattern: the Nucleus (logic) is decoupled
    from the Membrane (display). CLI passes print, API passes a buffer collector.
    """

    db_path: str
    store: Optional[Any] = None  # Injected EventStore instance (avoid circular import)
    persona_id: Optional[str] = None
    state_id: Optional[str] = None

    # The Membrane Injection - excluded from serialization (Callable can't be JSON)
    output_sink: Optional[Callable[[str], None]] = Field(default=None, exclude=True)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def emit(self, content: str) -> None:
        """Send output to the configured sink, or stdout as fallback."""
        if self.output_sink:
            self.output_sink(content)
        else:
            print(content)


class EventType(str, Enum):
    MANIFEST = "manifest"
    BOND = "bond"
    SIGNAL = "signal"
    PROTOCOL_SPAWN = "protocol_spawn"
    PROTOCOL_STEP = "protocol_step"


class EventOp(str, Enum):
    SUCCESS = "success"
    ERROR = "error"
    RETRY = "retry"
    SUSPEND = "suspend"
    RESUME = "resume"


class EventClock(BaseModel):
    actor: str
    seq: int


class EventRecord(BaseModel):
    id: str
    clock: EventClock
    type: EventType
    op: EventOp
    persona_id: Optional[str] = None
    signature: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
