"""
Backward compatibility shim: Re-export from kernel/schema.py.

The actual implementation lives in kernel/schema.py.
This shim allows existing imports like `from .schema import X` to continue working.
"""
from .kernel.schema import *  # noqa: F401, F403
from .kernel.schema import (
    AssetEntity,
    CircleEntity,
    ConditionOp,
    EdgeCondition,
    EventClock,
    EventOp,
    EventRecord,
    EventType,
    ExecutionContext,
    GenericEntity,
    PrimitiveData,
    PrimitiveEntity,
    PrimitiveInterfaceSchema,
    ProtocolData,
    ProtocolEdge,
    ProtocolEntity,
    ProtocolGraph,
    ProtocolInterface,
    ProtocolNode,
    ProtocolNodeKind,
    StateData,
    StateEntity,
    StateError,
    StateStatus,
)

__all__ = [
    "AssetEntity",
    "CircleEntity",
    "ConditionOp",
    "EdgeCondition",
    "EventClock",
    "EventOp",
    "EventRecord",
    "EventType",
    "ExecutionContext",
    "GenericEntity",
    "PrimitiveData",
    "PrimitiveEntity",
    "PrimitiveInterfaceSchema",
    "ProtocolData",
    "ProtocolEdge",
    "ProtocolEntity",
    "ProtocolGraph",
    "ProtocolInterface",
    "ProtocolNode",
    "ProtocolNodeKind",
    "StateData",
    "StateEntity",
    "StateError",
    "StateStatus",
]
