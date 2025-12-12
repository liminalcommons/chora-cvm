"""
chora-cvm: The Chora Core Virtual Machine.

Public API re-exports from kernel/ (machinery) and lib/ (vocabulary).
"""
from .kernel.schema import (
    ExecutionContext,
    GenericEntity,
    PrimitiveEntity,
    ProtocolEntity,
    StateEntity,
    EventRecord,
    PrimitiveData,
    ProtocolData,
)
from .kernel.store import EventStore
from .kernel.registry import PrimitiveRegistry
from .kernel.vm import ProtocolVM
from .kernel.engine import CvmEngine

__all__ = [
    # Schema
    "ExecutionContext",
    "GenericEntity",
    "PrimitiveEntity",
    "ProtocolEntity",
    "StateEntity",
    "EventRecord",
    "PrimitiveData",
    "ProtocolData",
    # Store
    "EventStore",
    # Registry
    "PrimitiveRegistry",
    # VM
    "ProtocolVM",
    # Engine
    "CvmEngine",
]
