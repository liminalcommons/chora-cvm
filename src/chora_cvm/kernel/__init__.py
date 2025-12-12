"""
Kernel: The machinery of the CVM.

This module contains the execution infrastructure:
- schema: Entity and event data structures
- store: Event-sourced persistence layer
- registry: Primitive function registry
- vm: Protocol execution virtual machine
- engine: High-level CVM orchestration

The kernel is distinct from lib/ (the vocabulary/primitives).
Kernel = machinery. Lib = language.
"""
from .schema import (
    ExecutionContext,
    GenericEntity,
    PrimitiveEntity,
    ProtocolEntity,
    StateEntity,
    EventRecord,
    PrimitiveData,
    ProtocolData,
)
from .store import EventStore
from .registry import PrimitiveRegistry
from .vm import ProtocolVM
from .engine import CvmEngine

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
