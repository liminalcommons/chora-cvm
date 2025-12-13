from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any, Callable, Dict, Optional

from .schema import PrimitiveEntity


PrimitiveFn = Callable[..., Any]


@dataclass
class PrimitiveRecord:
    entity: PrimitiveEntity
    handler: Optional[PrimitiveFn]


ProtocolInvoker = Callable[[str, Dict[str, Any]], Dict[str, Any]]


class PrimitiveRegistry:
    def __init__(self) -> None:
        self._registry: Dict[str, PrimitiveRecord] = {}
        self._invoke_protocol: Optional[ProtocolInvoker] = None

    def set_protocol_invoker(self, invoker: ProtocolInvoker) -> None:
        """Register the protocol invocation callback.

        This allows primitives to invoke sub-protocols without importing runner,
        preventing circular dependencies.
        """
        self._invoke_protocol = invoker

    def invoke_protocol(
        self, protocol_id: str, inputs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Invoke a protocol from within a primitive.

        Raises RuntimeError if no invoker is registered.
        """
        if self._invoke_protocol is None:
            raise RuntimeError("No protocol invoker registered")
        return self._invoke_protocol(protocol_id, inputs)

    def register_from_entity(self, entity: PrimitiveEntity) -> None:
        handler: Optional[PrimitiveFn] = None
        python_ref = entity.data.python_ref
        try:
            module_name, func_name = python_ref.rsplit(".", 1)
            module = import_module(module_name)
            handler = getattr(module, func_name)
        except Exception:
            handler = None

        self._registry[entity.id] = PrimitiveRecord(entity=entity, handler=handler)

    def get(self, primitive_id: str) -> PrimitiveRecord:
        return self._registry[primitive_id]
