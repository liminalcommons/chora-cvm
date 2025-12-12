"""
Backward compatibility shim: Re-export from kernel/registry.py.

The actual implementation lives in kernel/registry.py.
This shim allows existing imports like `from .registry import X` to continue working.
"""
from .kernel.registry import *  # noqa: F401, F403
from .kernel.registry import PrimitiveRegistry

__all__ = ["PrimitiveRegistry"]
