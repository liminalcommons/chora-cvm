"""
Backward compatibility shim: Re-export from kernel/runner.py.

The actual implementation lives in kernel/runner.py.
This shim allows existing imports like `from .runner import X` to continue working.
"""
from .kernel.runner import *  # noqa: F401, F403
from .kernel.runner import execute_protocol, hydrate_primitives, load_protocol

__all__ = ["execute_protocol", "hydrate_primitives", "load_protocol"]
