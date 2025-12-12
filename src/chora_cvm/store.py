"""
Backward compatibility shim: Re-export from kernel/store.py.

The actual implementation lives in kernel/store.py.
This shim allows existing imports like `from .store import X` to continue working.
"""
from .kernel.store import *  # noqa: F401, F403
from .kernel.store import EventStore

__all__ = ["EventStore"]
