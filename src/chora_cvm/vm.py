"""
Backward compatibility shim: Re-export from kernel/vm.py.

The actual implementation lives in kernel/vm.py.
This shim allows existing imports like `from .vm import X` to continue working.
"""
from .kernel.vm import *  # noqa: F401, F403
from .kernel.vm import ProtocolVM

__all__ = ["ProtocolVM"]
