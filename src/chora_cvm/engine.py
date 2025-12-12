"""
Backward compatibility shim: Re-export from kernel/engine.py.

The actual implementation lives in kernel/engine.py.
This shim allows existing imports like `from .engine import X` to continue working.
"""
from .kernel.engine import *  # noqa: F401, F403
from .kernel.engine import CvmEngine

__all__ = ["CvmEngine"]
