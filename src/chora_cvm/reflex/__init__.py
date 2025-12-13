"""
Reflex Arc: Self-Healing Autonomic Responses.

The reflex module contains autonomic responses that detect system health issues
and emit signals to the Loom. These are the system's proprioceptive sensors -
they allow the Loom to feel its own structural integrity.

Available reflexes:
- build: Detects build quality regressions (lint, typecheck, test failures)
"""

from .build import run_build_reflex, BuildReflexResult

__all__ = ["run_build_reflex", "BuildReflexResult"]
