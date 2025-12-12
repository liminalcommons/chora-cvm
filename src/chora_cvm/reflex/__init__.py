"""
Reflex Arc: Self-Healing Autonomic Responses.

The reflex module contains autonomic responses that detect system health issues
and emit signals to the Loom. These are the system's proprioceptive sensors -
they allow the Loom to feel its own structural integrity.

Available reflexes:
- build: Detects build quality regressions (lint, typecheck, test failures)

The build reflex arc includes:
- Signal emission for failures
- Focus triggering (signal → focus via triggers bond)
- Learning harvesting (patterns become learnings)
- Observability surface (get_build_learnings, get_active_build_signals)
"""

from .build import (
    run_build_reflex,
    BuildReflexResult,
    get_active_build_signals,
    get_build_learnings,
    trigger_focus,
    harvest_learning,
)

__all__ = [
    "run_build_reflex",
    "BuildReflexResult",
    "get_active_build_signals",
    "get_build_learnings",
    "trigger_focus",
    "harvest_learning",
]
