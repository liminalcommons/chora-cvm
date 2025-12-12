"""
Bootstrap: Self-Manifestation for CVM Subsystems.

The bootstrap module contains functions that manifest structural entities
into the Loom. These are the system's self-definition capabilities -
allowing the CVM to extend itself through entity creation.

Available bootstraps:
- build: Manifest build governance entities (principles, patterns, behaviors, tools)
"""

from .build import bootstrap_build_entities, BuildBootstrapResult

__all__ = ["bootstrap_build_entities", "BuildBootstrapResult"]
