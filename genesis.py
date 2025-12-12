"""
Genesis: Bootstrapping the Chora Core Virtual Machine.

This orchestrator coordinates bootstrapping across four domains:

1. genesis_crystal.py — The Crystal Palace
   - Domain-organized primitives using domain.noun.verb naming
   - 8 domains: attention, code, io, sys, logic, cognition, chronos, graph
   - Total: ~57 domain.* primitives

2. genesis_protocols.py — The Logic
   - Protocol entities that compose primitives into workflows
   - Wave 2: orient, digest, induce, sense-entropy
   - Wave 3: horizon, compost, detect-stagnation
   - Total: 7 protocol-* entities

3. genesis_behaviors.py — The Expectations
   - Behavior entities extracted from feature file @behavior:* tags
   - Total: ~103 behaviors across ~29 stories

4. genesis_stories.py — The Desires
   - Story entities with specifies bonds to behaviors
   - Total: ~29 stories

5. genesis_provenance.py — The Chain
   - implements bonds from behaviors to primitives
   - Total: ~150+ bonds

Usage:
    python genesis.py [db_path]
    # Default: chora-cvm.db

Idempotency:
    This script is safe to run multiple times. If the database already
    contains genesis entities (detected via sentinel check), it will
    skip with a message rather than re-creating entities.
"""

from __future__ import annotations

import sys
from pathlib import Path

from chora_cvm.store import EventStore
from chora_cvm.genesis_crystal import bootstrap_crystal_palace
from chora_cvm.genesis_protocols import bootstrap_protocols
from chora_cvm.genesis_behaviors import bootstrap_behaviors
from chora_cvm.genesis_stories import bootstrap_stories, bootstrap_specifies_bonds
from chora_cvm.genesis_provenance import bootstrap_implements_bonds

# Sentinel entity: if this exists, genesis has already run
GENESIS_SENTINEL = "graph.entity.create"


def is_genesis_complete(store: EventStore) -> bool:
    """Check if genesis has already been run by looking for sentinel entity."""
    entity = store.get_entity(GENESIS_SENTINEL)
    return entity is not None


def main(db_path: str = "chora-cvm.db", verbose: bool = True, force: bool = False) -> dict:
    """
    Bootstrap the complete Chora CVM genesis.

    This orchestrator calls bootstrap functions in sequence:
    1. Crystal Palace domains (domain.* primitives)
    2. Protocols (protocol-* entities)
    3. Behaviors (expectations from feature files)
    4. Stories (desires with specifies bonds)
    5. Provenance (implements bonds)

    Args:
        db_path: Path to the SQLite database
        verbose: Print progress messages
        force: If True, run genesis even if already complete

    Returns:
        Summary dict with counts and IDs of created entities.
        If genesis was skipped (already complete), returns {"skipped": True}.
    """
    store = EventStore(db_path)

    # =========================================================================
    # IDEMPOTENCY CHECK: Skip if genesis has already run
    # =========================================================================
    if not force and is_genesis_complete(store):
        if verbose:
            print(f"[*] Genesis: Already complete in {db_path}")
            print("    (Sentinel entity '{0}' exists)".format(GENESIS_SENTINEL))
            print("    To force re-run, use: python genesis.py {0} --force".format(db_path))
        store.close()
        return {"skipped": True, "db_path": db_path}

    if verbose:
        print(f"[*] Genesis: Bootstrapping Chora CVM in {db_path}")
        print("=" * 60)

    # =========================================================================
    # PHASE 1: CRYSTAL PALACE (domain.* primitives)
    # =========================================================================
    # Domain-organized primitives with domain.noun.verb naming
    if verbose:
        print("\n[PHASE 1] Crystal Palace Domains")
        print("-" * 60)

    crystal_result = bootstrap_crystal_palace(store, verbose=verbose)

    # =========================================================================
    # PHASE 2: PROTOCOLS (The Logic)
    # =========================================================================
    # Protocol graph entities that compose primitives
    if verbose:
        print("\n[PHASE 2] Protocols (Wave 2 + Wave 3)")
        print("-" * 60)

    protocols = bootstrap_protocols(store, verbose=verbose)

    # =========================================================================
    # PHASE 3: BEHAVIORS (The Expectations)
    # =========================================================================
    # Behavior entities from feature file @behavior:* tags
    if verbose:
        print("\n[PHASE 3] Behaviors (Expectations)")
        print("-" * 60)

    behaviors = bootstrap_behaviors(store, verbose=verbose)

    # =========================================================================
    # PHASE 4: STORIES (The Desires)
    # =========================================================================
    # Story entities and specifies bonds
    if verbose:
        print("\n[PHASE 4] Stories (Desires)")
        print("-" * 60)

    stories = bootstrap_stories(store, verbose=verbose)
    specifies_bonds = bootstrap_specifies_bonds(store, verbose=verbose)

    # =========================================================================
    # PHASE 5: PROVENANCE (The Chain)
    # =========================================================================
    # implements bonds from behaviors to primitives
    if verbose:
        print("\n[PHASE 5] Provenance (Implements Bonds)")
        print("-" * 60)

    implements_bonds = bootstrap_implements_bonds(store, verbose=verbose)

    # =========================================================================
    # SUMMARY
    # =========================================================================
    crystal_count = len(crystal_result.get('primitives', []))
    if verbose:
        print("\n" + "=" * 60)
        print("[*] Genesis Complete!")
        print(f"    - Crystal Palace: {crystal_count} primitives across {len(crystal_result.get('domains', {}))} domains")
        print(f"    - Protocols: {len(protocols)}")
        print(f"    - Behaviors: {len(behaviors)}")
        print(f"    - Stories: {len(stories)}")
        print(f"    - Specifies Bonds: {len(specifies_bonds)}")
        print(f"    - Implements Bonds: {len(implements_bonds)}")
        total_entities = crystal_count + len(protocols) + len(behaviors) + len(stories)
        total_bonds = len(specifies_bonds) + len(implements_bonds)
        print(f"    - TOTAL: {total_entities} entities, {total_bonds} bonds")
        print("=" * 60)

    store.close()

    return {
        "crystal": crystal_result,
        "protocols": protocols,
        "behaviors": behaviors,
        "stories": stories,
        "specifies_bonds": specifies_bonds,
        "implements_bonds": implements_bonds,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Bootstrap Chora CVM genesis (idempotent)"
    )
    parser.add_argument(
        "db_path",
        nargs="?",
        default="chora-cvm.db",
        help="Path to SQLite database (default: chora-cvm.db)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-run even if genesis already complete",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress messages",
    )

    args = parser.parse_args()
    main(args.db_path, verbose=not args.quiet, force=args.force)
