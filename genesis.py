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


def main(db_path: str = "chora-cvm.db", verbose: bool = True) -> dict:
    """
    Bootstrap the complete Chora CVM genesis (IDEMPOTENT).

    This orchestrator calls bootstrap functions in sequence:
    1. Crystal Palace domains (domain.* primitives)
    2. Protocols (protocol-* entities)
    3. Behaviors (behavior-* entities)
    4. Stories (story-* entities) with specifies bonds
    5. Provenance (implements bonds)

    IDEMPOTENCY: If genesis has already run (primitives exist), this is a no-op.
    The litmus test: `just setup` three times in a row should be near-instant
    on the second and third runs.

    Args:
        db_path: Path to the SQLite database
        verbose: Print progress messages

    Returns:
        Summary dict with counts and IDs of created entities
    """
    store = EventStore(db_path)

    if verbose:
        print(f"[*] Genesis: Bootstrapping Chora CVM in {db_path}")
        print("=" * 60)

    # =========================================================================
    # IDEMPOTENCY CHECK: Skip if already populated
    # =========================================================================
    # Check for primitives (the foundation of everything else)
    cur = store._conn.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM entities WHERE type = 'primitive'")
    primitive_count = cur.fetchone()["cnt"]
    if primitive_count >= 50:  # Crystal Palace has ~57 primitives
        if verbose:
            print(f"\n[✓] Genesis already complete: {primitive_count} primitives found")
            print("    (Skipping - database is already populated)")
            print("=" * 60)
        store.close()
        return {"status": "already_populated", "primitives": primitive_count}

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
    db_arg = sys.argv[1] if len(sys.argv) > 1 else "chora-cvm.db"
    main(db_arg)
