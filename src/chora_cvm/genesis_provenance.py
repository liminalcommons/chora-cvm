"""
Genesis Provenance: Bootstrap `implements` Bonds from Behaviors to Primitives.

This module establishes the provenance chain by creating `implements` bonds
that connect behaviors (expectations) to the primitives/protocols that implement them.

The physics:
  Story --specifies--> Behavior --implements--> Tool/Primitive
                                <--verifies---

`verifies` bonds are created dynamically by the integrity system after tests pass.
`implements` bonds are created here during genesis.
"""

from __future__ import annotations

from chora_cvm.store import EventStore


# =============================================================================
# BEHAVIOR → PRIMITIVE MAPPINGS
# =============================================================================
# Maps behavior IDs to the primitive(s) that implement them.
# Each behavior can be implemented by multiple primitives.

BEHAVIOR_IMPLEMENTS: dict[str, list[str]] = {
    # =========================================================================
    # ATTENTION DOMAIN
    # =========================================================================
    "behavior-create-focus-declares-attention": [
        "attention.focus.create",
    ],
    "behavior-resolve-focus-closes-attention-loop": [
        "attention.focus.resolve",
    ],
    "behavior-list-focuses-shows-current-attention": [
        "attention.focus.list",
    ],
    "behavior-emit-signal-demands-attention": [
        "attention.signal.emit",
    ],

    # =========================================================================
    # ENGINE/I-O DOMAIN
    # =========================================================================
    "behavior-engine-routes-protocol-invocation": [
        "protocol-orient",  # Example protocol
    ],
    "behavior-engine-routes-primitive-invocation": [
        "chronos.now",  # Example primitive
    ],
    "behavior-engine-lists-capabilities": [
        "graph.query",
    ],
    "behavior-engine-handles-errors-gracefully": [
        "graph.entity.get",
    ],
    "behavior-i-o-primitives-route-output-through-injected-sink": [
        "io.ui.render",
        "io.sys.log",
    ],
    "behavior-i-o-primitives-fall-back-to-stdout-without-context": [
        "io.ui.render",
        "io.sys.log",
    ],

    # =========================================================================
    # METABOLIC PROTOCOLS
    # =========================================================================
    "behavior-sense-entropy-reports-metabolic-health": [
        "protocol-sense-entropy",
    ],
    "behavior-digest-transforms-entity-into-learning": [
        "protocol-digest",
    ],
    "behavior-compost-archives-orphan-entity": [
        "protocol-compost",
    ],
    "behavior-induce-proposes-pattern-from-learnings": [
        "protocol-induce",
    ],
    "behavior-stagnation-emits-signal-when-threshold-exceeded": [
        "protocol-detect-stagnation",
    ],
    "behavior-signal-auto-resolves-when-void-clears": [
        "attention.signal.emit",
    ],

    # =========================================================================
    # HORIZON PROTOCOL
    # =========================================================================
    "behavior-horizon-protocol-returns-semantic-recommendations": [
        "protocol-horizon",
    ],
    "behavior-horizon-gracefully-handles-cold-start": [
        "protocol-horizon",
    ],

    # =========================================================================
    # INTEGRITY DOMAIN
    # =========================================================================
    "behavior-integrity-discovers-scenarios": [
        "code.scan.features",
    ],
    "behavior-integrity-runs-tests": [
        "code.build.test",
    ],
    "behavior-integrity-reports-status": [
        "graph.query",
    ],
    "behavior-verifies-bond-tracks-results": [
        "graph.bond.manage",
    ],

    # =========================================================================
    # PULSE/AUTONOMIC
    # =========================================================================
    "behavior-pulse-processes-signals": [
        "attention.signal.emit",
        "graph.query",
    ],
    "behavior-pulse-preview": [
        "graph.query",
    ],
    "behavior-pulse-status": [
        "graph.query",
    ],
    "behavior-pulse-runs-periodically": [
        "chronos.now",
    ],
    "behavior-signal-outcomes-recorded": [
        "graph.entity.update",
    ],

    # =========================================================================
    # GRAPH DOMAIN
    # =========================================================================
    "behavior-bond-created-with-confidence-emits-signal-when-tentative": [
        "graph.bond.manage",
        "attention.signal.emit",
    ],
    "behavior-bond-confidence-update-emits-signal": [
        "graph.bond.manage",
        "attention.signal.emit",
    ],
    "behavior-entity-bonded-with-inhabits-appears-in-circle-constellation": [
        "graph.bond.manage",
        "graph.bond.list",
    ],
    "behavior-circle-constellation-shows-all-inhabitants": [
        "graph.bond.list",
    ],

    # =========================================================================
    # EMBEDDING/SEMANTIC DOMAIN
    # =========================================================================
    "behavior-embedding-stored-for-entity": [
        "cognition.embed.text",
    ],
    "behavior-embedding-retrieved-by-entity-id": [
        "cognition.embed.batch_load",
    ],
    "behavior-embedding-invalidated-on-entity-update": [
        "graph.entity.update",
    ],
    "behavior-embed-entity-computes-and-stores-vector": [
        "cognition.embed.text",
    ],
    "behavior-embed-text-computes-vector-for-arbitrary-text": [
        "cognition.embed.text",
    ],
    "behavior-semantic-similarity-computes-cosine-distance": [
        "cognition.vector.sim",
    ],
    "behavior-semantic-search-ranks-entities-by-meaning": [
        "cognition.vector.rank",
    ],
    "behavior-detect-clusters-groups-similar-entities": [
        "cognition.cluster",
    ],
    "behavior-suggest-bonds-finds-relationship-candidates": [
        "cognition.vector.rank",
        "graph.bond.manage",
    ],

    # =========================================================================
    # CONVERGENCE DOMAIN
    # =========================================================================
    "behavior-detect-isolated-entities": [
        "graph.query.orphans",
    ],
    "behavior-suggest-surfaces-bonds-for-learnings": [
        "cognition.vector.rank",
    ],
    "behavior-suggest-verifies-bonds-for-behaviors": [
        "cognition.vector.rank",
    ],
    "behavior-score-entity-coherence": [
        "graph.bond.count",
    ],
    "behavior-emit-convergence-signal": [
        "attention.signal.emit",
    ],

    # =========================================================================
    # PRUNE DOMAIN
    # =========================================================================
    "behavior-prune-detect-finds-orphan-tools": [
        "graph.query.orphans",
    ],
    "behavior-prune-detect-finds-deprecated-tools": [
        "graph.query",
    ],
    "behavior-prune-detect-respects-internal-flag": [
        "graph.entity.get",
    ],
    "behavior-prune-detect-returns-standardized-shape": [
        "logic.json.set",
    ],
    "behavior-prune-detects-orphan-and-deprecated-tools": [
        "graph.query.orphans",
        "graph.query",
    ],
    "behavior-prune-emits-signals-for-threshold-breaches": [
        "attention.signal.emit",
    ],
    "behavior-prune-proposes-focus-for-human-approval": [
        "attention.focus.create",
    ],
    "behavior-prune-approve-composts-and-learns": [
        "graph.entity.archive",
        "graph.entity.create",
    ],
    "behavior-prune-reject-captures-reason": [
        "graph.entity.create",
    ],

    # =========================================================================
    # RHYTHM/KAIROTIC DOMAIN
    # =========================================================================
    "behavior-sense-rhythm-detects-kairotic-phase": [
        "graph.query",
        "chronos.now",
    ],
    "behavior-sense-rhythm-returns-standardized-shape": [
        "logic.json.set",
    ],
    "behavior-sense-rhythm-computes-satiation": [
        "logic.list.map",
    ],
    "behavior-sense-rhythm-computes-temporal-health": [
        "graph.query.recent",
        "chronos.diff",
    ],
    "behavior-sense-kairotic-state-returns-phase-weights": [
        "graph.query",
    ],
    "behavior-satiation-computed-from-integrity-entropy-and-growth": [
        "graph.query",
    ],
    "behavior-temporal-health-tracks-rolling-window": [
        "graph.query.recent",
        "chronos.diff",
    ],

    # =========================================================================
    # SYNC DOMAIN
    # =========================================================================
    "behavior-circle-with-local-only-policy-does-not-sync": [
        "graph.entity.get",
    ],
    "behavior-circle-with-cloud-policy-enables-sync-routing": [
        "graph.entity.get",
    ],
    "behavior-entity-in-local-only-circle-does-not-sync": [
        "graph.bond.list",
    ],
    "behavior-entity-in-cloud-circle-syncs": [
        "graph.bond.list",
    ],
    "behavior-entity-in-multiple-circles-syncs-to-cloud-ones-only": [
        "graph.bond.list",
    ],

    # =========================================================================
    # KEYRING/INVITATION DOMAIN
    # =========================================================================
    "behavior-keyring-loads-identity-from-file": [
        "io.fs.read",
    ],
    "behavior-keyring-lists-accessible-circles": [
        "logic.json.get",
    ],
    "behavior-keyring-stores-circle-encryption-keys-securely": [
        "io.fs.write",
    ],
    "behavior-encrypt-circle-key-for-recipient": [
        "sys.shell.run",  # calls age
    ],
    "behavior-decrypt-invitation-with-local-ssh-key": [
        "sys.shell.run",  # calls age
    ],
    "behavior-fetch-ssh-public-key-from-github": [
        "sys.shell.run",  # curl
    ],
    "behavior-invite-fetches-github-ssh-key-and-encrypts-circle-key": [
        "sys.shell.run",
    ],
    "behavior-list-circle-members-from-access-directory": [
        "io.fs.read_tree",
    ],
    "behavior-invite-fetches-github-key-and-encrypts": [
        "sys.shell.run",
    ],
    "behavior-arrive-decrypts-pending-invitations": [
        "sys.shell.run",
    ],
    "behavior-list-circle-members": [
        "io.fs.read_tree",
    ],

    # =========================================================================
    # ASSET DOMAIN
    # =========================================================================
    "behavior-asset-bonded-belongs-to-appears-in-circle-constellation": [
        "graph.bond.manage",
    ],
    "behavior-asset-can-belong-to-multiple-circles": [
        "graph.bond.manage",
    ],
    "behavior-circle-shows-all-owned-assets": [
        "graph.bond.list",
    ],

    # =========================================================================
    # DOC MAINTENANCE DOMAIN
    # =========================================================================
    "behavior-detect-doc-voids-emits-signals": [
        "attention.signal.emit",
        "io.fs.read",
    ],
    "behavior-repair-syntactic-fixes-broken-refs": [
        "io.fs.patch",
    ],
    "behavior-propose-semantic-creates-focus": [
        "attention.focus.create",
    ],
    "behavior-approve-applies-change": [
        "io.fs.patch",
        "attention.focus.resolve",
    ],
    "behavior-reject-captures-learning": [
        "graph.entity.create",
        "attention.focus.resolve",
    ],

    # =========================================================================
    # BUILD GOVERNANCE
    # =========================================================================
    "behavior-behavior-lint-passes": [
        "code.build.lint",
    ],
    "behavior-behavior-types-check": [
        "code.build.typecheck",
    ],
    "behavior-behavior-tests-pass": [
        "code.build.test",
    ],
    "behavior-behavior-coverage-threshold": [
        "code.build.test",
    ],
    "behavior-behavior-security-scan-clean": [
        "sys.shell.run",  # bandit
    ],
    "behavior-behavior-build-integrity-check": [
        "code.build.lint",
        "code.build.typecheck",
        "code.build.test",
    ],

    # =========================================================================
    # COMMAND PALETTE
    # =========================================================================
    "behavior-command-palette-shows-cvm-tools-dynamically": [
        "graph.query",
    ],
    "behavior-command-palette-lists-and-invokes-cvm-protocols": [
        "graph.query",
    ],

    # =========================================================================
    # HARVESTER/LEGACY
    # =========================================================================
    "behavior-harvester-indexes-files-from-configured-repositories": [
        "io.fs.read_tree",
        "graph.entity.create",
    ],
    "behavior-harvester-extracts-legacy-entities": [
        "graph.entity.create",
    ],
    "behavior-harvester-extracts-archive-content": [
        "io.fs.read",
    ],
    "behavior-harvester-deduplicates-content-by-priority": [
        "logic.list.sort",
    ],
    "behavior-search-returns-relevant-chunks-for-fts-query": [
        "graph.query",
    ],

    # =========================================================================
    # REACTIVE PATTERNS
    # =========================================================================
    "behavior-store-fires-hook-when-entity-saved": [
        "graph.entity.create",
    ],
    "behavior-system-indexes-learning-on-creation": [
        "cognition.embed.text",
    ],

    # =========================================================================
    # DYNAMIC TOOLS
    # =========================================================================
    "behavior-voice-command-manifests-tool-entity-that-appears-in-palette": [
        "graph.entity.create",
    ],

    # =========================================================================
    # UI STATE
    # =========================================================================
    "behavior-layout-entity-responds-to-signal-urgency": [
        "graph.query",
    ],

    # =========================================================================
    # TEST BEHAVIORS (internal)
    # =========================================================================
    "behavior-test-alpha": [
        "graph.entity.get",
    ],
}


def bootstrap_implements_bonds(store: EventStore, verbose: bool = True) -> list[str]:
    """
    Bootstrap `implements` bonds from behaviors to primitives/protocols.

    This establishes the provenance chain:
      Behavior --implements--> Primitive/Protocol

    Returns list of created bond IDs.
    """
    created = []

    for behavior_id, primitives in BEHAVIOR_IMPLEMENTS.items():
        for primitive_id in primitives:
            # Create slug from IDs
            behavior_slug = behavior_id.replace("behavior-", "")[:25]
            primitive_slug = primitive_id.replace(".", "-")[:25]
            bond_id = f"rel-implements-{behavior_slug}-{primitive_slug}"

            store.save_bond(
                bond_id=bond_id,
                bond_type="implements",
                from_id=behavior_id,
                to_id=primitive_id,
                status="active",
                confidence=1.0,
                data={
                    "source": "genesis",
                }
            )
            created.append(bond_id)

    if verbose:
        print(f"    [implements] {len(created)} bonds (behavior → primitive)")

    return created


def count_unmapped_behaviors() -> tuple[int, list[str]]:
    """
    Count behaviors that don't have implements mappings.

    Returns (count, list of unmapped behavior IDs).
    """
    from chora_cvm.genesis_behaviors import BEHAVIORS

    mapped = set(BEHAVIOR_IMPLEMENTS.keys())
    all_behaviors = set(BEHAVIORS.keys())
    unmapped = all_behaviors - mapped

    return len(unmapped), sorted(unmapped)
