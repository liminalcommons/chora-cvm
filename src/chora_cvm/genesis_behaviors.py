"""
Genesis Behaviors: Bootstrap Behavior Entities from Feature Files.

This module creates behavior entities for every @behavior:* tag found in
the test feature files. Each behavior represents an expected system capability
that can be verified through BDD testing.

Generated from 103 unique behaviors across 34 feature files.
"""

from __future__ import annotations

from chora_cvm.kernel.schema import GenericEntity
from chora_cvm.store import EventStore


# =============================================================================
# BEHAVIOR DEFINITIONS
# =============================================================================
# Extracted from tests/features/*.feature @behavior:* tags
# Each behavior links to its parent story via cognition.links.story_id

BEHAVIORS: dict[str, dict] = {
    "behavior-approve-applies-change": {
        "title": "Approving Focus applies the change",
        "story": "story-docs-detect-their-own-staleness",
    },
    "behavior-arrive-decrypts-pending-invitations": {
        "title": "Arrive decrypts matching invitation",
        "story": "story-git-native-circle-invitations",
    },
    "behavior-asset-bonded-belongs-to-appears-in-circle-constellation": {
        "title": "Asset belongs to circle via bond",
        "story": "story-assets-belong-to-circles",
    },
    "behavior-asset-can-belong-to-multiple-circles": {
        "title": "Asset belongs to multiple circles",
        "story": "story-assets-belong-to-circles",
    },
    "behavior-behavior-build-integrity-check": {
        "title": "Build integrity check runs all checks",
        "story": "story-build-governance-ensures-quality",
    },
    "behavior-behavior-coverage-threshold": {
        "title": "Coverage threshold is met",
        "story": "story-build-governance-ensures-quality",
    },
    "behavior-behavior-lint-passes": {
        "title": "Lint passes on clean code",
        "story": "story-build-governance-ensures-quality",
    },
    "behavior-behavior-security-scan-clean": {
        "title": "Security scan finds no issues",
        "story": "story-build-governance-ensures-quality",
    },
    "behavior-behavior-tests-pass": {
        "title": "Tests pass on working code",
        "story": "story-build-governance-ensures-quality",
    },
    "behavior-behavior-types-check": {
        "title": "Type check passes on typed code",
        "story": "story-build-governance-ensures-quality",
    },
    "behavior-bond-confidence-update-emits-signal": {
        "title": "Update bond confidence downward emits signal",
        "story": "story-bonds-carry-epistemic-confidence",
    },
    "behavior-bond-created-with-confidence-emits-signal-when-tentative": {
        "title": "Create bond with full confidence (no signal)",
        "story": "story-bonds-carry-epistemic-confidence",
    },
    "behavior-circle-constellation-shows-all-inhabitants": {
        "title": "Query circle inhabitants",
        "story": "story-entities-inhabit-circles",
    },
    "behavior-circle-shows-all-owned-assets": {
        "title": "Query all circle assets",
        "story": "story-assets-belong-to-circles",
    },
    "behavior-circle-with-cloud-policy-enables-sync-routing": {
        "title": "Create cloud-syncing circle",
        "story": "story-circles-declare-their-sync-boundary",
    },
    "behavior-circle-with-local-only-policy-does-not-sync": {
        "title": "Create local-only circle",
        "story": "story-circles-declare-their-sync-boundary",
    },
    "behavior-command-palette-lists-and-invokes-cvm-protocols": {
        "title": "Listing protocols returns protocol entities",
        "story": "story-users-invoke-protocols-directly-from-the-command-palette",
    },
    "behavior-command-palette-shows-cvm-tools-dynamically": {
        "title": "Listing tools returns tool entities",
        "story": "story-homoiconic-command-palette",
    },
    "behavior-compost-archives-orphan-entity": {
        "title": "Compost archives an orphan entity",
        "story": "story-system-metabolizes-entropy-into-growth",
    },
    "behavior-create-focus-declares-attention": {
        "title": "Creating a focus declares what is being attended to",
        "story": "story-attention-declares-what-matters",
    },
    "behavior-decrypt-invitation-with-local-ssh-key": {
        "title": "Decrypt invitation with matching key",
        "story": "story-invite-collaborator-to-circle",
    },
    "behavior-detect-clusters-groups-similar-entities": {
        "title": "Detect clusters groups by semantic similarity",
        "story": "story-semantic-primitives-enable-inference",
    },
    "behavior-detect-doc-voids-emits-signals": {
        "title": "Detect stale reference emits signal",
        "story": "story-docs-detect-their-own-staleness",
    },
    "behavior-detect-isolated-entities": {
        "title": "Detect entities with no bonds",
        "story": "story-system-suggests-convergences",
    },
    "behavior-digest-transforms-entity-into-learning": {
        "title": "Digest transforms a pattern into a learning",
        "story": "story-system-metabolizes-entropy-into-growth",
    },
    "behavior-embed-entity-computes-and-stores-vector": {
        "title": "Embed entity computes and stores vector when inference available",
        "story": "story-semantic-primitives-enable-inference",
    },
    "behavior-embed-text-computes-vector-for-arbitrary-text": {
        "title": "Embed text computes vector when inference available",
        "story": "story-semantic-primitives-enable-inference",
    },
    "behavior-embedding-invalidated-on-entity-update": {
        "title": "Update entity data invalidates embedding",
        "story": "story-embeddings-persist-in-sqlite",
    },
    "behavior-embedding-retrieved-by-entity-id": {
        "title": "Retrieve stored embedding by entity_id",
        "story": "story-embeddings-persist-in-sqlite",
    },
    "behavior-embedding-stored-for-entity": {
        "title": "Store embedding for entity",
        "story": "story-embeddings-persist-in-sqlite",
    },
    "behavior-emit-convergence-signal": {
        "title": "Scan with emit_signals creates signal entities",
        "story": "story-system-suggests-convergences",
    },
    "behavior-emit-signal-demands-attention": {
        "title": "Emitting a signal creates an active signal entity",
        "story": "story-attention-declares-what-matters",
    },
    "behavior-encrypt-circle-key-for-recipient": {
        "title": "Create invitation with public key",
        "story": "story-invite-collaborator-to-circle",
    },
    "behavior-engine-handles-errors-gracefully": {
        "title": "Dispatch returns error for unknown intent",
        "story": "story-cvm-enables-multimodal-deployment",
    },
    "behavior-engine-lists-capabilities": {
        "title": "Engine lists all available capabilities",
        "story": "story-cvm-enables-multimodal-deployment",
    },
    "behavior-engine-routes-primitive-invocation": {
        "title": "Dispatch routes to primitive execution",
        "story": "story-cvm-enables-multimodal-deployment",
    },
    "behavior-engine-routes-protocol-invocation": {
        "title": "Dispatch routes to protocol execution",
        "story": "story-cvm-enables-multimodal-deployment",
    },
    "behavior-entity-bonded-with-inhabits-appears-in-circle-constellation": {
        "title": "Entity inhabits circle",
        "story": "story-entities-inhabit-circles",
    },
    "behavior-entity-in-cloud-circle-syncs": {
        "title": "Entity in cloud circle should sync",
        "story": "story-system-decides-what-to-sync",
    },
    "behavior-entity-in-local-only-circle-does-not-sync": {
        "title": "Entity in local-only circle does not sync",
        "story": "story-system-decides-what-to-sync",
    },
    "behavior-entity-in-multiple-circles-syncs-to-cloud-ones-only": {
        "title": "Entity in multiple circles syncs to cloud ones only",
        "story": "story-system-decides-what-to-sync",
    },
    "behavior-fetch-ssh-public-key-from-github": {
        "title": "Fetch SSH public key from GitHub API (mocked)",
        "story": "story-invite-collaborator-to-circle",
    },
    "behavior-harvester-deduplicates-content-by-priority": {
        "title": "Higher priority repository becomes canonical",
        "story": "story-agents-can-search-legacy-content-to-understand-historical-patterns-and-decisions",
    },
    "behavior-harvester-extracts-archive-content": {
        "title": "Harvester indexes archive repositories when flag is set",
        "story": "story-agents-can-search-legacy-content-to-understand-historical-patterns-and-decisions",
    },
    "behavior-harvester-extracts-legacy-entities": {
        "title": "Entity extractor reads entities from legacy database",
        "story": "story-agents-can-search-legacy-content-to-understand-historical-patterns-and-decisions",
    },
    "behavior-harvester-indexes-files-from-configured-repositories": {
        "title": "Harvester discovers and indexes markdown files",
        "story": "story-agents-can-search-legacy-content-to-understand-historical-patterns-and-decisions",
    },
    "behavior-horizon-gracefully-handles-cold-start": {
        "title": "Cold start with no recent learnings",
        "story": "story-cvm-enables-multimodal-deployment",
    },
    "behavior-horizon-protocol-returns-semantic-recommendations": {
        "title": "Semantic ranking with recent learnings and unverified tools",
        "story": "story-cvm-enables-multimodal-deployment",
    },
    "behavior-i-o-primitives-fall-back-to-stdout-without-context": {
        "title": "ui_render falls back to stdout without context",
        "story": "story-cvm-enables-multimodal-deployment",
    },
    "behavior-i-o-primitives-route-output-through-injected-sink": {
        "title": "ui_render routes output through custom sink",
        "story": "story-cvm-enables-multimodal-deployment",
    },
    "behavior-induce-proposes-pattern-from-learnings": {
        "title": "Induce proposes pattern from clustered learnings",
        "story": "story-system-metabolizes-entropy-into-growth",
    },
    "behavior-integrity-discovers-scenarios": {
        "title": "Integrity check discovers behaviors with BDD scenarios",
        "story": "story-system-integrity-truth",
    },
    "behavior-integrity-reports-status": {
        "title": "Integrity report shows honest verification status",
        "story": "story-system-integrity-truth",
    },
    "behavior-integrity-runs-tests": {
        "title": "Integrity check executes tests and captures results",
        "story": "story-system-integrity-truth",
    },
    "behavior-invite-fetches-github-key-and-encrypts": {
        "title": "Invite user with circle auto-detection",
        "story": "story-git-native-circle-invitations",
    },
    "behavior-invite-fetches-github-ssh-key-and-encrypts-circle-key": {
        "title": "Full invite flow fetches GitHub key and encrypts circle key",
        "story": "story-invite-collaborator-to-circle",
    },
    "behavior-keyring-lists-accessible-circles": {
        "title": "Keyring contains circle bindings",
        "story": "story-dweller-has-local-keyring",
    },
    "behavior-keyring-loads-identity-from-file": {
        "title": "Load existing keyring",
        "story": "story-dweller-has-local-keyring",
    },
    "behavior-keyring-stores-circle-encryption-keys-securely": {
        "title": "Circle binding can store encryption key",
        "story": "story-dweller-has-local-keyring",
    },
    "behavior-layout-entity-responds-to-signal-urgency": {
        "title": "Layout Entity Responds To Signal Urgency",
        "story": "story-ui-responds-to-system-state",
    },
    "behavior-list-circle-members": {
        "title": "List members of a circle",
        "story": "story-git-native-circle-invitations",
    },
    "behavior-list-circle-members-from-access-directory": {
        "title": "List circle members from access directory",
        "story": "story-invite-collaborator-to-circle",
    },
    "behavior-list-focuses-shows-current-attention": {
        "title": "Listing focuses shows only active ones",
        "story": "story-attention-declares-what-matters",
    },
    "behavior-propose-semantic-creates-focus": {
        "title": "Semantic issue creates Focus for review",
        "story": "story-docs-detect-their-own-staleness",
    },
    "behavior-prune-approve-composts-and-learns": {
        "title": "Approve composts deprecated tool and creates learning",
        "story": "story-prune-approval-rejection-flow",
    },
    "behavior-prune-detect-finds-deprecated-tools": {
        "title": "Detect tools marked as deprecated",
        "story": "story-cvm-enables-self-healing",
    },
    "behavior-prune-detect-finds-orphan-tools": {
        "title": "Detect tools without implements bonds",
        "story": "story-cvm-enables-self-healing",
    },
    "behavior-prune-detect-respects-internal-flag": {
        "title": "Internal tools are excluded from orphan detection",
        "story": "story-cvm-enables-self-healing",
    },
    "behavior-prune-detect-returns-standardized-shape": {
        "title": "Protocol returns standard response shape",
        "story": "story-cvm-enables-self-healing",
    },
    "behavior-prune-detects-orphan-and-deprecated-tools": {
        "title": "Detect orphan tools with no behavior implementing them",
        "story": "story-system-can-prune-unused-code-and-entities",
    },
    "behavior-prune-emits-signals-for-threshold-breaches": {
        "title": "Emit signal when orphan count exceeds threshold",
        "story": "story-system-can-prune-unused-code-and-entities",
    },
    "behavior-prune-proposes-focus-for-human-approval": {
        "title": "Propose focus for deprecated tool",
        "story": "story-system-can-prune-unused-code-and-entities",
    },
    "behavior-prune-reject-captures-reason": {
        "title": "Reject captures reason as learning",
        "story": "story-prune-approval-rejection-flow",
    },
    "behavior-pulse-preview": {
        "title": "Preview shows what pulse would process",
        "story": "story-autonomic-heartbeat",
    },
    "behavior-pulse-processes-signals": {
        "title": "Pulse processes active signals",
        "story": "story-autonomic-heartbeat",
    },
    "behavior-pulse-runs-periodically": {
        "title": "Pulse can be configured with interval",
        "story": "story-autonomic-heartbeat",
    },
    "behavior-pulse-status": {
        "title": "Pulse status shows recent history",
        "story": "story-autonomic-heartbeat",
    },
    "behavior-reject-captures-learning": {
        "title": "Rejecting Focus creates learning",
        "story": "story-docs-detect-their-own-staleness",
    },
    "behavior-repair-syntactic-fixes-broken-refs": {
        "title": "Repair comments out broken reference",
        "story": "story-docs-detect-their-own-staleness",
    },
    "behavior-resolve-focus-closes-attention-loop": {
        "title": "Resolving a focus marks it as complete",
        "story": "story-attention-declares-what-matters",
    },
    "behavior-satiation-computed-from-integrity-entropy-and-growth": {
        "title": "Satiation computed from system health metrics",
        "story": "story-system-senses-kairotic-rhythm",
    },
    "behavior-score-entity-coherence": {
        "title": "Coherence score reflects bond count",
        "story": "story-system-suggests-convergences",
    },
    "behavior-search-returns-relevant-chunks-for-fts-query": {
        "title": "Search finds matching content",
        "story": "story-agents-can-search-legacy-content-to-understand-historical-patterns-and-decisions",
    },
    "behavior-semantic-search-ranks-entities-by-meaning": {
        "title": "Semantic search ranks by meaning when inference available",
        "story": "story-semantic-primitives-enable-inference",
    },
    "behavior-semantic-similarity-computes-cosine-distance": {
        "title": "Semantic similarity computes cosine when both embeddings exist",
        "story": "story-semantic-primitives-enable-inference",
    },
    "behavior-sense-entropy-reports-metabolic-health": {
        "title": "Sense entropy returns metabolic health report",
        "story": "story-system-metabolizes-entropy-into-growth",
    },
    "behavior-sense-kairotic-state-returns-phase-weights": {
        "title": "Sense kairotic state returns all six phase weights",
        "story": "story-system-senses-kairotic-rhythm",
    },
    "behavior-sense-rhythm-computes-satiation": {
        "title": "Compute satiation score with label",
        "story": "story-cvm-enables-self-awareness",
    },
    "behavior-sense-rhythm-computes-temporal-health": {
        "title": "Compute temporal health metrics over window",
        "story": "story-cvm-enables-self-awareness",
    },
    "behavior-sense-rhythm-detects-kairotic-phase": {
        "title": "Detect dominant kairotic phase",
        "story": "story-cvm-enables-self-awareness",
    },
    "behavior-sense-rhythm-returns-standardized-shape": {
        "title": "Primitives return standard response shape",
        "story": "story-cvm-enables-self-awareness",
    },
    "behavior-signal-auto-resolves-when-void-clears": {
        "title": "Signal auto-resolves when orphan gets bonded",
        "story": "story-system-metabolizes-entropy-into-growth",
    },
    "behavior-signal-outcomes-recorded": {
        "title": "Signal outcomes include processing details",
        "story": "story-autonomic-heartbeat",
    },
    "behavior-stagnation-emits-signal-when-threshold-exceeded": {
        "title": "Stagnant inquiry emits signal after 30 days",
        "story": "story-system-metabolizes-entropy-into-growth",
    },
    "behavior-store-fires-hook-when-entity-saved": {
        "title": "Hook is called when entity is saved via save_generic_entity",
        "story": "story-system-enables-reactive-patterns",
    },
    "behavior-suggest-bonds-finds-relationship-candidates": {
        "title": "Suggest bonds finds candidates using semantic similarity",
        "story": "story-semantic-primitives-enable-inference",
    },
    "behavior-suggest-surfaces-bonds-for-learnings": {
        "title": "Suggest surfaces bond when learning matches principle keywords",
        "story": "story-system-suggests-convergences",
    },
    "behavior-suggest-verifies-bonds-for-behaviors": {
        "title": "Suggest verifies bond when behavior matches tool keywords",
        "story": "story-system-suggests-convergences",
    },
    "behavior-system-indexes-learning-on-creation": {
        "title": "System indexes learning on creation for semantic search",
        "story": "story-system-enables-reactive-patterns",
    },
    "behavior-temporal-health-tracks-rolling-window": {
        "title": "Temporal health returns rolling window metrics",
        "story": "story-system-senses-kairotic-rhythm",
    },
    "behavior-test-alpha": {
        "title": "Test behavior for integrity verification",
        "story": "story-system-integrity-truth",
    },
    "behavior-verifies-bond-tracks-results": {
        "title": "verifies bond stores verification metadata",
        "story": "story-system-integrity-truth",
    },
    "behavior-voice-command-manifests-tool-entity-that-appears-in-palette": {
        "title": "Voice command manifests tool entity that appears in palette",
        "story": "story-system-creates-tools-dynamically",
    },
}


# =============================================================================
# STORY GROUPINGS
# =============================================================================
# Maps stories to their child behaviors for `specifies` bonds

STORY_BEHAVIORS: dict[str, list[str]] = {
    "story-agents-can-search-legacy-content-to-understand-historical-patterns-and-decisions": [
        "behavior-harvester-deduplicates-content-by-priority",
        "behavior-harvester-extracts-archive-content",
        "behavior-harvester-extracts-legacy-entities",
        "behavior-harvester-indexes-files-from-configured-repositories",
        "behavior-search-returns-relevant-chunks-for-fts-query",
    ],
    "story-assets-belong-to-circles": [
        "behavior-asset-bonded-belongs-to-appears-in-circle-constellation",
        "behavior-asset-can-belong-to-multiple-circles",
        "behavior-circle-shows-all-owned-assets",
    ],
    "story-attention-declares-what-matters": [
        "behavior-create-focus-declares-attention",
        "behavior-emit-signal-demands-attention",
        "behavior-list-focuses-shows-current-attention",
        "behavior-resolve-focus-closes-attention-loop",
    ],
    "story-autonomic-heartbeat": [
        "behavior-pulse-preview",
        "behavior-pulse-processes-signals",
        "behavior-pulse-runs-periodically",
        "behavior-pulse-status",
        "behavior-signal-outcomes-recorded",
    ],
    "story-bonds-carry-epistemic-confidence": [
        "behavior-bond-confidence-update-emits-signal",
        "behavior-bond-created-with-confidence-emits-signal-when-tentative",
    ],
    "story-build-governance-ensures-quality": [
        "behavior-behavior-build-integrity-check",
        "behavior-behavior-coverage-threshold",
        "behavior-behavior-lint-passes",
        "behavior-behavior-security-scan-clean",
        "behavior-behavior-tests-pass",
        "behavior-behavior-types-check",
    ],
    "story-circles-declare-their-sync-boundary": [
        "behavior-circle-with-cloud-policy-enables-sync-routing",
        "behavior-circle-with-local-only-policy-does-not-sync",
    ],
    "story-cvm-enables-multimodal-deployment": [
        "behavior-engine-handles-errors-gracefully",
        "behavior-engine-lists-capabilities",
        "behavior-engine-routes-primitive-invocation",
        "behavior-engine-routes-protocol-invocation",
        "behavior-horizon-gracefully-handles-cold-start",
        "behavior-horizon-protocol-returns-semantic-recommendations",
        "behavior-i-o-primitives-fall-back-to-stdout-without-context",
        "behavior-i-o-primitives-route-output-through-injected-sink",
    ],
    "story-cvm-enables-self-awareness": [
        "behavior-sense-rhythm-computes-satiation",
        "behavior-sense-rhythm-computes-temporal-health",
        "behavior-sense-rhythm-detects-kairotic-phase",
        "behavior-sense-rhythm-returns-standardized-shape",
    ],
    "story-cvm-enables-self-healing": [
        "behavior-prune-detect-finds-deprecated-tools",
        "behavior-prune-detect-finds-orphan-tools",
        "behavior-prune-detect-respects-internal-flag",
        "behavior-prune-detect-returns-standardized-shape",
    ],
    "story-docs-detect-their-own-staleness": [
        "behavior-approve-applies-change",
        "behavior-detect-doc-voids-emits-signals",
        "behavior-propose-semantic-creates-focus",
        "behavior-reject-captures-learning",
        "behavior-repair-syntactic-fixes-broken-refs",
    ],
    "story-dweller-has-local-keyring": [
        "behavior-keyring-lists-accessible-circles",
        "behavior-keyring-loads-identity-from-file",
        "behavior-keyring-stores-circle-encryption-keys-securely",
    ],
    "story-embeddings-persist-in-sqlite": [
        "behavior-embedding-invalidated-on-entity-update",
        "behavior-embedding-retrieved-by-entity-id",
        "behavior-embedding-stored-for-entity",
    ],
    "story-entities-inhabit-circles": [
        "behavior-circle-constellation-shows-all-inhabitants",
        "behavior-entity-bonded-with-inhabits-appears-in-circle-constellation",
    ],
    "story-git-native-circle-invitations": [
        "behavior-arrive-decrypts-pending-invitations",
        "behavior-invite-fetches-github-key-and-encrypts",
        "behavior-list-circle-members",
    ],
    "story-homoiconic-command-palette": [
        "behavior-command-palette-shows-cvm-tools-dynamically",
    ],
    "story-invite-collaborator-to-circle": [
        "behavior-decrypt-invitation-with-local-ssh-key",
        "behavior-encrypt-circle-key-for-recipient",
        "behavior-fetch-ssh-public-key-from-github",
        "behavior-invite-fetches-github-ssh-key-and-encrypts-circle-key",
        "behavior-list-circle-members-from-access-directory",
    ],
    "story-prune-approval-rejection-flow": [
        "behavior-prune-approve-composts-and-learns",
        "behavior-prune-reject-captures-reason",
    ],
    "story-semantic-primitives-enable-inference": [
        "behavior-detect-clusters-groups-similar-entities",
        "behavior-embed-entity-computes-and-stores-vector",
        "behavior-embed-text-computes-vector-for-arbitrary-text",
        "behavior-semantic-search-ranks-entities-by-meaning",
        "behavior-semantic-similarity-computes-cosine-distance",
        "behavior-suggest-bonds-finds-relationship-candidates",
    ],
    "story-system-can-prune-unused-code-and-entities": [
        "behavior-prune-detects-orphan-and-deprecated-tools",
        "behavior-prune-emits-signals-for-threshold-breaches",
        "behavior-prune-proposes-focus-for-human-approval",
    ],
    "story-system-creates-tools-dynamically": [
        "behavior-voice-command-manifests-tool-entity-that-appears-in-palette",
    ],
    "story-system-decides-what-to-sync": [
        "behavior-entity-in-cloud-circle-syncs",
        "behavior-entity-in-local-only-circle-does-not-sync",
        "behavior-entity-in-multiple-circles-syncs-to-cloud-ones-only",
    ],
    "story-system-enables-reactive-patterns": [
        "behavior-store-fires-hook-when-entity-saved",
        "behavior-system-indexes-learning-on-creation",
    ],
    "story-system-integrity-truth": [
        "behavior-integrity-discovers-scenarios",
        "behavior-integrity-reports-status",
        "behavior-integrity-runs-tests",
        "behavior-test-alpha",
        "behavior-verifies-bond-tracks-results",
    ],
    "story-system-metabolizes-entropy-into-growth": [
        "behavior-compost-archives-orphan-entity",
        "behavior-digest-transforms-entity-into-learning",
        "behavior-induce-proposes-pattern-from-learnings",
        "behavior-sense-entropy-reports-metabolic-health",
        "behavior-signal-auto-resolves-when-void-clears",
        "behavior-stagnation-emits-signal-when-threshold-exceeded",
    ],
    "story-system-senses-kairotic-rhythm": [
        "behavior-satiation-computed-from-integrity-entropy-and-growth",
        "behavior-sense-kairotic-state-returns-phase-weights",
        "behavior-temporal-health-tracks-rolling-window",
    ],
    "story-system-suggests-convergences": [
        "behavior-detect-isolated-entities",
        "behavior-emit-convergence-signal",
        "behavior-score-entity-coherence",
        "behavior-suggest-surfaces-bonds-for-learnings",
        "behavior-suggest-verifies-bonds-for-behaviors",
    ],
    "story-ui-responds-to-system-state": [
        "behavior-layout-entity-responds-to-signal-urgency",
    ],
    "story-users-invoke-protocols-directly-from-the-command-palette": [
        "behavior-command-palette-lists-and-invokes-cvm-protocols",
    ],
}


def bootstrap_behaviors(store: EventStore, verbose: bool = True) -> list[str]:
    """
    Bootstrap all behavior entities into the store.

    Each behavior is created as a GenericEntity with:
    - type: "behavior"
    - data.title: Human-readable description
    - data.status: "active"
    - data.cognition.links.story_id: Parent story reference

    Returns list of created behavior IDs.
    """
    created = []

    for behavior_id, bdata in BEHAVIORS.items():
        entity = GenericEntity(
            id=behavior_id,
            type="behavior",
            data={
                "title": bdata["title"],
                "status": "active",
                "cognition": {
                    "links": {
                        "story_id": bdata["story"],
                    }
                }
            }
        )
        store.save_entity(entity)
        created.append(behavior_id)

    if verbose:
        print(f"    [behaviors] {len(created)} behavior entities")

    return created
