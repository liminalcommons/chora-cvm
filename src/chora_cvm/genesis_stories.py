"""
Genesis Stories: Bootstrap Story Entities and Specifies Bonds.

This module creates story entities representing desires/goals for the system,
and establishes `specifies` bonds from stories to their child behaviors.

Stories are extracted from feature file headers (# Story: story-*).
"""

from __future__ import annotations

from chora_cvm.kernel.schema import GenericEntity
from chora_cvm.store import EventStore
from chora_cvm.genesis_behaviors import STORY_BEHAVIORS


# =============================================================================
# STORY DEFINITIONS
# =============================================================================
# Extracted from tests/features/*.feature headers
# Each story specifies one or more behaviors

STORIES: dict[str, dict] = {
    "story-agents-can-search-legacy-content-to-understand-historical-patterns-and-decisions": {
        "title": "Legacy Content Harvest",
        "description": "Agents can search archived content to understand historical patterns and past decisions.",
        "principle": None,
    },
    "story-assets-belong-to-circles": {
        "title": "Asset Ownership",
        "description": "Assets belong to circles through ownership bonds, creating clear provenance for external resources.",
        "principle": "principle-ownership-is-bond-geometry",
    },
    "story-attention-declares-what-matters": {
        "title": "Attention Layer",
        "description": "Focus declares what is being attended to. Signal demands attention. The attention layer tracks energy in the system.",
        "principle": "principle-attention-is-finite",
    },
    "story-autonomic-heartbeat": {
        "title": "Autonomic Pulse",
        "description": "The system has an autonomic heartbeat that processes signals automatically. Trust in autonomic behavior comes from: Observable, Predictable, Controllable.",
        "principle": "principle-autonomic-trust",
    },
    "story-bonds-carry-epistemic-confidence": {
        "title": "Bond Confidence",
        "description": "Bonds carry epistemic confidence levels, enabling the system to reason about relationship certainty.",
        "principle": None,
    },
    "story-build-governance-ensures-quality": {
        "title": "Build Governance",
        "description": "The build system ensures code quality through lint, type checking, testing, coverage, and security scanning.",
        "principle": None,
    },
    "story-circles-declare-their-sync-boundary": {
        "title": "Circle Sync Policy",
        "description": "Circles declare their sync boundary through policy, determining what flows to the cloud vs stays local.",
        "principle": "principle-privacy-boundary-is-sync-policy",
    },
    "story-cvm-enables-multimodal-deployment": {
        "title": "CVM Engine and I/O Membrane",
        "description": "The CVM enables deployment across CLI, API, and MCP through a unified dispatch interface. The I/O membrane routes output through injectable sinks.",
        "principle": "principle-nucleus-is-silent",
    },
    "story-cvm-enables-self-awareness": {
        "title": "Self-Awareness Through Rhythm Sensing",
        "description": "The system senses its own kairotic rhythm, computing satiation and temporal health metrics.",
        "principle": None,
    },
    "story-cvm-enables-self-healing": {
        "title": "Self-Healing Through Prune Detection",
        "description": "The system detects orphaned and deprecated tools, enabling self-healing through pruning.",
        "principle": None,
    },
    "story-docs-detect-their-own-staleness": {
        "title": "Documentation Self-Maintenance",
        "description": "Documentation detects its own staleness, proposes repairs, and requires human approval for semantic changes.",
        "principle": "principle-documentation-emerges-from-the-entity-graph-not-manual-editing",
    },
    "story-dweller-has-local-keyring": {
        "title": "Local Keyring",
        "description": "Each dweller has a local keyring storing identity and circle encryption keys securely.",
        "principle": "principle-identity-lives-in-keyring",
    },
    "story-embeddings-persist-in-sqlite": {
        "title": "Embedding Persistence",
        "description": "Vector embeddings persist in SQLite, enabling semantic search without external dependencies.",
        "principle": None,
    },
    "story-entities-inhabit-circles": {
        "title": "Entity Circle Membership",
        "description": "Entities inhabit circles through bonds, creating membership boundaries.",
        "principle": "principle-circle-boundary-is-membership-boundary",
    },
    "story-git-native-circle-invitations": {
        "title": "Git-Native Circle Invitations",
        "description": "Circle invitations flow through Git, using GitHub SSH keys for zero-friction collaboration.",
        "principle": "principle-git-becomes-keychain",
    },
    "story-homoiconic-command-palette": {
        "title": "Homoiconic Command Palette",
        "description": "The command palette shows CVM tools dynamically, reading from the entity graph.",
        "principle": None,
    },
    "story-invite-collaborator-to-circle": {
        "title": "Circle Invitation Flow",
        "description": "Collaborators are invited to circles by encrypting circle keys with their GitHub SSH public keys.",
        "principle": "principle-github-ssh-keys-enable-zero-friction-invitation",
    },
    "story-prune-approval-rejection-flow": {
        "title": "Prune Approval and Rejection",
        "description": "Pruning requires human approval. Approval composts and creates learnings. Rejection captures reasons.",
        "principle": "principle-logic-derives-from-graph",
    },
    "story-semantic-primitives-enable-inference": {
        "title": "Semantic Primitives",
        "description": "Semantic primitives enable inference through embedding, similarity computation, and relationship suggestion.",
        "principle": None,
    },
    "story-system-can-prune-unused-code-and-entities": {
        "title": "System Pruning",
        "description": "The system detects orphaned and deprecated entities, proposing them for human-approved pruning.",
        "principle": None,
    },
    "story-system-creates-tools-dynamically": {
        "title": "Dynamic Tool Creation",
        "description": "The system can create tool entities dynamically from voice commands, which then appear in the command palette.",
        "principle": None,
    },
    "story-system-decides-what-to-sync": {
        "title": "Sync Routing",
        "description": "The system decides what to sync based on circle membership and sync policy.",
        "principle": "principle-geometry-determines-sync",
    },
    "story-system-enables-reactive-patterns": {
        "title": "Reactive Patterns",
        "description": "The system enables reactive patterns through entity hooks and automatic indexing.",
        "principle": None,
    },
    "story-system-integrity-truth": {
        "title": "System Integrity Truth",
        "description": "The system tells the truth about its own verification status. Truth flows from observed test results back into entity status.",
        "principle": None,
    },
    "story-system-metabolizes-entropy-into-growth": {
        "title": "Metabolic Operations",
        "description": "Dead branches, stale signals, and deprecated entities are not waste—they are compost. The system digests entropy and radiates learnings.",
        "principle": "principle-friction-is-invitation",
    },
    "story-system-senses-kairotic-rhythm": {
        "title": "Kairotic Rhythm Sensing",
        "description": "The system senses kairotic rhythm through six phase weights, computing satiation from integrity, entropy, and growth metrics.",
        "principle": None,
    },
    "story-system-suggests-convergences": {
        "title": "Convergence Scanning",
        "description": "Convergence is docs as a converging force—not just finding what's wrong, but suggesting what wants to connect.",
        "principle": "principle-convergence-is-active-not-passive",
    },
    "story-ui-responds-to-system-state": {
        "title": "UI State Response",
        "description": "The UI responds to system state, adjusting layout based on signal urgency and other factors.",
        "principle": None,
    },
    "story-users-invoke-protocols-directly-from-the-command-palette": {
        "title": "Protocol Command Palette",
        "description": "Users can invoke CVM protocols directly from the command palette.",
        "principle": None,
    },
}


def bootstrap_stories(store: EventStore, verbose: bool = True) -> list[str]:
    """
    Bootstrap all story entities into the store.

    Each story is created as a GenericEntity with:
    - type: "story"
    - data.title: Human-readable title
    - data.description: Story description
    - data.status: "active"
    - data.cognition.links.principle_id: Governing principle (if any)

    Returns list of created story IDs.
    """
    created = []

    for story_id, sdata in STORIES.items():
        entity = GenericEntity(
            id=story_id,
            type="story",
            data={
                "title": sdata["title"],
                "description": sdata["description"],
                "status": "active",
                "cognition": {
                    "links": {
                        "principle_id": sdata.get("principle"),
                    }
                }
            }
        )
        store.save_entity(entity)
        created.append(story_id)

    if verbose:
        print(f"    [stories] {len(created)} story entities")

    return created


def bootstrap_specifies_bonds(store: EventStore, verbose: bool = True) -> list[str]:
    """
    Bootstrap `specifies` bonds from stories to behaviors.

    A story specifies one or more behaviors, representing the desire
    that those behaviors fulfill.

    Returns list of created bond IDs.
    """
    created = []

    for story_id, behavior_ids in STORY_BEHAVIORS.items():
        for behavior_id in behavior_ids:
            bond_id = f"rel-specifies-{story_id.replace('story-', '')[:30]}-{behavior_id.replace('behavior-', '')[:30]}"

            store.save_bond(
                bond_id=bond_id,
                bond_type="specifies",
                from_id=story_id,
                to_id=behavior_id,
                status="active",
                confidence=1.0,
                data={
                    "source": "genesis",
                }
            )
            created.append(bond_id)

    if verbose:
        print(f"    [specifies] {len(created)} bonds (story → behavior)")

    return created
