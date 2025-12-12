"""
SyncRouter: Routes entity changes based on circle membership.

The SyncRouter answers: "Where should this entity's changes go?"

It queries inhabits bonds to find which circles an entity belongs to,
then uses the Keyring to determine which of those circles should receive
sync events.

The geometry (bonds) determines behavior, not conditional logic.

Ported from archive/v4/chora-store for Circle Physics.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .keyring import Keyring, CircleBinding
    from .store import EventStore


@dataclass
class SyncTarget:
    """A circle that should receive sync events for an entity."""

    circle_id: str
    binding: "CircleBinding"


class SyncRouter:
    """
    Routes entity changes based on inhabits bonds and keyring policies.

    This is the decision point for sync: it determines which circles
    should receive changes for a given entity.

    The router uses:
    - EventStore.get_inhabited_circles() to find which circles an entity inhabits
    - Keyring.is_local_only() to check if a circle should sync
    """

    def __init__(self, store: "EventStore", keyring: "Keyring"):
        """
        Initialize the SyncRouter.

        Args:
            store: EventStore to query for inhabits bonds
            keyring: Keyring to check sync policies
        """
        self._store = store
        self._keyring = keyring

    def get_target_circles(self, entity_id: str) -> list[str]:
        """
        Get all circles this entity inhabits.

        Queries inhabits bonds where entity is the source.

        Args:
            entity_id: ID of the entity

        Returns:
            List of circle IDs the entity inhabits
        """
        return self._store.get_inhabited_circles(entity_id)

    def should_emit(self, entity_id: str) -> bool:
        """
        Should this entity emit sync events?

        Returns True if the entity inhabits at least one cloud circle.

        Args:
            entity_id: ID of the entity

        Returns:
            True if entity should emit sync events
        """
        circles = self.get_target_circles(entity_id)

        for circle_id in circles:
            if not self._keyring.is_local_only(circle_id):
                return True

        return False

    def route_entity(self, entity_id: str) -> list[SyncTarget]:
        """
        Get sync targets for an entity.

        Returns only cloud circles (local-only circles are filtered out).

        Args:
            entity_id: ID of the entity

        Returns:
            List of SyncTarget for circles that should receive sync events
        """
        circles = self.get_target_circles(entity_id)
        targets = []

        for circle_id in circles:
            binding = self._keyring.get_binding(circle_id)
            if binding and binding.sync_policy == "cloud":
                targets.append(
                    SyncTarget(
                        circle_id=circle_id,
                        binding=binding,
                    )
                )

        return targets

    def get_cloud_circle_ids(self, entity_id: str) -> list[str]:
        """
        Get circle IDs for sync (cloud circles only).

        This is a convenience method that returns just the circle IDs
        that should receive sync events.

        Args:
            entity_id: ID of the entity

        Returns:
            List of circle IDs with cloud sync policy
        """
        return [t.circle_id for t in self.route_entity(entity_id)]
