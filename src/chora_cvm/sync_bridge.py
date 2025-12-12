"""
SyncBridge: Bridges CVM store events to the sync layer.

This module connects the entity save hooks in EventStore to the
sync infrastructure (ChangeTracker, encryption, transport).

The flow:
  1. Entity saved → EventStore fires hook
  2. SyncBridge receives hook
  3. SyncRouter decides: should_emit()?
  4. If yes: record change, encrypt, queue for transport

This is the nervous system that connects independent nodes.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .store import EventStore
    from .keyring import Keyring


class SyncBridge:
    """
    Bridges CVM store events to sync layer.

    The SyncBridge listens for entity save events and routes them
    to the appropriate sync channels based on circle membership.
    """

    def __init__(
        self,
        store: "EventStore",
        keyring: "Keyring",
        site_id: str | None = None,
    ):
        """
        Initialize the SyncBridge.

        Args:
            store: EventStore to hook into
            keyring: Keyring for sync policies and encryption keys
            site_id: Unique identifier for this site (auto-generated if None)
        """
        from .sync_router import SyncRouter

        self._store = store
        self._keyring = keyring
        self._site_id = site_id or f"site-{uuid.uuid4()}"
        self._router = SyncRouter(store, keyring)

        # Pending changes queued for transport
        self._pending_changes: list[dict] = []

        # Optional callback when changes are ready
        self._on_change_ready: Callable[[list[dict]], None] | None = None

        # Hook into store
        store.add_entity_hook(self._on_entity_saved)

    @property
    def site_id(self) -> str:
        """Unique identifier for this site."""
        return self._site_id

    @property
    def pending_changes(self) -> list[dict]:
        """Changes waiting to be synced."""
        return self._pending_changes.copy()

    def set_change_callback(
        self, callback: Callable[[list[dict]], None] | None
    ) -> None:
        """
        Set callback for when changes are ready to sync.

        The callback receives a list of change dicts. Each dict contains:
        - entity_id: str
        - entity_type: str
        - data: dict
        - circle_ids: list[str]
        - timestamp: str (ISO format)

        Args:
            callback: Function to call with pending changes, or None to clear
        """
        self._on_change_ready = callback

    def _on_entity_saved(
        self, entity_id: str, entity_type: str, data: dict
    ) -> None:
        """
        Handle entity save event from EventStore.

        This is the hook callback registered with the store.
        """
        # Check if entity should emit sync events
        if not self._router.should_emit(entity_id):
            return

        # Get cloud circles for this entity
        circle_ids = self._router.get_cloud_circle_ids(entity_id)
        if not circle_ids:
            return

        # Create change record
        change = {
            "id": f"change-{uuid.uuid4()}",
            "entity_id": entity_id,
            "entity_type": entity_type,
            "data": data,
            "circle_ids": circle_ids,
            "site_id": self._site_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Queue for transport
        self._pending_changes.append(change)

        # Notify callback if registered
        if self._on_change_ready:
            self._on_change_ready([change])

    def flush_changes(self) -> list[dict]:
        """
        Flush and return all pending changes.

        Returns:
            List of change dicts that were pending
        """
        changes = self._pending_changes
        self._pending_changes = []
        return changes

    def close(self) -> None:
        """Unhook from the store."""
        self._store.remove_entity_hook(self._on_entity_saved)
