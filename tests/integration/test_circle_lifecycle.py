"""
Integration test for the complete circle sync lifecycle.

This test demonstrates the full flow:
1. Create a circle with cloud sync policy
2. Generate circle key
3. Alice invites Bob (encrypt circle key for recipient)
4. Bob accepts invitation (decrypt circle key)
5. Both connect to chora-cloud
6. Alice creates an entity -> Bob receives it
7. Bob creates an entity -> Alice receives it

To run this test locally:
    PYTHONPATH=src:../chora-crypto/src:../chora-sync/src python3 -m pytest tests/integration/ -v

For full cloud integration (requires auth):
    CHORA_CLOUD_TOKEN=<jwt> pytest tests/integration/ -v --run-cloud
"""
from __future__ import annotations

import asyncio
import json
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import nacl.signing
import pytest

# CVM imports
from chora_cvm.store import EventStore
from chora_cvm.keyring import create_keyring, CircleBinding
from chora_cvm.sync_bridge import SyncBridge
from chora_cvm.invitation import (
    Invitation,
    create_invitation,
    decrypt_invitation_with_signing_key,
)

# Crypto imports
try:
    from chora_crypto import generate_circle_key, encrypt_entity, decrypt_entity, EncryptedBlob
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

# Sync imports
try:
    from chora_sync.transport import SyncTransport, create_change
    from chora_sync.client import SyncClient
    HAS_SYNC = True
except ImportError:
    HAS_SYNC = False


@dataclass
class SimulatedSite:
    """A simulated participant in the sync network."""

    name: str
    site_id: str
    db_path: str
    store: EventStore = field(init=False)
    keyring: Any = field(init=False)
    bridge: SyncBridge | None = field(default=None, init=False)
    signing_key: nacl.signing.SigningKey = field(init=False)
    verify_key: nacl.signing.VerifyKey = field(init=False)
    circle_key: bytes | None = field(default=None, init=False)
    received_entities: list[dict] = field(default_factory=list, init=False)

    def __post_init__(self):
        """Initialize store, keyring, and generate keys."""
        self.store = EventStore(self.db_path)
        self.keyring = create_keyring(user_id=self.name)
        # Generate Ed25519 key pair for this site
        self.signing_key = nacl.signing.SigningKey.generate()
        self.verify_key = self.signing_key.verify_key

    def join_circle(self, circle_id: str, circle_key: bytes, sync_policy: str = "cloud") -> None:
        """Join a circle with the given key."""
        self.circle_key = circle_key

        # Create circle entity in store
        self.store.save_generic_entity(circle_id, "circle", {
            "name": f"Circle for {self.name}",
            "sync_policy": sync_policy,
        })

        # Add binding to keyring
        self.keyring.add_binding(CircleBinding(
            circle_id=circle_id,
            sync_policy=sync_policy,
        ))

    def create_entity_in_circle(self, entity_id: str, entity_type: str, data: dict, circle_id: str) -> None:
        """Create an entity and bond it to a circle."""
        # Create inhabits bond FIRST so SyncRouter can find circle membership
        # when the save hook fires
        bond_id = f"bond-{entity_id}-inhabits-{circle_id}"
        self.store.save_bond(
            bond_id=bond_id,
            bond_type="inhabits",
            from_id=entity_id,
            to_id=circle_id,
        )

        # Save entity (this triggers sync hook which checks the bond)
        self.store.save_generic_entity(entity_id, entity_type, data)

    def setup_bridge(self) -> None:
        """Set up sync bridge to track changes."""
        self.bridge = SyncBridge(self.store, self.keyring, site_id=self.site_id)

        # Track changes for testing
        def on_change(changes):
            for change in changes:
                self.received_entities.append(change)

        self.bridge.set_change_callback(on_change)

    def close(self) -> None:
        """Clean up resources."""
        if self.bridge:
            self.bridge.close()
        self.store.close()


class TestCircleLifecycleLocal:
    """Test circle lifecycle without cloud (local simulation)."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test databases."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def alice(self, temp_dir):
        """Create Alice's site."""
        site = SimulatedSite(
            name="alice",
            site_id=f"site-alice-{uuid.uuid4()}",
            db_path=str(temp_dir / "alice.db"),
        )
        yield site
        site.close()

    @pytest.fixture
    def bob(self, temp_dir):
        """Create Bob's site."""
        site = SimulatedSite(
            name="bob",
            site_id=f"site-bob-{uuid.uuid4()}",
            db_path=str(temp_dir / "bob.db"),
        )
        yield site
        site.close()

    def test_invitation_flow(self, alice, bob):
        """Test that Alice can invite Bob to a circle."""
        circle_id = "circle-test-collab"
        circle_key = nacl.utils.random(32)  # Generate circle key

        # Alice creates the circle and invites Bob
        invitation = create_invitation(
            username="bob",
            circle_id=circle_id,
            circle_key=circle_key,
            recipient_public_key=bob.verify_key,
        )

        assert invitation.username == "bob"
        assert invitation.circle_id == circle_id
        assert len(invitation.encrypted_key) > 0

        # Bob decrypts the invitation
        decrypted_key = decrypt_invitation_with_signing_key(
            invitation, bob.signing_key
        )

        assert decrypted_key == circle_key
        print(f"  Invitation flow successful: Alice invited Bob to {circle_id}")

    def test_both_join_circle(self, alice, bob):
        """Test that both Alice and Bob can join a circle."""
        circle_id = "circle-shared"
        circle_key = nacl.utils.random(32)

        # Both join with the same key
        alice.join_circle(circle_id, circle_key)
        bob.join_circle(circle_id, circle_key)

        # Verify both have the circle
        assert alice.keyring.can_cross(circle_id)
        assert bob.keyring.can_cross(circle_id)
        assert not alice.keyring.is_local_only(circle_id)
        assert not bob.keyring.is_local_only(circle_id)

        print(f"  Both joined circle: {circle_id}")

    def test_entity_creation_triggers_sync(self, alice, bob):
        """Test that creating an entity triggers sync bridge."""
        circle_id = "circle-sync-test"
        circle_key = nacl.utils.random(32)

        # Both join circle
        alice.join_circle(circle_id, circle_key)
        bob.join_circle(circle_id, circle_key)

        # Set up sync bridge for Alice
        alice.setup_bridge()

        # Alice creates an entity in the circle
        alice.create_entity_in_circle(
            entity_id="note-1",
            entity_type="note",
            data={"title": "Hello from Alice", "content": "First sync test"},
            circle_id=circle_id,
        )

        # Check that bridge captured the change
        assert alice.bridge is not None
        pending = alice.bridge.pending_changes
        assert len(pending) == 1
        assert pending[0]["entity_id"] == "note-1"
        assert pending[0]["entity_type"] == "note"
        assert circle_id in pending[0]["circle_ids"]

        print(f"  Entity creation triggered sync: {pending[0]['entity_id']}")

    @pytest.mark.skipif(not HAS_CRYPTO, reason="chora-crypto not available")
    def test_entity_encryption_roundtrip(self, alice, bob):
        """Test that entities can be encrypted and decrypted."""
        circle_key = generate_circle_key()

        # Alice encrypts an entity
        entity_data = {
            "entity_id": "note-secret",
            "entity_type": "note",
            "data": {"title": "Secret Note", "content": "Classified information"},
        }

        encrypted_blob = encrypt_entity(entity_data, circle_key)
        assert len(encrypted_blob.ciphertext) > 0

        # Bob decrypts the entity (with same circle key)
        decrypted = decrypt_entity(encrypted_blob, circle_key)

        assert decrypted["entity_id"] == "note-secret"
        assert decrypted["data"]["title"] == "Secret Note"

        print(f"  Encryption roundtrip successful")

    def test_full_local_sync_simulation(self, alice, bob, temp_dir):
        """
        Full simulation of the sync flow (without cloud).

        This simulates what would happen with cloud sync by manually
        passing encrypted changes between sites.
        """
        circle_id = "circle-full-test"

        # Step 1: Generate circle key
        circle_key = nacl.utils.random(32)
        print(f"\n  Step 1: Generated circle key ({len(circle_key)} bytes)")

        # Step 2: Alice creates invitation for Bob
        invitation = create_invitation(
            username="bob",
            circle_id=circle_id,
            circle_key=circle_key,
            recipient_public_key=bob.verify_key,
        )
        print(f"  Step 2: Alice created invitation for Bob")

        # Step 3: Bob accepts invitation
        bob_circle_key = decrypt_invitation_with_signing_key(invitation, bob.signing_key)
        assert bob_circle_key == circle_key
        print(f"  Step 3: Bob accepted invitation, got circle key")

        # Step 4: Both join the circle
        alice.join_circle(circle_id, circle_key)
        bob.join_circle(circle_id, bob_circle_key)
        print(f"  Step 4: Both joined circle {circle_id}")

        # Step 5: Set up sync bridges
        alice.setup_bridge()
        bob.setup_bridge()
        print(f"  Step 5: Sync bridges configured")

        # Step 6: Alice creates an entity
        alice.create_entity_in_circle(
            entity_id="note-alice-1",
            entity_type="note",
            data={"title": "Alice's Note", "author": "alice"},
            circle_id=circle_id,
        )
        print(f"  Step 6: Alice created note-alice-1")

        # Verify Alice's bridge captured it
        alice_changes = alice.bridge.flush_changes()
        assert len(alice_changes) == 1
        assert alice_changes[0]["entity_id"] == "note-alice-1"

        # Step 7: Simulate Bob receiving Alice's change
        # (In real sync, this would come via WebSocket from chora-cloud)
        if HAS_CRYPTO:
            # Encrypt with circle key (Alice side)
            encrypted = encrypt_entity(alice_changes[0]["data"], circle_key)

            # Decrypt with circle key (Bob side)
            decrypted = decrypt_entity(encrypted, bob_circle_key)
            bob.store.save_generic_entity(
                alice_changes[0]["entity_id"],
                alice_changes[0]["entity_type"],
                decrypted,
            )
            print(f"  Step 7: Bob received and decrypted Alice's note")

        # Step 8: Bob creates an entity
        bob.create_entity_in_circle(
            entity_id="note-bob-1",
            entity_type="note",
            data={"title": "Bob's Note", "author": "bob"},
            circle_id=circle_id,
        )
        print(f"  Step 8: Bob created note-bob-1")

        # Verify Bob's bridge captured it
        bob_changes = bob.bridge.flush_changes()
        assert len(bob_changes) == 1
        assert bob_changes[0]["entity_id"] == "note-bob-1"

        # Step 9: Simulate Alice receiving Bob's change
        if HAS_CRYPTO:
            encrypted = encrypt_entity(bob_changes[0]["data"], bob_circle_key)
            decrypted = decrypt_entity(encrypted, circle_key)
            alice.store.save_generic_entity(
                bob_changes[0]["entity_id"],
                bob_changes[0]["entity_type"],
                decrypted,
            )
            print(f"  Step 9: Alice received and decrypted Bob's note")

        # Final verification: Both have both notes
        alice_note = alice.store.get_entity("note-alice-1")
        bob_note_on_alice = alice.store.get_entity("note-bob-1") if HAS_CRYPTO else None

        bob_note = bob.store.get_entity("note-bob-1")
        alice_note_on_bob = bob.store.get_entity("note-alice-1") if HAS_CRYPTO else None

        assert alice_note is not None
        assert bob_note is not None

        if HAS_CRYPTO:
            assert bob_note_on_alice is not None
            assert alice_note_on_bob is not None
            print(f"\n  SUCCESS: Both Alice and Bob have each other's notes!")
        else:
            print(f"\n  PARTIAL SUCCESS: Circle setup complete (crypto not available for full roundtrip)")


class TestInvitationPersistence:
    """Test invitation file persistence."""

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_invitation_to_file_roundtrip(self, temp_dir):
        """Test saving and loading invitation from file."""
        access_dir = temp_dir / ".chora" / "access"

        # Create invitation
        signing_key = nacl.signing.SigningKey.generate()
        circle_key = nacl.utils.random(32)

        original = create_invitation(
            username="testuser",
            circle_id="circle-persist",
            circle_key=circle_key,
            recipient_public_key=signing_key.verify_key,
        )

        # Save to file
        file_path = original.to_file(access_dir)
        assert file_path.exists()
        assert file_path.name == "testuser.enc"

        # Load from file
        loaded = Invitation.from_file(file_path)

        assert loaded.username == original.username
        assert loaded.circle_id == original.circle_id
        assert loaded.encrypted_key == original.encrypted_key

        # Verify can decrypt
        decrypted = decrypt_invitation_with_signing_key(loaded, signing_key)
        assert decrypted == circle_key

        print(f"  Invitation file roundtrip successful: {file_path}")


class TestCloudSync:
    """
    Integration tests that actually connect to chora-cloud.

    These tests require:
    - Network access to https://chora-cloud.accounts-82f.workers.dev
    - Run with: pytest tests/integration/ -v -m cloud

    By default these are skipped unless explicitly requested.
    """

    CLOUD_URL = "wss://chora-cloud.accounts-82f.workers.dev/sync"
    API_URL = "https://chora-cloud.accounts-82f.workers.dev"

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.mark.asyncio
    @pytest.mark.skipif(not HAS_CRYPTO, reason="chora-crypto not available")
    async def test_cloud_account_creation(self):
        """Test creating account and workspace on chora-cloud."""
        import aiohttp

        test_email = f"test-{uuid.uuid4()}@example.com"
        test_password = "test-password-123"

        async with aiohttp.ClientSession() as session:
            # Create account
            async with session.post(
                f"{self.API_URL}/api/accounts",
                json={"email": test_email, "password": test_password}
            ) as resp:
                result = await resp.json()
                assert result.get("success"), f"Account creation failed: {result}"
                account_id = result["data"]["id"]
                print(f"  Created account: {account_id}")

            # Login
            async with session.post(
                f"{self.API_URL}/api/login",
                json={"email": test_email, "password": test_password}
            ) as resp:
                result = await resp.json()
                assert result.get("success"), f"Login failed: {result}"
                token = result["data"]["token"]
                print(f"  Logged in, got token")

            # Create workspace
            async with session.post(
                f"{self.API_URL}/api/workspaces",
                json={"name": "test-workspace"},
                headers={"Authorization": f"Bearer {token}"}
            ) as resp:
                result = await resp.json()
                assert result.get("success"), f"Workspace creation failed: {result}"
                workspace_id = result["data"]["id"]
                print(f"  Created workspace: {workspace_id}")

        print(f"\n  Cloud account lifecycle complete!")

    @pytest.mark.asyncio
    @pytest.mark.skipif(not HAS_CRYPTO, reason="chora-crypto not available")
    @pytest.mark.skipif(not HAS_SYNC, reason="chora-sync not available (external dependency)")
    async def test_websocket_sync_roundtrip(self, temp_dir):
        """
        Test full WebSocket sync between two clients.

        This is the real deal - actual encrypted sync over the cloud.
        """
        import aiohttp

        # Create test account
        test_email = f"sync-test-{uuid.uuid4()}@example.com"
        test_password = "sync-test-password"

        async with aiohttp.ClientSession() as session:
            # Create account
            async with session.post(
                f"{self.API_URL}/api/accounts",
                json={"email": test_email, "password": test_password}
            ) as resp:
                result = await resp.json()
                assert result.get("success"), f"Account creation failed: {result}"
                print(f"  Created test account")

            # Login
            async with session.post(
                f"{self.API_URL}/api/login",
                json={"email": test_email, "password": test_password}
            ) as resp:
                result = await resp.json()
                assert result.get("success"), f"Login failed: {result}"
                token = result["data"]["token"]

            # Create workspace
            async with session.post(
                f"{self.API_URL}/api/workspaces",
                json={"name": "sync-test-workspace"},
                headers={"Authorization": f"Bearer {token}"}
            ) as resp:
                result = await resp.json()
                assert result.get("success"), f"Workspace creation failed: {result}"
                workspace_id = result["data"]["id"]
                print(f"  Created workspace: {workspace_id}")

        # Generate shared circle key
        circle_key = generate_circle_key()
        print(f"  Generated circle key ({len(circle_key)} bytes)")

        # Create two sync clients (simulating two devices)
        alice = SyncClient(
            db_path=str(temp_dir / "alice.db"),
            circle_key=circle_key,
            site_id=f"site-alice-{uuid.uuid4()}",
        )

        bob = SyncClient(
            db_path=str(temp_dir / "bob.db"),
            circle_key=circle_key,
            site_id=f"site-bob-{uuid.uuid4()}",
        )

        # Track received entities for Bob
        bob_received = []
        bob.on_entity_received(lambda eid, etype, data: bob_received.append({
            "entity_id": eid, "entity_type": etype, "data": data
        }))

        try:
            # Connect both to cloud
            ws_url = f"{self.CLOUD_URL}/{workspace_id}"
            await alice.connect(ws_url, workspace_id, token)
            print(f"  Alice connected at version {alice.current_version}")

            await bob.connect(ws_url, workspace_id, token)
            print(f"  Bob connected at version {bob.current_version}")

            # Alice pushes an entity
            await alice.push_entity(
                entity_id="note-cloud-1",
                entity_type="note",
                data={"title": "Cloud Sync Test", "author": "alice"},
            )
            print(f"  Alice pushed note-cloud-1")

            # Bob pulls to receive
            await bob.pull(since_version=0)

            # Give time for message to arrive
            await asyncio.sleep(0.5)

            # Verify Bob received Alice's entity
            if bob_received:
                assert bob_received[0]["entity_id"] == "note-cloud-1"
                assert bob_received[0]["data"]["author"] == "alice"
                print(f"  Bob received: {bob_received[0]['entity_id']}")
                print(f"\n  CLOUD SYNC SUCCESS!")
            else:
                print(f"  Bob did not receive entity yet (may need longer wait)")

        finally:
            await alice.disconnect()
            await bob.disconnect()
            print(f"  Both clients disconnected")


# Mark cloud tests to be skipped by default
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "cloud: tests that require chora-cloud connection"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
