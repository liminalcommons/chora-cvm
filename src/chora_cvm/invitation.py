"""
Git-based invitation flow for circle membership.

Enables zero-friction invitation using SSH keys from GitHub.
Circle keys are encrypted for recipients and committed to git.

Invitation Flow:
    just invite <github-username> <circle-id>
        │
        ▼
    Fetch SSH key from https://github.com/<username>.keys
        │
        ▼
    Encrypt circle key for recipient
        │
        ▼
    Write to .chora/access/<circle-id>/<username>.enc
        │
        ▼
    git add && git commit && git push

Init Flow:
    git pull && just init
        │
        ▼
    Find .chora/access/<circle-id>/<username>.enc
        │
        ▼
    Decrypt with local SSH private key
        │
        ▼
    Add circle to local keyring

Ported from archive/v4/chora-store for Circle Physics.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import nacl.public
import nacl.signing


class InvitationError(Exception):
    """Error in invitation flow."""

    pass


class GitHubKeyNotFoundError(InvitationError):
    """GitHub user not found or has no SSH keys."""

    pass


@dataclass
class Invitation:
    """An encrypted invitation to a circle."""

    username: str
    circle_id: str
    encrypted_key: bytes

    def to_file(self, access_dir: Path) -> Path:
        """
        Write invitation to access directory.

        Creates: <access_dir>/<circle_id>/<username>.enc

        The file contains base64-encoded encrypted key with metadata.
        """
        circle_dir = access_dir / self.circle_id
        circle_dir.mkdir(parents=True, exist_ok=True)

        file_path = circle_dir / f"{self.username}.enc"

        # Store as JSON for extensibility
        data = {
            "version": 1,
            "username": self.username,
            "circle_id": self.circle_id,
            "encrypted_key": base64.b64encode(self.encrypted_key).decode("ascii"),
        }

        file_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return file_path

    @classmethod
    def from_file(cls, file_path: Path) -> "Invitation":
        """Load invitation from file."""
        data = json.loads(file_path.read_text(encoding="utf-8"))

        if data.get("version") != 1:
            raise InvitationError(f"Unsupported invitation version: {data.get('version')}")

        return cls(
            username=data["username"],
            circle_id=data["circle_id"],
            encrypted_key=base64.b64decode(data["encrypted_key"]),
        )


def fetch_github_ssh_key(username: str) -> str:
    """
    Fetch SSH public key from GitHub.

    GitHub exposes public keys at https://github.com/<username>.keys

    Args:
        username: GitHub username

    Returns:
        First Ed25519 SSH public key for the user

    Raises:
        GitHubKeyNotFoundError: If user not found or no Ed25519 key
    """
    url = f"https://github.com/{username}.keys"

    try:
        with urlopen(url, timeout=10) as response:
            keys_text = response.read().decode("utf-8")
    except HTTPError as e:
        if e.code == 404:
            raise GitHubKeyNotFoundError(f"GitHub user not found: {username}") from e
        raise InvitationError(f"Failed to fetch keys from GitHub: {e}") from e
    except URLError as e:
        raise InvitationError(f"Network error fetching keys: {e}") from e

    # Parse keys, find first Ed25519 key
    for line in keys_text.strip().split("\n"):
        line = line.strip()
        if line.startswith("ssh-ed25519"):
            return line

    raise GitHubKeyNotFoundError(
        f"No Ed25519 SSH key found for {username}. "
        "Please ensure the user has an Ed25519 key on GitHub."
    )


def create_invitation(
    username: str,
    circle_id: str,
    circle_key: bytes,
    recipient_public_key: nacl.signing.VerifyKey,
) -> Invitation:
    """
    Create an encrypted invitation for a user.

    Args:
        username: GitHub username of recipient
        circle_id: Circle being invited to
        circle_key: Symmetric key for the circle
        recipient_public_key: Recipient's Ed25519 verify key

    Returns:
        Invitation with encrypted key
    """
    # Convert Ed25519 to X25519 for encryption
    x25519_public = recipient_public_key.to_curve25519_public_key()

    # Create sealed box (anonymous encryption to known recipient)
    sealed_box = nacl.public.SealedBox(x25519_public)
    encrypted = bytes(sealed_box.encrypt(circle_key))

    return Invitation(
        username=username,
        circle_id=circle_id,
        encrypted_key=encrypted,
    )


def decrypt_invitation_with_signing_key(
    invitation: Invitation,
    private_key: nacl.signing.SigningKey,
) -> bytes:
    """
    Decrypt an invitation using a signing key.

    Args:
        invitation: The invitation to decrypt
        private_key: Ed25519 signing key (will be converted to X25519)

    Returns:
        Decrypted circle key
    """
    # Convert Ed25519 to X25519 for decryption
    x25519_private = private_key.to_curve25519_private_key()

    sealed_box = nacl.public.SealedBox(x25519_private)
    return bytes(sealed_box.decrypt(invitation.encrypted_key))


def decrypt_invitation(
    invitation: Invitation,
    private_key_path: Path,
) -> bytes:
    """
    Decrypt an invitation using local SSH private key.

    Args:
        invitation: The invitation to decrypt
        private_key_path: Path to SSH private key (e.g., ~/.ssh/id_ed25519)

    Returns:
        Decrypted circle key
    """
    # Import from chora_crypto if available, otherwise raise helpful error
    try:
        from chora_crypto.ssh_keys import load_ssh_private_key
    except ImportError:
        raise InvitationError(
            "chora_crypto package not available. "
            "Add packages/chora-crypto/src to PYTHONPATH."
        )

    keypair = load_ssh_private_key(private_key_path)

    # Convert Ed25519 to X25519 for decryption
    x25519_private = keypair.private_key.to_curve25519_private_key()

    sealed_box = nacl.public.SealedBox(x25519_private)
    return bytes(sealed_box.decrypt(invitation.encrypted_key))


def list_circle_members(access_dir: Path, circle_id: str) -> list[str]:
    """
    List members of a circle from access directory.

    Args:
        access_dir: Path to .chora/access/
        circle_id: Circle to list members for

    Returns:
        List of usernames with invitations
    """
    circle_dir = access_dir / circle_id
    if not circle_dir.exists():
        return []

    members = []
    for enc_file in circle_dir.glob("*.enc"):
        # Username is filename without .enc
        members.append(enc_file.stem)

    return sorted(members)


def get_default_access_dir() -> Path:
    """Get the default access directory (.chora/access/)."""
    return Path.cwd() / ".chora" / "access"


def get_default_private_key() -> Path:
    """Get the default SSH private key path."""
    return Path.home() / ".ssh" / "id_ed25519"
