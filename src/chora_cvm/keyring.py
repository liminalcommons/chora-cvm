"""
Keyring: The artifact of access.

The Keyring holds crystallized capability to cross boundaries.
It answers: "Which membranes can I cross?"

This is not configuration - it is Situatedness.

Ported from archive/v4/chora-store for Circle Physics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[import-not-found]


@dataclass
class CircleBinding:
    """A binding to a circle - the key to cross its membrane."""

    circle_id: str
    sync_policy: Literal["local-only", "cloud"]
    is_default: bool = False
    cloud_workspace: str | None = None
    cloud_url: str | None = None
    encryption_key: bytes | None = None  # Decrypted at runtime


@dataclass
class Identity:
    """Who am I in the commons?"""

    user_id: str
    signing_key_path: Path | None = None  # Ed25519 for non-repudiation
    bindings: list[CircleBinding] = field(default_factory=list)


class Keyring:
    """The artifact of access - holds keys to cross membranes."""

    def __init__(self, identity: Identity):
        self._identity = identity
        self._bindings = {b.circle_id: b for b in identity.bindings}

    @property
    def identity(self) -> Identity:
        """Who am I?"""
        return self._identity

    @property
    def user_id(self) -> str:
        """Shortcut to identity.user_id."""
        return self._identity.user_id

    def can_cross(self, circle_id: str) -> bool:
        """Can I cross into this circle?"""
        return circle_id in self._bindings

    def get_binding(self, circle_id: str) -> CircleBinding | None:
        """Get the key to cross a specific membrane."""
        return self._bindings.get(circle_id)

    def is_local_only(self, circle_id: str) -> bool:
        """Does this circle stay on my machine?

        Returns True if:
        - We have no binding to the circle (unknown circles are local-only)
        - We have a binding with sync_policy="local-only"

        Returns False if:
        - We have a binding with sync_policy="cloud"
        """
        binding = self.get_binding(circle_id)
        if binding is None:
            return True
        return binding.sync_policy == "local-only"

    def get_default_circle(self) -> CircleBinding | None:
        """Get the default circle binding, if any."""
        for binding in self._identity.bindings:
            if binding.is_default:
                return binding
        return None

    def list_bindings(self) -> list[CircleBinding]:
        """List all circle bindings."""
        return list(self._bindings.values())

    def list_cloud_circles(self) -> list[CircleBinding]:
        """List circles that sync to cloud."""
        return [b for b in self._bindings.values() if b.sync_policy == "cloud"]

    def list_local_circles(self) -> list[CircleBinding]:
        """List circles that stay local."""
        return [b for b in self._bindings.values() if b.sync_policy == "local-only"]

    def add_binding(self, binding: CircleBinding) -> None:
        """Add a circle binding to the keyring."""
        self._bindings[binding.circle_id] = binding
        # Keep identity.bindings in sync
        self._identity.bindings.append(binding)


def load_keyring(path: Path | str | None = None) -> Keyring:
    """Load identity and bindings from keyring.toml.

    The keyring file format:

    ```toml
    [user]
    id = "victor"
    signing_key_path = "~/.ssh/id_ed25519"

    [[circles]]
    id = "circle-victor"
    sync_policy = "local-only"
    is_default = true

    [[circles]]
    id = "circle-chora"
    sync_policy = "cloud"
    cloud_workspace = "ws-chora-shared"
    cloud_url = "wss://sync.chora.dev/v1/room/xyz"
    ```
    """
    if path is None:
        path = Path.home() / ".chora" / "keyring.toml"
    else:
        path = Path(path)

    if not path.exists():
        # Return empty keyring with anonymous identity
        return Keyring(Identity(user_id="anonymous"))

    with open(path, "rb") as f:
        data = tomllib.load(f)

    # Parse user section
    user_data = data.get("user", {})
    user_id = user_data.get("id", "anonymous")
    signing_key_path = user_data.get("signing_key_path")
    if signing_key_path:
        signing_key_path = Path(signing_key_path).expanduser()

    # Parse circles section
    bindings = []
    for circle_data in data.get("circles", []):
        binding = CircleBinding(
            circle_id=circle_data["id"],
            sync_policy=circle_data.get("sync_policy", "local-only"),
            is_default=circle_data.get("is_default", False),
            cloud_workspace=circle_data.get("cloud_workspace"),
            cloud_url=circle_data.get("cloud_url"),
        )
        bindings.append(binding)

    identity = Identity(
        user_id=user_id,
        signing_key_path=signing_key_path,
        bindings=bindings,
    )

    return Keyring(identity)


def create_keyring(
    user_id: str,
    bindings: list[CircleBinding] | None = None,
    signing_key_path: Path | str | None = None,
) -> Keyring:
    """Create a keyring programmatically (for testing or initialization)."""
    if signing_key_path:
        signing_key_path = Path(signing_key_path).expanduser()

    identity = Identity(
        user_id=user_id,
        signing_key_path=signing_key_path,
        bindings=bindings or [],
    )

    return Keyring(identity)


def save_keyring(keyring: Keyring, path: Path | str | None = None) -> None:
    """Save keyring to a TOML file.

    Args:
        keyring: The keyring to save
        path: Target path (defaults to ~/.chora/keyring.toml)
    """
    if path is None:
        path = Path.home() / ".chora" / "keyring.toml"
    else:
        path = Path(path)

    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # Build TOML content manually
    lines = ["[user]", f'id = "{keyring.user_id}"']

    if keyring.identity.signing_key_path:
        lines.append(f'signing_key_path = "{keyring.identity.signing_key_path}"')

    lines.append("")  # Blank line before circles

    for binding in keyring.list_bindings():
        lines.append("[[circles]]")
        lines.append(f'id = "{binding.circle_id}"')
        lines.append(f'sync_policy = "{binding.sync_policy}"')
        if binding.is_default:
            lines.append("is_default = true")
        if binding.cloud_workspace:
            lines.append(f'cloud_workspace = "{binding.cloud_workspace}"')
        if binding.cloud_url:
            lines.append(f'cloud_url = "{binding.cloud_url}"')
        lines.append("")  # Blank line between circles

    content = "\n".join(lines)
    path.write_text(content)
