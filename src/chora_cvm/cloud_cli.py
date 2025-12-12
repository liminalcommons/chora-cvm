"""
CLI for circle invitation and sync operations.

Commands:
    python -m chora_cvm.cloud_cli invite <username> [circle_id]
    python -m chora_cvm.cloud_cli arrive
    python -m chora_cvm.cloud_cli members [circle_id]

The git repository becomes the keychain. Access flows through git.

Design principles:
    - One command to invite (just invite bob)
    - One command to join (just arrive)
    - Stage only, no auto-commit (respect developer workflow)
    - Global keyring at ~/.chora/keyring.toml (identity transcends repos)
    - Offer to create SSH key if missing (warm arrival)
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from .invitation import (
    create_invitation,
    decrypt_invitation,
    fetch_github_ssh_key,
    get_default_access_dir,
    get_default_private_key,
    GitHubKeyNotFoundError,
    Invitation,
    InvitationError,
    list_circle_members,
)
from .keyring import (
    CircleBinding,
    load_keyring,
    save_keyring,
)

# Lazy import chora_crypto to allow graceful error if not available
try:
    from chora_crypto.ssh_keys import parse_ssh_public_key, generate_circle_key
except ImportError:
    parse_ssh_public_key = None  # type: ignore
    generate_circle_key = None  # type: ignore


def get_current_circle() -> str | None:
    """
    Get current circle from .chora/circle.json.

    Returns:
        Circle ID or None if no circle configured
    """
    circle_config = Path.cwd() / ".chora" / "circle.json"
    if not circle_config.exists():
        return None

    try:
        data = json.loads(circle_config.read_text())
        return data.get("circle_id")
    except (json.JSONDecodeError, KeyError):
        return None


def ensure_ssh_key() -> Path | None:
    """
    Ensure SSH key exists, offering to create if missing.

    Returns:
        Path to SSH private key, or None if user declines
    """
    private_key_path = get_default_private_key()

    if private_key_path.exists():
        return private_key_path

    print("No SSH key found at ~/.ssh/id_ed25519")
    print()
    response = input("Would you like to generate a new Identity for Chora? [Y/n] ")

    if response.strip().lower() in ("", "y", "yes"):
        print()
        print("Generating Ed25519 SSH key...")
        print("(You may be prompted for a passphrase)")
        print()

        # Ensure .ssh directory exists
        ssh_dir = Path.home() / ".ssh"
        ssh_dir.mkdir(mode=0o700, exist_ok=True)

        # Run ssh-keygen interactively
        result = subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-f", str(private_key_path)],
        )

        if result.returncode == 0 and private_key_path.exists():
            print()
            print(f"SSH key created: {private_key_path}")
            return private_key_path
        else:
            print("SSH key generation failed or was cancelled.")
            return None
    else:
        print("SSH key is required for circle membership.")
        return None


def git_stage_file(file_path: Path) -> bool:
    """
    Stage a file with git add.

    Returns:
        True if successful
    """
    try:
        result = subprocess.run(
            ["git", "add", str(file_path)],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def add_github_collaborator(username: str) -> None:
    """
    Add user as GitHub repo collaborator via gh CLI.

    This is optional - continues silently if gh CLI not available.
    """
    try:
        result = subprocess.run(
            ["gh", "repo", "collaborator", "add", username, "--permission", "read"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print(f"  Added {username} as repo collaborator")
        elif "already a collaborator" in result.stderr.lower():
            pass  # Already a collaborator, fine
        else:
            # Log but don't fail - gh CLI is optional
            pass
    except FileNotFoundError:
        # gh CLI not installed - this is fine
        pass


def cmd_invite(username: str, circle_id: str | None = None) -> int:
    """
    Invite a GitHub user to the circle.

    Flow:
        1. Determine circle (from .chora/circle.json if not specified)
        2. Fetch recipient's GitHub SSH key
        3. Get or create circle key
        4. Create encrypted invitation
        5. Write to .chora/access/<circle-id>/<username>.enc
        6. Add as GitHub collaborator (optional)
        7. Git add (stage only)

    Args:
        username: GitHub username to invite
        circle_id: Circle to invite to (auto-detected if None)

    Returns:
        Exit code (0 = success)
    """
    if parse_ssh_public_key is None or generate_circle_key is None:
        print("Error: chora_crypto package not available.")
        print("Add packages/chora-crypto/src to PYTHONPATH.")
        return 1

    # 1. Determine circle
    if circle_id is None:
        circle_id = get_current_circle()
        if circle_id is None:
            print("Error: No circle configured.")
            print("Either specify circle_id or create .chora/circle.json")
            return 1

    print(f"Inviting {username} to {circle_id}...")

    # 2. Fetch recipient's GitHub SSH key
    try:
        print(f"  Fetching SSH key for {username}...")
        public_key_str = fetch_github_ssh_key(username)
        print("  Found Ed25519 key")
    except GitHubKeyNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except InvitationError as e:
        print(f"Error: {e}")
        return 1

    # Parse SSH public key string to VerifyKey
    try:
        recipient_public_key = parse_ssh_public_key(public_key_str)
    except Exception as e:
        print(f"Error parsing SSH key: {e}")
        return 1

    # 3. Get or create circle key
    keyring = load_keyring()  # Loads from ~/.chora/keyring.toml
    binding = keyring.get_binding(circle_id)

    if binding and binding.encryption_key:
        circle_key = binding.encryption_key
        print("  Using existing circle key")
    else:
        # Generate new circle key (owner is first citizen)
        circle_key = generate_circle_key()
        print("  Generated new circle key")

        # Add to owner's keyring
        new_binding = CircleBinding(
            circle_id=circle_id,
            sync_policy="cloud",
            encryption_key=circle_key,
        )
        keyring.add_binding(new_binding)
        save_keyring(keyring)
        print("  Added to keyring")

    # 4. Create encrypted invitation
    print("  Encrypting circle key...")
    invitation = create_invitation(
        username=username,
        circle_id=circle_id,
        circle_key=circle_key,
        recipient_public_key=recipient_public_key,
    )

    # 5. Write to access directory
    access_dir = get_default_access_dir()
    file_path = invitation.to_file(access_dir)
    print(f"  Created: {file_path}")

    # 6. Add as GitHub collaborator (optional)
    add_github_collaborator(username)

    # 7. Git add (stage only - respect developer workflow)
    if git_stage_file(file_path):
        print(f"  Staged: {file_path}")

    print()
    print(f"Invitation created. Run: git commit -m 'Invite {username}'")

    return 0


def cmd_arrive() -> int:
    """
    Accept pending invitations for current user.

    Scans all .enc files in .chora/access/ and attempts to decrypt
    each one with the local SSH private key. Successfully decrypted
    invitations are added to the global keyring.

    Returns:
        Exit code (0 = success)
    """
    # Ensure SSH key exists
    private_key_path = ensure_ssh_key()
    if private_key_path is None:
        return 1

    access_dir = get_default_access_dir()
    if not access_dir.exists():
        print("No access directory found. You may not have any pending invitations.")
        return 0

    keyring = load_keyring()
    joined_circles: list[str] = []

    # Scan all circles for invitations we can decrypt
    for circle_dir in access_dir.iterdir():
        if not circle_dir.is_dir():
            continue

        circle_id = circle_dir.name

        # Skip if already have this binding
        if keyring.get_binding(circle_id) is not None:
            continue

        # Try each .enc file
        for enc_file in circle_dir.glob("*.enc"):
            try:
                invitation = Invitation.from_file(enc_file)
                circle_key = decrypt_invitation(invitation, private_key_path)

                # Success! Add to keyring
                binding = CircleBinding(
                    circle_id=circle_id,
                    sync_policy="cloud",
                    encryption_key=circle_key,
                )
                keyring.add_binding(binding)
                joined_circles.append(circle_id)
                print(f"  Joined circle: {circle_id}")
                break  # Only need one successful decrypt per circle

            except Exception:
                # Not for us, or already processed - try next file
                continue

    if joined_circles:
        save_keyring(keyring)
        print()
        print(f"Joined {len(joined_circles)} circle(s). Keyring updated.")
    else:
        print("No new invitations found that match your key.")

    return 0


def cmd_members(circle_id: str | None = None) -> int:
    """
    List members of a circle.

    Args:
        circle_id: Circle to list (auto-detected if None)

    Returns:
        Exit code (0 = success)
    """
    if circle_id is None:
        circle_id = get_current_circle()
        if circle_id is None:
            print("Error: No circle configured.")
            print("Either specify circle_id or create .chora/circle.json")
            return 1

    access_dir = get_default_access_dir()
    members = list_circle_members(access_dir, circle_id)

    if not members:
        print(f"No members found for {circle_id}")
        return 0

    print(f"Members of {circle_id}:")
    for member in members:
        print(f"  {member}")

    return 0


def main() -> int:
    """CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: python -m chora_cvm.cloud_cli <command> [args]")
        print()
        print("Commands:")
        print("  invite <username> [circle_id]  - Invite GitHub user to circle")
        print("  arrive                         - Accept pending invitations")
        print("  members [circle_id]            - List circle members")
        return 1

    cmd = sys.argv[1]

    if cmd == "invite":
        if len(sys.argv) < 3:
            print("Usage: python -m chora_cvm.cloud_cli invite <username> [circle_id]")
            return 1
        username = sys.argv[2]
        circle_id = sys.argv[3] if len(sys.argv) > 3 else None
        return cmd_invite(username, circle_id)

    elif cmd == "arrive":
        return cmd_arrive()

    elif cmd == "members":
        circle_id = sys.argv[2] if len(sys.argv) > 2 else None
        return cmd_members(circle_id)

    else:
        print(f"Unknown command: {cmd}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
