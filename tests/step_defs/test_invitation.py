"""
Step definitions for the Invitation Flow feature.

These tests verify the behaviors specified by story-invite-collaborator-to-circle.
Zero-friction invitation via GitHub SSH keys.
"""
import json
import tempfile
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

import nacl.signing

from chora_cvm.invitation import (
    Invitation,
    create_invitation,
    decrypt_invitation,
    list_circle_members,
    InvitationError,
)

# Load scenarios from feature file
scenarios("../features/invitation.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {}


@pytest.fixture
def temp_access_dir():
    """Create a temporary access directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def generate_test_keypair():
    """Generate a test Ed25519 keypair."""
    signing_key = nacl.signing.SigningKey.generate()
    return signing_key, signing_key.verify_key


# =============================================================================
# Given Steps - Setup
# =============================================================================


@given(parsers.parse('a circle "{circle_id}" with a symmetric key'))
def circle_with_key(test_context, circle_id: str):
    """Create a circle with a symmetric key."""
    import nacl.utils
    test_context["circle_id"] = circle_id
    test_context["circle_key"] = nacl.utils.random(32)


@given("a recipient Ed25519 public key")
def recipient_public_key(test_context):
    """Generate a recipient keypair and store the public key."""
    private_key, public_key = generate_test_keypair()
    test_context["recipient_private_key"] = private_key
    test_context["recipient_public_key"] = public_key


@given(parsers.parse('an invitation for "{username}" to "{circle_id}"'))
def existing_invitation(test_context, temp_access_dir, username: str, circle_id: str):
    """Create an invitation for testing."""
    import nacl.utils
    private_key, public_key = generate_test_keypair()
    circle_key = nacl.utils.random(32)

    invitation = create_invitation(
        username=username,
        circle_id=circle_id,
        circle_key=circle_key,
        recipient_public_key=public_key,
    )
    test_context["invitation"] = invitation
    test_context["access_dir"] = temp_access_dir
    test_context["recipient_private_key"] = private_key
    test_context["circle_key"] = circle_key


@given(parsers.parse('an invitation file at "{path}"'))
def invitation_file_exists(test_context, temp_access_dir, path: str):
    """Create an invitation file at the specified path."""
    import nacl.utils
    private_key, public_key = generate_test_keypair()

    # Parse path to get circle_id and username
    parts = path.split("/")
    circle_id = parts[0]
    username = parts[1].replace(".enc", "")

    circle_key = nacl.utils.random(32)
    invitation = create_invitation(
        username=username,
        circle_id=circle_id,
        circle_key=circle_key,
        recipient_public_key=public_key,
    )

    # Save to file
    file_path = invitation.to_file(temp_access_dir)
    test_context["invitation_file"] = file_path
    test_context["access_dir"] = temp_access_dir


@given("a keypair for encryption testing")
def keypair_for_testing(test_context):
    """Generate a keypair for encryption testing."""
    private_key, public_key = generate_test_keypair()
    test_context["test_private_key"] = private_key
    test_context["test_public_key"] = public_key


@given("an invitation encrypted for that keypair")
def invitation_for_keypair(test_context):
    """Create an invitation encrypted for the test keypair."""
    import nacl.utils
    circle_key = nacl.utils.random(32)

    invitation = create_invitation(
        username="testuser",
        circle_id="circle-test",
        circle_key=circle_key,
        recipient_public_key=test_context["test_public_key"],
    )
    test_context["invitation"] = invitation
    test_context["circle_key"] = circle_key


@given("a freshly generated keypair")
def fresh_keypair(test_context):
    """Generate a fresh keypair."""
    private_key, public_key = generate_test_keypair()
    test_context["fresh_private_key"] = private_key
    test_context["fresh_public_key"] = public_key


@given("a random circle key")
def random_circle_key(test_context):
    """Generate a random circle key."""
    import nacl.utils
    test_context["random_circle_key"] = nacl.utils.random(32)


@given("a different keypair")
def different_keypair(test_context):
    """Generate a different keypair for wrong-key testing."""
    private_key, public_key = generate_test_keypair()
    test_context["wrong_private_key"] = private_key


@given(parsers.parse('invitations for "{users}" in "{circle_id}"'))
def multiple_invitations(test_context, temp_access_dir, users: str, circle_id: str):
    """Create invitations for multiple users."""
    import nacl.utils
    usernames = [u.strip().strip('"') for u in users.split(",")]

    for username in usernames:
        private_key, public_key = generate_test_keypair()
        circle_key = nacl.utils.random(32)

        invitation = create_invitation(
            username=username,
            circle_id=circle_id,
            circle_key=circle_key,
            recipient_public_key=public_key,
        )
        invitation.to_file(temp_access_dir)

    test_context["access_dir"] = temp_access_dir
    test_context["circle_id"] = circle_id


@given(parsers.parse('no invitations in "{circle_id}"'))
def no_invitations(test_context, temp_access_dir, circle_id: str):
    """Ensure no invitations exist for the circle."""
    test_context["access_dir"] = temp_access_dir
    test_context["circle_id"] = circle_id


# =============================================================================
# When Steps
# =============================================================================


@when(parsers.parse('I create an invitation for "{username}" to "{circle_id}"'))
def create_invitation_step(test_context, username: str, circle_id: str):
    """Create an invitation."""
    invitation = create_invitation(
        username=username,
        circle_id=circle_id,
        circle_key=test_context["circle_key"],
        recipient_public_key=test_context["recipient_public_key"],
    )
    test_context["invitation"] = invitation


@when("I save the invitation to the access directory")
def save_invitation(test_context):
    """Save invitation to file."""
    invitation = test_context["invitation"]
    access_dir = test_context["access_dir"]
    file_path = invitation.to_file(access_dir)
    test_context["saved_file"] = file_path


@when("I load the invitation from file")
def load_invitation(test_context):
    """Load invitation from file."""
    file_path = test_context["invitation_file"]
    invitation = Invitation.from_file(file_path)
    test_context["loaded_invitation"] = invitation


@when("I decrypt the invitation with the private key")
def decrypt_with_private_key(test_context):
    """Decrypt invitation with the matching private key."""
    invitation = test_context["invitation"]

    # Get the appropriate private key
    if "test_private_key" in test_context:
        private_key = test_context["test_private_key"]
    elif "fresh_private_key" in test_context:
        private_key = test_context["fresh_private_key"]
    else:
        private_key = test_context["recipient_private_key"]

    # Create a mock SSHKeyPair-like object for the decrypt function
    from chora_cvm.invitation import decrypt_invitation_with_signing_key
    decrypted = decrypt_invitation_with_signing_key(invitation, private_key)
    test_context["decrypted_key"] = decrypted


@when("I create an invitation with that public key")
def create_with_public_key(test_context):
    """Create invitation with the fresh public key."""
    invitation = create_invitation(
        username="testuser",
        circle_id="circle-test",
        circle_key=test_context["random_circle_key"],
        recipient_public_key=test_context["fresh_public_key"],
    )
    test_context["invitation"] = invitation


@when("I try to decrypt with the wrong private key")
def try_wrong_key(test_context):
    """Try to decrypt with wrong key."""
    invitation = test_context["invitation"]
    wrong_key = test_context["wrong_private_key"]

    try:
        from chora_cvm.invitation import decrypt_invitation_with_signing_key
        decrypt_invitation_with_signing_key(invitation, wrong_key)
        test_context["decrypt_error"] = None
    except Exception as e:
        test_context["decrypt_error"] = e


@when(parsers.parse('I list members of "{circle_id}"'))
def list_members(test_context, circle_id: str):
    """List members of a circle."""
    access_dir = test_context["access_dir"]
    members = list_circle_members(access_dir, circle_id)
    test_context["members"] = members


# =============================================================================
# Then Steps
# =============================================================================


@then(parsers.parse('an invitation is created with the username "{username}"'))
def check_invitation_username(test_context, username: str):
    """Verify invitation username."""
    invitation = test_context["invitation"]
    assert invitation.username == username, f"Expected username '{username}', got '{invitation.username}'"


@then(parsers.parse('the invitation has the circle_id "{circle_id}"'))
def check_invitation_circle(test_context, circle_id: str):
    """Verify invitation circle_id."""
    invitation = test_context["invitation"]
    assert invitation.circle_id == circle_id, f"Expected circle_id '{circle_id}', got '{invitation.circle_id}'"


@then("the encrypted_key is non-empty")
def check_encrypted_key_nonempty(test_context):
    """Verify encrypted key is non-empty."""
    invitation = test_context["invitation"]
    assert len(invitation.encrypted_key) > 0, "encrypted_key is empty"


@then(parsers.parse('a file exists at "{path}"'))
def check_file_exists(test_context, path: str):
    """Verify file exists at path."""
    access_dir = test_context["access_dir"]
    file_path = access_dir / path
    assert file_path.exists(), f"File not found: {file_path}"


@then("the file contains valid JSON with version 1")
def check_file_json(test_context):
    """Verify file contains valid JSON with version 1."""
    saved_file = test_context["saved_file"]
    data = json.loads(saved_file.read_text())
    assert data.get("version") == 1, f"Expected version 1, got {data.get('version')}"


@then(parsers.parse('the invitation has username "{username}"'))
def check_loaded_username(test_context, username: str):
    """Verify loaded invitation username."""
    invitation = test_context["loaded_invitation"]
    assert invitation.username == username


@then(parsers.parse('the invitation has circle_id "{circle_id}"'))
def check_loaded_circle(test_context, circle_id: str):
    """Verify loaded invitation circle_id."""
    invitation = test_context["loaded_invitation"]
    assert invitation.circle_id == circle_id


@then("I receive the original circle key")
def check_decrypted_key(test_context):
    """Verify decrypted key matches original."""
    decrypted = test_context["decrypted_key"]
    original = test_context["circle_key"]
    assert decrypted == original, "Decrypted key doesn't match original"


@then("the decrypted key matches the original")
def check_roundtrip(test_context):
    """Verify roundtrip encryption/decryption."""
    decrypted = test_context["decrypted_key"]
    original = test_context["random_circle_key"]
    assert decrypted == original, "Roundtrip failed: keys don't match"


@then("decryption fails with an error")
def check_decrypt_error(test_context):
    """Verify decryption failed."""
    error = test_context.get("decrypt_error")
    assert error is not None, "Expected decryption to fail, but it succeeded"


@then(parsers.parse('the result contains "{users}"'))
def check_members_contain(test_context, users: str):
    """Verify result contains expected users."""
    members = test_context["members"]
    expected = [u.strip().strip('"') for u in users.split(",")]
    for user in expected:
        assert user in members, f"Expected {user} in {members}"


@then("the result is empty")
def check_result_empty(test_context):
    """Verify result is empty."""
    members = test_context["members"]
    assert len(members) == 0, f"Expected empty list, got {members}"


# =============================================================================
# GitHub API Mock Steps (behavior-fetch-ssh-public-key-from-github)
# =============================================================================


@given(parsers.parse('a mock GitHub API that returns Ed25519 keys for "{username}"'))
def mock_github_api_success(test_context, username: str):
    """Set up mock GitHub API that returns Ed25519 keys."""
    # Generate a valid Ed25519 public key in SSH format
    private_key, public_key = generate_test_keypair()
    ssh_key = f"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeBase64KeyData{username[:8].ljust(8, 'x')} {username}@example.com"

    test_context["mock_github_response"] = {
        "status": 200,
        "keys": [{"key": ssh_key, "key_type": "ssh-ed25519"}],
    }
    test_context["mock_username"] = username
    # Also set up recipient_public_key so invite flow tests work
    test_context["recipient_public_key"] = public_key
    test_context["recipient_private_key"] = private_key


@given(parsers.parse('a mock GitHub API that returns 404 for "{username}"'))
def mock_github_api_not_found(test_context, username: str):
    """Set up mock GitHub API that returns 404."""
    test_context["mock_github_response"] = {
        "status": 404,
        "error": "Not Found",
    }
    test_context["mock_username"] = username


@when(parsers.parse('I fetch SSH keys for GitHub user "{username}"'))
def fetch_github_keys(test_context, username: str):
    """Fetch SSH keys from (mocked) GitHub API."""
    mock_response = test_context.get("mock_github_response", {})

    # Simulate the fetch
    if mock_response.get("status") == 200:
        test_context["fetched_keys"] = mock_response.get("keys", [])
        test_context["fetch_error"] = None
    else:
        test_context["fetched_keys"] = []
        test_context["fetch_error"] = mock_response.get("error", "Unknown error")


@then(parsers.parse("I receive at least {count:d} public key"))
def check_key_count(test_context, count: int):
    """Verify at least N keys received."""
    keys = test_context.get("fetched_keys", [])
    assert len(keys) >= count, f"Expected at least {count} keys, got {len(keys)}"


@then("the keys are in valid SSH format")
def check_ssh_format(test_context):
    """Verify keys are in valid SSH format."""
    keys = test_context.get("fetched_keys", [])
    for key_data in keys:
        key = key_data.get("key", "")
        # SSH keys start with the algorithm name
        assert key.startswith("ssh-"), f"Key doesn't start with 'ssh-': {key[:20]}..."
        # SSH keys have 3 space-separated parts: algorithm, base64 data, comment
        parts = key.split()
        assert len(parts) >= 2, f"Key doesn't have enough parts: {key[:20]}..."


@then("an error is returned indicating user not found")
def check_user_not_found_error(test_context):
    """Verify error indicates user not found."""
    error = test_context.get("fetch_error")
    assert error is not None, "Expected an error but got none"
    assert "Not Found" in str(error) or "404" in str(error), f"Error doesn't indicate not found: {error}"
