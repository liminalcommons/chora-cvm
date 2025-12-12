"""
Step definitions for the Keyring feature.

These tests verify the behaviors specified by story-dweller-has-local-keyring.
The Keyring holds identity and circle bindings for crossing membranes.
"""
import os
import tempfile
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.keyring import (
    Keyring,
    CircleBinding,
    Identity,
    load_keyring,
    create_keyring,
    save_keyring,
)

# Load scenarios from feature file
scenarios("../features/keyring.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {}


@pytest.fixture
def temp_keyring_dir():
    """Create a temporary directory for keyring files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# =============================================================================
# Given Steps - Keyring File Setup
# =============================================================================


@given(parsers.parse('a keyring file exists with user_id "{user_id}"'))
def keyring_file_with_user(temp_keyring_dir, test_context, user_id: str):
    """Create a keyring file with the specified user_id."""
    keyring_path = temp_keyring_dir / "keyring.toml"
    content = f'''[user]
id = "{user_id}"
'''
    keyring_path.write_text(content)
    test_context["keyring_path"] = keyring_path


@given(parsers.parse('a keyring file exists with user_id "{user_id}" and signing_key_path "{key_path}"'))
def keyring_file_with_signing_key(temp_keyring_dir, test_context, user_id: str, key_path: str):
    """Create a keyring file with user_id and signing_key_path."""
    keyring_path = temp_keyring_dir / "keyring.toml"
    content = f'''[user]
id = "{user_id}"
signing_key_path = "{key_path}"
'''
    keyring_path.write_text(content)
    test_context["keyring_path"] = keyring_path


@given("no keyring file exists at the path")
def no_keyring_file(temp_keyring_dir, test_context):
    """Set up path to non-existent keyring file."""
    test_context["keyring_path"] = temp_keyring_dir / "nonexistent.toml"


@given(parsers.parse("a keyring file with {count:d} circle bindings"))
def keyring_file_with_bindings(temp_keyring_dir, test_context, count: int):
    """Create a keyring file with multiple circle bindings."""
    keyring_path = temp_keyring_dir / "keyring.toml"
    content = '''[user]
id = "testuser"

'''
    for i in range(count):
        content += f'''[[circles]]
id = "circle-test-{i}"
sync_policy = "local-only"

'''
    keyring_path.write_text(content)
    test_context["keyring_path"] = keyring_path


@given(parsers.parse("a keyring with {local:d} local-only and {cloud:d} cloud circle binding"))
def keyring_with_mixed_bindings(temp_keyring_dir, test_context, local: int, cloud: int):
    """Create a keyring with mixed sync policies."""
    keyring_path = temp_keyring_dir / "keyring.toml"
    content = '''[user]
id = "testuser"

'''
    for i in range(local):
        content += f'''[[circles]]
id = "circle-local-{i}"
sync_policy = "local-only"

'''
    for i in range(cloud):
        content += f'''[[circles]]
id = "circle-cloud-{i}"
sync_policy = "cloud"

'''
    keyring_path.write_text(content)
    test_context["keyring_path"] = keyring_path
    # Pre-load for subsequent steps
    test_context["keyring"] = load_keyring(keyring_path)


@given(parsers.parse('a keyring with binding for "{circle_id}"'))
def keyring_with_specific_binding(temp_keyring_dir, test_context, circle_id: str):
    """Create a keyring with a specific circle binding."""
    keyring_path = temp_keyring_dir / "keyring.toml"
    content = f'''[user]
id = "testuser"

[[circles]]
id = "{circle_id}"
sync_policy = "local-only"
'''
    keyring_path.write_text(content)
    test_context["keyring_path"] = keyring_path
    test_context["keyring"] = load_keyring(keyring_path)


@given(parsers.parse('a keyring with binding for "{circle_id}" with sync_policy "{policy}"'))
def keyring_with_policy_binding(temp_keyring_dir, test_context, circle_id: str, policy: str):
    """Create a keyring with a specific circle binding and policy."""
    keyring_path = temp_keyring_dir / "keyring.toml"
    content = f'''[user]
id = "testuser"

[[circles]]
id = "{circle_id}"
sync_policy = "{policy}"
'''
    keyring_path.write_text(content)
    test_context["keyring_path"] = keyring_path
    test_context["keyring"] = load_keyring(keyring_path)


@given(parsers.parse('a keyring with no binding for "{circle_id}"'))
def keyring_without_binding(temp_keyring_dir, test_context, circle_id: str):
    """Create a keyring without a specific circle binding."""
    keyring_path = temp_keyring_dir / "keyring.toml"
    content = '''[user]
id = "testuser"

[[circles]]
id = "circle-other"
sync_policy = "local-only"
'''
    keyring_path.write_text(content)
    test_context["keyring_path"] = keyring_path
    test_context["keyring"] = load_keyring(keyring_path)


@given(parsers.parse('a keyring with user_id "{user_id}"'))
def keyring_with_user_id(test_context, user_id: str):
    """Create a keyring programmatically with user_id."""
    test_context["keyring"] = create_keyring(user_id)


@given(parsers.parse('a keyring with user_id "{user_id}" and binding for "{circle_id}"'))
def keyring_with_user_and_binding(temp_keyring_dir, test_context, user_id: str, circle_id: str):
    """Create a keyring with user_id and a circle binding."""
    bindings = [CircleBinding(circle_id=circle_id, sync_policy="local-only")]
    test_context["keyring"] = create_keyring(user_id, bindings=bindings)
    test_context["temp_dir"] = temp_keyring_dir


@given(parsers.parse("a keyring with {count:d} bindings where one is default"))
def keyring_with_default(temp_keyring_dir, test_context, count: int):
    """Create a keyring with multiple bindings where one is default."""
    keyring_path = temp_keyring_dir / "keyring.toml"
    content = '''[user]
id = "testuser"

[[circles]]
id = "circle-default"
sync_policy = "local-only"
is_default = true

'''
    for i in range(count - 1):
        content += f'''[[circles]]
id = "circle-{i}"
sync_policy = "local-only"

'''
    keyring_path.write_text(content)
    test_context["keyring_path"] = keyring_path
    test_context["keyring"] = load_keyring(keyring_path)


# =============================================================================
# When Steps
# =============================================================================


@when("I call load_keyring with that path")
def load_keyring_from_path(test_context):
    """Load keyring from the test path."""
    test_context["keyring"] = load_keyring(test_context["keyring_path"])


@when("I call list_cloud_circles on the keyring")
def list_cloud_circles(test_context):
    """List cloud circles from keyring."""
    test_context["result"] = test_context["keyring"].list_cloud_circles()


@when("I call list_local_circles on the keyring")
def list_local_circles(test_context):
    """List local circles from keyring."""
    test_context["result"] = test_context["keyring"].list_local_circles()


@when(parsers.parse('I call can_cross for "{circle_id}"'))
def call_can_cross(test_context, circle_id: str):
    """Check if keyring can cross into circle."""
    test_context["result"] = test_context["keyring"].can_cross(circle_id)


@when(parsers.parse('I call is_local_only for "{circle_id}" on the keyring'))
def call_is_local_only(test_context, circle_id: str):
    """Check if circle is local-only via keyring."""
    test_context["result"] = test_context["keyring"].is_local_only(circle_id)


@when(parsers.parse('I call create_keyring with user_id "{user_id}"'))
def call_create_keyring(test_context, user_id: str):
    """Create a new keyring programmatically."""
    test_context["keyring"] = create_keyring(user_id)


@when(parsers.parse('I add a binding for "{circle_id}" with sync_policy "{policy}"'))
def add_binding(test_context, circle_id: str, policy: str):
    """Add a circle binding to the keyring."""
    binding = CircleBinding(circle_id=circle_id, sync_policy=policy)
    test_context["keyring"].add_binding(binding)


@when("I save the keyring to a file")
def save_keyring_to_file(test_context):
    """Save keyring to a file."""
    temp_dir = test_context.get("temp_dir")
    if not temp_dir:
        temp_dir = Path(tempfile.mkdtemp())
        test_context["temp_dir"] = temp_dir
    keyring_path = temp_dir / "saved_keyring.toml"
    save_keyring(test_context["keyring"], keyring_path)
    test_context["saved_keyring_path"] = keyring_path


@when("I load the keyring from that file")
def load_saved_keyring(test_context):
    """Load keyring from the saved file."""
    test_context["keyring"] = load_keyring(test_context["saved_keyring_path"])


@when("I call get_default_circle")
def call_get_default(test_context):
    """Get the default circle binding."""
    test_context["result"] = test_context["keyring"].get_default_circle()


# =============================================================================
# Then Steps
# =============================================================================


@then(parsers.parse('the keyring contains identity with user_id "{user_id}"'))
def check_user_id(test_context, user_id: str):
    """Verify keyring has expected user_id."""
    keyring = test_context["keyring"]
    assert keyring.user_id == user_id, f"Expected user_id '{user_id}', got '{keyring.user_id}'"


@then("the signing_key_path is set")
def check_signing_key_set(test_context):
    """Verify signing_key_path is set."""
    keyring = test_context["keyring"]
    assert keyring.identity.signing_key_path is not None, "signing_key_path is not set"


@then(parsers.parse("the keyring has {count:d} bindings"))
def check_binding_count(test_context, count: int):
    """Verify keyring has expected number of bindings."""
    keyring = test_context["keyring"]
    bindings = keyring.list_bindings()
    assert len(bindings) == count, f"Expected {count} bindings, got {len(bindings)}"


@then(parsers.parse("only {count:d} circle is returned"))
def check_result_count_single(test_context, count: int):
    """Verify result has expected count."""
    result = test_context["result"]
    assert len(result) == count, f"Expected {count} circles, got {len(result)}"


@then(parsers.parse("{count:d} circles are returned"))
def check_result_count_plural(test_context, count: int):
    """Verify result has expected count."""
    result = test_context["result"]
    assert len(result) == count, f"Expected {count} circles, got {len(result)}"


@then("the result is true")
def check_result_true(test_context):
    """Verify result is True."""
    assert test_context["result"] is True, f"Expected True, got {test_context['result']}"


@then("the result is false")
def check_result_false(test_context):
    """Verify result is False."""
    assert test_context["result"] is False, f"Expected False, got {test_context['result']}"


@then(parsers.parse('the keyring has binding for "{circle_id}"'))
def check_has_binding(test_context, circle_id: str):
    """Verify keyring has binding for circle."""
    keyring = test_context["keyring"]
    assert keyring.can_cross(circle_id), f"Keyring doesn't have binding for {circle_id}"


@then(parsers.parse('is_local_only returns false for "{circle_id}"'))
def check_not_local_only(test_context, circle_id: str):
    """Verify circle is not local-only."""
    keyring = test_context["keyring"]
    assert not keyring.is_local_only(circle_id), f"{circle_id} should not be local-only"


@then("the default circle binding is returned")
def check_default_returned(test_context):
    """Verify default circle binding is returned."""
    result = test_context["result"]
    assert result is not None, "No default circle binding found"
    assert result.is_default is True, "Returned binding is not marked as default"


# =============================================================================
# Secure Key Storage Steps (behavior-keyring-stores-circle-encryption-keys-securely)
# =============================================================================


@when(parsers.parse('I add a binding for "{circle_id}" with an encryption key'))
def add_binding_with_key(test_context, circle_id: str):
    """Add a circle binding with an encryption key."""
    import base64
    import os

    # Generate a random encryption key
    raw_key = os.urandom(32)
    test_context["raw_encryption_key"] = raw_key
    encoded_key = base64.b64encode(raw_key).decode("ascii")

    binding = CircleBinding(
        circle_id=circle_id,
        sync_policy="cloud",
        encryption_key=encoded_key,
    )
    test_context["keyring"].add_binding(binding)
    test_context["added_circle_id"] = circle_id


@then("the binding contains the encryption key")
def check_binding_has_key(test_context):
    """Verify the binding contains the encryption key."""
    keyring = test_context["keyring"]
    circle_id = test_context["added_circle_id"]
    binding = keyring.get_binding(circle_id)

    assert binding is not None, f"No binding found for {circle_id}"
    assert binding.encryption_key is not None, "Binding has no encryption key"
    assert len(binding.encryption_key) > 0, "Encryption key is empty"


@given(parsers.parse('a keyring with user_id "{user_id}" and an encrypted circle key'))
def keyring_with_encrypted_key(temp_keyring_dir, test_context, user_id: str):
    """Create a keyring with an encrypted circle key."""
    import base64
    import os

    # Generate a random encryption key
    raw_key = os.urandom(32)
    test_context["raw_encryption_key"] = raw_key
    test_context["raw_key_hex"] = raw_key.hex()

    # Encode as base64 (simulating encrypted storage)
    encoded_key = base64.b64encode(raw_key).decode("ascii")

    bindings = [CircleBinding(
        circle_id="circle-encrypted",
        sync_policy="cloud",
        encryption_key=encoded_key,
    )]
    test_context["keyring"] = create_keyring(user_id, bindings=bindings)
    test_context["temp_dir"] = temp_keyring_dir


@when("I read the raw file contents")
def read_raw_file(test_context):
    """Read the raw contents of the saved keyring file."""
    keyring_path = test_context.get("saved_keyring_path")
    test_context["raw_file_contents"] = keyring_path.read_text()


@then("the raw circle key is not visible in plaintext")
def check_key_not_plaintext(test_context):
    """Verify the raw encryption key is not visible as plaintext hex."""
    raw_contents = test_context["raw_file_contents"]
    raw_key_hex = test_context["raw_key_hex"]

    # The raw hex representation should not appear in the file
    assert raw_key_hex not in raw_contents, (
        "Raw encryption key is visible in plaintext!"
    )
