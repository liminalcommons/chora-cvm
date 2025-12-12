# Feature: Keyring
# Story: story-dweller-has-local-keyring
# Principle: principle-identity-lives-in-keyring
#
# The Keyring holds crystallized capability to cross boundaries.
# It answers: "Which membranes can I cross?"
# This is not configuration - it is Situatedness.

Feature: Keyring
  As a dweller in the commons
  I want a local keyring that stores my identity and circle bindings
  So that I can cross circle membranes and route sync decisions

  # Behavior: behavior-keyring-loads-identity-from-file
  @behavior:keyring-loads-identity-from-file
  Scenario: Load existing keyring
    Given a keyring file exists with user_id "victor"
    When I call load_keyring with that path
    Then the keyring contains identity with user_id "victor"

  @behavior:keyring-loads-identity-from-file
  Scenario: Load keyring with signing key path
    Given a keyring file exists with user_id "alice" and signing_key_path "~/.ssh/id_ed25519"
    When I call load_keyring with that path
    Then the keyring contains identity with user_id "alice"
    And the signing_key_path is set

  @behavior:keyring-loads-identity-from-file
  Scenario: Missing keyring file returns anonymous identity
    Given no keyring file exists at the path
    When I call load_keyring with that path
    Then the keyring contains identity with user_id "anonymous"

  # Behavior: behavior-keyring-lists-accessible-circles
  @behavior:keyring-lists-accessible-circles
  Scenario: Keyring contains circle bindings
    Given a keyring file with 2 circle bindings
    When I call load_keyring with that path
    Then the keyring has 2 bindings

  @behavior:keyring-lists-accessible-circles
  Scenario: List cloud circles
    Given a keyring with 2 local-only and 1 cloud circle binding
    When I call list_cloud_circles on the keyring
    Then only 1 circle is returned

  @behavior:keyring-lists-accessible-circles
  Scenario: List local circles
    Given a keyring with 2 local-only and 1 cloud circle binding
    When I call list_local_circles on the keyring
    Then 2 circles are returned

  @behavior:keyring-lists-accessible-circles
  Scenario: Check can_cross for bound circle
    Given a keyring with binding for "circle-research"
    When I call can_cross for "circle-research"
    Then the result is true

  @behavior:keyring-lists-accessible-circles
  Scenario: Check can_cross for unbound circle
    Given a keyring with binding for "circle-research"
    When I call can_cross for "circle-unknown"
    Then the result is false

  # Sync policy from keyring
  Scenario: Check is_local_only for local binding
    Given a keyring with binding for "circle-research" with sync_policy "local-only"
    When I call is_local_only for "circle-research" on the keyring
    Then the result is true

  Scenario: Check is_local_only for cloud binding
    Given a keyring with binding for "circle-shared" with sync_policy "cloud"
    When I call is_local_only for "circle-shared" on the keyring
    Then the result is false

  Scenario: Check is_local_only for unknown circle defaults to true
    Given a keyring with no binding for "circle-unknown"
    When I call is_local_only for "circle-unknown" on the keyring
    Then the result is true

  # Creating and saving keyring
  Scenario: Create keyring programmatically
    When I call create_keyring with user_id "bob"
    Then the keyring contains identity with user_id "bob"
    And the keyring has 0 bindings

  Scenario: Add circle binding to keyring
    Given a keyring with user_id "carol"
    When I add a binding for "circle-work" with sync_policy "cloud"
    Then the keyring has binding for "circle-work"
    And is_local_only returns false for "circle-work"

  Scenario: Save and reload keyring
    Given a keyring with user_id "dave" and binding for "circle-team"
    When I save the keyring to a file
    And I load the keyring from that file
    Then the keyring contains identity with user_id "dave"
    And the keyring has binding for "circle-team"

  # Default circle
  Scenario: Get default circle binding
    Given a keyring with 2 bindings where one is default
    When I call get_default_circle
    Then the default circle binding is returned

  # Behavior: behavior-keyring-stores-circle-encryption-keys-securely
  @behavior:keyring-stores-circle-encryption-keys-securely
  Scenario: Circle binding can store encryption key
    Given a keyring with user_id "victor"
    When I add a binding for "circle-secure" with an encryption key
    Then the keyring has binding for "circle-secure"
    And the binding contains the encryption key

  @behavior:keyring-stores-circle-encryption-keys-securely
  Scenario: Encryption key is not stored in plaintext in file
    Given a keyring with user_id "eve" and an encrypted circle key
    When I save the keyring to a file
    And I read the raw file contents
    Then the raw circle key is not visible in plaintext
