# Feature: Invitation Flow
# Story: story-invite-collaborator-to-circle
# Principle: principle-github-ssh-keys-enable-zero-friction-invitation
#
# Zero-friction invitation via GitHub SSH keys.
# Circle keys are encrypted for recipients and committed to git.

Feature: Invitation Flow
  As a circle steward
  I want to invite collaborators using their GitHub SSH keys
  So that onboarding requires no pre-shared secrets

  # Behavior: behavior-encrypt-circle-key-for-recipient
  @behavior:encrypt-circle-key-for-recipient
  Scenario: Create invitation with public key
    Given a circle "shared-work" with a symmetric key
    And a recipient Ed25519 public key
    When I create an invitation for "alice" to "shared-work"
    Then an invitation is created with the username "alice"
    And the invitation has the circle_id "shared-work"
    And the encrypted_key is non-empty

  @behavior:encrypt-circle-key-for-recipient
  Scenario: Save invitation to file
    Given an invitation for "bob" to "circle-team"
    When I save the invitation to the access directory
    Then a file exists at "circle-team/bob.enc"
    And the file contains valid JSON with version 1

  @behavior:encrypt-circle-key-for-recipient
  Scenario: Load invitation from file
    Given an invitation file at "circle-project/carol.enc"
    When I load the invitation from file
    Then the invitation has username "carol"
    And the invitation has circle_id "circle-project"

  # Behavior: behavior-decrypt-invitation-with-local-ssh-key
  @behavior:decrypt-invitation-with-local-ssh-key
  Scenario: Decrypt invitation with matching key
    Given a keypair for encryption testing
    And an invitation encrypted for that keypair
    When I decrypt the invitation with the private key
    Then I receive the original circle key

  @behavior:decrypt-invitation-with-local-ssh-key
  Scenario: Full roundtrip - create and decrypt invitation
    Given a freshly generated keypair
    And a random circle key
    When I create an invitation with that public key
    And I decrypt the invitation with the private key
    Then the decrypted key matches the original

  # Error handling
  Scenario: Decrypt with wrong key fails
    Given a keypair for encryption testing
    And an invitation encrypted for that keypair
    And a different keypair
    When I try to decrypt with the wrong private key
    Then decryption fails with an error

  # Behavior: behavior-list-circle-members-from-access-directory
  @behavior:list-circle-members-from-access-directory
  Scenario: List circle members from access directory
    Given invitations for "alice", "bob", "carol" in "circle-team"
    When I list members of "circle-team"
    Then the result contains "alice", "bob", "carol"

  @behavior:list-circle-members-from-access-directory
  Scenario: List members of empty circle returns empty list
    Given no invitations in "circle-empty"
    When I list members of "circle-empty"
    Then the result is empty

  # Behavior: behavior-fetch-ssh-public-key-from-github
  @behavior:fetch-ssh-public-key-from-github
  Scenario: Fetch SSH public key from GitHub API (mocked)
    Given a mock GitHub API that returns Ed25519 keys for "testuser"
    When I fetch SSH keys for GitHub user "testuser"
    Then I receive at least 1 public key
    And the keys are in valid SSH format

  @behavior:fetch-ssh-public-key-from-github
  Scenario: GitHub API returns 404 for unknown user
    Given a mock GitHub API that returns 404 for "unknownuser"
    When I fetch SSH keys for GitHub user "unknownuser"
    Then an error is returned indicating user not found

  # Behavior: behavior-invite-fetches-github-ssh-key-and-encrypts-circle-key
  # This is the high-level invite flow combining key fetching and encryption
  @behavior:invite-fetches-github-ssh-key-and-encrypts-circle-key
  Scenario: Full invite flow fetches GitHub key and encrypts circle key
    Given a circle "shared-work" with a symmetric key
    And a mock GitHub API that returns Ed25519 keys for "collaborator"
    When I fetch SSH keys for GitHub user "collaborator"
    And I create an invitation for "collaborator" to "shared-work"
    Then an invitation is created with the username "collaborator"
    And the encrypted_key is non-empty
