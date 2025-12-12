# Feature: Cloud CLI Commands
# Story: story-git-native-circle-invitations
# Principle: principle-git-becomes-keychain
#
# Git-native invitation flow: "just invite bob" and "just arrive"
# The git repository becomes the keychain. Access flows through git.

Feature: Cloud CLI Commands
  As a circle steward
  I want one-command invite and arrive flows
  So that circle membership is frictionless

  Background:
    Given a clean test environment
    And a test SSH keypair exists

  # Behavior: behavior-invite-fetches-github-key-and-encrypts
  @behavior:invite-fetches-github-key-and-encrypts
  Scenario: Invite user with circle auto-detection
    Given a circle config at ".chora/circle.json" with circle_id "circle-test"
    And the owner has a keyring with binding to "circle-test"
    And GitHub user "testuser" has an Ed25519 key
    When I run "cmd_invite" for "testuser"
    Then an invitation file exists at ".chora/access/circle-test/testuser.enc"
    And the invitation is encrypted for "testuser"

  @behavior:invite-fetches-github-key-and-encrypts
  Scenario: Invite generates circle key if owner lacks one
    Given a circle config at ".chora/circle.json" with circle_id "circle-new"
    And the owner has a keyring without binding to "circle-new"
    And GitHub user "testuser" has an Ed25519 key
    When I run "cmd_invite" for "testuser"
    Then a new circle key is generated
    And the owner's keyring has a binding to "circle-new"
    And an invitation file exists at ".chora/access/circle-new/testuser.enc"

  @behavior:invite-fetches-github-key-and-encrypts
  Scenario: Invite with explicit circle_id
    Given the owner has a keyring with binding to "circle-explicit"
    And GitHub user "alice" has an Ed25519 key
    When I run "cmd_invite" for "alice" with circle_id "circle-explicit"
    Then an invitation file exists at ".chora/access/circle-explicit/alice.enc"

  # Behavior: behavior-arrive-decrypts-pending-invitations
  @behavior:arrive-decrypts-pending-invitations
  Scenario: Arrive decrypts matching invitation
    Given an invitation file for me in "circle-team"
    And the invitation was encrypted with my public key
    When I run "cmd_arrive"
    Then my keyring has a binding to "circle-team"
    And the binding has the decrypted circle key

  @behavior:arrive-decrypts-pending-invitations
  Scenario: Arrive skips invitations for other users
    Given an invitation file for "other-user" in "circle-team"
    When I run "cmd_arrive"
    Then my keyring does not have a binding to "circle-team"

  @behavior:arrive-decrypts-pending-invitations
  Scenario: Arrive processes multiple circles
    Given invitation files for me in "circle-a" and "circle-b"
    When I run "cmd_arrive"
    Then my keyring has bindings to "circle-a" and "circle-b"

  # Behavior: behavior-list-circle-members
  @behavior:list-circle-members
  Scenario: List members of a circle
    Given invitation files for "alice", "bob", "carol" in "circle-team"
    When I run "cmd_members" for "circle-team"
    Then the output lists "alice", "bob", "carol"

  # Error cases
  Scenario: Invite fails when no circle config and no explicit circle_id
    Given no circle config exists
    When I run "cmd_invite" for "testuser"
    Then the command fails with "No circle configured"

  Scenario: Invite fails when GitHub user has no Ed25519 key
    Given a circle config at ".chora/circle.json" with circle_id "circle-test"
    And the owner has a keyring with binding to "circle-test"
    And GitHub user "nokey" has no Ed25519 key
    When I run "cmd_invite" for "nokey"
    Then the command fails with "No Ed25519 SSH key"

  Scenario: Arrive fails when no SSH private key exists
    Given no SSH private key exists
    When I run "cmd_arrive"
    Then the command prompts to create SSH key
