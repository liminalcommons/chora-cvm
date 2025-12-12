# Feature: Sync Router
# Story: story-system-decides-what-to-sync
# Principle: principle-geometry-determines-sync
#
# Routes entity changes based on inhabits bonds and keyring policies.
# The geometry (bonds) determines behavior, not conditional logic.

Feature: Sync Router
  As a dweller syncing knowledge across circles
  I want the system to route sync based on circle membership
  So that local research stays private and shared work syncs

  Background:
    Given a fresh CVM database
    And a keyring with test bindings

  # Behavior: behavior-entity-in-local-only-circle-does-not-sync
  @behavior:entity-in-local-only-circle-does-not-sync
  Scenario: Entity in local-only circle does not sync
    Given a circle "circle-research" with sync_policy "local-only" in keyring
    And a learning "learning-private" that inhabits "circle-research"
    When I call should_emit for "learning-private"
    Then the result is false

  @behavior:entity-in-local-only-circle-does-not-sync
  Scenario: Entity in local-only circle returns no sync targets
    Given a circle "circle-research" with sync_policy "local-only" in keyring
    And a learning "learning-private" that inhabits "circle-research"
    When I call get_cloud_circle_ids for "learning-private"
    Then the result is empty

  # Behavior: behavior-entity-in-cloud-circle-syncs
  @behavior:entity-in-cloud-circle-syncs
  Scenario: Entity in cloud circle should sync
    Given a circle "circle-shared" with sync_policy "cloud" in keyring
    And a learning "learning-shared" that inhabits "circle-shared"
    When I call should_emit for "learning-shared"
    Then the result is true

  @behavior:entity-in-cloud-circle-syncs
  Scenario: Entity in cloud circle returns sync target
    Given a circle "circle-shared" with sync_policy "cloud" in keyring
    And a learning "learning-shared" that inhabits "circle-shared"
    When I call get_cloud_circle_ids for "learning-shared"
    Then the result contains "circle-shared"

  # Behavior: behavior-entity-in-multiple-circles-syncs-to-cloud-ones-only
  @behavior:entity-in-multiple-circles-syncs-to-cloud-ones-only
  Scenario: Entity in multiple circles syncs to cloud ones only
    Given a circle "circle-research" with sync_policy "local-only" in keyring
    And a circle "circle-shared" with sync_policy "cloud" in keyring
    And a learning "learning-both" that inhabits both circles
    When I call get_cloud_circle_ids for "learning-both"
    Then the result contains "circle-shared"
    And the result does not contain "circle-research"

  @behavior:entity-in-multiple-circles-syncs-to-cloud-ones-only
  Scenario: Entity in multiple circles should emit
    Given a circle "circle-research" with sync_policy "local-only" in keyring
    And a circle "circle-shared" with sync_policy "cloud" in keyring
    And a learning "learning-both" that inhabits both circles
    When I call should_emit for "learning-both"
    Then the result is true

  # Edge cases
  Scenario: Entity with no circle membership does not sync
    Given a learning "learning-orphan" with no inhabits bonds
    When I call should_emit for "learning-orphan"
    Then the result is false

  Scenario: Entity with no circle membership returns no targets
    Given a learning "learning-orphan" with no inhabits bonds
    When I call get_cloud_circle_ids for "learning-orphan"
    Then the result is empty

  Scenario: Get all target circles includes all inhabited
    Given a circle "circle-a" with sync_policy "cloud" in keyring
    And a circle "circle-b" with sync_policy "cloud" in keyring
    And a learning "learning-multi" that inhabits both circles
    When I call get_target_circles for "learning-multi"
    Then the result contains "circle-a"
    And the result contains "circle-b"
