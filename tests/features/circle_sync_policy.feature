# Feature: Circle Sync Policy
# Story: story-circles-declare-their-sync-boundary
# Principle: principle-privacy-boundary-is-sync-policy
#
# Circles declare whether their contents sync to cloud or stay local.
# This is the foundation for sync routing - the privacy boundary.

Feature: Circle Sync Policy
  As a dweller managing knowledge privacy
  I want circles to declare their sync policy
  So that I can control what stays local vs what syncs to cloud

  Background:
    Given a fresh CVM database

  # Behavior: behavior-circle-with-local-only-policy-does-not-sync
  @behavior:circle-with-local-only-policy-does-not-sync
  Scenario: Create local-only circle
    Given I create a circle "my-research" with sync_policy "local-only"
    When I query is_local_only for "my-research"
    Then the result is true

  @behavior:circle-with-local-only-policy-does-not-sync
  Scenario: Local-only circle data includes sync_policy
    Given I create a circle "private-notes" with sync_policy "local-only"
    When I query the circle data for "private-notes"
    Then the sync_policy field is "local-only"

  # Behavior: behavior-circle-with-cloud-policy-enables-sync-routing
  @behavior:circle-with-cloud-policy-enables-sync-routing
  Scenario: Create cloud-syncing circle
    Given I create a circle "shared-work" with sync_policy "cloud"
    When I query is_local_only for "shared-work"
    Then the result is false

  @behavior:circle-with-cloud-policy-enables-sync-routing
  Scenario: Cloud circle data includes sync_policy
    Given I create a circle "team-collaboration" with sync_policy "cloud"
    When I query the circle data for "team-collaboration"
    Then the sync_policy field is "cloud"

  # Default behavior
  Scenario: Circles default to local-only when unspecified
    Given I create a circle "exploration" without specifying sync_policy
    When I query is_local_only for "exploration"
    Then the result is true

  Scenario: Default sync_policy is stored in data
    Given I create a circle "unnamed-research" without specifying sync_policy
    When I query the circle data for "unnamed-research"
    Then the sync_policy field is "local-only"

  # Query helpers
  Scenario: List all local-only circles
    Given I create a circle "research-a" with sync_policy "local-only"
    And I create a circle "research-b" with sync_policy "local-only"
    And I create a circle "shared-c" with sync_policy "cloud"
    When I query get_local_only_circles
    Then the result contains "circle-research-a"
    And the result contains "circle-research-b"
    And the result does not contain "circle-shared-c"

  Scenario: List all cloud circles
    Given I create a circle "local-a" with sync_policy "local-only"
    And I create a circle "cloud-b" with sync_policy "cloud"
    And I create a circle "cloud-c" with sync_policy "cloud"
    When I query get_cloud_circles
    Then the result contains "circle-cloud-b"
    And the result contains "circle-cloud-c"
    And the result does not contain "circle-local-a"
