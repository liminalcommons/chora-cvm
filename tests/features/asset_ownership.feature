# Feature: Asset Ownership
# Story: story-assets-belong-to-circles
# Principle: principle-ownership-is-bond-geometry
#
# Assets belong to circles via belongs-to bonds.
# Ownership is visible in the graph — no hidden relationships.

Feature: Asset Ownership
  As a circle steward managing infrastructure
  I want assets to belong to circles via bonds
  So that ownership is visible and queryable

  Background:
    Given a fresh CVM database

  # Behavior: behavior-asset-bonded-belongs-to-appears-in-circle-constellation
  @behavior:asset-bonded-belongs-to-appears-in-circle-constellation
  Scenario: Asset belongs to circle via bond
    Given a circle "circle-shared" exists
    And an asset "asset-r2-bucket" of kind "r2-bucket" exists
    When I bond belongs-to from "asset-r2-bucket" to "circle-shared"
    Then the asset appears in circle-shared's constellation
    And get_assets for "circle-shared" returns the asset

  @behavior:asset-bonded-belongs-to-appears-in-circle-constellation
  Scenario: Asset data includes kind field
    Given a circle "circle-team" exists
    And an asset "asset-git-repo" of kind "git-repo" exists
    When I bond belongs-to from "asset-git-repo" to "circle-team"
    And I query get_assets for "circle-team"
    Then the result includes an asset with kind "git-repo"

  # Behavior: behavior-circle-shows-all-owned-assets
  @behavior:circle-shows-all-owned-assets
  Scenario: Query all circle assets
    Given a circle "circle-workspace" exists
    And an asset "asset-repo" of kind "git-repo" exists
    And an asset "asset-bucket" of kind "r2-bucket" exists
    And an asset "asset-channel" of kind "zulip-stream" exists
    And all 3 assets belong to "circle-workspace"
    When I query get_assets for "circle-workspace"
    Then 3 assets are returned

  @behavior:circle-shows-all-owned-assets
  Scenario: Circle with no assets returns empty list
    Given a circle "circle-empty" exists
    When I query get_assets for "circle-empty"
    Then an empty list is returned

  # Behavior: behavior-asset-can-belong-to-multiple-circles
  @behavior:asset-can-belong-to-multiple-circles
  Scenario: Asset belongs to multiple circles
    Given a circle "circle-team-a" exists
    And a circle "circle-team-b" exists
    And an asset "asset-shared-repo" of kind "git-repo" exists
    When I bond belongs-to from "asset-shared-repo" to "circle-team-a"
    And I bond belongs-to from "asset-shared-repo" to "circle-team-b"
    Then get_assets for "circle-team-a" includes "asset-shared-repo"
    And get_assets for "circle-team-b" includes "asset-shared-repo"

  @behavior:asset-can-belong-to-multiple-circles
  Scenario: Query circles an asset belongs to
    Given a circle "circle-dev" exists
    And a circle "circle-prod" exists
    And an asset "asset-config" of kind "config-store" exists
    And "asset-config" belongs to both circles
    When I query get_owner_circles for "asset-config"
    Then both circles are returned

  # Asset result structure
  Scenario: Asset result includes full entity data
    Given a circle "circle-infra" exists
    And an asset "asset-s3" of kind "s3-bucket" with uri "s3://my-bucket" exists
    When I bond belongs-to from "asset-s3" to "circle-infra"
    And I query get_assets for "circle-infra"
    Then each result includes entity id, type, and data
    And the data includes uri field
