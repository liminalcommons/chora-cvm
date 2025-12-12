# Feature: inhabits Bond
# Story: story-entities-inhabit-circles
# Principle: principle-circle-boundary-is-membership-boundary
#
# Entities belong to circles via the inhabits bond.
# This is the foundation for sync routing - the system queries
# which circles an entity inhabits to determine where it syncs.

Feature: inhabits Bond
  As a dweller organizing knowledge into circles
  I want entities to inhabit circles via bonds
  So that I can route sync and query membership

  Background:
    Given a fresh CVM database
    And a circle "circle-research" exists
    And a circle "circle-shared" exists

  # Behavior: behavior-entity-bonded-with-inhabits-appears-in-circle-constellation
  @behavior:entity-bonded-with-inhabits-appears-in-circle-constellation
  Scenario: Entity inhabits circle
    Given a learning "learning-new-insight" exists
    When I bond inhabits from "learning-new-insight" to "circle-research"
    Then the learning appears in circle-research's inhabitants
    And get_inhabited_circles for "learning-new-insight" returns "circle-research"

  @behavior:entity-bonded-with-inhabits-appears-in-circle-constellation
  Scenario: Entity can inhabit multiple circles
    Given a learning "learning-shared-insight" exists
    When I bond inhabits from "learning-shared-insight" to "circle-research"
    And I bond inhabits from "learning-shared-insight" to "circle-shared"
    Then get_inhabited_circles for "learning-shared-insight" returns both circles
    And the learning appears in both circles' inhabitants

  # Behavior: behavior-circle-constellation-shows-all-inhabitants
  @behavior:circle-constellation-shows-all-inhabitants
  Scenario: Query circle inhabitants
    Given 3 learnings exist
    And all 3 inhabit "circle-research"
    When I query get_inhabitants for "circle-research"
    Then all 3 learnings are returned
    And each result includes entity id, type, and data

  @behavior:circle-constellation-shows-all-inhabitants
  Scenario: Circle with no inhabitants returns empty list
    When I query get_inhabitants for "circle-shared"
    Then an empty list is returned

  # Routing foundation
  Scenario: Get target circles for sync routing
    Given a learning that inhabits "circle-research" and "circle-shared"
    When I query get_inhabited_circles for the learning
    Then both "circle-research" and "circle-shared" are returned
    And the result can be used for sync routing decisions

  # Entity with no circle membership
  Scenario: Entity with no inhabits bonds
    Given a learning "learning-orphan" exists with no inhabits bonds
    When I query get_inhabited_circles for "learning-orphan"
    Then an empty list is returned
