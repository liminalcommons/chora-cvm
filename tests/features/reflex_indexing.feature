@behavior:system-indexes-learning-on-creation
Feature: Reflex Learning Indexing
  When learnings are created, the reflex arc should index them into FTS
  for immediate searchability. Future sessions can then discover relevant context.

  Background:
    Given a fresh CVM database

  # ==========================================================================
  # Scenario 1: Learning becomes searchable after creation
  # ==========================================================================

  Scenario: Learning is indexed immediately upon creation
    Given the reflex indexing is active
    When I create a learning "Discovered that X helps with Y"
    Then the learning is indexed in FTS
    And I can search for "X helps" and find the learning

  # ==========================================================================
  # Scenario 2: Multiple learnings are searchable
  # ==========================================================================

  Scenario: Multiple learnings are all searchable
    Given the reflex indexing is active
    When I create the following learnings:
      | title                                |
      | Performance improves with caching    |
      | User feedback reveals UX friction    |
      | Testing reveals edge cases           |
    Then all learnings are indexed in FTS
    And I can search for "caching" and find 1 result
    And I can search for "reveals" and find 2 results
