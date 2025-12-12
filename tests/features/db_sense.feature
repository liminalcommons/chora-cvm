# Feature: Database Health Sensing
# Story: story-agents-can-sense-database-health
# Pattern: pattern-sense-before-act
#
# Agents need to sense database health on arrival to orient before acting.
# The friction of blindness is an invitation to create visibility.

Feature: Database Health Sensing
  As a Chora agent
  I want to sense database health on arrival
  So that I can orient before taking action

  Background:
    Given a database with entities and bonds

  @behavior:graph-db-sense-returns-health-summary
  Scenario: graph.db.sense returns health summary
    When I call graph.db.sense with the database path
    Then I receive a structured health summary
    And the summary contains entity counts by type
    And the summary contains total bond count
    And the summary contains orphan bond count
    And the summary contains last modified timestamp
