# Feature: Protocol Sense Rhythm — Kairotic Phase Detection
# Story: story-cvm-enables-self-awareness
# Behaviors:
#   - behavior-sense-rhythm-detects-kairotic-phase
#   - behavior-sense-rhythm-returns-standardized-shape
#   - behavior-sense-rhythm-computes-satiation
#   - behavior-sense-rhythm-computes-temporal-health
#
# The system can sense its own rhythm — which phase of the kairotic cycle
# it is currently in. This enables adaptive behavior and self-regulation.

Feature: Protocol Sense Rhythm
  As a CVM system
  I want to sense my current kairotic state
  So that I can adapt my behavior to the natural cycle

  Background:
    Given a bootstrapped CVM database with rhythm sensing primitives

  # Behavior: behavior-sense-rhythm-detects-kairotic-phase
  @behavior:sense-rhythm-detects-kairotic-phase
  Scenario: Detect dominant kairotic phase
    Given the database has recent inquiry activity
    When the sense-kairotic-state primitive is executed
    Then the result status is "success"
    And the result data contains phases
    And the phases include all six archetypes
    And the data contains a dominant phase
    And the data contains a side value

  # Behavior: behavior-sense-rhythm-returns-standardized-shape
  @behavior:sense-rhythm-returns-standardized-shape
  Scenario: Primitives return standard response shape
    When the sense-kairotic-state primitive is executed
    Then the result has key "status"
    And the result has key "data"
    And the data has key "phases"
    And the data has key "dominant"
    And the data has key "side"

  # Behavior: behavior-sense-rhythm-computes-satiation
  @behavior:sense-rhythm-computes-satiation
  Scenario: Compute satiation score with label
    When the sense-satiation primitive is executed
    Then the result status is "success"
    And the result data contains score
    And the score is between 0.0 and 1.0
    And the result data contains label
    And the label is a valid satiation label

  # Behavior: behavior-sense-rhythm-computes-temporal-health
  @behavior:sense-rhythm-computes-temporal-health
  Scenario: Compute temporal health metrics over window
    When the sense-temporal-health primitive is executed with window_days 7
    Then the result status is "success"
    And the result data contains metrics
    And the metrics include creation and decomposition rates
    And the data contains growth_rate
    And the data contains metabolic_balance

  # Integration: Dispatch primitive through CvmEngine
  @integration:engine-dispatch-primitive
  Scenario: Rhythm sensing primitive accessible via engine dispatch
    When I dispatch "primitive-sense-kairotic-state" through CvmEngine
    Then the dispatch result is successful
    And the result data contains phase information

  # Integration: Full protocol through CvmEngine
  @integration:engine-dispatch-protocol
  Scenario: Full rhythm protocol combines all sensing
    When I dispatch "protocol-sense-rhythm" through CvmEngine
    Then the dispatch result is successful
    And the result contains kairotic data
    And the result contains satiation data
    And the result contains health data

