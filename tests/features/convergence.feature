# Feature: Convergence Scanner
# Story: story-system-suggests-convergences
# Principle: principle-convergence-is-active-not-passive
#
# Convergence is docs as a converging force — not just finding what's wrong,
# but suggesting what wants to connect. Detection finds breakage;
# convergence finds loneliness.

Feature: Convergence Scanner
  As a Chora dweller
  I want the system to suggest bonds for isolated entities
  So that knowledge doesn't float in isolation

  Background:
    Given a fresh CVM database

  # ===========================================================================
  # Detection of Isolated Entities
  # ===========================================================================

  # Behavior: behavior-detect-isolated-entities
  @behavior:detect-isolated-entities
  Scenario: Detect entities with no bonds
    Given a learning entity "learning-orphan" with no bonds
    When I scan for convergence opportunities
    Then the result should report 1 unsurfaced learning

  @behavior:detect-isolated-entities
  Scenario: Well-connected entities are not flagged
    Given a learning entity "learning-connected" that surfaces to a principle
    When I scan for convergence opportunities
    Then the result should report 0 unsurfaced learnings

  # ===========================================================================
  # Suggest Surfaces Bonds for Learnings
  # ===========================================================================

  # Behavior: behavior-suggest-surfaces-bonds-for-learnings
  @behavior:suggest-surfaces-bonds-for-learnings
  Scenario: Suggest surfaces bond when learning matches principle keywords
    Given a learning "learning-about-testing" with title "Testing is essential for quality"
    And a principle "principle-quality-through-testing" with statement "Quality emerges from rigorous testing"
    When I scan for convergence opportunities
    Then a suggestion should exist for "learning-about-testing" to surface to "principle-quality-through-testing"

  @behavior:suggest-surfaces-bonds-for-learnings
  Scenario: No suggestion when learning already surfaces to principle
    Given a learning "learning-surfaced" that surfaces to "principle-existing"
    When I scan for convergence opportunities
    Then no suggestion should exist for "learning-surfaced"

  # ===========================================================================
  # Suggest Verifies Bonds for Behaviors
  # ===========================================================================

  # Behavior: behavior-suggest-verifies-bonds-for-behaviors
  @behavior:suggest-verifies-bonds-for-behaviors
  Scenario: Suggest verifies bond when behavior matches tool keywords
    Given a behavior "behavior-emit-logs" with title "System emits log entries"
    And a tool "tool-logger" with title "Logger tool for system events"
    When I scan for convergence opportunities
    Then a suggestion should exist for "tool-logger" to verify "behavior-emit-logs"

  @behavior:suggest-verifies-bonds-for-behaviors
  Scenario: No suggestion when behavior already verified
    Given a behavior "behavior-verified" that is verified by "tool-existing"
    When I scan for convergence opportunities
    Then no suggestion should exist for "behavior-verified"

  # ===========================================================================
  # Score Entity Coherence
  # ===========================================================================

  # Behavior: behavior-score-entity-coherence
  @behavior:score-entity-coherence
  Scenario: Coherence score reflects bond count
    Given an entity with 3 outgoing bonds
    When I compute its coherence score
    Then the score should be greater than 0

  @behavior:score-entity-coherence
  Scenario: Isolated entity has zero coherence
    Given an entity with no bonds
    When I compute its coherence score
    Then the score should be 0

  # ===========================================================================
  # Emit Convergence Signals
  # ===========================================================================

  # Behavior: behavior-emit-convergence-signal
  @behavior:emit-convergence-signal
  Scenario: Scan with emit_signals creates signal entities
    Given a learning entity "learning-lonely" with no bonds
    And a principle "principle-matching" with matching keywords
    When I scan for convergence opportunities with emit_signals enabled
    Then a signal of type "convergence-suggestion" should be emitted

  @behavior:emit-convergence-signal
  Scenario: Signal contains suggested bond details
    Given a learning entity "learning-lonely" with no bonds
    And a principle "principle-matching" with matching keywords
    When I scan for convergence opportunities with emit_signals enabled
    Then the emitted signal should contain from_id and to_id
