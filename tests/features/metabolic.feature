# Feature: Autophagy — Metabolic Protocols for System Health
# Story: story-system-metabolizes-entropy-into-growth
# Principle: principle-friction-is-invitation
#
# Dead branches, stale signals, and deprecated entities are not waste — they are compost.
# The system digests entropy and radiates learnings.
# Every decomposition creates provenance via crystallized-from bonds.

Feature: Metabolic Operations
  As a Chora dweller
  I want the system to metabolize entropy into growth
  So that dead matter becomes compost for new entities

  Background:
    Given a fresh CVM database

  # Behavior: behavior-sense-entropy-reports-metabolic-health
  @behavior:sense-entropy-reports-metabolic-health
  Scenario: Sense entropy returns metabolic health report
    Given the Loom has 10 entities with 5 bonds
    And 3 entities are orphans (no bonds)
    And 2 signals are older than 7 days
    When sense_entropy is invoked
    Then the result includes a MetabolicHealth report
    And the report shows orphan_count = 3
    And the report shows stale_signal_count = 2

  @behavior:sense-entropy-reports-metabolic-health
  Scenario: Sense entropy emits signals when thresholds exceeded
    Given the Loom has 5 stale signals older than the 7-day threshold
    When sense_entropy is invoked
    Then a signal "signal-stale-signals-detected" is emitted
    And the signal metadata includes count = 5

  # Behavior: behavior-digest-transforms-entity-into-learning
  @behavior:digest-transforms-entity-into-learning
  Scenario: Digest transforms a pattern into a learning
    Given a pattern "pattern-old-approach" exists with problem/solution data
    When digest is invoked with entity_id "pattern-old-approach"
    Then a new learning entity is created
    And the learning title includes "pattern-old-approach"
    And a crystallized-from bond connects the learning to "pattern-old-approach"

  @behavior:digest-transforms-entity-into-learning
  Scenario: Digest extracts wisdom from tool phenomenology
    Given a tool "tool-legacy-helper" exists with phenomenology data
    When digest is invoked with entity_id "tool-legacy-helper"
    Then a new learning entity is created
    And the learning insight includes the phenomenology content
    And a crystallized-from bond connects the learning to "tool-legacy-helper"

  # Behavior: behavior-compost-archives-orphan-entity
  @behavior:compost-archives-orphan-entity
  Scenario: Compost archives an orphan entity
    Given an entity "learning-forgotten" exists with no bonds
    When compost is invoked with entity_id "learning-forgotten"
    Then the entity is moved to the archive table
    And a learning about the composting is created
    And the original entity no longer exists in entities table

  @behavior:compost-archives-orphan-entity
  Scenario: Compost archives dangling bonds first
    Given an entity "pattern-deprecated" exists with bonds to deleted entities
    When compost is invoked with entity_id "pattern-deprecated"
    Then the dangling bonds are archived first
    And then the entity is archived
    And a learning records the bond cleanup

  @behavior:compost-archives-orphan-entity
  Scenario: Compost refuses to archive non-orphan
    Given an entity "story-active" exists with active bonds
    When compost is invoked with entity_id "story-active"
    Then the operation returns an error
    And the error message says "Entity has active bonds; use digest or force"

  # Behavior: behavior-induce-proposes-pattern-from-learnings
  @behavior:induce-proposes-pattern-from-learnings
  Scenario: Induce proposes pattern from clustered learnings
    Given 3 learnings exist with common domain "metabolic"
    When induce is invoked with those learning_ids
    Then a new pattern entity is created with status "proposed"
    And crystallized-from bonds connect the pattern to all 3 learnings
    And a signal is emitted for human review

  @behavior:induce-proposes-pattern-from-learnings
  Scenario: Induce requires minimum cluster size
    Given 2 learnings exist with common domain "test"
    When induce is invoked with those learning_ids
    Then the operation returns an error
    And the error message says "Minimum cluster size is 3"

  # Behavior: behavior-stagnation-emits-signal-when-threshold-exceeded
  @behavior:stagnation-emits-signal-when-threshold-exceeded
  Scenario: Stagnant inquiry emits signal after 30 days
    Given an inquiry "inquiry-old" was created 31 days ago
    And principle "principle-inquiry-stagnates-after-30-days" defines TTL = 30
    When pulse detects stagnation
    Then a signal is emitted for "inquiry-old"
    And the signal category is "stagnation"

  @behavior:stagnation-emits-signal-when-threshold-exceeded
  Scenario: Stagnant signal emits escalation after 7 days
    Given a signal "signal-stuck" was created 8 days ago
    And principle "principle-signal-stagnates-after-7-days" defines TTL = 7
    When pulse detects stagnation
    Then a new escalation signal is emitted
    And the escalation references "signal-stuck"

  # Behavior: behavior-signal-auto-resolves-when-void-clears
  @behavior:signal-auto-resolves-when-void-clears
  Scenario: Signal auto-resolves when orphan gets bonded
    Given a signal "signal-orphan-learning-x" tracks orphan "learning-x"
    And "learning-x" has no bonds
    When a bond is created from "learning-x" to another entity
    And pulse detects the void condition has cleared
    Then signal "signal-orphan-learning-x" status becomes "resolved"
    And the resolution metadata includes "auto-resolved: void cleared"

  @behavior:signal-auto-resolves-when-void-clears
  Scenario: Signal auto-resolves when stagnant entity is updated
    Given a signal "signal-stagnant-inquiry-y" tracks stagnant "inquiry-y"
    And "inquiry-y" was last updated 32 days ago
    When "inquiry-y" yields a new learning
    And pulse detects the void condition has cleared
    Then signal "signal-stagnant-inquiry-y" status becomes "resolved"
