# Feature: Kairotic Rhythm — Sensing System Phases
# Story: story-system-senses-kairotic-rhythm
#
# The system can sense its current phase in the kairotic cycle:
# - Pioneer (genesis), Cultivator (growth), Regulator (stabilization)
# - Steward (tending), Curator (discernment), Scout (preparation)
#
# Orange/Lit side: Pioneer → Cultivator → Regulator (active, visible, forward)
# Purple/Shadow side: Steward → Curator → Scout (preparatory, receptive)
#
# "Power only gets corrupted when it accumulates and stagnates."
# The system senses its rhythm to know when to build, tend, or wait.

Feature: Kairotic Rhythm Sensing
  As a Chora dweller
  I want the system to sense its kairotic phase
  So that I know whether we are growing, mature, or fallow

  Background:
    Given a fresh CVM database

  # Behavior: behavior-sense-kairotic-state-returns-phase-weights
  @behavior:sense-kairotic-state-returns-phase-weights
  Scenario: Sense kairotic state returns all six phase weights
    Given the Loom has 10 entities with varied types
    And recent activity shows high inquiry creation
    When sense_kairotic_state is invoked
    Then the result includes KairoticState with 6 phase weights
    And each phase weight is between 0.0 and 1.0
    And the result includes a dominant_phase field
    And the result includes a side field (orange or purple)

  @behavior:sense-kairotic-state-returns-phase-weights
  Scenario: Pioneer phase detected when exploration is high
    Given the Loom has 5 active inquiries created this week
    And verification rate is below 50%
    When sense_kairotic_state is invoked
    Then the dominant_phase is "pioneer"
    And the side is "orange"

  @behavior:sense-kairotic-state-returns-phase-weights
  Scenario: Steward phase detected when system is stable
    Given the Loom has integrity_score above 0.9
    And change rate is below 0.1 entities per day
    When sense_kairotic_state is invoked
    Then the dominant_phase is "steward"
    And the side is "purple"

  @behavior:sense-kairotic-state-returns-phase-weights
  Scenario: Cultivator phase detected when bonding activity is high
    Given the Loom has 15 new bonds created this week
    And 5 new learnings captured this week
    When sense_kairotic_state is invoked
    Then the dominant_phase is "cultivator"
    And the side is "orange"

  # Behavior: behavior-temporal-health-tracks-rolling-window
  @behavior:temporal-health-tracks-rolling-window
  Scenario: Temporal health returns rolling window metrics
    Given the Loom has activity over the past 7 days
    When temporal_health is invoked with window_days 7
    Then the result includes entities_created count
    And the result includes bonds_created count
    And the result includes growth_rate as float
    And the result includes metabolic_balance as float

  @behavior:temporal-health-tracks-rolling-window
  Scenario: Temporal health shows positive growth rate
    Given 5 entities were created in the last 7 days
    And 1 entity was composted in the last 7 days
    When temporal_health is invoked with window_days 7
    Then growth_rate is approximately 0.57 per day

  @behavior:temporal-health-tracks-rolling-window
  Scenario: Temporal health shows metabolic balance
    Given 10 entities and 20 bonds were created in the last 7 days
    And 3 entities were composted in the last 7 days
    When temporal_health is invoked with window_days 7
    Then metabolic_balance is 10.0 (anabolic / catabolic)

  # Behavior: behavior-satiation-computed-from-integrity-entropy-and-growth
  @behavior:satiation-computed-from-integrity-entropy-and-growth
  Scenario: Satiation computed from system health metrics
    Given the Loom has integrity_score 0.85
    And entropy_score 0.2
    And 2 active inquiries
    When compute_satiation is invoked
    Then the satiation score is between 0.0 and 1.0

  @behavior:satiation-computed-from-integrity-entropy-and-growth
  Scenario: High satiation when system is healthy and quiet
    Given the Loom has integrity_score 0.95
    And entropy_score 0.1
    And 0 active inquiries
    And 0 unresolved signals
    When compute_satiation is invoked
    Then the satiation score is above 0.7

  @behavior:satiation-computed-from-integrity-entropy-and-growth
  Scenario: Low satiation when system is under pressure
    Given the Loom has integrity_score 0.5
    And entropy_score 0.8
    And 5 active inquiries
    And 3 unresolved signals
    When compute_satiation is invoked
    Then the satiation score is below 0.3
