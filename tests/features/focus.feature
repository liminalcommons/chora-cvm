# Feature: Attention Layer (Focus & Signal)
# Story: story-attention-declares-what-matters
# Principle: principle-attention-is-finite
#
# Focus is plasma - the energy of attention. When you engage, you declare
# what you're attending to. When you resolve, you close the loop.
# Signal is impulse - something demands attention.

Feature: Attention Layer
  As a Chora dweller
  I want to declare focus and emit signals
  So that attention is tracked and the system can route interrupts

  Background:
    Given a fresh CVM database

  # ===========================================================================
  # Focus Creation
  # ===========================================================================

  # Behavior: behavior-create-focus-declares-attention
  @behavior:create-focus-declares-attention
  Scenario: Creating a focus declares what is being attended to
    When I create a focus with title "Implementing the audit tool"
    Then a focus entity should exist with title "Implementing the audit tool"
    And the focus status should be "active"
    And the focus should have an engaged_at timestamp

  @behavior:create-focus-declares-attention
  Scenario: Focus can be triggered by a signal
    Given an active signal "signal-void-detected" exists
    When I create a focus with title "Addressing the void" triggered by "signal-void-detected"
    Then the focus should have triggered_by "signal-void-detected"

  # ===========================================================================
  # Focus Resolution
  # ===========================================================================

  # Behavior: behavior-resolve-focus-closes-attention-loop
  @behavior:resolve-focus-closes-attention-loop
  Scenario: Resolving a focus marks it as complete
    Given an active focus "focus-test-task" exists
    When I resolve focus "focus-test-task" with outcome "completed"
    Then the focus status should be "resolved"
    And the focus should have a resolved_at timestamp

  @behavior:resolve-focus-closes-attention-loop
  Scenario: Resolving a focus can yield a learning
    Given an active focus "focus-exploration" exists
    When I resolve focus "focus-exploration" with learning "Discovered the audit pattern"
    Then a learning entity should be created
    And the learning should reference the focus

  # ===========================================================================
  # Focus Listing
  # ===========================================================================

  # Behavior: behavior-list-focuses-shows-current-attention
  @behavior:list-focuses-shows-current-attention
  Scenario: Listing focuses shows only active ones
    Given an active focus "focus-active-one" exists
    And an active focus "focus-active-two" exists
    And a resolved focus "focus-resolved" exists
    When I list active focuses
    Then the result should contain "focus-active-one"
    And the result should contain "focus-active-two"
    And the result should not contain "focus-resolved"

  @behavior:list-focuses-shows-current-attention
  Scenario: Listing focuses can filter by persona
    Given an active focus "focus-victor" exists for persona "persona-victor"
    And an active focus "focus-architect" exists for persona "persona-resident-architect"
    When I list active focuses for persona "persona-victor"
    Then the result should contain "focus-victor"
    And the result should not contain "focus-architect"

  # ===========================================================================
  # Signal Emission
  # ===========================================================================

  # Behavior: behavior-emit-signal-demands-attention
  @behavior:emit-signal-demands-attention
  Scenario: Emitting a signal creates an active signal entity
    When I emit a signal with title "Void detected in behavior coverage"
    Then a signal entity should exist with title "Void detected in behavior coverage"
    And the signal status should be "active"
    And the signal should have an emitted_at timestamp

  @behavior:emit-signal-demands-attention
  Scenario: Signal can have urgency level
    When I emit a signal with title "Critical issue" and urgency "critical"
    Then the signal urgency should be "critical"

  @behavior:emit-signal-demands-attention
  Scenario: Signal can reference its source
    Given a tool entity "tool-audit" exists
    When I emit a signal with title "Audit found gaps" from source "tool-audit"
    Then the signal source_id should be "tool-audit"
