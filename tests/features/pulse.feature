# Feature: Autonomic Heartbeat (The Reflex)
# Story: story-autonomic-heartbeat
# Principle: principle-autonomic-trust
#
# Trust in autonomic behavior comes from: Observable, Predictable, Controllable

Feature: Autonomic Pulse
  As a Chora user
  I want the CVM to have an autonomic heartbeat
  So that signals are processed automatically while I can observe and control the system

  Background:
    Given a fresh CVM database

  # Behavior: behavior-pulse-processes-signals
  @behavior:pulse-processes-signals
  Scenario: Pulse processes signals with triggers bonds
    Given an active signal "signal-test" exists
    And a protocol "protocol-test-handler" exists
    And signal "signal-test" has a triggers bond to "protocol-test-handler"
    When the pulse runs
    Then signal "signal-test" status should be "resolved"
    And the protocol "protocol-test-handler" should have been executed

  # Behavior: behavior-signal-outcomes-recorded
  @behavior:signal-outcomes-recorded
  Scenario: Signal outcomes include processing details
    Given an active signal "signal-outcome-test" exists
    And a protocol "protocol-simple" exists that returns success
    And signal "signal-outcome-test" has a triggers bond to "protocol-simple"
    When the pulse runs
    Then signal "signal-outcome-test" should have outcome_data
    And the outcome_data should include "protocol_id"
    And the outcome_data should include "duration_ms"

  @behavior:signal-outcomes-recorded
  Scenario: Failed protocol records error in outcome
    Given an active signal "signal-fail-test" exists
    And a protocol "protocol-failing" exists that returns an error
    And signal "signal-fail-test" has a triggers bond to "protocol-failing"
    When the pulse runs
    Then signal "signal-fail-test" status should be "failed"
    And the outcome_data should include "error"

  # Behavior: behavior-pulse-preview
  @behavior:pulse-preview
  Scenario: Preview shows what pulse would process
    Given an active signal "signal-preview-a" exists
    And a protocol "protocol-handler-a" exists
    And signal "signal-preview-a" has a triggers bond to "protocol-handler-a"
    And an active signal "signal-preview-b" exists with no triggers bond
    When I request a pulse preview
    Then the preview should show signal "signal-preview-a" would trigger "protocol-handler-a"
    And the preview should not include signal "signal-preview-b"
    And no signals should have been processed

  # Behavior: behavior-pulse-status
  @behavior:pulse-status
  Scenario: Pulse status shows recent history
    Given the pulse has run 3 times
    And the first pulse processed 2 signals
    And the second pulse processed 1 signal
    And the third pulse had an error
    When I request pulse status
    Then the status should show 3 recent pulses
    And the status should show signals processed count for each pulse
    And the status should show error count for the third pulse

  # Behavior: behavior-pulse-runs-periodically
  @behavior:pulse-runs-periodically
  Scenario: Pulse can be configured with interval
    Given a pulse configuration with interval 60 seconds
    When the pulse configuration is loaded
    Then the pulse interval is 60 seconds

  @behavior:pulse-runs-periodically
  Scenario: Pulse respects enabled/disabled setting
    Given a pulse configuration with enabled = false
    When pulse_should_run is checked
    Then the result is false
