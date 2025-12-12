# Feature: System Integrity Truth
# Story: story-system-integrity-truth
# Inquiry: inquiry-system-self-truth
#
# The system tells the truth about its own verification status.
# Truth flows from observed test results back into entity status.

Feature: System Integrity Truth
  As a dweller in this system
  I want the system to honestly report which behaviors are verified by passing tests
  So that I can trust claims of capability and see where gaps exist

  Background:
    Given a fresh CVM database

  # Behavior: behavior-integrity-discovers-scenarios
  @behavior:integrity-discovers-scenarios
  Scenario: Integrity check discovers behaviors with BDD scenarios
    Given behavior "behavior-test-alpha" exists
    And behavior "behavior-test-beta" exists
    And a feature file exists with @behavior:test-alpha tag
    When I run integrity discovery
    Then behavior "behavior-test-alpha" should show "has_scenarios: true"
    And behavior "behavior-test-beta" should show "has_scenarios: false"

  @behavior:integrity-discovers-scenarios
  Scenario: Discovery maps feature tags to behavior entities
    Given behavior "behavior-pulse-processes-signals" exists
    And the pulse.feature file has @behavior:pulse-processes-signals tag
    When I run integrity discovery
    Then the mapping shows behavior-pulse-processes-signals -> tests/features/pulse.feature

  # Behavior: behavior-integrity-runs-tests
  @behavior:integrity-runs-tests
  Scenario: Integrity check executes tests and captures results
    Given behavior "behavior-with-passing-test" exists
    And a feature file with passing scenario for that behavior
    When I run integrity check with execute=true
    Then the result shows behavior "behavior-with-passing-test" passed

  @behavior:integrity-runs-tests
  Scenario: Failed tests are captured accurately
    Given behavior "behavior-with-failing-test" exists
    And a feature file with failing scenario for that behavior
    When I run integrity check with execute=true
    Then the result shows behavior "behavior-with-failing-test" failed
    And the failure reason is captured

  # Behavior: behavior-integrity-reports-status
  @behavior:integrity-reports-status
  Scenario: Integrity report shows honest verification status
    Given these behaviors exist:
      | behavior_id              | has_scenarios | test_result |
      | behavior-verified-one    | true          | passed      |
      | behavior-failing-one     | true          | failed      |
      | behavior-unverified-one  | false         | none        |
    When I view the integrity report
    Then behavior "behavior-verified-one" shows status "verified"
    And behavior "behavior-failing-one" shows status "failing"
    And behavior "behavior-unverified-one" shows status "unverified"

  @behavior:integrity-reports-status
  Scenario: Integrity report shows coverage statistics
    Given 10 behaviors exist
    And 6 have passing tests
    And 2 have failing tests
    And 2 have no tests
    When I view the integrity report
    Then the summary shows "6 verified, 2 failing, 2 unverified (60% coverage)"

  # Behavior: behavior-verifies-bond-tracks-results
  @behavior:verifies-bond-tracks-results
  Scenario: verifies bond stores verification metadata
    Given tool "tool-pulse" exists
    And behavior "behavior-pulse-processes-signals" exists
    And a verifies bond from tool-pulse to behavior-pulse-processes-signals
    When tests for behavior-pulse-processes-signals pass
    Then the verifies bond has last_verified_at timestamp
    And the verifies bond has verification_result "passed"

  @behavior:verifies-bond-tracks-results
  Scenario: verifies bond reflects test failures
    Given a verifies bond exists
    When the associated tests fail
    Then the verifies bond has verification_result "failed"
    And the verifies bond has failure_summary
