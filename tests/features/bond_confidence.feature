# Feature: Bond Confidence
# Story: story-bonds-carry-epistemic-confidence
#
# Bonds carry a confidence value (0.0-1.0) representing epistemic certainty.
# Tentative bonds (confidence < 1.0) emit signals to draw attention.
# Confidence updates emit signals when confidence is reduced.

Feature: Bond Confidence
  As a dweller working with uncertain knowledge
  I want bonds to carry confidence values
  So that I can distinguish certain from tentative relationships

  Background:
    Given a fresh CVM database
    And a learning "learning-test-insight" exists
    And a principle "principle-test-truth" exists

  # Behavior: behavior-bond-created-with-confidence-emits-signal-when-tentative
  @behavior:bond-created-with-confidence-emits-signal-when-tentative
  Scenario: Create bond with full confidence (no signal)
    When I create a bond surfaces from "learning-test-insight" to "principle-test-truth" with confidence 1.0
    Then the bond has confidence 1.0
    And no signal is emitted

  @behavior:bond-created-with-confidence-emits-signal-when-tentative
  Scenario: Create tentative bond (confidence < 1.0) emits signal
    When I create a bond surfaces from "learning-test-insight" to "principle-test-truth" with confidence 0.7
    Then the bond has confidence 0.7
    And a signal is emitted with title containing "Tentative bond created"
    And the signal has source_id equal to the bond id

  @behavior:bond-created-with-confidence-emits-signal-when-tentative
  Scenario: Confidence below 0.5 emits higher urgency signal
    When I create a bond surfaces from "learning-test-insight" to "principle-test-truth" with confidence 0.3
    Then a signal is emitted with urgency "normal"

  # Behavior: behavior-bond-confidence-update-emits-signal
  @behavior:bond-confidence-update-emits-signal
  Scenario: Update bond confidence downward emits signal
    Given a bond exists with confidence 1.0
    When I update the bond confidence to 0.5
    Then a signal is emitted with title containing "Bond confidence dropped"
    And the signal shows the confidence drop

  @behavior:bond-confidence-update-emits-signal
  Scenario: Update bond confidence upward does not emit signal
    Given a bond exists with confidence 0.5
    When I update the bond confidence to 0.8
    Then no signal is emitted

  @behavior:bond-confidence-update-emits-signal
  Scenario: Large confidence drop emits high urgency signal
    Given a bond exists with confidence 1.0
    When I update the bond confidence to 0.5
    Then a signal is emitted with urgency "high"

  # Confidence clamping
  Scenario: Confidence is clamped to valid range
    When I create a bond with confidence 1.5
    Then the bond has confidence 1.0

  Scenario: Negative confidence is clamped to 0.0
    When I create a bond with confidence -0.5
    Then the bond has confidence 0.0

  # Gradient mapping (status inference)
  Scenario Outline: Confidence maps to bond status gradient
    When I create a bond with confidence <confidence>
    Then the effective certainty level is "<certainty>"

    Examples:
      | confidence | certainty   |
      | 1.0        | certain     |
      | 0.85       | high        |
      | 0.6        | hypothesis  |
      | 0.3        | speculation |
