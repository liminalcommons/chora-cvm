@behavior:layout-entity-responds-to-signal-urgency
Feature: Stigmergic Layout

  The interface is a projection of system state. Layout configuration
  is stored as a CVM entity and responds to system signals.

  Background:
    Given a fresh CVM database

  # --- Layout Entity Management ---

  Scenario: Layout entity can be created via API
    When I create a layout entity with:
      | key           | value   |
      | mode          | split   |
      | panels.context | true   |
      | panels.events  | false  |
      | panels.signals | false  |
    Then the response should be successful
    And a layout entity "pattern-hud-layout-default" should exist

  Scenario: Layout entity can be retrieved
    Given a layout entity exists with signals panel closed
    When I fetch the layout endpoint
    Then the response should include panel configuration
    And the signals panel should be closed

  # --- Signal-Triggered Layout Mutation ---

  Scenario: High urgency signal opens signals panel
    Given a layout entity exists with signals panel closed
    When a signal with urgency "high" is emitted
    And I fetch the layout endpoint
    Then the signals panel should be open

  Scenario: Normal urgency signal does not change layout
    Given a layout entity exists with signals panel closed
    When a signal with urgency "normal" is emitted
    And I fetch the layout endpoint
    Then the signals panel should be closed

  Scenario: Critical urgency signal opens signals panel
    Given a layout entity exists with signals panel closed
    When a signal with urgency "critical" is emitted
    And I fetch the layout endpoint
    Then the signals panel should be open

  # --- Layout Persistence ---

  Scenario: Layout changes persist across requests
    Given a layout entity exists with mode "single"
    When I update the layout to mode "quad"
    And I fetch the layout endpoint
    Then the layout mode should be "quad"
