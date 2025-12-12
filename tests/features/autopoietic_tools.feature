@behavior:voice-command-manifests-tool-entity-that-appears-in-palette
Feature: Autopoietic Tool Creation

  As a user
  I want to create tool entities via the API
  So that new tools appear in the command palette without code deployment

  Background:
    Given a fresh CVM database

  Scenario: Create tool entity via API
    When I create a tool entity with title "Quick Note" and handler "protocol-create-note"
    Then the response should be successful
    And a tool entity "tool-quick-note" should exist in the database
    And the tool should have handler "protocol-create-note"

  Scenario: Created tool appears in tools list
    Given I create a tool entity with title "Quick Note" and handler "protocol-create-note"
    When I fetch the tools list
    Then the response should contain a tool with title "Quick Note"
    And the tool should have group "CVM Tools"

  Scenario: Tool with phenomenology gets description in palette
    When I create a tool entity with:
      | field         | value                                     |
      | title         | Capture Insight                           |
      | handler       | protocol-capture-insight                  |
      | phenomenology | When an idea strikes and needs capturing  |
    Then the tool should appear in tools list with description "When an idea strikes and needs capturing"

  Scenario: Tool with group appears in correct category
    When I create a tool entity with:
      | field   | value                |
      | title   | Daily Standup        |
      | handler | protocol-standup     |
      | group   | Rituals              |
    Then the tool should appear in tools list with group "Rituals"

  Scenario: Tool with shortcut appears with keyboard hint
    When I create a tool entity with:
      | field    | value                  |
      | title    | Quick Capture          |
      | handler  | protocol-quick-capture |
      | shortcut | ⌘⇧Q                    |
    Then the tool should appear in tools list with shortcut "⌘⇧Q"

  Scenario: Internal tools are excluded from palette
    When I create a tool entity with:
      | field    | value             |
      | title    | Internal Debug    |
      | handler  | debug-handler     |
      | internal | true              |
    And I fetch the tools list
    Then the response should not contain a tool with title "Internal Debug"
