# Feature: Tools API for Homoiconic Command Palette
# Story: story-homoiconic-command-palette
# Behavior: behavior-command-palette-shows-cvm-tools-dynamically
#
# The Command Palette should query the CVM for available tools,
# enabling the "Menu is Territory" principle - when you create a tool
# in the database, it appears in the UI without code changes.

Feature: Tools API for Command Palette
  As the HUD interface
  I want to fetch available tools from the CVM
  So that the command palette reflects the current system capabilities

  Background:
    Given a fresh CVM database

  # ===========================================================================
  # Tools Listing
  # ===========================================================================

  # Behavior: behavior-command-palette-shows-cvm-tools-dynamically
  @behavior:command-palette-shows-cvm-tools-dynamically
  Scenario: Listing tools returns tool entities
    Given a tool entity "tool-test-orient" exists with title "Orient System"
    When I fetch the tools list
    Then the response should contain a tool with id "tool-test-orient"
    And the tool should have title "Orient System"

  @behavior:command-palette-shows-cvm-tools-dynamically
  Scenario: Tool includes handler and description
    Given a tool entity "tool-with-handler" exists with:
      | title        | Test Tool               |
      | handler      | protocol-orient         |
      | phenomenology| Shows system orientation |
    When I fetch the tools list
    Then the tool "tool-with-handler" should have handler "protocol-orient"
    And the tool "tool-with-handler" should have description "Shows system orientation"

  @behavior:command-palette-shows-cvm-tools-dynamically
  Scenario: Tool includes group for categorization
    Given a tool entity "tool-grouped" exists with:
      | title | Grouped Tool    |
      | group | System Tools    |
    When I fetch the tools list
    Then the tool "tool-grouped" should have group "System Tools"

  @behavior:command-palette-shows-cvm-tools-dynamically
  Scenario: Tools without explicit group get default
    Given a tool entity "tool-no-group" exists with title "Ungrouped Tool"
    When I fetch the tools list
    Then the tool "tool-no-group" should have group "CVM Tools"

  # ===========================================================================
  # Tool Filtering
  # ===========================================================================

  @behavior:command-palette-shows-cvm-tools-dynamically
  Scenario: Internal tools are excluded by default
    Given a tool entity "tool-public" exists with title "Public Tool"
    And a tool entity "tool-internal" exists with internal flag set
    When I fetch the tools list
    Then the response should contain a tool with id "tool-public"
    And the response should not contain a tool with id "tool-internal"

  @behavior:command-palette-shows-cvm-tools-dynamically
  Scenario: Inactive tools are excluded when filtering active
    Given a tool entity "tool-active" exists with title "Active Tool"
    And a tool entity "tool-inactive" exists with status "inactive"
    When I fetch the tools list with active_only true
    Then the response should contain a tool with id "tool-active"
    And the response should not contain a tool with id "tool-inactive"

  # ===========================================================================
  # Description Fallback Chain
  # ===========================================================================

  @behavior:command-palette-shows-cvm-tools-dynamically
  Scenario: Description falls back from phenomenology to description field
    Given a tool entity "tool-desc-fallback" exists with:
      | title       | Desc Fallback Tool |
      | description | From description   |
    When I fetch the tools list
    Then the tool "tool-desc-fallback" should have description "From description"

  @behavior:command-palette-shows-cvm-tools-dynamically
  Scenario: Description falls back to cognition.ready_at_hand
    Given a tool entity "tool-cognition-desc" exists with cognition ready_at_hand "Use when orienting"
    When I fetch the tools list
    Then the tool "tool-cognition-desc" should have description "Use when orienting"
