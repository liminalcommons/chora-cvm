# Feature: Protocol Invocation from Command Palette
# Story: story-users-invoke-protocols-directly-from-the-command-palette
# Behavior: behavior-command-palette-lists-and-invokes-cvm-protocols
#
# Protocols are just tools with graph-defined behavior. When they appear
# in the command palette alongside tools, users can invoke any capability
# through a unified interface.

Feature: Protocol Invocation from Command Palette
  As a HUD user
  I want to see and invoke protocols from the command palette
  So that I can trigger CVM workflows without memorizing commands

  Background:
    Given a fresh CVM database

  # ===========================================================================
  # Protocol Listing
  # ===========================================================================

  # Behavior: behavior-command-palette-lists-and-invokes-cvm-protocols
  @behavior:command-palette-lists-and-invokes-cvm-protocols
  Scenario: Listing protocols returns protocol entities
    Given a protocol entity "protocol-test-orient" exists with title "Orient System"
    When I fetch the protocols list
    Then the response should contain a protocol with id "protocol-test-orient"
    And the protocol should have title "Orient System"

  @behavior:command-palette-lists-and-invokes-cvm-protocols
  Scenario: Protocol includes description for palette display
    Given a protocol entity "protocol-with-desc" exists with:
      | title       | Test Protocol           |
      | description | Shows system status     |
    When I fetch the protocols list
    Then the protocol "protocol-with-desc" should have description "Shows system status"

  @behavior:command-palette-lists-and-invokes-cvm-protocols
  Scenario: Protocol includes group for categorization
    Given a protocol entity "protocol-grouped" exists with:
      | title | Grouped Protocol  |
      | group | System Protocols  |
    When I fetch the protocols list
    Then the protocol "protocol-grouped" should have group "System Protocols"

  @behavior:command-palette-lists-and-invokes-cvm-protocols
  Scenario: Protocols without explicit group get default
    Given a protocol entity "protocol-no-group" exists with title "Ungrouped Protocol"
    When I fetch the protocols list
    Then the protocol "protocol-no-group" should have group "CVM Protocols"

  # ===========================================================================
  # Protocol Invocation
  # ===========================================================================

  @behavior:command-palette-lists-and-invokes-cvm-protocols
  Scenario: Protocol without inputs can be invoked directly
    Given a protocol entity "protocol-simple" exists with:
      | title   | Simple Protocol |
      | graph   | {"nodes": [], "edges": []} |
    When I invoke protocol "protocol-simple" from the API
    Then the invocation should succeed
    And the result should include execution status

  @behavior:command-palette-lists-and-invokes-cvm-protocols
  Scenario: Protocol with inputs schema is flagged
    Given a protocol entity "protocol-with-inputs" exists with:
      | title        | Protocol With Inputs |
      | inputs_schema | {"type": "object", "properties": {"title": {"type": "string"}}, "required": ["title"]} |
    When I fetch the protocols list
    Then the protocol "protocol-with-inputs" should have requires_inputs true

  # ===========================================================================
  # Protocol Filtering
  # ===========================================================================

  @behavior:command-palette-lists-and-invokes-cvm-protocols
  Scenario: Internal protocols are excluded by default
    Given a protocol entity "protocol-public" exists with title "Public Protocol"
    And a protocol entity "protocol-internal" exists with internal flag set
    When I fetch the protocols list
    Then the response should contain a protocol with id "protocol-public"
    And the response should not contain a protocol with id "protocol-internal"
