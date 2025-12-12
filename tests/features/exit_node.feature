# Feature: Exit Node Recording
# Addresses: Inspection recommendation #2 - Output Extraction
#
# Records which RETURN node was hit during protocol execution,
# making branching protocols debuggable.

Feature: Exit Node Recording
  As a protocol developer
  I want the system to record which return node was executed
  So that branching protocols are debuggable

  Scenario: Protocol with single return records exit node
    Given a protocol with a single RETURN node "return-success"
    When I execute the protocol
    Then the state exit_node is "return-success"

  Scenario: Protocol with conditional branches records correct exit
    Given a protocol with two paths leading to "return-a" and "return-b"
    And the input triggers the path to "return-b"
    When I execute the protocol
    Then the state exit_node is "return-b"

  Scenario: Extract output uses recorded exit node
    Given a fulfilled state with exit_node "return-special"
    And a protocol with multiple RETURN nodes with different outputs
    When I extract output from the state
    Then the output comes from "return-special" node
