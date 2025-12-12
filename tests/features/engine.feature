# Feature: CvmEngine — The Unified Entry Point
# Story: story-cvm-enables-multimodal-deployment
# Behaviors:
#   - behavior-engine-routes-protocol-invocation
#   - behavior-engine-routes-primitive-invocation
#   - behavior-engine-lists-capabilities
#
# The CvmEngine is the Event Horizon — all external interfaces converge here.
# CLI, API, and MCP all speak to the same dispatch() method.

Feature: CvmEngine Unified Dispatch
  As a CVM interface developer
  I want a single entry point for all invocations
  So that CLI, API, and MCP behave identically

  Background:
    Given a bootstrapped CVM database with primitives and protocols

  # Behavior: behavior-engine-routes-protocol-invocation
  @behavior:engine-routes-protocol-invocation
  Scenario: Dispatch routes to protocol execution
    Given the database contains protocol-horizon
    When I dispatch intent "protocol-horizon" with inputs
    Then the dispatch result is successful
    And the result contains protocol output

  @behavior:engine-routes-protocol-invocation
  Scenario: Dispatch resolves short protocol names
    Given the database contains protocol-horizon
    When I dispatch intent "horizon" with inputs
    Then the dispatch result is successful
    And the result contains protocol output

  # Behavior: behavior-engine-routes-primitive-invocation
  @behavior:engine-routes-primitive-invocation
  Scenario: Dispatch routes to primitive execution
    Given the database contains primitive-timestamp-now
    When I dispatch intent "primitive-timestamp-now" with empty inputs
    Then the dispatch result is successful
    And the result contains primitive output

  @behavior:engine-routes-primitive-invocation
  Scenario: Dispatch resolves short primitive names
    Given the database contains primitive-timestamp-now
    When I dispatch intent "timestamp_now" with empty inputs
    Then the dispatch result is successful
    And the result contains primitive output

  # Behavior: behavior-engine-lists-capabilities
  @behavior:engine-lists-capabilities
  Scenario: Engine lists all available capabilities
    Given the database contains multiple protocols and primitives
    When I request the capability list
    Then the list contains protocols
    And the list contains primitives
    And each capability has an id and kind

  # Error handling
  @behavior:engine-handles-errors-gracefully
  Scenario: Dispatch returns error for unknown intent
    When I dispatch intent "nonexistent-thing" with empty inputs
    Then the dispatch result is not successful
    And the error kind is "intent_not_found"

  # I/O Membrane integration
  @integration:io-membrane
  Scenario: Dispatch passes output sink to protocol
    Given an output capturing sink
    And the database contains protocol-manifest-entity
    When I dispatch intent "manifest-entity" through the engine with sink
    Then any protocol output flows through the sink
