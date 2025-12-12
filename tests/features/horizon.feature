# Feature: Protocol Horizon — Semantic Attention Guidance
# Story: story-cvm-enables-multimodal-deployment
# Behaviors:
#   - behavior-horizon-protocol-returns-semantic-recommendations
#   - behavior-horizon-gracefully-handles-cold-start
#
# The Horizon protocol identifies unverified tools that are semantically
# near recent learnings, helping prioritize verification work.

Feature: Protocol Horizon
  As a CVM user
  I want the horizon command to show what needs attention
  So that I can prioritize verification work based on semantic relevance

  Background:
    Given a bootstrapped CVM database with protocol-horizon

  # Behavior: behavior-horizon-protocol-returns-semantic-recommendations
  @behavior:horizon-protocol-returns-semantic-recommendations
  Scenario: Semantic ranking with recent learnings and unverified tools
    Given the database contains recent learnings with embeddings
    And the database contains unverified tools with embeddings
    When the horizon protocol is executed
    Then the result method is "semantic"
    And the result contains ranked recommendations
    And recommendations are ordered by similarity

  # Behavior: behavior-horizon-gracefully-handles-cold-start
  @behavior:horizon-gracefully-handles-cold-start
  Scenario: Cold start with no recent learnings
    Given the database contains no recent learnings
    And the database contains unverified tools
    When the horizon protocol is executed
    Then the result method is "cold_start"
    And the result contains empty recommendations
    And the result contains a note explaining why
    And the result still lists unverified tools

  @behavior:horizon-gracefully-handles-cold-start
  Scenario: Empty system with no learnings and no tools
    Given the database contains no recent learnings
    And the database contains no unverified tools
    When the horizon protocol is executed
    Then the result method is "cold_start"
    And the result contains empty recommendations
    And the result contains empty unverified tools

  # Integration with I/O Membrane
  @integration:io-membrane
  Scenario: Horizon output flows through I/O membrane
    Given an execution context with a capturing output sink
    And the database contains recent learnings with embeddings
    And the database contains unverified tools with embeddings
    When the horizon protocol is executed with the context
    Then any ui_render calls route through the sink
