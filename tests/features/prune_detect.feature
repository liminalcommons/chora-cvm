# Feature: Protocol Prune Detect — Physics-Driven Code Lifecycle
# Story: story-cvm-enables-self-healing
# Behaviors:
#   - behavior-prune-detect-finds-orphan-tools
#   - behavior-prune-detect-finds-deprecated-tools
#   - behavior-prune-detect-returns-standardized-shape
#   - behavior-prune-detect-respects-gpu-doctrine
#
# Entity lifecycle drives code lifecycle. The protocol orchestrates detection
# while primitives handle the heavy lifting (graph queries, AST parsing).

Feature: Protocol Prune Detect
  As a CVM maintainer
  I want to detect prunable entities through a protocol
  So that the system can self-heal and stay coherent

  Background:
    Given a bootstrapped CVM database with prune detection primitives

  # Behavior: behavior-prune-detect-finds-orphan-tools
  @behavior:prune-detect-finds-orphan-tools
  Scenario: Detect tools without implements bonds
    Given the database contains a tool "tool-orphan" with no implements bond
    And the database contains a tool "tool-healthy" with an implements bond
    When the prune-detect protocol is executed
    Then the result status is "success"
    And the result data contains orphan_tools
    And orphan_tools includes "tool-orphan"
    And orphan_tools does not include "tool-healthy"

  # Behavior: behavior-prune-detect-finds-deprecated-tools
  @behavior:prune-detect-finds-deprecated-tools
  Scenario: Detect tools marked as deprecated
    Given the database contains a tool "tool-deprecated" with status "deprecated"
    And the database contains a tool "tool-active" with status "active"
    When the prune-detect protocol is executed
    Then the result status is "success"
    And the result data contains deprecated_tools
    And deprecated_tools includes "tool-deprecated"
    And deprecated_tools does not include "tool-active"

  # Behavior: behavior-prune-detect-returns-standardized-shape
  @behavior:prune-detect-returns-standardized-shape
  Scenario: Protocol returns standard response shape
    When the prune-detect protocol is executed
    Then the result has key "status"
    And the result has key "data"
    And the data has key "orphan_tools"
    And the data has key "deprecated_tools"
    And the data has key "summary"

  # Behavior: behavior-prune-detect-respects-internal-flag
  @behavior:prune-detect-respects-internal-flag
  Scenario: Internal tools are excluded from orphan detection
    Given the database contains an internal tool "tool-internal-util"
    When the prune-detect protocol is executed
    Then orphan_tools does not include "tool-internal-util"

  # Integration: Dispatch through CvmEngine
  @integration:engine-dispatch
  Scenario: Prune detect accessible via engine dispatch
    When I dispatch "prune-detect" through CvmEngine
    Then the dispatch result is successful
    And the result data contains a summary
