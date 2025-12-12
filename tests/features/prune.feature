# Feature: Prune Detection — Physics-Driven Code Lifecycle
# Story: story-system-can-prune-unused-code-and-entities
# Pattern: pattern-tool-creation (pilot validation)
#
# Entity lifecycle drives code lifecycle. The graph is the source of truth;
# code becomes prunable when its corresponding entities are orphaned,
# deprecated, or have broken handlers.
#
# Based on the principle: "Logic derives from the graph"

Feature: Prune Detection
  As a Chora dweller
  I want to detect prunable entities and code
  So that the codebase stays clean and aligned with the graph

  Background:
    Given a fresh CVM database
    And axiom entities define the physics rules

  # Behavior: behavior-prune-detects-orphan-and-deprecated-tools
  @behavior:prune-detects-orphan-and-deprecated-tools
  Scenario: Detect orphan tools with no behavior implementing them
    Given a tool "tool-orphan-widget" exists with no implements bond
    When detect_prunable is invoked
    Then the report includes "tool-orphan-widget" in orphan_tools

  @behavior:prune-detects-orphan-and-deprecated-tools
  Scenario: Tool with implements bond is not orphaned
    Given a tool "tool-verified-feature" exists
    And a behavior "behavior-feature-works" implements "tool-verified-feature"
    When detect_prunable is invoked
    Then the report does not include "tool-verified-feature" in orphan_tools

  @behavior:prune-detects-orphan-and-deprecated-tools
  Scenario: Detect deprecated tools
    Given a tool "tool-legacy-export" exists with status "deprecated"
    When detect_prunable is invoked
    Then the report includes "tool-legacy-export" in deprecated_tools
    And the deprecation reason starts with "Marked deprecated"

  @behavior:prune-detects-orphan-and-deprecated-tools
  Scenario: Active tool is not marked deprecated
    Given a tool "tool-active-feature" exists with status "active"
    When detect_prunable is invoked
    Then the report does not include "tool-active-feature" in deprecated_tools

  # Behavior: behavior-prune-emits-signals-for-threshold-breaches
  @behavior:prune-emits-signals-for-threshold-breaches
  Scenario: Emit signal when orphan count exceeds threshold
    Given 4 orphan tools exist
    When emit_prune_signals is invoked
    Then a signal entity is created with category "orphan-tools"
    And the signal count is 4

  @behavior:prune-emits-signals-for-threshold-breaches
  Scenario: No signal when orphan count below threshold
    Given 2 orphan tools exist
    When emit_prune_signals is invoked
    Then no signal is emitted for "orphan-tools"

  @behavior:prune-emits-signals-for-threshold-breaches
  Scenario: Emit signal for any deprecated tools
    Given a tool "tool-deprecated-1" exists with status "deprecated"
    When emit_prune_signals is invoked
    Then a signal entity is created with category "deprecated-tools"

  @behavior:prune-emits-signals-for-threshold-breaches
  Scenario: Dry run does not emit signals
    Given 4 orphan tools exist
    When emit_prune_signals is invoked with dry_run
    Then no signal entities are created

  # Behavior: behavior-prune-proposes-focus-for-human-approval
  @behavior:prune-proposes-focus-for-human-approval
  Scenario: Propose focus for deprecated tool
    Given a tool "tool-old-sync" exists with status "deprecated"
    When propose_prune is invoked
    Then a focus entity is created for "tool-old-sync"
    And the focus category is "deprecated"
    And the focus references the tool_id

  @behavior:prune-proposes-focus-for-human-approval
  Scenario: Dry run does not create focus entities
    Given a tool "tool-deprecated-dry" exists with status "deprecated"
    When propose_prune is invoked with dry_run
    Then no focus entities are created
