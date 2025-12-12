# Feature: Prune Approval and Rejection — Human Gate for Code Lifecycle
# Story: story-prune-approval-rejection-flow
# Principle: principle-logic-derives-from-graph
#
# Entity lifecycle drives code lifecycle. Before any entity is removed,
# the system proposes via Focus and captures wisdom as Learning.
# Nothing is deleted without consent and provenance.

Feature: Prune Approval and Rejection
  As a Chora dweller
  I want to approve or reject prune proposals
  So that unused code is removed with captured wisdom

  Background:
    Given a fresh CVM database
    And axiom entities define the physics rules

  # Behavior: behavior-prune-approve-composts-and-learns
  @behavior:prune-approve-composts-and-learns
  Scenario: Approve composts deprecated tool and creates learning
    Given a tool "tool-deprecated-export" exists with status "deprecated"
    And a focus "focus-prune-tool-deprecated-export-abc123" exists for prune proposal
    And the focus references tool_id "tool-deprecated-export"
    When prune_approve is invoked with focus_id "focus-prune-tool-deprecated-export-abc123"
    Then the tool "tool-deprecated-export" is composted
    And a learning entity is created with title containing "Pruned:"
    And a crystallized-from bond connects the learning to the archived entity
    And the focus status becomes "resolved"
    And the focus outcome is "completed"

  @behavior:prune-approve-composts-and-learns
  Scenario: Approve extracts wisdom from tool phenomenology
    Given a tool "tool-legacy-sync" exists with phenomenology "Synchronizes data when network available"
    And a focus exists for prune proposal of "tool-legacy-sync"
    When prune_approve is invoked with the focus_id
    Then the created learning insight includes "Synchronizes data when network available"
    And the learning domain is "prune"

  @behavior:prune-approve-composts-and-learns
  Scenario: Approve fails if focus does not exist
    When prune_approve is invoked with focus_id "focus-nonexistent"
    Then the operation returns an error
    And the error message says "Focus not found"

  @behavior:prune-approve-composts-and-learns
  Scenario: Approve fails if focus is not a prune proposal
    Given a focus "focus-other-work" exists with category "work"
    When prune_approve is invoked with focus_id "focus-other-work"
    Then the operation returns an error
    And the error message says "Focus is not a prune proposal"

  @behavior:prune-approve-composts-and-learns
  Scenario: Approve fails if target entity does not exist
    Given a focus "focus-prune-ghost" exists for prune proposal
    And the focus references tool_id "tool-nonexistent"
    When prune_approve is invoked with focus_id "focus-prune-ghost"
    Then the operation returns an error
    And the error message says "Target entity not found"

  # Behavior: behavior-prune-reject-captures-reason
  @behavior:prune-reject-captures-reason
  Scenario: Reject captures reason as learning
    Given a tool "tool-seems-unused" exists
    And a focus "focus-prune-tool-seems-unused-def456" exists for prune proposal
    When prune_reject is invoked with focus_id "focus-prune-tool-seems-unused-def456" and reason "Actually still needed for edge case X"
    Then a learning entity is created with title containing "Prune rejected"
    And the learning insight includes "Actually still needed for edge case X"
    And the focus status becomes "resolved"
    And the focus outcome is "abandoned"
    And the tool "tool-seems-unused" status remains unchanged

  @behavior:prune-reject-captures-reason
  Scenario: Reject without reason uses default message
    Given a focus exists for prune proposal of "tool-keep-this"
    When prune_reject is invoked with focus_id and no reason
    Then a learning is created with insight "Rejected without specified reason"
    And the focus is resolved with outcome "abandoned"

  @behavior:prune-reject-captures-reason
  Scenario: Reject fails if focus does not exist
    When prune_reject is invoked with focus_id "focus-nonexistent"
    Then the operation returns an error
    And the error message says "Focus not found"

  @behavior:prune-reject-captures-reason
  Scenario: Reject fails if focus already resolved
    Given a focus "focus-already-done" exists with status "resolved"
    When prune_reject is invoked with focus_id "focus-already-done"
    Then the operation returns an error
    And the error message says "Focus already resolved"

  # ==========================================================================
  # Integration: Protocol Dispatch through CvmEngine
  # ==========================================================================

  @integration:engine-dispatch-prune-approve
  Scenario: Prune approval accessible via CvmEngine dispatch
    Given a tool "tool-via-protocol" exists with status "deprecated"
    And a focus "focus-prune-tool-via-protocol-xyz" exists for prune proposal
    And the focus references tool_id "tool-via-protocol"
    When I dispatch "prune-approve" through CvmEngine with focus_id "focus-prune-tool-via-protocol-xyz"
    Then the dispatch result is successful
    And the result data shows archived is true
    And a learning entity exists for the pruned tool

  @integration:engine-dispatch-prune-reject
  Scenario: Prune rejection accessible via CvmEngine dispatch
    Given a tool "tool-reject-via-protocol" exists
    And a focus "focus-prune-reject-via-protocol-abc" exists for prune proposal
    And the focus references tool_id "tool-reject-via-protocol"
    When I dispatch "prune-reject" through CvmEngine with focus_id "focus-prune-reject-via-protocol-abc" and reason "Testing protocol path"
    Then the dispatch result is successful
    And a learning entity is created with title containing "Prune rejected"
    And the tool "tool-reject-via-protocol" status remains unchanged
