# Feature: Documentation Self-Maintenance
# Story: story-docs-detect-their-own-staleness
# Story: story-docs-auto-repair-syntactic-issues
# Story: story-docs-propose-semantic-changes-for-review
# Principle: principle-documentation-emerges-from-the-entity-graph-not-manual-editing
#
# Documentation maintains itself through detection, auto-repair (syntactic),
# and human approval (semantic). Two types of staleness require different responses:
# - Syntactic: broken refs, outdated paths → auto-repair
# - Semantic: principle drift, unsurfaced research → Focus for human review

Feature: Documentation Self-Maintenance
  As a Chora dweller
  I want documentation to maintain itself
  So that I always work from accurate context

  Background:
    Given a fresh CVM database
    And a temporary workspace directory

  # ===========================================================================
  # Detection (Phase 2 - Detection Only)
  # ===========================================================================

  # Behavior: behavior-detect-doc-voids-emits-signals
  @behavior:detect-doc-voids-emits-signals
  Scenario: Detect stale reference emits signal
    Given a CLAUDE.md with reference to "packages/nonexistent/file.py"
    When I run doc detection
    Then a signal "doc-stale-ref" should be emitted
    And the signal data should contain the stale path "packages/nonexistent/file.py"

  @behavior:detect-doc-voids-emits-signals
  Scenario: Detect unsurfaced research emits signal
    Given an inquiry file "docs/research/inquiry-autoevolution.md" not mentioned in main docs
    When I run doc detection
    Then a signal "doc-unsurfaced-research" should be emitted
    And the signal data should reference "inquiry-autoevolution"

  @behavior:detect-doc-voids-emits-signals
  Scenario: Detect missing CLAUDE.md emits signal
    Given a package directory "packages/orphan-pkg" without CLAUDE.md
    When I run doc detection
    Then a signal "doc-missing-claude-md" should be emitted
    And the signal data should reference "orphan-pkg"

  @behavior:detect-doc-voids-emits-signals
  Scenario: Detect outdated noun count emits signal
    Given an AGENTS.md with reference to "7 Nouns"
    When I run doc detection
    Then a signal "doc-outdated-count" should be emitted
    And the signal data should indicate the stale reference

  @behavior:detect-doc-voids-emits-signals
  Scenario: No issues detected emits no signals
    Given a well-formed workspace with no doc issues
    When I run doc detection
    Then no signals should be emitted

  # ===========================================================================
  # Syntactic Auto-Repair (Phase 3 - Requires Validation Gate)
  # ===========================================================================

  # Behavior: behavior-repair-syntactic-fixes-broken-refs
  @behavior:repair-syntactic-fixes-broken-refs
  Scenario: Repair comments out broken reference
    Given a signal "doc-stale-ref" for path "packages/old/removed.py"
    And a CLAUDE.md containing a reference to that path
    When the repair protocol runs
    Then the reference should be commented with "<!-- STALE: path not found -->"
    And the signal should be resolved

  @behavior:repair-syntactic-fixes-broken-refs
  Scenario: Repair in dry-run mode shows changes without applying
    Given a signal "doc-stale-ref" for path "packages/old/removed.py"
    And a CLAUDE.md containing a reference to that path
    When the repair protocol runs in dry-run mode
    Then the proposed change should be shown
    And the file should not be modified
    And the signal should remain active

  @behavior:repair-syntactic-fixes-broken-refs
  Scenario: Repair creates backup before modification
    Given a signal "doc-stale-ref" for path "packages/old/removed.py"
    And a CLAUDE.md containing a reference to that path
    When the repair protocol runs
    Then a backup file should exist with .bak extension

  # ===========================================================================
  # Semantic Proposal (Phase 4)
  # ===========================================================================

  # Behavior: behavior-propose-semantic-creates-focus
  @behavior:propose-semantic-creates-focus
  Scenario: Semantic issue creates Focus for review
    Given a signal "doc-unsurfaced-research" for "inquiry-autoevolution"
    When the propose protocol runs
    Then a Focus should be created with title containing "inquiry-autoevolution"
    And the Focus review_data should contain the proposed integration text
    And the Focus should have status "active"

  @behavior:propose-semantic-creates-focus
  Scenario: Outdated noun count creates Focus for review
    Given a signal "doc-outdated-count" for "7 Nouns"
    When the propose protocol runs
    Then a Focus should be created for reviewing the noun count update
    And the Focus review_data should contain the proposed correction

  # ===========================================================================
  # Approval Flow (Phase 4)
  # ===========================================================================

  # Behavior: behavior-approve-applies-change
  @behavior:approve-applies-change
  Scenario: Approving Focus applies the change
    Given a Focus for doc change with target file and proposed content
    When I approve the Focus
    Then the change should be applied to the target file
    And the Focus should be resolved with outcome "approved"

  @behavior:approve-applies-change
  Scenario: Approving Focus creates backup
    Given a Focus for doc change with target file and proposed content
    When I approve the Focus
    Then a backup of the original file should exist

  # Behavior: behavior-reject-captures-learning
  @behavior:reject-captures-learning
  Scenario: Rejecting Focus creates learning
    Given a Focus for doc change
    When I reject the Focus with reason "Not relevant to current work"
    Then a learning should be created with the rejection reason
    And the learning should be bonded to the Focus
    And the Focus should be resolved with outcome "rejected"

  @behavior:reject-captures-learning
  Scenario: Rejecting Focus with pattern suggestion
    Given a Focus for doc change
    When I reject the Focus with reason "This pattern is deprecated" and suggest "Use new pattern X"
    Then a learning should be created capturing both reason and suggestion
    And the Focus should be resolved with outcome "rejected"
