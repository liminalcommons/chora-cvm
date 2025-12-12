# Feature: Legacy Content Harvest
# Story: story-agents-can-search-legacy-content-to-understand-historical-patterns-and-decisions
# Inquiry: inquiry-what-is-it-like-when-agents-need-to-research-historical-content-from-legacy-repositories
#
# Harvest content from legacy repositories into a searchable FTS5 database.
# Handles deduplication, multi-format parsing, and priority-based canonical selection.

Feature: Legacy Content Harvest
  As an agent in this workspace
  I want to search legacy content from historical repositories
  So that I can understand past patterns, decisions, and research

  Background:
    Given a temporary test database

  # Behavior: behavior-harvester-indexes-files-from-configured-repositories
  @behavior:harvester-indexes-files-from-configured-repositories
  Scenario: Harvester discovers and indexes markdown files
    Given a test repository "test-repo" with priority 10
    And a markdown file "docs/test.md" with content:
      """
      # Test Document

      ## Section One
      Content of section one.

      ## Section Two
      Content of section two.
      """
    When I run the harvester
    Then the database should contain document "docs/test.md"
    And the database should contain 2 chunks for that document

  @behavior:harvester-indexes-files-from-configured-repositories
  Scenario: Harvester respects exclude patterns
    Given a test repository "test-repo" with priority 10
    And exclude pattern "*.pyc"
    And a file "test.pyc" exists
    And a file "test.py" exists
    When I run the harvester
    Then the database should not contain "test.pyc"
    And the database should contain "test.py"

  @behavior:harvester-indexes-files-from-configured-repositories
  Scenario: Harvester respects include patterns
    Given a test repository "test-repo" with include patterns ["docs/**/*.md"]
    And a file "docs/included.md" exists
    And a file "other/excluded.md" exists
    When I run the harvester
    Then the database should contain "docs/included.md"
    And the database should not contain "other/excluded.md"

  # Behavior: behavior-harvester-deduplicates-content-by-priority
  @behavior:harvester-deduplicates-content-by-priority
  Scenario: Higher priority repository becomes canonical
    Given a test repository "high-priority" with priority 10
    And a test repository "low-priority" with priority 5
    And identical content "Same content" in both repositories
    When I run the harvester
    Then the "high-priority" version should be canonical
    And the "low-priority" version should be marked as duplicate

  @behavior:harvester-deduplicates-content-by-priority
  Scenario: Equal priority prefers first encountered
    Given a test repository "repo-a" with priority 10
    And a test repository "repo-b" with priority 10
    And identical content in both repositories
    When I harvest "repo-a" first then "repo-b"
    Then the "repo-a" version should be canonical
    And the "repo-b" version should be marked as duplicate

  @behavior:harvester-deduplicates-content-by-priority
  Scenario: Duplicates are not indexed for search
    Given duplicate content exists across repositories
    When I run the harvester
    Then only the canonical version should have chunks in chunks_fts

  # Behavior: behavior-search-returns-relevant-chunks-for-fts-query
  @behavior:search-returns-relevant-chunks-for-fts-query
  Scenario: Search finds matching content
    Given a harvested document containing "skilled awareness package"
    When I search for "skilled awareness"
    Then the search should return that document
    And the result should include repository name
    And the result should include document title
    And the result should include a content snippet

  @behavior:search-returns-relevant-chunks-for-fts-query
  Scenario: Search excludes duplicate documents
    Given a canonical document containing "test content"
    And a duplicate document containing the same "test content"
    When I search for "test content"
    Then only the canonical version should appear in results

  @behavior:search-returns-relevant-chunks-for-fts-query
  Scenario: Search respects limit parameter
    Given 50 documents matching "common term"
    When I search for "common term" with limit 10
    Then the search should return exactly 10 results

  # Behavior: behavior-harvester-extracts-archive-content
  @behavior:harvester-extracts-archive-content
  Scenario: Harvester indexes archive repositories when flag is set
    Given archive repositories are configured
    And archive/v4/chora-store contains Python files
    When I run the harvester with --archive flag
    Then the database should contain documents from "archive-v4-store"
    And Python files should be parsed for functions and classes

  @behavior:harvester-extracts-archive-content
  Scenario: Archive repositories have lower priority than active packages
    Given "chora-base" repository with priority 10
    And "archive-v4-store" repository with priority 6
    And identical content exists in both
    When I run the harvester with --archive flag
    Then the "chora-base" version should be canonical
    And the "archive-v4-store" version should be marked as duplicate

  @behavior:harvester-extracts-archive-content
  Scenario: Feature files from archive are indexed
    Given archive/v4/chora-store contains feature files
    When I run the harvester with --archive flag
    Then feature file scenarios should be searchable
    And @behavior tags should be extractable

  # Behavior: behavior-harvester-extracts-legacy-entities
  @behavior:harvester-extracts-legacy-entities
  Scenario: Entity extractor reads entities from legacy database
    Given a legacy database "chora-old-v3.db" exists
    And it contains story entities
    And it contains behavior entities
    When I run the entity harvester
    Then legacy_entities table should contain those entities
    And entity content should include name and description

  @behavior:harvester-extracts-legacy-entities
  Scenario: Entity extractor extracts relationships
    Given a legacy database with relationship entities
    When I run the entity harvester
    Then legacy_relationships table should contain those relationships
    And relationship bond_type should be preserved

  @behavior:harvester-extracts-legacy-entities
  Scenario: Entity search finds historical decisions
    Given harvested entities from "chora-old-v3.db"
    And a story entity with title "Agents can navigate"
    When I search legacy entities for "navigate"
    Then the search should return that story
    And the result should include source database name
    And the result should include entity type

  @behavior:harvester-extracts-legacy-entities
  Scenario: Multiple legacy databases are harvested
    Given legacy databases "chora-old-v3.db" and "chora-old.db" exist
    When I run the entity harvester
    Then entities from both databases should be indexed
    And source_db field distinguishes their origin
