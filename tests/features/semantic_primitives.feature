# Feature: Semantic Primitives
# Story: story-semantic-primitives-enable-inference
#
# Core primitives for embedding computation, similarity search, bond suggestion,
# and clustering - all with graceful degradation when chora-inference is unavailable.
#
# Follows principle-semantic-capability-degrades-gracefully:
# "Semantic features enhance but never gate core functionality."

Feature: Semantic Primitives
  As a dweller working with semantic relationships
  I want primitives that compute embeddings and find semantic connections
  So that the system understands meaning, not just structure

  Background:
    Given a fresh CVM database
    And a learning "learning-semantic-test" exists with content "Machine learning models learn patterns from data"
    And a principle "principle-patterns-emerge" exists with content "Patterns emerge from repeated observation"

  # Behavior: behavior-embed-entity-computes-and-stores-vector
  @behavior:embed-entity-computes-and-stores-vector
  Scenario: Embed entity computes and stores vector when inference available
    Given chora-inference is available
    When I call embed_entity for "learning-semantic-test"
    Then the embedding is stored in the database
    And the result includes method "semantic"
    And the result includes the entity_id

  @behavior:embed-entity-computes-and-stores-vector
  Scenario: Embed entity returns None gracefully when inference unavailable
    Given chora-inference is not available
    When I call embed_entity for "learning-semantic-test"
    Then the result embedding is None
    And the result includes method "fallback"
    And no exception is raised

  @behavior:embed-entity-computes-and-stores-vector
  Scenario: Embed entity for non-existent entity returns error
    When I call embed_entity for "entity-does-not-exist"
    Then the result includes an error message

  # Behavior: behavior-embed-text-computes-vector-for-arbitrary-text
  @behavior:embed-text-computes-vector-for-arbitrary-text
  Scenario: Embed text computes vector when inference available
    Given chora-inference is available
    When I call embed_text with "semantic similarity enables understanding"
    Then the result includes a vector
    And the result includes method "semantic"

  @behavior:embed-text-computes-vector-for-arbitrary-text
  Scenario: Embed text returns None gracefully when inference unavailable
    Given chora-inference is not available
    When I call embed_text with "semantic similarity enables understanding"
    Then the result vector is None
    And the result includes method "fallback"
    And no exception is raised

  # Behavior: behavior-semantic-similarity-computes-cosine-distance
  @behavior:semantic-similarity-computes-cosine-distance
  Scenario: Semantic similarity computes cosine when both embeddings exist
    Given chora-inference is available
    And "learning-semantic-test" has a stored embedding
    And "principle-patterns-emerge" has a stored embedding
    When I call semantic_similarity for "learning-semantic-test" and "principle-patterns-emerge"
    Then the result includes a similarity score between 0.0 and 1.0
    And the result includes method "semantic"

  @behavior:semantic-similarity-computes-cosine-distance
  Scenario: Semantic similarity returns 0.0 when embeddings missing
    Given chora-inference is not available
    When I call semantic_similarity for "learning-semantic-test" and "principle-patterns-emerge"
    Then the result similarity is 0.0
    And the result includes method "fallback"

  @behavior:semantic-similarity-computes-cosine-distance
  Scenario: Semantic similarity with identical entities returns 1.0
    Given chora-inference is available
    And "learning-semantic-test" has a stored embedding
    When I call semantic_similarity for "learning-semantic-test" and "learning-semantic-test"
    Then the result similarity is 1.0

  # Behavior: behavior-semantic-search-ranks-entities-by-meaning
  @behavior:semantic-search-ranks-entities-by-meaning
  Scenario: Semantic search ranks by meaning when inference available
    Given chora-inference is available
    And multiple entities exist with embeddings
    When I call semantic_search with query "learning from patterns"
    Then the results are ranked by semantic similarity
    And the result includes method "semantic"

  @behavior:semantic-search-ranks-entities-by-meaning
  Scenario: Semantic search falls back to FTS5 when inference unavailable
    Given chora-inference is not available
    And multiple entities exist
    When I call semantic_search with query "patterns"
    Then the results come from FTS5 search
    And the result includes method "fts5"

  @behavior:semantic-search-ranks-entities-by-meaning
  Scenario: Semantic search with type filter limits results
    Given chora-inference is available
    And multiple entities exist with embeddings
    When I call semantic_search with query "patterns" and type filter "learning"
    Then all results are of type "learning"

  # Behavior: behavior-suggest-bonds-finds-relationship-candidates
  @behavior:suggest-bonds-finds-relationship-candidates
  Scenario: Suggest bonds finds candidates using semantic similarity
    Given chora-inference is available
    And "learning-semantic-test" has a stored embedding
    And multiple entities exist with embeddings
    When I call suggest_bonds for "learning-semantic-test"
    Then the result includes candidate bonds
    And candidates are ranked by semantic similarity
    And the result includes method "semantic"

  @behavior:suggest-bonds-finds-relationship-candidates
  Scenario: Suggest bonds uses type-based suggestions when inference unavailable
    Given chora-inference is not available
    And multiple entities exist
    When I call suggest_bonds for "learning-semantic-test"
    Then the result includes candidate bonds based on type compatibility
    And the result includes method "type-based"

  @behavior:suggest-bonds-finds-relationship-candidates
  Scenario: Suggest bonds respects bond physics constraints
    Given chora-inference is available
    And "learning-semantic-test" has a stored embedding
    When I call suggest_bonds for "learning-semantic-test"
    Then candidates respect valid bond types for learning entities
    And suggestions include "surfaces" to principles
    And suggestions include "induces" to patterns

  # Behavior: behavior-detect-clusters-groups-similar-entities
  @behavior:detect-clusters-groups-similar-entities
  Scenario: Detect clusters groups by semantic similarity
    Given chora-inference is available
    And multiple entities exist with embeddings
    When I call detect_clusters for entity type "learning"
    Then the result includes clusters of similar entities
    And the result includes method "semantic"

  @behavior:detect-clusters-groups-similar-entities
  Scenario: Detect clusters uses keyword clustering when inference unavailable
    Given chora-inference is not available
    And multiple entities exist
    When I call detect_clusters for entity type "learning"
    Then the result includes clusters based on shared keywords
    And the result includes method "keyword"

  @behavior:detect-clusters-groups-similar-entities
  Scenario: Detect clusters returns empty for single entity
    Given only one entity of type "learning" exists
    When I call detect_clusters for entity type "learning"
    Then the result includes a single cluster with one entity
