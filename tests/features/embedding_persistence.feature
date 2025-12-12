# Feature: Embedding Persistence
# Story: story-embeddings-persist-in-sqlite
#
# Entity embeddings are stored persistently in SQLite, enabling fast
# similarity lookups without recomputation. Embeddings are computed lazily,
# cached persistently, and invalidated when entities are updated.
#
# Follows principle-embeddings-are-per-entity-truth:
# "Each entity has exactly one canonical embedding vector."

Feature: Embedding Persistence
  As a semantic system
  I want entity embeddings stored persistently in SQLite
  So they survive across sessions and enable fast similarity lookups

  Background:
    Given a fresh CVM database
    And a learning "learning-test-semantic" exists with title "Test learning about semantics"

  # Behavior: behavior-embedding-stored-for-entity
  @behavior:embedding-stored-for-entity
  Scenario: Store embedding for entity
    When I compute and store an embedding for "learning-test-semantic"
    Then the embeddings table contains a row for "learning-test-semantic"
    And the embedding has model_name "text-embedding-3-small"
    And the embedding has dimension 1536
    And the embedding has valid timestamps

  @behavior:embedding-stored-for-entity
  Scenario: Store embedding with custom model
    When I compute and store an embedding for "learning-test-semantic" with model "text-embedding-ada-002"
    Then the embedding has model_name "text-embedding-ada-002"

  @behavior:embedding-stored-for-entity
  Scenario: Embedding vector is stored as BLOB
    When I compute and store an embedding for "learning-test-semantic"
    Then the vector can be retrieved and deserialized to numpy array
    And the vector has the correct number of dimensions

  # Behavior: behavior-embedding-retrieved-by-entity-id
  @behavior:embedding-retrieved-by-entity-id
  Scenario: Retrieve stored embedding by entity_id
    Given an embedding exists for "learning-test-semantic"
    When I retrieve the embedding for "learning-test-semantic"
    Then I get a numpy array with 1536 dimensions
    And the embedding is normalized (unit vector)

  @behavior:embedding-retrieved-by-entity-id
  Scenario: Retrieve non-existent embedding returns None
    When I retrieve the embedding for "entity-does-not-exist"
    Then the result is None

  @behavior:embedding-retrieved-by-entity-id
  Scenario: Embedding retrieval is fast (cached)
    Given an embedding exists for "learning-test-semantic"
    When I retrieve the embedding 100 times
    Then all retrievals complete in under 100ms total

  # Behavior: behavior-embedding-invalidated-on-entity-update
  @behavior:embedding-invalidated-on-entity-update
  Scenario: Update entity data invalidates embedding
    Given an embedding exists for "learning-test-semantic"
    When I update the entity "learning-test-semantic" with new data
    Then the embedding for "learning-test-semantic" no longer exists
    And the embedding must be recomputed

  @behavior:embedding-invalidated-on-entity-update
  Scenario: Delete entity cascades to embedding
    Given an embedding exists for "learning-test-semantic"
    When I delete the entity "learning-test-semantic"
    Then the embedding for "learning-test-semantic" no longer exists

  # Graceful degradation (principle-semantic-capability-degrades-gracefully)
  @graceful-degradation
  Scenario: Embedding operations work when inference unavailable
    Given chora-inference is not installed
    When I attempt to compute an embedding for "learning-test-semantic"
    Then the operation returns None gracefully
    And no exception is raised
    And the result includes method "fallback"

  # Schema correctness
  Scenario: Embeddings table has correct schema
    Then the embeddings table exists
    And it has columns: entity_id, model_name, vector, dimension, created_at, updated_at
    And entity_id is the primary key
    And entity_id has foreign key to entities(id) with CASCADE delete
