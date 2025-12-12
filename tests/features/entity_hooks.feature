Feature: Entity Save Hooks
  As a sync system
  I need to observe when entities are saved
  So I can emit changes to the cloud

  Background:
    Given a fresh EventStore database

  # Behavior: behavior-store-fires-hook-when-entity-saved
  @behavior:store-fires-hook-when-entity-saved
  Scenario: Hook is called when entity is saved via save_generic_entity
    Given a registered entity save hook
    When I save an entity with id "test-entity" type "note" and data '{"title": "Hello"}'
    Then the hook should be called once
    And the hook should receive entity_id "test-entity"
    And the hook should receive entity_type "note"
    And the hook should receive data containing title "Hello"

  @behavior:store-fires-hook-when-entity-saved
  Scenario: Hook is called after database commit succeeds
    Given a registered entity save hook
    When I save an entity with id "test-entity" type "note" and data '{"title": "Hello"}'
    Then the entity should exist in the database before hook completes

  Scenario: Multiple hooks are all called
    Given two registered entity save hooks
    When I save an entity with id "test-entity" type "note" and data '{"title": "Hello"}'
    Then both hooks should be called

  Scenario: Hook can be removed
    Given a registered entity save hook
    And the hook is removed
    When I save an entity with id "test-entity" type "note" and data '{"title": "Hello"}'
    Then the hook should not be called

  Scenario: Hook is called for save_entity with Pydantic model
    Given a registered entity save hook
    When I save a PrimitiveEntity via save_entity
    Then the hook should be called once
