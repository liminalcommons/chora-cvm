Feature: Sync Bridge
  As a sync system
  I need to bridge store events to the sync layer
  So entity changes can be emitted to cloud circles

  Background:
    Given a fresh EventStore with keyring

  Scenario: SyncBridge queues changes for cloud-synced entities
    Given an entity "note-1" that inhabits a cloud circle
    And a SyncBridge connected to the store
    When I save entity "note-1" with data '{"title": "Hello"}'
    Then the bridge should have 1 pending change
    And the pending change should have entity_id "note-1"
    And the pending change should have the circle_id

  Scenario: SyncBridge ignores local-only entities
    Given an entity "note-2" that inhabits a local-only circle
    And a SyncBridge connected to the store
    When I save entity "note-2" with data '{"title": "Private"}'
    Then the bridge should have 0 pending changes

  Scenario: SyncBridge invokes callback when changes are ready
    Given an entity "note-3" that inhabits a cloud circle
    And a SyncBridge connected to the store
    And a change callback is registered
    When I save entity "note-3" with data '{"title": "Callback test"}'
    Then the callback should have been invoked with 1 change

  Scenario: flush_changes returns and clears pending
    Given an entity "note-4" that inhabits a cloud circle
    And a SyncBridge connected to the store
    When I save entity "note-4" with data '{"title": "Flush test"}'
    And I flush the changes
    Then flush should return 1 change
    And the bridge should have 0 pending changes

  Scenario: close removes the hook
    Given an entity "note-5" that inhabits a cloud circle
    And a SyncBridge connected to the store
    When I close the bridge
    And I save entity "note-5" with data '{"title": "After close"}'
    Then the bridge should have 0 pending changes
