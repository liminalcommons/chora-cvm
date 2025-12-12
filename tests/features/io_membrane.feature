# Feature: I/O Membrane — Decoupling Nucleus from Display
# Story: story-cvm-enables-multimodal-deployment
# Principle: principle-nucleus-is-silent
#
# The CVM Nucleus (logic) is decoupled from the Membrane (display).
# Output flows through an injected sink, enabling the same protocols
# to work across CLI, REST API, and MCP Server contexts.

Feature: I/O Membrane
  As a CVM protocol developer
  I want output to flow through an injected sink
  So that protocols work identically in CLI, API, and MCP contexts

  Background:
    Given the chora_cvm.std module is imported

  # Behavior: behavior-i-o-primitives-route-output-through-injected-sink
  @behavior:i-o-primitives-route-output-through-injected-sink
  Scenario: ui_render routes output through custom sink
    Given an execution context with a capturing output sink
    When ui_render is called with content "Hello World" and style "plain"
    Then the sink captures "Hello World"
    And the return value is {"status": "success", "rendered": True}

  @behavior:i-o-primitives-route-output-through-injected-sink
  Scenario: sys_log routes output through custom sink
    Given an execution context with a capturing output sink
    When sys_log is called with message "Test message"
    Then the sink captures "[CVM LOG] Test message"

  @behavior:i-o-primitives-route-output-through-injected-sink
  Scenario: ui_render box style routes through sink
    Given an execution context with a capturing output sink
    When ui_render is called with content "Box content" and style "box" and title "Test Box"
    Then the sink captures multiple lines including "Box content"
    And the sink captures box border characters

  # Behavior: behavior-i-o-primitives-fall-back-to-stdout-without-context
  @behavior:i-o-primitives-fall-back-to-stdout-without-context
  Scenario: ui_render falls back to stdout without context
    Given no execution context
    When ui_render is called with content "Fallback test" and style "plain"
    Then stdout receives "Fallback test"

  @behavior:i-o-primitives-fall-back-to-stdout-without-context
  Scenario: sys_log falls back to stdout without context
    Given no execution context
    When sys_log is called with message "Fallback log"
    Then stdout receives "[CVM LOG] Fallback log"

  @behavior:i-o-primitives-fall-back-to-stdout-without-context
  Scenario: ui_render with context but no sink falls back to stdout
    Given an execution context without an output sink
    When ui_render is called with content "No sink test" and style "plain"
    Then stdout receives "No sink test"
