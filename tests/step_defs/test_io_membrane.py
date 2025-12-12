"""
Step definitions for I/O Membrane feature.

These tests verify the behaviors for output sink injection:
- behavior-i-o-primitives-route-output-through-injected-sink
- behavior-i-o-primitives-fall-back-to-stdout-without-context

BDD Flow: Feature file -> Step definitions -> Implementation
Tests should FAIL initially until schema.py and std.py are updated.
"""
from io import StringIO
from typing import Any, List
from unittest.mock import patch

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

# Load scenarios from feature file
scenarios("../features/io_membrane.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {
        "captured_output": [],
        "ctx": None,
        "result": None,
        "stdout_capture": None,
    }


# =============================================================================
# Background Steps
# =============================================================================


@given("the chora_cvm.std module is imported")
def import_std_module(test_context):
    """Import the std module for testing."""
    from chora_cvm import std

    test_context["std"] = std


# =============================================================================
# Context Setup Steps
# =============================================================================


@given("an execution context with a capturing output sink")
def setup_context_with_sink(test_context):
    """Create an ExecutionContext with a list-capturing sink."""
    from chora_cvm.schema import ExecutionContext

    captured: List[str] = []
    test_context["captured_output"] = captured

    # Create context with capturing sink
    test_context["ctx"] = ExecutionContext(
        db_path="/tmp/test.db",
        output_sink=captured.append,
    )


@given("no execution context")
def no_context(test_context):
    """No context - primitives should fall back to stdout."""
    test_context["ctx"] = None


@given("an execution context without an output sink")
def context_without_sink(test_context):
    """Create an ExecutionContext without a sink (should fall back to stdout)."""
    from chora_cvm.schema import ExecutionContext

    test_context["ctx"] = ExecutionContext(
        db_path="/tmp/test.db",
        output_sink=None,
    )


# =============================================================================
# When Steps - ui_render
# =============================================================================


@when(parsers.parse('ui_render is called with content "{content}" and style "{style}"'))
def call_ui_render_plain(test_context, content: str, style: str):
    """Call ui_render with the given content and style."""
    std = test_context["std"]
    ctx = test_context.get("ctx")

    # Capture stdout if no custom sink
    if ctx is None or (ctx and ctx.output_sink is None):
        with patch("builtins.print") as mock_print:
            test_context["mock_print"] = mock_print
            test_context["result"] = std.ui_render(content, style=style, _ctx=ctx)
    else:
        test_context["result"] = std.ui_render(content, style=style, _ctx=ctx)


@when(
    parsers.parse(
        'ui_render is called with content "{content}" and style "{style}" and title "{title}"'
    )
)
def call_ui_render_with_title(test_context, content: str, style: str, title: str):
    """Call ui_render with content, style, and title."""
    std = test_context["std"]
    ctx = test_context.get("ctx")

    if ctx is None or (ctx and ctx.output_sink is None):
        with patch("builtins.print") as mock_print:
            test_context["mock_print"] = mock_print
            test_context["result"] = std.ui_render(
                content, style=style, title=title, _ctx=ctx
            )
    else:
        test_context["result"] = std.ui_render(content, style=style, title=title, _ctx=ctx)


# =============================================================================
# When Steps - sys_log
# =============================================================================


@when(parsers.parse('sys_log is called with message "{message}"'))
def call_sys_log(test_context, message: str):
    """Call sys_log with the given message."""
    std = test_context["std"]
    ctx = test_context.get("ctx")

    if ctx is None or (ctx and ctx.output_sink is None):
        with patch("builtins.print") as mock_print:
            test_context["mock_print"] = mock_print
            std.sys_log(message, _ctx=ctx)
    else:
        std.sys_log(message, _ctx=ctx)


# =============================================================================
# Then Steps - Sink Capture
# =============================================================================


@then(parsers.parse('the sink captures "{expected}"'))
def sink_captures_exact(test_context, expected: str):
    """Verify the sink captured the expected output."""
    captured = test_context["captured_output"]
    assert (
        expected in captured
    ), f"Expected '{expected}' in captured output: {captured}"


@then(parsers.parse('the sink captures multiple lines including "{expected}"'))
def sink_captures_including(test_context, expected: str):
    """Verify the sink captured output including the expected text."""
    captured = test_context["captured_output"]
    all_output = "\n".join(captured)
    assert expected in all_output, f"Expected '{expected}' in output: {all_output}"


@then("the sink captures box border characters")
def sink_captures_box_borders(test_context):
    """Verify the sink captured box border characters."""
    captured = test_context["captured_output"]
    all_output = "\n".join(captured)
    # Check for Unicode box-drawing characters
    assert any(
        char in all_output for char in ["╭", "╮", "╰", "╯", "│", "─"]
    ), f"Expected box characters in output: {all_output}"


@then(parsers.parse("the return value is {expected}"))
def check_return_value(test_context, expected: str):
    """Verify the return value matches expected."""
    import json

    expected_dict = json.loads(expected.replace("True", "true").replace("False", "false"))
    assert test_context["result"] == expected_dict


# =============================================================================
# Then Steps - Stdout Fallback
# =============================================================================


@then(parsers.parse('stdout receives "{expected}"'))
def stdout_receives(test_context, expected: str):
    """Verify that print was called with the expected output."""
    mock_print = test_context.get("mock_print")
    assert mock_print is not None, "print was not mocked"

    # Check all print calls
    all_calls = [str(call) for call in mock_print.call_args_list]
    called_with = [
        call[0][0] if call[0] else "" for call in mock_print.call_args_list
    ]

    assert any(
        expected in output for output in called_with
    ), f"Expected '{expected}' in print calls: {called_with}"
