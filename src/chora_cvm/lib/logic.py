"""
Domain: Logic (Data Manipulation)
ID Prefix: logic.*

Pure data transformation primitives for JSON, lists, and strings.
These enable complex data flows within protocols without side effects.

Primitives:
  - logic.json.get: Extract value from nested JSON using dot-notation
  - logic.json.set: Set value in nested JSON using dot-notation
  - logic.list.map: Extract field from each item in a list
  - logic.list.filter: Filter list items by predicate
  - logic.list.sort: Sort list by field
  - logic.string.format: Format string template with values
"""
from __future__ import annotations

import copy
from typing import Any, Callable, Dict, List

from ..schema import ExecutionContext


def json_get(
    data: Dict[str, Any],
    path: str,
    _ctx: ExecutionContext,
    default: Any = None,
) -> Dict[str, Any]:
    """
    Primitive: logic.json.get

    Extract a value from nested JSON using dot-notation path.

    Args:
        data: The JSON object to extract from
        path: Dot-separated path (e.g., "user.profile.name")
        _ctx: Execution context (MANDATORY in lib/)
        default: Value to return if path not found

    Returns:
        {"status": "success", "value": <extracted>, "found": True}
        {"status": "success", "value": <default>, "found": False}
    """
    keys = path.split(".")
    current = data

    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        elif isinstance(current, list):
            # Support array indexing: "items.0.name"
            try:
                index = int(key)
                if 0 <= index < len(current):
                    current = current[index]
                else:
                    return {"status": "success", "value": default, "found": False}
            except ValueError:
                return {"status": "success", "value": default, "found": False}
        else:
            return {"status": "success", "value": default, "found": False}

    return {"status": "success", "value": current, "found": True}


def json_set(
    data: Dict[str, Any],
    path: str,
    value: Any,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: logic.json.set

    Set a value in nested JSON using dot-notation path.
    Creates intermediate dicts as needed. Returns a new dict (immutable).

    Args:
        data: The JSON object to modify
        path: Dot-separated path (e.g., "user.profile.name")
        value: Value to set at the path
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "data": <modified copy>}
        {"status": "error", "message": str} on failure
    """
    try:
        result = copy.deepcopy(data)
        keys = path.split(".")

        current = result
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value
        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def list_map(
    items: List[Any],
    key: str,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: logic.list.map

    Extract a field from each dict in a list.
    Supports dot-notation paths for nested extraction.

    Args:
        items: List of dicts
        key: Field name to extract (supports dot-notation)
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "values": [...], "count": int}
    """

    def extract_nested(obj: Any, path: str) -> Any:
        """Extract value from nested dict using dot-notation path."""
        keys = path.split(".")
        current = obj
        for k in keys:
            if isinstance(current, dict) and k in current:
                current = current[k]
            else:
                return None
        return current

    values = [extract_nested(item, key) for item in items]
    return {"status": "success", "values": values, "count": len(values)}


def list_filter(
    items: List[Any],
    key: str,
    op: str,
    value: Any,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: logic.list.filter

    Filter list items by a predicate on a field.

    Args:
        items: List of dicts to filter
        key: Field name to test (supports dot-notation)
        op: Operator - "eq", "neq", "gt", "lt", "gte", "lte", "contains", "exists"
        value: Value to compare against (ignored for "exists")
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "items": [...filtered...], "count": int}
        {"status": "error", "message": str} on invalid operator
    """

    def extract_nested(obj: Any, path: str) -> Any:
        keys = path.split(".")
        current = obj
        for k in keys:
            if isinstance(current, dict) and k in current:
                current = current[k]
            else:
                return None
        return current

    def matches(item: Any) -> bool:
        field_value = extract_nested(item, key)

        if op == "exists":
            return field_value is not None
        elif op == "eq":
            return field_value == value
        elif op == "neq":
            return field_value != value
        elif op == "gt":
            return field_value is not None and field_value > value
        elif op == "lt":
            return field_value is not None and field_value < value
        elif op == "gte":
            return field_value is not None and field_value >= value
        elif op == "lte":
            return field_value is not None and field_value <= value
        elif op == "contains":
            if isinstance(field_value, str):
                return value in field_value
            elif isinstance(field_value, list):
                return value in field_value
            return False
        else:
            return False

    valid_ops = ["eq", "neq", "gt", "lt", "gte", "lte", "contains", "exists"]
    if op not in valid_ops:
        return {
            "status": "error",
            "message": f"Invalid operator '{op}'. Valid: {valid_ops}",
        }

    filtered = [item for item in items if matches(item)]
    return {"status": "success", "items": filtered, "count": len(filtered)}


def list_sort(
    items: List[Dict[str, Any]],
    key: str,
    _ctx: ExecutionContext,
    reverse: bool = False,
) -> Dict[str, Any]:
    """
    Primitive: logic.list.sort

    Sort a list of dicts by a field.

    Args:
        items: List of dicts to sort
        key: Field name to sort by (supports dot-notation)
        _ctx: Execution context (MANDATORY in lib/)
        reverse: If True, sort descending (default False)

    Returns:
        {"status": "success", "items": [...sorted...], "count": int}
    """

    def extract_nested(obj: Any, path: str) -> Any:
        keys = path.split(".")
        current = obj
        for k in keys:
            if isinstance(current, dict) and k in current:
                current = current[k]
            else:
                return None
        return current

    # Sort with None values at the end
    def sort_key(item: Any) -> tuple:
        val = extract_nested(item, key)
        # Put None values at the end
        return (val is None, val if val is not None else "")

    sorted_items = sorted(items, key=sort_key, reverse=reverse)
    return {"status": "success", "items": sorted_items, "count": len(sorted_items)}


def string_format(
    template: str,
    values: Dict[str, Any],
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: logic.string.format

    Format a string template with values using Python's format_map.

    Args:
        template: String template with {name} placeholders
        values: Dict of name -> value mappings
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "result": "formatted string"}
        {"status": "error", "message": str, "result": <original template>}
    """
    try:
        result = template.format_map(values)
        return {"status": "success", "result": result}
    except KeyError as e:
        return {
            "status": "error",
            "message": f"Missing key: {e}",
            "result": template,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "result": template,
        }
