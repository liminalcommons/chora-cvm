"""
Genesis Crystal: Crystal Palace Domain Primitives and Protocols.

This module registers the new domain-organized primitives that live in lib/*.py.
These use the domain.noun.verb naming convention and form the Minimum Viable Toolset.

Following the Strangler Fig pattern, these new primitives coexist with the old
primitive-* entities during transition. Eventually, the old entities will be
unregistered and only these domain-organized primitives will remain.

Domains:
- attention.*: Focus/Signal lifecycle (plasma layer)
- code.*: AST scanning, build operations
- io.*: Filesystem operations (read, write, patch, tree)
- sys.*: Sandboxed subprocess execution
- graph.*: Entity and bond operations (already in lib/graph.py)

Protocols (Phase 3 - Graph-Defined):
- protocol-integrity-check: BDD integrity verification (scans features → runs tests → signals failures)
- protocol-reflex-build: Build reflex arc (tests → signals on failure)
"""

from __future__ import annotations

from chora_cvm.schema import (
    EdgeCondition,
    ConditionOp,
    PrimitiveData,
    PrimitiveEntity,
    ProtocolData,
    ProtocolEntity,
    ProtocolGraph,
    ProtocolInterface,
    ProtocolNode,
    ProtocolNodeKind,
    ProtocolEdge,
)
from chora_cvm.store import EventStore


# =============================================================================
# DEPRECATION MAPPING: Old primitive-* → New domain.* IDs
# =============================================================================
# This mapping documents which old primitives have Crystal Palace equivalents.
# The old primitives remain registered for protocol compatibility, but new code
# should use the domain.* IDs. When protocols are updated to use new IDs,
# the old primitives can be unregistered.

DEPRECATION_MAP: dict[str, str] = {
    # Attention domain
    "primitive-create-focus": "attention.focus.create",
    "primitive-resolve-focus": "attention.focus.resolve",
    "primitive-emit-signal": "attention.signal.emit",
    # I/O domain
    "primitive-sys-log": "io.sys.log",
    "primitive-ui-render": "io.ui.render",
    # Code/Build domain
    "primitive-run-lint": "code.build.lint",
    "primitive-run-tests": "code.build.test",
    "primitive-run-typecheck": "code.build.typecheck",
}


def get_new_primitive_id(old_id: str) -> str | None:
    """
    Get the new domain.* primitive ID for an old primitive-* ID.

    Returns None if no Crystal Palace equivalent exists.
    """
    return DEPRECATION_MAP.get(old_id)


def bootstrap_attention_primitives(store: EventStore) -> list[str]:
    """
    Register attention domain primitives (lib/attention.py).

    The attention layer tracks what the agent is focusing on and what demands
    attention. Focus is plasma - the energy of attention in the system.

    Returns list of created primitive IDs.
    """
    created = []

    # attention.focus.create
    prim = PrimitiveEntity(
        id="attention.focus.create",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.attention.focus_create",
            description="Declare what is being attended to. Creates a Focus entity.",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "description": "What is being attended to"},
                        "description": {"type": "string"},
                        "signal_id": {"type": "string", "description": "Signal that triggered this focus"},
                        "persona_id": {"type": "string"},
                        "data": {"type": "object"},
                    },
                    "required": ["title"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "id": {"type": "string"},
                        "focus_status": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # attention.focus.resolve
    prim = PrimitiveEntity(
        id="attention.focus.resolve",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.attention.focus_resolve",
            description="Close the attention loop. Optionally yields a Learning entity.",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "focus_id": {"type": "string"},
                        "outcome": {"type": "string"},
                        "learning_title": {"type": "string"},
                        "learning_insight": {"type": "string"},
                    },
                    "required": ["focus_id"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "id": {"type": "string"},
                        "focus_status": {"type": "string"},
                        "learning_id": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # attention.focus.list
    prim = PrimitiveEntity(
        id="attention.focus.list",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.attention.focus_list",
            description="List all active (unresolved) focus entities.",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "persona_id": {"type": "string"},
                    },
                    "required": [],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "focuses": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # attention.signal.emit
    prim = PrimitiveEntity(
        id="attention.signal.emit",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.attention.signal_emit",
            description="Emit a Signal entity - something demands attention.",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "source_id": {"type": "string"},
                        "signal_type": {"type": "string"},
                        "urgency": {"type": "string"},
                        "description": {"type": "string"},
                        "data": {"type": "object"},
                    },
                    "required": ["title"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "id": {"type": "string"},
                        "signal_status": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    return created


def bootstrap_code_primitives(store: EventStore) -> list[str]:
    """
    Register code domain primitives (lib/build.py).

    These are the "eyes for code" - primitives that help agents understand
    code structure without executing it.

    Returns list of created primitive IDs.
    """
    created = []

    # code.ast.scan
    prim = PrimitiveEntity(
        id="code.ast.scan",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.build.ast_scan",
            description="Parse a Python file and extract structural elements (classes, functions, methods).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "file_path": {"type": "string"},
                        "include_docstrings": {"type": "boolean"},
                        "include_imports": {"type": "boolean"},
                    },
                    "required": ["file_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "path": {"type": "string"},
                        "elements": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # code.build.lint (points to lib/build.py)
    prim = PrimitiveEntity(
        id="code.build.lint",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.build.build_lint",
            description="Run ruff linter on a Python package (allow-listed).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "package_path": {"type": "string"},
                        "fix": {"type": "boolean"},
                    },
                    "required": ["package_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "success": {"type": "boolean"},
                        "exit_code": {"type": "integer"},
                        "stdout": {"type": "string"},
                        "stderr": {"type": "string"},
                        "tool": {"type": "string"},
                        "package": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # code.build.test
    prim = PrimitiveEntity(
        id="code.build.test",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.build.build_test",
            description="Run pytest on a Python package (allow-listed).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "package_path": {"type": "string"},
                        "coverage": {"type": "boolean"},
                        "coverage_threshold": {"type": "integer"},
                    },
                    "required": ["package_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "success": {"type": "boolean"},
                        "exit_code": {"type": "integer"},
                        "stdout": {"type": "string"},
                        "stderr": {"type": "string"},
                        "tool": {"type": "string"},
                        "package": {"type": "string"},
                        "coverage_met": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # code.build.typecheck
    prim = PrimitiveEntity(
        id="code.build.typecheck",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.build.build_typecheck",
            description="Run mypy type checker on a Python package (allow-listed).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "package_path": {"type": "string"},
                    },
                    "required": ["package_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "success": {"type": "boolean"},
                        "exit_code": {"type": "integer"},
                        "stdout": {"type": "string"},
                        "stderr": {"type": "string"},
                        "tool": {"type": "string"},
                        "package": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # code.scan.features
    prim = PrimitiveEntity(
        id="code.scan.features",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.build.scan_features",
            description="Scan BDD feature files for behavior tags (@behavior:*).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "features_dir": {"type": "string", "description": "Directory containing .feature files"},
                        "tag_pattern": {"type": "string", "description": "Regex pattern (default: @behavior:*)"},
                    },
                    "required": ["features_dir"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "root": {"type": "string"},
                        "features": {"type": "array"},
                        "all_tags": {"type": "array"},
                        "feature_count": {"type": "integer"},
                        "total_scenarios": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    return created


def bootstrap_io_primitives(store: EventStore) -> list[str]:
    """
    Register I/O domain primitives (lib/io.py).

    These are the "hands" - primitives for safe filesystem operations
    and user-facing output.

    Returns list of created primitive IDs.
    """
    created = []

    # io.ui.render
    prim = PrimitiveEntity(
        id="io.ui.render",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.io.ui_render",
            description="Render user-facing output to the configured sink.",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "content": {"type": "string"},
                        "style": {"type": "string"},
                        "title": {"type": "string"},
                    },
                    "required": ["content"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "rendered": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # io.sys.log
    prim = PrimitiveEntity(
        id="io.sys.log",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.io.sys_log",
            description="Log a message to the configured output sink.",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "message": {"type": "string"},
                        "level": {"type": "string"},
                    },
                    "required": ["message"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "logged": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # io.fs.read
    prim = PrimitiveEntity(
        id="io.fs.read",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.io.fs_read",
            description="Read text content from a file (sandboxed).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "base_dir": {"type": "string"},
                        "rel_path": {"type": "string"},
                    },
                    "required": ["base_dir", "rel_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "content": {"type": "string"},
                        "path": {"type": "string"},
                        "bytes_read": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # io.fs.write
    prim = PrimitiveEntity(
        id="io.fs.write",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.io.fs_write",
            description="Write text content to a file (sandboxed).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "base_dir": {"type": "string"},
                        "rel_path": {"type": "string"},
                        "content": {"type": "string"},
                    },
                    "required": ["base_dir", "rel_path", "content"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "path": {"type": "string"},
                        "bytes_written": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # io.fs.read_tree
    prim = PrimitiveEntity(
        id="io.fs.read_tree",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.io.fs_read_tree",
            description="List files in a directory tree (respects .gitignore).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "base_dir": {"type": "string"},
                        "max_depth": {"type": "integer"},
                        "include_hidden": {"type": "boolean"},
                        "respect_gitignore": {"type": "boolean"},
                        "file_extensions": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["base_dir"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "root": {"type": "string"},
                        "files": {"type": "array"},
                        "file_count": {"type": "integer"},
                        "dir_count": {"type": "integer"},
                        "total_size": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # io.fs.patch
    prim = PrimitiveEntity(
        id="io.fs.patch",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.io.fs_patch",
            description="Apply unified diff patches to files for surgical edits (sandboxed).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "base_dir": {"type": "string"},
                        "rel_path": {"type": "string"},
                        "diff": {"type": "string", "description": "Unified diff format patch"},
                        "create_if_missing": {"type": "boolean"},
                        "dry_run": {"type": "boolean"},
                    },
                    "required": ["base_dir", "rel_path", "diff"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "path": {"type": "string"},
                        "patched": {"type": "boolean"},
                        "lines_added": {"type": "integer"},
                        "lines_removed": {"type": "integer"},
                        "dry_run": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # io.teach.format - Format Diataxis-style documentation
    prim = PrimitiveEntity(
        id="io.teach.format",
        data=PrimitiveData(
            python_ref="chora_cvm.std.teach_format",
            description="Format Diataxis-style explanation from entity doc bundle",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "bundle": {"type": "object"},
                    },
                    "required": ["bundle"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    return created


def bootstrap_sys_primitives(store: EventStore) -> list[str]:
    """
    Register system domain primitives (lib/sys.py).

    These provide sandboxed subprocess execution - the "environment interface"
    for running external tools safely.

    Returns list of created primitive IDs.
    """
    created = []

    # sys.shell.run
    prim = PrimitiveEntity(
        id="sys.shell.run",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.sys.shell_run",
            description="Execute a command in a subprocess with safety constraints (no shell=True).",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "cmd": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Command as list of strings, e.g., ['python', '-m', 'pytest']",
                        },
                        "cwd": {"type": "string", "description": "Working directory (optional)"},
                        "timeout": {"type": "integer", "description": "Timeout in seconds (default: 300)"},
                        "env": {"type": "object", "description": "Additional environment variables"},
                        "capture_output": {"type": "boolean", "description": "Capture stdout/stderr (default: true)"},
                        "max_output_size": {"type": "integer", "description": "Max bytes per stream (default: 256KB)"},
                    },
                    "required": ["cmd"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "exit_code": {"type": "integer"},
                        "stdout": {"type": "string"},
                        "stderr": {"type": "string"},
                        "truncated": {"type": "boolean"},
                        "timed_out": {"type": "boolean"},
                        "command": {"type": "array"},
                        "cwd": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # sys.uuid.short
    prim = PrimitiveEntity(
        id="sys.uuid.short",
        data=PrimitiveData(
            python_ref="chora_cvm.std.uuid_short",
            description="Generate a short UUID (8 chars)",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {},
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "uuid": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    return created


def bootstrap_logic_primitives(store: EventStore) -> list[str]:
    """
    Register logic domain primitives (lib/logic.py).

    JSON manipulation, list operations, and string formatting.

    Returns list of created primitive IDs.
    """
    created = []

    # logic.json.get
    prim = PrimitiveEntity(
        id="logic.json.get",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.logic.json_get",
            description="Extract value from nested JSON using dot-notation",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "data": {"type": "object"},
                        "path": {"type": "string"},
                        "default": {},
                    },
                    "required": ["data", "path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "value": {},
                        "found": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.json.set
    prim = PrimitiveEntity(
        id="logic.json.set",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.logic.json_set",
            description="Set value in nested JSON using dot-notation",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "data": {"type": "object"},
                        "path": {"type": "string"},
                        "value": {},
                    },
                    "required": ["data", "path", "value"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "data": {"type": "object"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.list.map
    prim = PrimitiveEntity(
        id="logic.list.map",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.logic.list_map",
            description="Extract field from each item in a list",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                        "key": {"type": "string"},
                    },
                    "required": ["items", "key"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "values": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.list.filter
    prim = PrimitiveEntity(
        id="logic.list.filter",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.logic.list_filter",
            description="Filter list items by predicate",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                        "key": {"type": "string"},
                        "op": {"type": "string", "enum": ["eq", "neq", "gt", "lt", "gte", "lte", "contains", "exists"]},
                        "value": {},
                    },
                    "required": ["items", "key", "op"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "items": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.list.sort
    prim = PrimitiveEntity(
        id="logic.list.sort",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.logic.list_sort",
            description="Sort list by field",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                        "key": {"type": "string"},
                        "reverse": {"type": "boolean"},
                    },
                    "required": ["items", "key"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "items": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.string.format
    prim = PrimitiveEntity(
        id="logic.string.format",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.logic.string_format",
            description="Format string template with values",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "template": {"type": "string"},
                        "values": {"type": "object"},
                    },
                    "required": ["template", "values"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "result": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.list.length
    prim = PrimitiveEntity(
        id="logic.list.length",
        data=PrimitiveData(
            python_ref="chora_cvm.std.list_length",
            description="Get the length of a list",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                    },
                    "required": ["items"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "length": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.list.mode
    prim = PrimitiveEntity(
        id="logic.list.mode",
        data=PrimitiveData(
            python_ref="chora_cvm.std.list_mode",
            description="Get the most common value in a list",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                    },
                    "required": ["items"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "mode": {},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.list.slice
    prim = PrimitiveEntity(
        id="logic.list.slice",
        data=PrimitiveData(
            python_ref="chora_cvm.std.list_slice",
            description="Get a slice of a list",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                        "start": {"type": "integer"},
                        "end": {"type": "integer"},
                    },
                    "required": ["items"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                        "length": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.string.join
    prim = PrimitiveEntity(
        id="logic.string.join",
        data=PrimitiveData(
            python_ref="chora_cvm.std.string_join",
            description="Join list items into a string with separator",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                        "separator": {"type": "string"},
                    },
                    "required": ["items"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "result": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # logic.json.parse - Parse JSON string (Crystal Palace replacement for primitive-json-parse)
    prim = PrimitiveEntity(
        id="logic.json.parse",
        data=PrimitiveData(
            python_ref="chora_cvm.std.json_parse",
            description="Parse JSON string into structured data",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "json_str": {"type": "string"},
                    },
                    "required": ["json_str"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "data": {"type": "object"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    return created


def bootstrap_cognition_primitives(store: EventStore) -> list[str]:
    """
    Register cognition domain primitives (lib/cognition.py).

    Semantic embeddings and vector operations.

    Returns list of created primitive IDs.
    """
    created = []

    # cognition.embed.text
    prim = PrimitiveEntity(
        id="cognition.embed.text",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.cognition.embed_text",
            description="Generate embedding for text (lazy chora-inference)",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"},
                    },
                    "required": ["text"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "vector": {"type": "string", "format": "binary"},
                        "dimension": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.vector.sim
    prim = PrimitiveEntity(
        id="cognition.vector.sim",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.cognition.vector_sim",
            description="Compute cosine similarity between two vectors",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "vector_a": {"type": "string", "format": "binary"},
                        "vector_b": {"type": "string", "format": "binary"},
                        "dimension": {"type": "integer"},
                    },
                    "required": ["vector_a", "vector_b", "dimension"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "similarity": {"type": "number"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.vector.rank
    prim = PrimitiveEntity(
        id="cognition.vector.rank",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.cognition.vector_rank",
            description="Rank items by similarity to a query vector",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "query_vector": {"type": "string", "format": "binary"},
                        "candidates": {"type": "array"},
                        "dimension": {"type": "integer"},
                        "top_k": {"type": "integer"},
                    },
                    "required": ["query_vector", "candidates", "dimension"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "ranked": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.cluster
    prim = PrimitiveEntity(
        id="cognition.cluster",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.cognition.cluster",
            description="Cluster items by vector similarity",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array"},
                        "dimension": {"type": "integer"},
                        "threshold": {"type": "number"},
                    },
                    "required": ["items", "dimension"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "clusters": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.wisdom.extract
    prim = PrimitiveEntity(
        id="cognition.wisdom.extract",
        data=PrimitiveData(
            python_ref="chora_cvm.std.wisdom_extract",
            description="Extract wisdom/insights from entity data",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "entity_type": {"type": "string"},
                        "data": {"type": "object"},
                    },
                    "required": ["entity_type", "data"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "wisdom": {"type": "string"},
                        "keywords": {"type": "array"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.embed.batch_load
    prim = PrimitiveEntity(
        id="cognition.embed.batch_load",
        data=PrimitiveData(
            python_ref="chora_cvm.std.batch_load_embeddings",
            description="Load embeddings for multiple entity IDs",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "entity_ids": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["db_path", "entity_ids"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "embeddings": {"type": "object"},
                        "found_count": {"type": "integer"},
                        "missing_ids": {"type": "array"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.embed.to_vectors
    prim = PrimitiveEntity(
        id="cognition.embed.to_vectors",
        data=PrimitiveData(
            python_ref="chora_cvm.std.embeddings_to_vectors",
            description="Convert embeddings dict to numpy vectors",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "embeddings": {"type": "object"},
                    },
                    "required": ["embeddings"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "vectors": {"type": "object"},
                        "dimension": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.embed.to_candidates
    prim = PrimitiveEntity(
        id="cognition.embed.to_candidates",
        data=PrimitiveData(
            python_ref="chora_cvm.std.embeddings_to_candidates",
            description="Convert embeddings to candidate format for ranking",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "embeddings": {"type": "object"},
                    },
                    "required": ["embeddings"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "candidates": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.vector.mean
    prim = PrimitiveEntity(
        id="cognition.vector.mean",
        data=PrimitiveData(
            python_ref="chora_cvm.std.vector_mean",
            description="Compute mean of multiple vectors",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "vectors": {"type": "array"},
                    },
                    "required": ["vectors"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "mean": {"type": "array"},
                        "dimension": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # cognition.semantic.rank_loop (GPU Doctrine: heavy math stays in Python)
    prim = PrimitiveEntity(
        id="cognition.semantic.rank_loop",
        data=PrimitiveData(
            python_ref="chora_cvm.std.semantic_ranking_loop",
            description="Full semantic ranking pipeline: load entities, compute embeddings, rank by similarity",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "query_text": {"type": "string"},
                        "entity_ids": {"type": "array", "items": {"type": "string"}},
                        "top_k": {"type": "integer"},
                    },
                    "required": ["db_path", "query_text", "entity_ids"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "ranked": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    return created


def bootstrap_chronos_primitives(store: EventStore) -> list[str]:
    """
    Register chronos domain primitives (lib/chronos.py).

    Time operations: now, offset, diff.

    Returns list of created primitive IDs.
    """
    created = []

    # chronos.now
    prim = PrimitiveEntity(
        id="chronos.now",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.chronos.now",
            description="Get current UTC timestamp in ISO 8601 format",
            interface={
                "inputs": {"type": "object", "properties": {}},
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "timestamp": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # chronos.offset
    prim = PrimitiveEntity(
        id="chronos.offset",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.chronos.offset",
            description="Get timestamp offset from now",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "days": {"type": "integer"},
                        "hours": {"type": "integer"},
                        "minutes": {"type": "integer"},
                    },
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "timestamp": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # chronos.diff
    prim = PrimitiveEntity(
        id="chronos.diff",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.chronos.diff",
            description="Compute time difference between two timestamps",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "timestamp_a": {"type": "string"},
                        "timestamp_b": {"type": "string"},
                    },
                    "required": ["timestamp_a", "timestamp_b"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "days": {"type": "number"},
                        "hours": {"type": "number"},
                        "seconds": {"type": "number"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    return created


def bootstrap_graph_primitives(store: EventStore) -> list[str]:
    """
    Register graph domain primitives (lib/graph.py).

    Entity and bond operations for the knowledge graph.

    Returns list of created primitive IDs.
    """
    created = []

    # graph.entity.get
    prim = PrimitiveEntity(
        id="graph.entity.get",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.graph.entity_get",
            description="Load a single entity by ID",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "entity_id": {"type": "string"},
                    },
                    "required": ["entity_id"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "entity": {"type": "object"},
                        "found": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.entity.create
    prim = PrimitiveEntity(
        id="graph.entity.create",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.graph.entity_create",
            description="Create a new entity",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "entity_type": {"type": "string"},
                        "entity_id": {"type": "string"},
                        "data": {"type": "object"},
                    },
                    "required": ["entity_type", "data"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "id": {"type": "string"},
                        "type": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.entity.update
    prim = PrimitiveEntity(
        id="graph.entity.update",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.graph.entity_update",
            description="Update fields on an existing entity",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "entity_id": {"type": "string"},
                        "updates": {"type": "object"},
                    },
                    "required": ["entity_id", "updates"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "id": {"type": "string"},
                        "updated": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.entity.archive
    prim = PrimitiveEntity(
        id="graph.entity.archive",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.graph.entity_archive",
            description="Archive an entity (soft delete)",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "entity_id": {"type": "string"},
                    },
                    "required": ["entity_id"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "id": {"type": "string"},
                        "archived": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.bond.manage
    prim = PrimitiveEntity(
        id="graph.bond.manage",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.graph.bond_manage",
            description="Create or update a bond between entities",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "from_id": {"type": "string"},
                        "verb": {"type": "string"},
                        "to_id": {"type": "string"},
                        "data": {"type": "object"},
                    },
                    "required": ["from_id", "verb", "to_id"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "bond_id": {"type": "string"},
                        "created": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.bond.list
    prim = PrimitiveEntity(
        id="graph.bond.list",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.graph.bond_list",
            description="List bonds for an entity",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "entity_id": {"type": "string"},
                        "direction": {"type": "string", "enum": ["from", "to", "both"]},
                        "verb": {"type": "string"},
                    },
                    "required": ["entity_id"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "bonds": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.query
    prim = PrimitiveEntity(
        id="graph.query",
        data=PrimitiveData(
            python_ref="chora_cvm.lib.graph.query",
            description="Query entities with filters",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "entity_type": {"type": "string"},
                        "filters": {"type": "object"},
                        "limit": {"type": "integer"},
                    },
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "entities": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.query.count_by_type
    prim = PrimitiveEntity(
        id="graph.query.count_by_type",
        data=PrimitiveData(
            python_ref="chora_cvm.std.entities_count_by_type",
            description="Count entities grouped by type",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                    },
                    "required": ["db_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "counts": {"type": "object"},
                        "total": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.query.json
    prim = PrimitiveEntity(
        id="graph.query.json",
        data=PrimitiveData(
            python_ref="chora_cvm.std.entities_query_json",
            description="Query entities with JSON field conditions",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "entity_type": {"type": "string"},
                        "json_conditions": {"type": "object"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["db_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "entities": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.query.recent
    prim = PrimitiveEntity(
        id="graph.query.recent",
        data=PrimitiveData(
            python_ref="chora_cvm.std.entities_recent",
            description="Get the most recently created entities",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "entity_type": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["db_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "entities": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.query.orphans
    prim = PrimitiveEntity(
        id="graph.query.orphans",
        data=PrimitiveData(
            python_ref="chora_cvm.std.entities_orphans",
            description="Find entities with no bonds",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["db_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "orphans": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.query.unverified
    prim = PrimitiveEntity(
        id="graph.query.unverified",
        data=PrimitiveData(
            python_ref="chora_cvm.std.entities_unverified",
            description="Find tools that have no verifies bonds",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["db_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "tools": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.entity.get_batch
    prim = PrimitiveEntity(
        id="graph.entity.get_batch",
        data=PrimitiveData(
            python_ref="chora_cvm.std.entities_get_batch",
            description="Load multiple entities by ID list",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "entity_ids": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["db_path", "entity_ids"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "entities": {"type": "array"},
                        "found_count": {"type": "integer"},
                        "missing_ids": {"type": "array"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.entity.create_batch
    prim = PrimitiveEntity(
        id="graph.entity.create_batch",
        data=PrimitiveData(
            python_ref="chora_cvm.std.manifest_entities",
            description="Create multiple entities in a single call",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "entities": {"type": "array", "items": {"type": "object"}},
                    },
                    "required": ["db_path", "entities"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "created": {"type": "array", "items": {"type": "string"}},
                        "error": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.entity.to_text
    prim = PrimitiveEntity(
        id="graph.entity.to_text",
        data=PrimitiveData(
            python_ref="chora_cvm.std.entity_to_text",
            description="Extract semantic text from entity based on type",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "entity_type": {"type": "string"},
                        "data": {"type": "object"},
                    },
                    "required": ["entity_type", "data"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.bond.count
    prim = PrimitiveEntity(
        id="graph.bond.count",
        data=PrimitiveData(
            python_ref="chora_cvm.std.bonds_count",
            description="Count total bonds in the graph",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                    },
                    "required": ["db_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.bond.for_each
    prim = PrimitiveEntity(
        id="graph.bond.for_each",
        data=PrimitiveData(
            python_ref="chora_cvm.std.list_for_each_bond",
            description="Create bonds from one entity to multiple targets",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "bond_type": {"type": "string"},
                        "from_id": {"type": "string"},
                        "to_ids": {"type": "array", "items": {"type": "string"}},
                        "confidence": {"type": "number"},
                    },
                    "required": ["db_path", "bond_type", "from_id", "to_ids"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "created": {"type": "integer"},
                        "errors": {"type": "array"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.query.sql - Raw SQL query (Crystal Palace replacement for primitive-sqlite-query)
    prim = PrimitiveEntity(
        id="graph.query.sql",
        data=PrimitiveData(
            python_ref="chora_cvm.std.sqlite_query",
            description="Execute read-only SQL query on the database",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "sql": {"type": "string"},
                        "params": {"type": "object"},
                    },
                    "required": ["db_path", "sql"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "rows": {"type": "array"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    # graph.entity.doc_bundle - Load entity with linked Diataxis docs
    prim = PrimitiveEntity(
        id="graph.entity.doc_bundle",
        data=PrimitiveData(
            python_ref="chora_cvm.std.entity_doc_bundle",
            description="Load entity and linked Diataxis documentation (story, pattern, principle)",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "db_path": {"type": "string"},
                        "entity_id": {"type": "string"},
                    },
                    "required": ["db_path", "entity_id"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "entity": {"type": "object"},
                        "story": {"type": "object"},
                        "pattern": {"type": "object"},
                        "principle": {"type": "object"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim)
    created.append(prim.id)

    return created


def bootstrap_crystal_palace(store: EventStore, verbose: bool = True) -> dict:
    """
    Bootstrap all Crystal Palace domain primitives.

    This is the main entry point for registering the new domain-organized
    primitives that form the Minimum Viable Toolset.

    Args:
        store: EventStore instance
        verbose: Print progress

    Returns:
        Summary of created entities
    """
    if verbose:
        print("[*] Bootstrapping Crystal Palace Primitives...")

    all_primitives = []

    # Attention domain (plasma layer)
    attention_prims = bootstrap_attention_primitives(store)
    if verbose:
        print(f"    [attention.*] {len(attention_prims)} primitives")
        for p in attention_prims:
            print(f"      + {p}")
    all_primitives.extend(attention_prims)

    # Code domain (eyes for code)
    code_prims = bootstrap_code_primitives(store)
    if verbose:
        print(f"    [code.*] {len(code_prims)} primitives")
        for p in code_prims:
            print(f"      + {p}")
    all_primitives.extend(code_prims)

    # I/O domain (hands)
    io_prims = bootstrap_io_primitives(store)
    if verbose:
        print(f"    [io.*] {len(io_prims)} primitives")
        for p in io_prims:
            print(f"      + {p}")
    all_primitives.extend(io_prims)

    # Sys domain (environment interface)
    sys_prims = bootstrap_sys_primitives(store)
    if verbose:
        print(f"    [sys.*] {len(sys_prims)} primitives")
        for p in sys_prims:
            print(f"      + {p}")
    all_primitives.extend(sys_prims)

    # Logic domain (data transformations)
    logic_prims = bootstrap_logic_primitives(store)
    if verbose:
        print(f"    [logic.*] {len(logic_prims)} primitives")
        for p in logic_prims:
            print(f"      + {p}")
    all_primitives.extend(logic_prims)

    # Cognition domain (semantic operations)
    cognition_prims = bootstrap_cognition_primitives(store)
    if verbose:
        print(f"    [cognition.*] {len(cognition_prims)} primitives")
        for p in cognition_prims:
            print(f"      + {p}")
    all_primitives.extend(cognition_prims)

    # Chronos domain (time operations)
    chronos_prims = bootstrap_chronos_primitives(store)
    if verbose:
        print(f"    [chronos.*] {len(chronos_prims)} primitives")
        for p in chronos_prims:
            print(f"      + {p}")
    all_primitives.extend(chronos_prims)

    # Graph domain (entity and bond operations)
    graph_prims = bootstrap_graph_primitives(store)
    if verbose:
        print(f"    [graph.*] {len(graph_prims)} primitives")
        for p in graph_prims:
            print(f"      + {p}")
    all_primitives.extend(graph_prims)

    if verbose:
        print(f"[*] Crystal Palace complete: {len(all_primitives)} primitives registered.")

    return {
        "primitives": all_primitives,
        "domains": ["attention", "code", "io", "sys", "logic", "cognition", "chronos", "graph"],
    }


# =============================================================================
# PHASE 3: GRAPH-DEFINED PROTOCOLS
# =============================================================================


def bootstrap_protocol_integrity_check(store: EventStore) -> str:
    """
    Bootstrap protocol-integrity-check: BDD integrity verification.

    This protocol composes Crystal Palace primitives to verify BDD integrity:
    1. Scan feature files for behavior tags
    2. Run the test suite
    3. If tests fail, emit a signal

    The logic flows through the graph, not through Python.
    """
    protocol = ProtocolEntity(
        id="protocol-integrity-check",
        data=ProtocolData(
            title="Integrity Check",
            description="Verify BDD integrity: scan features, run tests, signal failures",
            interface=ProtocolInterface(
                inputs={
                    "type": "object",
                    "properties": {
                        "features_dir": {"type": "string"},
                        "package_path": {"type": "string"},
                        "db_path": {"type": "string"},
                    },
                    "required": ["features_dir", "package_path"],
                },
                outputs={
                    "type": "object",
                    "properties": {
                        "healthy": {"type": "boolean"},
                        "features_scanned": {"type": "integer"},
                        "behaviors_found": {"type": "integer"},
                        "tests_passed": {"type": "boolean"},
                        "signal_id": {"type": "string"},
                    },
                },
            ),
            graph=ProtocolGraph(
                start="scan_features",
                nodes={
                    # Step 1: Scan BDD feature files
                    "scan_features": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="code.scan.features",
                        inputs={
                            "features_dir": "$.inputs.features_dir",
                        },
                    ),
                    # Step 2: Run the test suite
                    "run_tests": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="code.build.test",
                        inputs={
                            "package_path": "$.inputs.package_path",
                        },
                    ),
                    # Step 3a: Tests passed - return healthy
                    "return_healthy": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "healthy": True,
                            "features_scanned": "$.scan_features.feature_count",
                            "behaviors_found": "$.scan_features.all_tags",
                            "tests_passed": True,
                            "signal_id": None,
                        },
                    ),
                    # Step 3b: Tests failed - emit signal
                    "emit_failure_signal": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="attention.signal.emit",
                        inputs={
                            "title": "Build Integrity Failure",
                            "source_id": "protocol-integrity-check",
                            "signal_type": "build-failure",
                            "urgency": "high",
                            "description": "Tests failed during integrity check",
                            "data": {
                                "features_scanned": "$.scan_features.feature_count",
                                "behaviors_found": "$.scan_features.all_tags",
                                "exit_code": "$.run_tests.exit_code",
                            },
                        },
                    ),
                    # Step 4: Return unhealthy with signal
                    "return_unhealthy": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "healthy": False,
                            "features_scanned": "$.scan_features.feature_count",
                            "behaviors_found": "$.scan_features.all_tags",
                            "tests_passed": False,
                            "signal_id": "$.emit_failure_signal.id",
                        },
                    ),
                },
                edges=[
                    # scan_features → run_tests (always)
                    ProtocolEdge(**{"from": "scan_features", "to": "run_tests"}),
                    # run_tests → return_healthy (if success=true)
                    ProtocolEdge(
                        **{
                            "from": "run_tests",
                            "to": "return_healthy",
                            "condition": EdgeCondition(
                                path="$.run_tests.success",
                                op=ConditionOp.EQ,
                                value=True,
                            ),
                        }
                    ),
                    # run_tests → emit_failure_signal (if success=false)
                    ProtocolEdge(
                        **{
                            "from": "run_tests",
                            "to": "emit_failure_signal",
                            "condition": EdgeCondition(
                                path="$.run_tests.success",
                                op=ConditionOp.EQ,
                                value=False,
                            ),
                        }
                    ),
                    # emit_failure_signal → return_unhealthy
                    ProtocolEdge(**{"from": "emit_failure_signal", "to": "return_unhealthy"}),
                ],
            ),
        ),
    )
    store.save_entity(protocol)
    return protocol.id


def bootstrap_protocol_reflex_build(store: EventStore) -> str:
    """
    Bootstrap protocol-reflex-build: Build reflex arc.

    This protocol implements the reflex arc pattern:
    1. Run tests
    2. If tests fail, emit a signal (the reflex response)

    This is the simplest form of the metabolism - action → signal → attention.
    """
    protocol = ProtocolEntity(
        id="protocol-reflex-build",
        data=ProtocolData(
            title="Reflex Build",
            description="Build reflex arc: run tests, signal on failure",
            interface=ProtocolInterface(
                inputs={
                    "type": "object",
                    "properties": {
                        "package_path": {"type": "string"},
                        "db_path": {"type": "string"},
                        "coverage": {"type": "boolean"},
                    },
                    "required": ["package_path"],
                },
                outputs={
                    "type": "object",
                    "properties": {
                        "success": {"type": "boolean"},
                        "exit_code": {"type": "integer"},
                        "signal_id": {"type": "string"},
                    },
                },
            ),
            graph=ProtocolGraph(
                start="run_tests",
                nodes={
                    # Step 1: Run the test suite
                    "run_tests": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="code.build.test",
                        inputs={
                            "package_path": "$.inputs.package_path",
                            "coverage": "$.inputs.coverage",
                        },
                    ),
                    # Step 2a: Tests passed - return success
                    "return_success": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "success": True,
                            "exit_code": "$.run_tests.exit_code",
                            "signal_id": None,
                        },
                    ),
                    # Step 2b: Tests failed - emit signal (reflex response)
                    "emit_reflex_signal": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="attention.signal.emit",
                        inputs={
                            "title": "Test Suite Failure",
                            "source_id": "protocol-reflex-build",
                            "signal_type": "test-failure",
                            "urgency": "medium",
                            "description": "Test suite failed - reflex arc triggered",
                            "data": {
                                "package_path": "$.inputs.package_path",
                                "exit_code": "$.run_tests.exit_code",
                                "stderr": "$.run_tests.stderr",
                            },
                        },
                    ),
                    # Step 3: Return failure with signal
                    "return_failure": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "success": False,
                            "exit_code": "$.run_tests.exit_code",
                            "signal_id": "$.emit_reflex_signal.id",
                        },
                    ),
                },
                edges=[
                    # run_tests → return_success (if success=true)
                    ProtocolEdge(
                        **{
                            "from": "run_tests",
                            "to": "return_success",
                            "condition": EdgeCondition(
                                path="$.run_tests.success",
                                op=ConditionOp.EQ,
                                value=True,
                            ),
                        }
                    ),
                    # run_tests → emit_reflex_signal (if success=false)
                    ProtocolEdge(
                        **{
                            "from": "run_tests",
                            "to": "emit_reflex_signal",
                            "condition": EdgeCondition(
                                path="$.run_tests.success",
                                op=ConditionOp.EQ,
                                value=False,
                            ),
                        }
                    ),
                    # emit_reflex_signal → return_failure
                    ProtocolEdge(**{"from": "emit_reflex_signal", "to": "return_failure"}),
                ],
            ),
        ),
    )
    store.save_entity(protocol)
    return protocol.id


def bootstrap_crystal_protocols(store: EventStore, verbose: bool = True) -> list[str]:
    """
    Bootstrap all Crystal Palace protocols.

    These are graph-defined protocols that compose from Crystal Palace primitives.
    The logic flows through the graph, not through Python code.

    Returns list of created protocol IDs.
    """
    if verbose:
        print("[*] Bootstrapping Crystal Palace Protocols (Phase 3)...")

    protocols = []

    # protocol-integrity-check
    proto_id = bootstrap_protocol_integrity_check(store)
    protocols.append(proto_id)
    if verbose:
        print(f"    + {proto_id}")

    # protocol-reflex-build
    proto_id = bootstrap_protocol_reflex_build(store)
    protocols.append(proto_id)
    if verbose:
        print(f"    + {proto_id}")

    if verbose:
        print(f"[*] Crystal Palace Protocols complete: {len(protocols)} protocols registered.")

    return protocols


if __name__ == "__main__":
    # Quick test
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    store = EventStore(db_path)

    # Bootstrap primitives
    result = bootstrap_crystal_palace(store)
    print(f"\nPrimitives: {len(result['primitives'])} across {len(result['domains'])} domains")

    # Bootstrap protocols
    protocols = bootstrap_crystal_protocols(store)
    print(f"Protocols: {len(protocols)} graph-defined protocols")

    store.close()

    print(f"\nTotal: {len(result['primitives'])} primitives + {len(protocols)} protocols")
