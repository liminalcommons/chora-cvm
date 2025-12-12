"""
Genesis Habitation: The Re-Genesis Ritual.

This module creates a fresh database populated with:
1. Crystal Palace primitives (domain.* naming)
2. Graph-defined protocols
3. Provenance entities documenting what was built

If we ever need to wipe the mind and restart, this script
rebuilds the system's self-knowledge from first principles.

The recursion: The Crystal Palace documents its own creation using its own tools.

Usage:
    python -m chora_cvm.genesis_habitation [--db-path PATH]
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from chora_cvm.genesis_crystal import (
    bootstrap_crystal_palace,
    bootstrap_crystal_protocols,
)
from chora_cvm.lib.build import ast_scan
from chora_cvm.lib.graph import bond_manage, entity_create
from chora_cvm.schema import ExecutionContext
from chora_cvm.store import EventStore


def slugify(title: str, max_length: int = 50) -> str:
    """Convert title to ID-friendly slug."""
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:max_length]


# =============================================================================
# PROVENANCE DATA: What was built
# =============================================================================

PRINCIPLES = [
    {
        "id": "principle-executioncontext-is-mandatory",
        "title": "ExecutionContext is Mandatory",
        "statement": "All lib/ primitives require ExecutionContext (_ctx) parameter.",
        "rationale": "Primitives should not hardcode database paths. Context makes them composable.",
    },
    {
        "id": "principle-domain-noun-verb-naming",
        "title": "Domain.Noun.Verb Naming",
        "statement": "Primitives follow domain.noun.verb naming: graph.entity.get, io.fs.write",
        "rationale": "Discoverability. Agents can guess what exists.",
    },
    {
        "id": "principle-the-purity-test",
        "title": "The Purity Test",
        "statement": "Does it DO something? → Primitive. Does it make DECISIONS? → Protocol.",
        "rationale": "lib/ is for Capabilities, not Policies.",
    },
    {
        "id": "principle-map-contains-territory-coordinates",
        "title": "Map Contains Territory Coordinates",
        "statement": "Tool entities must store source_file and source_line from ast_scan.",
        "rationale": "The Map (Entity) must contain coordinates to the Territory (Code).",
    },
]

DOMAINS = {
    "graph": {
        "title": "lib/graph.py implements entity and bond operations",
        "given": "std.py has entity_get, manage_bond scattered across 4,994 lines",
        "when": "We extract entity/bond operations to lib/graph.py with ExecutionContext",
        "then": "Entity operations available as graph.entity.*, graph.bond.*, graph.query",
    },
    "io": {
        "title": "lib/io.py implements filesystem and UI operations",
        "given": "std.py has fs_read, fs_write, ui_render mixed with other code",
        "when": "We extract I/O operations to lib/io.py with sandboxing",
        "then": "Safe filesystem operations: io.fs.read, io.fs.write, io.fs.patch",
    },
    "build": {
        "title": "lib/build.py implements code analysis and build operations",
        "given": "std.py has run_lint, run_tests but lacks AST scanning",
        "when": "We create lib/build.py with code.ast.scan and code.scan.features",
        "then": "Agents can see code structure and scan BDD features",
    },
    "attention": {
        "title": "lib/attention.py implements Focus and Signal lifecycle",
        "given": "std.py has create_focus, emit_signal as legacy primitives",
        "when": "We extract attention primitives to lib/attention.py (the plasma layer)",
        "then": "Agents can declare intent (Focus) and emit interrupts (Signal)",
    },
    "sys": {
        "title": "lib/sys.py implements sandboxed subprocess execution",
        "given": "No safe subprocess primitive existed",
        "when": "We create lib/sys.py with shell_run (no shell=True)",
        "then": "Agents can run external tools safely with sys.shell.run",
    },
    "logic": {
        "title": "lib/logic.py implements pure data transformation",
        "given": "std.py has json_get, list_map as utility functions",
        "when": "We consolidate pure transformations in lib/logic.py",
        "then": "Data transformations available as logic.json.*, logic.list.*",
    },
    "cognition": {
        "title": "lib/cognition.py implements embedding and similarity",
        "given": "std.py has embed_text, vector_sim for semantic operations",
        "when": "We extract semantic primitives to lib/cognition.py",
        "then": "Semantic operations: cognition.embed.text, cognition.vector.*",
    },
    "chronos": {
        "title": "lib/chronos.py implements time operations",
        "given": "Time operations needed for timestamps and durations",
        "when": "We create lib/chronos.py with pure time primitives",
        "then": "Time operations: chronos.now, chronos.offset, chronos.diff",
    },
}


def bootstrap_fresh_mind(db_path: str = "chora-habitation.db", verbose: bool = True) -> EventStore:
    """
    Create fresh database with Crystal Palace primitives.

    This is Phase H0: The Empty Loom.

    Args:
        db_path: Path to database file
        verbose: Print progress

    Returns:
        EventStore connected to fresh database
    """
    if verbose:
        print(f"[H0] Creating fresh database: {db_path}")

    store = EventStore(db_path)

    # Bootstrap only Crystal Palace (no legacy primitive-*)
    result = bootstrap_crystal_palace(store, verbose=verbose)
    protocols = bootstrap_crystal_protocols(store, verbose=verbose)

    if verbose:
        print(f"\nFresh mind created:")
        print(f"  Primitives: {len(result['primitives'])} across {len(result['domains'])} domains")
        print(f"  Protocols: {len(protocols)}")

    return store


def discover_lib_functions(ctx: ExecutionContext, base_path: str = "packages/chora-cvm/src/chora_cvm/lib") -> dict[str, list[tuple[str, int, str, str]]]:
    """
    Use code.ast.scan to discover functions in lib/*.py.

    Returns dict mapping domain → [(func_name, line, tool_id, description), ...]
    """
    # Map of func_name → (tool_id, description) for known functions
    known_functions: dict[str, dict[str, tuple[str, str]]] = {
        "graph": {
            "entity_get": ("graph.entity.get", "Retrieve any entity by ID"),
            "entity_create": ("graph.entity.create", "Create a new entity"),
            "entity_update": ("graph.entity.update", "Update an existing entity"),
            "entity_archive": ("graph.entity.archive", "Archive an entity"),
            "bond_manage": ("graph.bond.manage", "Create, update, or remove bonds"),
            "bond_list": ("graph.bond.list", "List bonds for an entity"),
            "query": ("graph.query", "Query entities by type or pattern"),
        },
        "io": {
            "ui_render": ("io.ui.render", "Render user-facing output"),
            "sys_log": ("io.sys.log", "Log a message"),
            "fs_write": ("io.fs.write", "Write content to a file (sandboxed)"),
            "fs_read": ("io.fs.read", "Read content from a file (sandboxed)"),
            "fs_read_tree": ("io.fs.read_tree", "List files in directory tree"),
            "fs_patch": ("io.fs.patch", "Apply unified diff patches to files"),
        },
        "build": {
            "build_lint": ("code.build.lint", "Run ruff linter"),
            "build_test": ("code.build.test", "Run pytest"),
            "build_typecheck": ("code.build.typecheck", "Run mypy type checker"),
            "ast_scan": ("code.ast.scan", "Parse Python file structure"),
            "scan_features": ("code.scan.features", "Scan BDD feature files"),
        },
        "attention": {
            "focus_create": ("attention.focus.create", "Declare what is being attended to"),
            "focus_resolve": ("attention.focus.resolve", "Close the attention loop"),
            "focus_list": ("attention.focus.list", "List active focuses"),
            "signal_emit": ("attention.signal.emit", "Emit a signal"),
        },
        "sys": {
            "shell_run": ("sys.shell.run", "Execute command safely (no shell=True)"),
        },
        "logic": {
            "json_get": ("logic.json.get", "Get value from JSON path"),
            "json_set": ("logic.json.set", "Set value at JSON path"),
            "list_map": ("logic.list.map", "Map function over list"),
            "list_filter": ("logic.list.filter", "Filter list by predicate"),
            "list_sort": ("logic.list.sort", "Sort list"),
            "string_format": ("logic.string.format", "Format string with template"),
        },
        "cognition": {
            "embed_text": ("cognition.embed.text", "Generate text embedding"),
            "vector_sim": ("cognition.vector.sim", "Calculate vector similarity"),
            "vector_rank": ("cognition.vector.rank", "Rank by similarity"),
            "cluster": ("cognition.cluster", "Cluster vectors"),
        },
        "chronos": {
            "now": ("chronos.now", "Get current timestamp"),
            "offset": ("chronos.offset", "Add/subtract time duration"),
            "diff": ("chronos.diff", "Calculate time difference"),
        },
    }

    discovered: dict[str, list[tuple[str, int, str, str]]] = {}

    for domain in known_functions.keys():
        file_path = f"{base_path}/{domain}.py"
        try:
            result = ast_scan(file_path, ctx)
            if result["status"] == "success":
                functions = []
                for el in result["elements"]:
                    if el["type"] == "function" and not el["name"].startswith("_"):
                        func_name = el["name"]
                        if func_name in known_functions.get(domain, {}):
                            tool_id, desc = known_functions[domain][func_name]
                            functions.append((func_name, el["line"], tool_id, desc))
                discovered[domain] = functions
        except Exception:
            pass

    return discovered


def genesis_narrative(store: EventStore, verbose: bool = True) -> dict[str, Any]:
    """
    Use Crystal Palace tools to document their own creation.

    This is Phase H1: Genesis Narrative.

    Creates:
    - 1 Story entity
    - 4 Principle entities
    - 8 Behavior entities (one per domain)
    - 36 Tool entities (with source coordinates)
    - 82+ bonds (clarifies, specifies, implements, verifies)

    Args:
        store: EventStore connected to database
        verbose: Print progress

    Returns:
        Summary of created entities
    """
    ctx = ExecutionContext(db_path=store._path, persona_id="genesis")

    if verbose:
        print("\n[H1] Genesis Narrative: Documenting Crystal Palace...")

    # Story
    story_id = "story-build-the-crystal-palace"
    entity_create(
        entity_type="story",
        entity_id=story_id,
        data={
            "title": "Build the Crystal Palace",
            "description": "Transform std.py monolith into domain-organized primitives",
            "completion_date": "2025-12-11",
        },
        _ctx=ctx,
    )
    if verbose:
        print(f"  Story: {story_id}")

    # Principles
    principle_ids = []
    for p in PRINCIPLES:
        entity_create(
            entity_type="principle",
            entity_id=p["id"],
            data={"title": p["title"], "statement": p["statement"], "rationale": p["rationale"]},
            _ctx=ctx,
        )
        principle_ids.append(p["id"])
    if verbose:
        print(f"  Principles: {len(principle_ids)}")

    # Behaviors
    behavior_ids = {}
    for domain, spec in DOMAINS.items():
        b_id = f"behavior-{domain}-domain"
        entity_create(
            entity_type="behavior",
            entity_id=b_id,
            data={"title": spec["title"], "given": spec["given"], "when": spec["when"], "then": spec["then"], "domain": domain},
            _ctx=ctx,
        )
        behavior_ids[domain] = b_id
    if verbose:
        print(f"  Behaviors: {len(behavior_ids)}")

    # Tools (with source coordinates from ast_scan)
    functions = discover_lib_functions(ctx)
    tool_ids: dict[str, list[str]] = {}
    total_tools = 0

    for domain, funcs in functions.items():
        source_file = f"packages/chora-cvm/src/chora_cvm/lib/{domain}.py"
        tool_ids[domain] = []
        for func_name, line, tool_id, description in funcs:
            entity_id = f"tool-{tool_id.replace('.', '-')}"
            entity_create(
                entity_type="tool",
                entity_id=entity_id,
                data={
                    "title": tool_id,
                    "handler": f"chora_cvm.lib.{domain}.{func_name}",
                    "source_file": source_file,
                    "source_line": line,
                    "phenomenology": description,
                    "domain": domain,
                },
                _ctx=ctx,
            )
            tool_ids[domain].append(entity_id)
            total_tools += 1

    if verbose:
        print(f"  Tools: {total_tools} (with source coordinates)")

    # Bonds
    bond_count = 0

    # Principles clarify Story
    for p_id in principle_ids:
        bond_manage(bond_type="clarifies", from_id=p_id, to_id=story_id, _ctx=ctx, enforce_physics=False)
        bond_count += 1

    # Story specifies Behaviors
    for b_id in behavior_ids.values():
        bond_manage(bond_type="specifies", from_id=story_id, to_id=b_id, _ctx=ctx, enforce_physics=False)
        bond_count += 1

    # Behaviors implement Tools, Tools verify Behaviors
    for domain, tools in tool_ids.items():
        b_id = behavior_ids.get(domain)
        if b_id:
            for t_id in tools:
                bond_manage(bond_type="implements", from_id=b_id, to_id=t_id, _ctx=ctx, enforce_physics=False)
                bond_manage(bond_type="verifies", from_id=t_id, to_id=b_id, _ctx=ctx, enforce_physics=False)
                bond_count += 2

    if verbose:
        print(f"  Bonds: {bond_count}")

    return {
        "story_id": story_id,
        "principles": len(principle_ids),
        "behaviors": len(behavior_ids),
        "tools": total_tools,
        "bonds": bond_count,
    }


def main(db_path: str = "chora-habitation.db") -> None:
    """
    Execute the full Fresh Mind Genesis.

    This is the re-genesis ritual that can rebuild the system's
    self-knowledge from first principles.
    """
    print("=" * 60)
    print("FRESH MIND GENESIS")
    print("=" * 60)

    # Phase H0: Fresh Mind
    store = bootstrap_fresh_mind(db_path)

    # Phase H1: Genesis Narrative
    result = genesis_narrative(store)

    # Summary
    print("\n" + "=" * 60)
    print("GENESIS COMPLETE")
    print("=" * 60)

    # Count entities
    cur = store._conn.execute("SELECT type, COUNT(*) FROM entities GROUP BY type ORDER BY type")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")

    store.close()

    print(f"\nDatabase: {db_path}")
    print("The Crystal Palace documents its own creation.")


if __name__ == "__main__":
    import sys

    db_path = "chora-habitation.db"
    if len(sys.argv) > 1 and sys.argv[1] == "--db-path" and len(sys.argv) > 2:
        db_path = sys.argv[2]

    main(db_path)
