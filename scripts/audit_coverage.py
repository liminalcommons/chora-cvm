#!/usr/bin/env python3
"""
Code-to-Behavior Audit Tool

Discovers which behavioral code has corresponding entity specifications
in the Loom. Reports gaps between implementation and specification.

Usage:
    python scripts/audit_coverage.py [--db PATH] [--check] [--verbose]

Categories:
    Behavioral: User-facing capability -> needs behavior + tool entities
    Primitive: Kernel building block -> needs primitive entity
    Infrastructure: Internal plumbing -> document only (no entity needed)
"""

import argparse
import ast
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# =============================================================================
# Configuration: Module Classification
# =============================================================================

# Functions classified as behavioral (user-facing capability)
# These should have behavior + tool entities
BEHAVIORAL_FUNCTIONS = {
    # Attention Layer (Focus/Signal)
    "create_focus",
    "resolve_focus",
    "list_active_focuses",
    "emit_signal",
    # Bonding Layer
    "manage_bond",
    "get_constellation",
    "update_bond_confidence",
    # Self-Teaching Layer
    "teach_scan_usage",
    "surface_by_context",
    "teach_format",
    "entity_doc_bundle",
    # Manifestation
    "manifest_entity",
    "manifest_entities",
    # Integrity
    "integrity_check",
    "integrity_report",
    "integrity_discover_scenarios",
    # Pulse
    "pulse_check_signals",
    "pulse_preview",
}

# Function names that map to differently-named tools (canonical names)
# Used when function name doesn't match tool name pattern
FUNCTION_TO_TOOL = {
    "create_focus": "tool-engage",      # Canonical: engage > create-focus
    "resolve_focus": "tool-resolve",    # Canonical: resolve > resolve-focus
}

# Functions classified as primitive (kernel building blocks)
# These should have primitive entities
PRIMITIVE_FUNCTIONS = {
    "sys_log",
    "identity_primitive",
    "ui_render",
    "sqlite_query",
    "json_parse",
    "entities_query",
    "fts_index_entity",
    "fts_search",
    "write_file",
    "update_verifies_bond_metadata",
}

# Functions classified as infrastructure (internal plumbing)
# These don't need entities but should be documented
INFRASTRUCTURE_FUNCTIONS = {
    "_resolve_entity",
    "_fire_entity_hooks",
    "_ensure_schema",
}

# Modules to audit
MODULES_TO_AUDIT = [
    "std.py",
    "cli.py",
    "vm.py",
    "runner.py",
    "worker.py",
    "keyring.py",
    "invitation.py",
    "sync_bridge.py",
    "sync_router.py",
]


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FunctionInfo:
    """Information about a discovered function."""
    name: str
    module: str
    line_number: int
    docstring: str | None = None
    is_public: bool = True


@dataclass
class EntityInfo:
    """Information about an entity from the Loom."""
    id: str
    type: str
    title: str | None = None
    python_ref: str | None = None


@dataclass
class SemanticSuggestion:
    """A semantic classification suggestion for an unclassified function."""
    func: FunctionInfo
    suggested_type: str  # "behavioral", "primitive", "infrastructure"
    matched_entity: str
    similarity: float
    reasoning: str


@dataclass
class AuditResult:
    """Results of the audit."""
    functions: list[FunctionInfo] = field(default_factory=list)
    behaviors: list[EntityInfo] = field(default_factory=list)
    primitives: list[EntityInfo] = field(default_factory=list)
    tools: list[EntityInfo] = field(default_factory=list)

    # Gaps
    behavioral_gaps: list[FunctionInfo] = field(default_factory=list)
    primitive_gaps: list[FunctionInfo] = field(default_factory=list)
    tool_gaps: list[EntityInfo] = field(default_factory=list)  # behaviors without tools
    infrastructure: list[FunctionInfo] = field(default_factory=list)
    unclassified: list[FunctionInfo] = field(default_factory=list)

    # Semantic suggestions for unclassified functions
    semantic_suggestions: list[SemanticSuggestion] = field(default_factory=list)
    semantic_method: str = "unavailable"


# =============================================================================
# Code Discovery (AST)
# =============================================================================


def extract_functions(module_path: Path) -> list[FunctionInfo]:
    """Extract public functions from a Python module using AST."""
    if not module_path.exists():
        return []

    try:
        source = module_path.read_text()
        tree = ast.parse(source)
    except SyntaxError:
        return []

    functions = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            is_public = not node.name.startswith("_")
            docstring = ast.get_docstring(node)

            functions.append(FunctionInfo(
                name=node.name,
                module=module_path.stem,
                line_number=node.lineno,
                docstring=docstring,
                is_public=is_public,
            ))

    return functions


def discover_code(src_dir: Path) -> list[FunctionInfo]:
    """Discover all functions in the codebase."""
    all_functions = []

    for module_name in MODULES_TO_AUDIT:
        module_path = src_dir / module_name
        functions = extract_functions(module_path)
        all_functions.extend(functions)

    return all_functions


# =============================================================================
# Loom Query
# =============================================================================


def query_entities(db_path: Path) -> tuple[list[EntityInfo], list[EntityInfo], list[EntityInfo]]:
    """Query behavior, primitive, and tool entities from the Loom."""
    behaviors = []
    primitives = []
    tools = []

    if not db_path.exists():
        return behaviors, primitives, tools

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Query behaviors
    cur = conn.execute("SELECT id, type, data_json FROM entities WHERE type = 'behavior'")
    for row in cur.fetchall():
        data = json.loads(row["data_json"])
        behaviors.append(EntityInfo(
            id=row["id"],
            type=row["type"],
            title=data.get("title"),
        ))

    # Query primitives
    cur = conn.execute("SELECT id, type, data_json FROM entities WHERE type = 'primitive'")
    for row in cur.fetchall():
        data = json.loads(row["data_json"])
        primitives.append(EntityInfo(
            id=row["id"],
            type=row["type"],
            title=data.get("title"),
            python_ref=data.get("python_ref"),
        ))

    # Query tools (excluding deprecated and internal)
    cur = conn.execute("""
        SELECT id, type, data_json FROM entities
        WHERE type = 'tool'
        AND COALESCE(json_extract(data_json, '$.status'), 'active') != 'deprecated'
        AND COALESCE(json_extract(data_json, '$.internal'), json('false')) != json('true')
    """)
    for row in cur.fetchall():
        data = json.loads(row["data_json"])
        tools.append(EntityInfo(
            id=row["id"],
            type=row["type"],
            title=data.get("title"),
            python_ref=data.get("handler"),
        ))

    # Query behaviors with implements bonds (behaviors wired to tools)
    cur = conn.execute("""
        SELECT b.from_id as behavior_id, b.to_id as tool_id
        FROM bonds b
        WHERE b.type = 'implements'
    """)
    implements_bonds = {row["behavior_id"]: row["tool_id"] for row in cur.fetchall()}

    conn.close()

    return behaviors, primitives, tools


def query_bonds(db_path: Path) -> dict[str, list[str]]:
    """Query implements bonds from behaviors to tools."""
    bonds = {}

    if not db_path.exists():
        return bonds

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    cur = conn.execute("""
        SELECT from_id, to_id FROM bonds WHERE type = 'implements'
    """)
    for row in cur.fetchall():
        if row["from_id"] not in bonds:
            bonds[row["from_id"]] = []
        bonds[row["from_id"]].append(row["to_id"])

    conn.close()
    return bonds


# =============================================================================
# Semantic Classification (Tiered Resolution)
# =============================================================================


def semantic_classify_functions(
    unclassified: list[FunctionInfo],
    db_path: Path,
    threshold: float = 0.20,
) -> tuple[list[SemanticSuggestion], str]:
    """
    Use semantic search to suggest classifications for unclassified functions.

    Returns tuple of (suggestions, method) where method is "semantic" or "fallback".
    Gracefully degrades when chora-inference is unavailable.
    """
    suggestions = []

    try:
        # Try semantic tier
        import sys
        from pathlib import Path as P
        workspace = P(__file__).parent.parent.parent.parent
        sys.path.insert(0, str(workspace / "packages" / "chora-cvm" / "src"))
        sys.path.insert(0, str(workspace / "packages" / "chora-inference" / "src"))

        from chora_cvm.semantic import semantic_search

        for func in unclassified[:20]:  # Limit to avoid excessive API calls
            # Build query from function name + docstring
            query_parts = [func.name.replace("_", " ")]
            if func.docstring:
                query_parts.append(func.docstring[:100])
            query = " ".join(query_parts)

            # Search for matching entities
            result = semantic_search(str(db_path), query, limit=3)

            if result.get("method") != "semantic":
                continue

            for match in result.get("results", []):
                similarity = match.get("similarity", 0)
                if similarity < threshold:
                    continue

                entity_type = match.get("type", "")
                entity_id = match.get("id", "")

                # Infer suggested classification from matched entity type
                if entity_type in ("behavior", "tool"):
                    suggested = "behavioral"
                    reasoning = f"Semantically similar to {entity_type} '{entity_id}'"
                elif entity_type == "primitive":
                    suggested = "primitive"
                    reasoning = f"Semantically similar to primitive '{entity_id}'"
                else:
                    # Skip non-behavioral entity matches
                    continue

                suggestions.append(SemanticSuggestion(
                    func=func,
                    suggested_type=suggested,
                    matched_entity=entity_id,
                    similarity=similarity,
                    reasoning=reasoning,
                ))
                break  # One suggestion per function

        return suggestions, "semantic"

    except ImportError:
        # Graceful degradation - semantic classification unavailable
        return [], "fallback"
    except Exception:
        # Other errors - degrade gracefully
        return [], "fallback"


# =============================================================================
# Audit Logic
# =============================================================================


def classify_function(func: FunctionInfo) -> str:
    """Classify a function as behavioral, primitive, or infrastructure."""
    if func.name in BEHAVIORAL_FUNCTIONS:
        return "behavioral"
    elif func.name in PRIMITIVE_FUNCTIONS:
        return "primitive"
    elif func.name in INFRASTRUCTURE_FUNCTIONS:
        return "infrastructure"
    elif func.name.startswith("_"):
        return "infrastructure"
    else:
        return "unclassified"


def run_audit(src_dir: Path, db_path: Path) -> AuditResult:
    """Run the full audit."""
    result = AuditResult()

    # Discover code
    all_functions = discover_code(src_dir)
    result.functions = [f for f in all_functions if f.is_public]

    # Query Loom
    behaviors, primitives, tools = query_entities(db_path)
    result.behaviors = behaviors
    result.primitives = primitives
    result.tools = tools

    implements_bonds = query_bonds(db_path)

    # Build lookup sets
    primitive_refs = {p.python_ref for p in primitives if p.python_ref}
    primitive_names = {p.python_ref.split(".")[-1] for p in primitives if p.python_ref}
    tool_handlers = {t.python_ref for t in tools if t.python_ref}
    tool_names = {t.python_ref.split(".")[-1] for t in tools if t.python_ref}

    # Classify functions and find gaps
    for func in all_functions:
        classification = classify_function(func)

        if classification == "behavioral":
            # Check if there's a behavior entity that maps to this function
            # Also check if there's a tool entity
            full_ref = f"chora_cvm.{func.module}.{func.name}"
            has_tool = full_ref in tool_handlers or func.name in tool_names

            # Check if this function maps to a canonical tool with different name
            if not has_tool and func.name in FUNCTION_TO_TOOL:
                canonical_tool = FUNCTION_TO_TOOL[func.name]
                has_tool = any(t.id == canonical_tool for t in tools)

            if not has_tool:
                result.behavioral_gaps.append(func)

        elif classification == "primitive":
            # Check if there's a primitive entity for this function
            full_ref = f"chora_cvm.{func.module}.{func.name}"
            has_primitive = full_ref in primitive_refs or func.name in primitive_names

            if not has_primitive:
                result.primitive_gaps.append(func)

        elif classification == "infrastructure":
            result.infrastructure.append(func)

        else:
            result.unclassified.append(func)

    # Find behaviors without implements bonds to tools
    behaviors_with_tools = set(implements_bonds.keys())
    for behavior in behaviors:
        if behavior.id not in behaviors_with_tools:
            result.tool_gaps.append(behavior)

    # Semantic classification of unclassified functions
    if result.unclassified:
        suggestions, method = semantic_classify_functions(result.unclassified, db_path)
        result.semantic_suggestions = suggestions
        result.semantic_method = method

    return result


# =============================================================================
# Reporting
# =============================================================================


def print_report(result: AuditResult, verbose: bool = False) -> None:
    """Print the audit report."""
    total_functions = len(result.functions)
    behavioral_count = len(BEHAVIORAL_FUNCTIONS)
    primitive_count = len(PRIMITIVE_FUNCTIONS)
    infra_count = len(result.infrastructure)

    print()
    print("Code Coverage Audit")
    print("=" * 60)
    print()

    # Summary
    print("Summary")
    print("-" * 40)
    print(f"  Total public functions discovered: {total_functions}")
    print(f"  Behavioral functions: {behavioral_count}")
    print(f"  Primitive functions: {primitive_count}")
    print(f"  Infrastructure functions: {infra_count}")
    print(f"  Unclassified: {len(result.unclassified)}")
    print()

    # Entity counts
    print("Loom Entities")
    print("-" * 40)
    print(f"  Behavior entities: {len(result.behaviors)}")
    print(f"  Primitive entities: {len(result.primitives)}")
    print(f"  Tool entities: {len(result.tools)}")
    print()

    # Gaps
    print("Gaps Detected")
    print("-" * 40)

    if result.behavioral_gaps:
        print(f"\n  Behavioral gaps (missing tool entities): {len(result.behavioral_gaps)}")
        for func in result.behavioral_gaps:
            print(f"    - {func.name} ({func.module}.py:{func.line_number})")

    if result.primitive_gaps:
        print(f"\n  Primitive gaps (missing primitive entities): {len(result.primitive_gaps)}")
        for func in result.primitive_gaps:
            print(f"    - {func.name} ({func.module}.py:{func.line_number})")

    if result.tool_gaps:
        print(f"\n  Behaviors without tool wiring: {len(result.tool_gaps)}")
        for behavior in result.tool_gaps:
            print(f"    - {behavior.id}")

    if not (result.behavioral_gaps or result.primitive_gaps or result.tool_gaps):
        print("  No gaps detected!")

    print()

    # Semantic suggestions for unclassified functions
    if result.semantic_suggestions:
        print("Semantic Classification Suggestions")
        print("-" * 40)
        print(f"  Method: {result.semantic_method}")
        print()
        for suggestion in result.semantic_suggestions:
            sim_pct = f"{suggestion.similarity:.0%}"
            print(f"  {suggestion.func.name} -> {suggestion.suggested_type} ({sim_pct})")
            print(f"    {suggestion.reasoning}")
        print()
    elif result.semantic_method == "fallback" and result.unclassified:
        print("Semantic Classification")
        print("-" * 40)
        print("  (chora-inference unavailable - using fallback)")
        print()

    # Unclassified (if verbose)
    if verbose and result.unclassified:
        print("Unclassified Functions (need manual review)")
        print("-" * 40)
        for func in result.unclassified:
            print(f"  - {func.name} ({func.module}.py:{func.line_number})")
        print()

    # Coverage calculation
    total_behavioral = len(BEHAVIORAL_FUNCTIONS)
    covered_behavioral = total_behavioral - len(result.behavioral_gaps)
    behavioral_coverage = (covered_behavioral / total_behavioral * 100) if total_behavioral else 0

    total_primitives = len(PRIMITIVE_FUNCTIONS)
    covered_primitives = total_primitives - len(result.primitive_gaps)
    primitive_coverage = (covered_primitives / total_primitives * 100) if total_primitives else 0

    print("Coverage")
    print("-" * 40)
    print(f"  Behavioral code -> tool entities: {covered_behavioral}/{total_behavioral} ({behavioral_coverage:.0f}%)")
    print(f"  Primitive code -> primitive entities: {covered_primitives}/{total_primitives} ({primitive_coverage:.0f}%)")
    print()


def check_mode(result: AuditResult) -> int:
    """Return exit code based on gaps found."""
    has_gaps = (
        len(result.behavioral_gaps) > 0 or
        len(result.primitive_gaps) > 0
    )
    return 1 if has_gaps else 0


# =============================================================================
# Main
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="Audit code-to-behavior coverage")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("chora-cvm-manifest.db"),
        help="Path to Loom database (default: chora-cvm-manifest.db)",
    )
    parser.add_argument(
        "--src",
        type=Path,
        default=Path("packages/chora-cvm/src/chora_cvm"),
        help="Path to source directory",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit with code 1 if gaps found (for CI)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show unclassified functions",
    )
    args = parser.parse_args()

    # Resolve paths
    workspace = Path(__file__).parent.parent.parent.parent
    db_path = args.db if args.db.is_absolute() else workspace / args.db
    src_dir = args.src if args.src.is_absolute() else workspace / args.src

    # Run audit
    result = run_audit(src_dir, db_path)

    # Report
    print_report(result, verbose=args.verbose)

    # Check mode
    if args.check:
        exit_code = check_mode(result)
        if exit_code:
            print("Gaps detected. Run without --check for details.")
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
