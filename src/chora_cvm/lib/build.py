"""
Domain: Code (Somatic/Build)
ID Prefix: code.*
Module: build.py (not code.py to avoid collision with Python's built-in code module)

Build and code analysis primitives. These are "allow-listed" operations
that only execute specific, safe tools (ruff, pytest, mypy) - not arbitrary
shell commands.

Primitives:
  - code.build.lint: Run ruff linter on a package
  - code.build.test: Run pytest on a package
  - code.build.typecheck: Run mypy type checker on a package
  - code.ast.scan: Parse Python file and extract structural elements
  - code.scan.features: Scan BDD feature files for behavior tags
"""
from __future__ import annotations

import ast
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from ..schema import ExecutionContext


def build_lint(
    package_path: str,
    _ctx: ExecutionContext,
    fix: bool = False,
) -> Dict[str, Any]:
    """
    Primitive: code.build.lint

    Run ruff linter on a Python package.

    This is an allow-listed build primitive - only runs ruff, not arbitrary
    shell commands. Safe for protocol invocation.

    Args:
        package_path: Path to the package directory
        _ctx: Execution context (MANDATORY in lib/)
        fix: If True, run ruff with --fix flag

    Returns:
        {
            "status": "success",
            "success": bool,
            "exit_code": int,
            "stdout": str,
            "stderr": str,
            "tool": "ruff",
            "package": str,
        }
    """
    pkg = Path(package_path)
    if not pkg.exists():
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": f"Package path does not exist: {package_path}",
            "tool": "ruff",
            "package": pkg.name,
        }

    cmd = ["ruff", "check", str(pkg)]
    if fix:
        cmd.append("--fix")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return {
            "status": "success",
            "success": result.returncode == 0,
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "tool": "ruff",
            "package": pkg.name,
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": "Lint operation timed out after 120 seconds",
            "tool": "ruff",
            "package": pkg.name,
        }
    except FileNotFoundError:
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": "ruff not found. Install with: pip install ruff",
            "tool": "ruff",
            "package": pkg.name,
        }


def build_test(
    package_path: str,
    _ctx: ExecutionContext,
    coverage: bool = True,
    coverage_threshold: int = 80,
) -> Dict[str, Any]:
    """
    Primitive: code.build.test

    Run pytest on a Python package.

    This is an allow-listed build primitive - only runs pytest, not arbitrary
    shell commands. Safe for protocol invocation.

    Args:
        package_path: Path to the package directory
        _ctx: Execution context (MANDATORY in lib/)
        coverage: If True, run with coverage reporting
        coverage_threshold: Minimum coverage percentage required

    Returns:
        {
            "status": "success",
            "success": bool,
            "exit_code": int,
            "stdout": str,
            "stderr": str,
            "tool": "pytest",
            "package": str,
            "coverage_met": bool | None,
        }
    """
    pkg = Path(package_path)
    if not pkg.exists():
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": f"Package path does not exist: {package_path}",
            "tool": "pytest",
            "package": pkg.name,
            "coverage_met": None,
        }

    cmd = ["pytest", str(pkg), "-v"]
    if coverage:
        cmd.extend([
            f"--cov={pkg / 'src'}",
            "--cov-report=term-missing",
            f"--cov-fail-under={coverage_threshold}",
        ])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
            cwd=str(pkg),
        )

        # Check if coverage threshold was met
        coverage_met = None
        if coverage:
            coverage_met = result.returncode == 0

        return {
            "status": "success",
            "success": result.returncode == 0,
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "tool": "pytest",
            "package": pkg.name,
            "coverage_met": coverage_met,
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": "Test operation timed out after 600 seconds",
            "tool": "pytest",
            "package": pkg.name,
            "coverage_met": None,
        }
    except FileNotFoundError:
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": "pytest not found. Install with: pip install pytest",
            "tool": "pytest",
            "package": pkg.name,
            "coverage_met": None,
        }


def build_typecheck(
    package_path: str,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: code.build.typecheck

    Run mypy type checker on a Python package.

    This is an allow-listed build primitive - only runs mypy, not arbitrary
    shell commands. Safe for protocol invocation.

    Args:
        package_path: Path to the package directory
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {
            "status": "success",
            "success": bool,
            "exit_code": int,
            "stdout": str,
            "stderr": str,
            "tool": "mypy",
            "package": str,
        }
    """
    pkg = Path(package_path)
    if not pkg.exists():
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": f"Package path does not exist: {package_path}",
            "tool": "mypy",
            "package": pkg.name,
        }

    cmd = ["mypy", str(pkg)]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        return {
            "status": "success",
            "success": result.returncode == 0,
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "tool": "mypy",
            "package": pkg.name,
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": "Type check timed out after 300 seconds",
            "tool": "mypy",
            "package": pkg.name,
        }
    except FileNotFoundError:
        return {
            "status": "error",
            "success": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": "mypy not found. Install with: pip install mypy",
            "tool": "mypy",
            "package": pkg.name,
        }


# =============================================================================
# AST Analysis
# =============================================================================


def ast_scan(
    file_path: str,
    _ctx: ExecutionContext,
    include_docstrings: bool = False,
    include_imports: bool = False,
) -> Dict[str, Any]:
    """
    Primitive: code.ast.scan

    Parse a Python file and extract structural elements (classes, functions,
    methods). This is a "Fat Primitive" - it does real work but returns
    structured data rather than making decisions.

    Cross-platform: Uses pathlib and returns POSIX-style paths.

    Args:
        file_path: Path to the Python file to scan
        _ctx: Execution context (MANDATORY in lib/)
        include_docstrings: If True, include docstrings in the output
        include_imports: If True, include import statements

    Returns:
        {
            "status": "success",
            "path": str (POSIX-style),
            "elements": [
                {
                    "type": "class" | "function" | "method" | "import",
                    "name": str,
                    "line": int,
                    "end_line": int,
                    "docstring": str | None (if include_docstrings),
                    "parent": str | None (for methods, the class name),
                    "decorators": [str],
                    "is_async": bool,
                },
                ...
            ],
            "count": int,
        }
        or {"status": "error", "error": str} on failure
    """
    path = Path(file_path)

    if not path.exists():
        return {
            "status": "error",
            "error": f"File not found: {file_path}",
            "path": path.as_posix(),
            "elements": [],
            "count": 0,
        }

    if not path.is_file():
        return {
            "status": "error",
            "error": f"Not a file: {file_path}",
            "path": path.as_posix(),
            "elements": [],
            "count": 0,
        }

    if path.suffix != ".py":
        return {
            "status": "error",
            "error": f"Not a Python file: {file_path}",
            "path": path.as_posix(),
            "elements": [],
            "count": 0,
        }

    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as e:
        return {
            "status": "error",
            "error": f"Syntax error: {e.msg} at line {e.lineno}",
            "path": path.as_posix(),
            "elements": [],
            "count": 0,
        }
    except Exception as e:
        return {
            "status": "error",
            "error": f"Parse error: {str(e)}",
            "path": path.as_posix(),
            "elements": [],
            "count": 0,
        }

    elements: List[Dict[str, Any]] = []

    def get_decorators(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> List[str]:
        """Extract decorator names from a node."""
        decorators = []
        for dec in node.decorator_list:
            if isinstance(dec, ast.Name):
                decorators.append(dec.id)
            elif isinstance(dec, ast.Attribute):
                decorators.append(f"{dec.value.id}.{dec.attr}" if isinstance(dec.value, ast.Name) else dec.attr)
            elif isinstance(dec, ast.Call):
                if isinstance(dec.func, ast.Name):
                    decorators.append(dec.func.id)
                elif isinstance(dec.func, ast.Attribute):
                    decorators.append(dec.func.attr)
        return decorators

    def get_docstring(node: ast.AST) -> str | None:
        """Extract docstring from a node if include_docstrings is True."""
        if not include_docstrings:
            return None
        return ast.get_docstring(node)

    # Extract imports if requested
    if include_imports:
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    elements.append({
                        "type": "import",
                        "name": alias.name,
                        "line": node.lineno,
                        "end_line": node.end_lineno or node.lineno,
                        "parent": None,
                        "decorators": [],
                        "is_async": False,
                    })
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    name = f"{module}.{alias.name}" if module else alias.name
                    elements.append({
                        "type": "import",
                        "name": name,
                        "line": node.lineno,
                        "end_line": node.end_lineno or node.lineno,
                        "parent": None,
                        "decorators": [],
                        "is_async": False,
                    })

    # Extract top-level functions and classes
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            element = {
                "type": "function",
                "name": node.name,
                "line": node.lineno,
                "end_line": node.end_lineno or node.lineno,
                "parent": None,
                "decorators": get_decorators(node),
                "is_async": isinstance(node, ast.AsyncFunctionDef),
            }
            if include_docstrings:
                element["docstring"] = get_docstring(node)
            elements.append(element)

        elif isinstance(node, ast.ClassDef):
            class_element = {
                "type": "class",
                "name": node.name,
                "line": node.lineno,
                "end_line": node.end_lineno or node.lineno,
                "parent": None,
                "decorators": get_decorators(node),
                "is_async": False,
            }
            if include_docstrings:
                class_element["docstring"] = get_docstring(node)
            elements.append(class_element)

            # Extract methods from the class
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    method_element = {
                        "type": "method",
                        "name": child.name,
                        "line": child.lineno,
                        "end_line": child.end_lineno or child.lineno,
                        "parent": node.name,
                        "decorators": get_decorators(child),
                        "is_async": isinstance(child, ast.AsyncFunctionDef),
                    }
                    if include_docstrings:
                        method_element["docstring"] = get_docstring(child)
                    elements.append(method_element)

    return {
        "status": "success",
        "path": path.as_posix(),
        "elements": elements,
        "count": len(elements),
    }


# =============================================================================
# Feature File Scanning
# =============================================================================


def scan_features(
    features_dir: str,
    _ctx: ExecutionContext,
    tag_pattern: str = r"@behavior:(\S+)",
) -> Dict[str, Any]:
    """
    Primitive: code.scan.features

    Scan BDD feature files for behavior tags. This is a pure primitive that
    only reads files - it doesn't access the database or make decisions.
    The protocol layer handles mapping to entities.

    Cross-platform: Uses pathlib and returns POSIX-style paths.

    Args:
        features_dir: Directory containing .feature files
        _ctx: Execution context (MANDATORY in lib/)
        tag_pattern: Regex pattern to extract tags (default: @behavior:*)

    Returns:
        {
            "status": "success",
            "root": str (POSIX-style),
            "features": [
                {
                    "path": str (POSIX-style relative path),
                    "tags": ["behavior-xyz", ...],
                    "scenario_count": int,
                },
                ...
            ],
            "all_tags": ["behavior-xyz", ...],  # Deduplicated list
            "feature_count": int,
            "total_scenarios": int,
        }
        or {"status": "error", "error": str} on failure
    """
    import re

    root = Path(features_dir).resolve()

    if not root.exists():
        return {
            "status": "error",
            "error": f"Directory not found: {features_dir}",
            "root": root.as_posix(),
            "features": [],
            "all_tags": [],
            "feature_count": 0,
            "total_scenarios": 0,
        }

    if not root.is_dir():
        return {
            "status": "error",
            "error": f"Not a directory: {features_dir}",
            "root": root.as_posix(),
            "features": [],
            "all_tags": [],
            "feature_count": 0,
            "total_scenarios": 0,
        }

    try:
        pattern = re.compile(tag_pattern)
    except re.error as e:
        return {
            "status": "error",
            "error": f"Invalid regex pattern: {e}",
            "root": root.as_posix(),
            "features": [],
            "all_tags": [],
            "feature_count": 0,
            "total_scenarios": 0,
        }

    features: List[Dict[str, Any]] = []
    all_tags: set = set()
    total_scenarios = 0

    # Scan all .feature files
    for feature_file in sorted(root.glob("**/*.feature")):
        try:
            content = feature_file.read_text(encoding="utf-8")
        except Exception:
            continue  # Skip unreadable files

        # Extract tags using the pattern
        tags = pattern.findall(content)

        # Count scenarios (lines starting with "Scenario:" or "Scenario Outline:")
        scenario_count = len(re.findall(r"^\s*Scenario( Outline)?:", content, re.MULTILINE))

        # Normalize tags (add behavior- prefix if missing)
        normalized_tags = []
        for tag in tags:
            if not tag.startswith("behavior-"):
                normalized_tags.append(f"behavior-{tag}")
            else:
                normalized_tags.append(tag)

        if normalized_tags:
            rel_path = feature_file.relative_to(root).as_posix()
            features.append({
                "path": rel_path,
                "tags": normalized_tags,
                "scenario_count": scenario_count,
            })
            all_tags.update(normalized_tags)
            total_scenarios += scenario_count

    return {
        "status": "success",
        "root": root.as_posix(),
        "features": features,
        "all_tags": sorted(all_tags),
        "feature_count": len(features),
        "total_scenarios": total_scenarios,
    }
