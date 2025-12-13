"""
Domain: I/O (Membrane)
ID Prefix: io.*

The only place allowed to touch sys.stdout (via _ctx.output_sink).
All user-facing output flows through this domain, enabling the Nucleus/Membrane
separation where logic is decoupled from display.

Primitives:
  - io.ui.render: Render content to output sink with optional styling
  - io.sys.log: Log a message to output sink
  - io.fs.write: Write to filesystem (sandboxed)
  - io.fs.read: Read from filesystem (sandboxed)
  - io.fs.read_tree: List files in a directory tree (respects .gitignore)
  - io.fs.patch: Apply unified diff patches to files (surgical edits)
"""
from __future__ import annotations

import fnmatch
import re
from pathlib import Path
from typing import Any, Dict, List, Set

from ..schema import ExecutionContext


def ui_render(
    content: str,
    _ctx: ExecutionContext,
    style: str = "plain",
    title: str | None = None,
) -> Dict[str, Any]:
    """
    Primitive: io.ui.render

    Render user-facing output to the configured sink.

    This is the voice of protocols. All user-visible output should flow
    through this primitive so the Nucleus (logic) is decoupled from
    the Membrane (display). CLI passes print, API passes a buffer collector.

    Args:
        content: The text to render (supports markdown)
        _ctx: Execution context with optional output_sink (MANDATORY in lib/)
        style: Rendering style - "plain", "box", "heading", "success", "warning", "error"
        title: Optional title for boxed content

    Returns:
        {"status": "success", "rendered": True}
    """
    # Helper for consistent output - uses context sink (via emit)
    def out(text: str) -> None:
        _ctx.emit(text)

    if style == "box":
        width = 60
        if title:
            out(f"╭{'─' * (width - 2)}╮")
            out(f"│  {title:<{width - 5}}│")
            out(f"╰{'─' * (width - 2)}╯")
        else:
            out(f"╭{'─' * (width - 2)}╮")
        out("")
        for line in content.split("\n"):
            out(f"  {line}")
        out("")
    elif style == "heading":
        out("")
        out(f"## {content}")
        out("")
    elif style == "success":
        out(f"✓ {content}")
    elif style == "warning":
        out(f"⚠️  {content}")
    elif style == "error":
        out(f"✗ {content}")
    else:  # plain
        out(content)

    return {"status": "success", "rendered": True}


def sys_log(
    message: str,
    _ctx: ExecutionContext,
    level: str = "info",
) -> Dict[str, Any]:
    """
    Primitive: io.sys.log

    Log a message to the configured output sink.

    Uses I/O Membrane pattern: routes output through context sink.
    The level prefix helps distinguish log severity in output streams.

    Args:
        message: The message to log
        _ctx: Execution context with optional output_sink (MANDATORY in lib/)
        level: Log level - "debug", "info", "warn", "error"

    Returns:
        {"status": "success", "logged": True}
    """
    prefixes = {
        "debug": "[DEBUG]",
        "info": "[CVM LOG]",
        "warn": "[WARN]",
        "error": "[ERROR]",
    }
    prefix = prefixes.get(level, "[CVM LOG]")
    output = f"{prefix} {message}"
    _ctx.emit(output)
    return {"status": "success", "logged": True}


def fs_write(
    base_dir: str,
    rel_path: str,
    content: str,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: io.fs.write

    Write text content to a file, sandboxed within base_dir.

    Safety:
    - Only allows writing within base_dir (no escaping via '..').
    - Creates parent directories as needed.
    - All writes are logged through the context.

    Args:
        base_dir: Root directory for sandboxing
        rel_path: Relative path within base_dir
        content: Text content to write
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "path": str, "bytes_written": int}
        {"status": "error", "message": str} on failure
    """
    try:
        root = Path(base_dir).resolve()
        target = (root / rel_path).resolve()

        # Security: Ensure target is within root
        if not str(target).startswith(str(root)):
            return {
                "status": "error",
                "message": "Path escapes sandbox: target must be within base_dir",
            }

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

        return {
            "status": "success",
            "path": str(target),
            "bytes_written": len(content.encode("utf-8")),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def fs_read(
    base_dir: str,
    rel_path: str,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: io.fs.read

    Read text content from a file, sandboxed within base_dir.

    Safety:
    - Only allows reading within base_dir (no escaping via '..').

    Args:
        base_dir: Root directory for sandboxing
        rel_path: Relative path within base_dir
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "content": str, "path": str, "bytes_read": int}
        {"status": "error", "message": str} on failure
    """
    try:
        root = Path(base_dir).resolve()
        target = (root / rel_path).resolve()

        # Security: Ensure target is within root
        if not str(target).startswith(str(root)):
            return {
                "status": "error",
                "message": "Path escapes sandbox: target must be within base_dir",
            }

        if not target.exists():
            return {"status": "error", "message": f"File not found: {rel_path}"}

        if not target.is_file():
            return {"status": "error", "message": f"Not a file: {rel_path}"}

        content = target.read_text()

        return {
            "status": "success",
            "content": content,
            "path": str(target),
            "bytes_read": len(content.encode("utf-8")),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# =============================================================================
# Directory Tree Operations
# =============================================================================


def _parse_gitignore(gitignore_path: Path) -> List[str]:
    """
    Parse a .gitignore file and return a list of patterns.

    Handles:
    - Comments (lines starting with #)
    - Empty lines
    - Negation patterns (lines starting with !)
    - Directory-specific patterns (ending with /)
    """
    patterns: List[str] = []
    if not gitignore_path.exists():
        return patterns

    try:
        for line in gitignore_path.read_text().splitlines():
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue
            patterns.append(line)
    except Exception:
        pass  # If we can't read gitignore, continue without it

    return patterns


def _should_ignore(
    rel_path: str,
    is_dir: bool,
    patterns: List[str],
) -> bool:
    """
    Check if a path should be ignored based on gitignore patterns.

    Args:
        rel_path: Relative path from the root (POSIX-style)
        is_dir: Whether the path is a directory
        patterns: List of gitignore patterns

    Returns:
        True if the path should be ignored
    """
    # Always ignore .git directory
    if rel_path == ".git" or rel_path.startswith(".git/"):
        return True

    path_parts = rel_path.split("/")
    name = path_parts[-1]

    for pattern in patterns:
        # Skip negation patterns for now (they're complex)
        if pattern.startswith("!"):
            continue

        # Directory-only patterns
        is_dir_pattern = pattern.endswith("/")
        if is_dir_pattern:
            pattern = pattern[:-1]
            if not is_dir:
                continue

        # Patterns with / match from root
        if "/" in pattern and not pattern.startswith("**/"):
            if fnmatch.fnmatch(rel_path, pattern):
                return True
            # Also check if pattern matches as a directory prefix
            if is_dir and fnmatch.fnmatch(rel_path + "/", pattern + "/"):
                return True
        else:
            # Patterns without / match anywhere in the path
            # Check against the full path and each component
            if fnmatch.fnmatch(rel_path, pattern):
                return True
            if fnmatch.fnmatch(name, pattern):
                return True
            # Check if any directory in the path matches
            for i, part in enumerate(path_parts[:-1]):
                if fnmatch.fnmatch(part, pattern):
                    return True

    return False


def fs_read_tree(
    base_dir: str,
    _ctx: ExecutionContext,
    max_depth: int | None = None,
    include_hidden: bool = False,
    respect_gitignore: bool = True,
    file_extensions: List[str] | None = None,
) -> Dict[str, Any]:
    """
    Primitive: io.fs.read_tree

    List files in a directory tree, respecting .gitignore patterns.

    Cross-platform: Uses pathlib and returns POSIX-style paths.
    This is a read-only operation that helps agents understand codebase structure.

    Args:
        base_dir: Root directory to scan
        _ctx: Execution context (MANDATORY in lib/)
        max_depth: Maximum directory depth (None = unlimited)
        include_hidden: If True, include hidden files (starting with .)
        respect_gitignore: If True, respect .gitignore patterns
        file_extensions: If provided, only include files with these extensions

    Returns:
        {
            "status": "success",
            "root": str (POSIX-style absolute path),
            "files": [
                {
                    "path": str (POSIX-style relative path),
                    "type": "file" | "directory",
                    "size": int (bytes, for files only),
                    "depth": int,
                },
                ...
            ],
            "file_count": int,
            "dir_count": int,
            "total_size": int,
        }
        or {"status": "error", "error": str} on failure
    """
    root = Path(base_dir).resolve()

    if not root.exists():
        return {
            "status": "error",
            "error": f"Directory not found: {base_dir}",
            "root": root.as_posix(),
            "files": [],
            "file_count": 0,
            "dir_count": 0,
            "total_size": 0,
        }

    if not root.is_dir():
        return {
            "status": "error",
            "error": f"Not a directory: {base_dir}",
            "root": root.as_posix(),
            "files": [],
            "file_count": 0,
            "dir_count": 0,
            "total_size": 0,
        }

    # Collect gitignore patterns
    gitignore_patterns: List[str] = []
    if respect_gitignore:
        gitignore_patterns = _parse_gitignore(root / ".gitignore")

    # Normalize extensions (ensure they start with .)
    if file_extensions:
        file_extensions = [ext if ext.startswith(".") else f".{ext}" for ext in file_extensions]

    files: List[Dict[str, Any]] = []
    file_count = 0
    dir_count = 0
    total_size = 0
    visited: Set[Path] = set()

    def walk(current: Path, depth: int) -> None:
        nonlocal file_count, dir_count, total_size

        if max_depth is not None and depth > max_depth:
            return

        # Avoid symlink loops
        try:
            resolved = current.resolve()
            if resolved in visited:
                return
            visited.add(resolved)
        except (OSError, ValueError):
            return

        try:
            entries = sorted(current.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
        except PermissionError:
            return

        for entry in entries:
            try:
                rel_path = entry.relative_to(root).as_posix()
                is_dir = entry.is_dir()
                name = entry.name

                # Skip hidden files unless requested
                if not include_hidden and name.startswith("."):
                    # But always include top-level directories like .github
                    if depth > 0 or not is_dir:
                        continue

                # Check gitignore
                if respect_gitignore and _should_ignore(rel_path, is_dir, gitignore_patterns):
                    continue

                # Filter by extension for files
                if not is_dir and file_extensions:
                    if entry.suffix.lower() not in file_extensions:
                        continue

                if is_dir:
                    dir_count += 1
                    files.append({
                        "path": rel_path,
                        "type": "directory",
                        "depth": depth,
                    })
                    # Recurse into directory
                    walk(entry, depth + 1)
                else:
                    try:
                        size = entry.stat().st_size
                    except (OSError, ValueError):
                        size = 0
                    file_count += 1
                    total_size += size
                    files.append({
                        "path": rel_path,
                        "type": "file",
                        "size": size,
                        "depth": depth,
                    })
            except (OSError, ValueError):
                continue

    try:
        walk(root, 0)
    except Exception as e:
        return {
            "status": "error",
            "error": f"Error scanning directory: {str(e)}",
            "root": root.as_posix(),
            "files": [],
            "file_count": 0,
            "dir_count": 0,
            "total_size": 0,
        }

    return {
        "status": "success",
        "root": root.as_posix(),
        "files": files,
        "file_count": file_count,
        "dir_count": dir_count,
        "total_size": total_size,
    }


# =============================================================================
# Patch Operations
# =============================================================================


def _apply_unified_diff(original: str, diff: str) -> tuple[str, List[str]]:
    """
    Apply a unified diff to original content.

    Args:
        original: Original file content
        diff: Unified diff format patch

    Returns:
        Tuple of (patched_content, list_of_errors)
    """
    original_lines = original.splitlines(keepends=True)
    # Ensure last line has newline for consistent handling
    if original_lines and not original_lines[-1].endswith("\n"):
        original_lines[-1] += "\n"

    result_lines: List[str] = []
    errors: List[str] = []
    original_idx = 0  # Current position in original file (0-indexed)

    # Parse diff into hunks
    diff_lines = diff.splitlines()
    i = 0

    while i < len(diff_lines):
        line = diff_lines[i]

        # Skip diff headers
        if line.startswith("---") or line.startswith("+++") or line.startswith("diff "):
            i += 1
            continue

        # Parse hunk header: @@ -start,count +start,count @@
        if line.startswith("@@"):
            match = re.match(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
            if not match:
                errors.append(f"Invalid hunk header: {line}")
                i += 1
                continue

            orig_start = int(match.group(1))
            # orig_count = int(match.group(2)) if match.group(2) else 1

            # Copy lines from original up to the hunk start (converting to 0-indexed)
            target_idx = orig_start - 1
            while original_idx < target_idx and original_idx < len(original_lines):
                result_lines.append(original_lines[original_idx])
                original_idx += 1

            # Process hunk lines
            i += 1
            while i < len(diff_lines) and not diff_lines[i].startswith("@@"):
                hunk_line = diff_lines[i]

                if not hunk_line:
                    i += 1
                    continue

                if hunk_line.startswith("-"):
                    # Remove line from original
                    if original_idx < len(original_lines):
                        expected = hunk_line[1:]
                        actual = original_lines[original_idx].rstrip("\n")
                        if expected != actual:
                            errors.append(f"Context mismatch at line {original_idx + 1}: expected '{expected}', got '{actual}'")
                        original_idx += 1
                elif hunk_line.startswith("+"):
                    # Add new line
                    new_line = hunk_line[1:]
                    if not new_line.endswith("\n"):
                        new_line += "\n"
                    result_lines.append(new_line)
                elif hunk_line.startswith(" ") or hunk_line == "":
                    # Context line - copy from original
                    if original_idx < len(original_lines):
                        result_lines.append(original_lines[original_idx])
                        original_idx += 1
                else:
                    # Treat as context line (some diffs omit the space prefix)
                    if original_idx < len(original_lines):
                        result_lines.append(original_lines[original_idx])
                        original_idx += 1

                i += 1
        else:
            i += 1

    # Copy remaining lines from original
    while original_idx < len(original_lines):
        result_lines.append(original_lines[original_idx])
        original_idx += 1

    # Join result and strip trailing newline if original didn't have one
    result = "".join(result_lines)
    if result.endswith("\n") and not original.endswith("\n"):
        result = result[:-1]

    return result, errors


def fs_patch(
    base_dir: str,
    rel_path: str,
    diff: str,
    _ctx: ExecutionContext,
    create_if_missing: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Primitive: io.fs.patch

    Apply a unified diff patch to a file. This is critical for surgical edits
    without full file overwrites - agents can specify just the changes.

    Cross-platform: Uses pathlib, sandboxed within base_dir.

    Args:
        base_dir: Root directory for sandboxing
        rel_path: Relative path to the file within base_dir
        diff: Unified diff format patch to apply
        _ctx: Execution context (MANDATORY in lib/)
        create_if_missing: If True, create file if it doesn't exist (treat as empty)
        dry_run: If True, validate patch but don't write

    Returns:
        {
            "status": "success",
            "path": str,
            "patched": True,
            "lines_added": int,
            "lines_removed": int,
            "dry_run": bool,
        }
        or {"status": "error", "error": str, "errors": [...]} on failure
    """
    try:
        root = Path(base_dir).resolve()
        target = (root / rel_path).resolve()

        # Security: Ensure target is within root
        if not str(target).startswith(str(root)):
            return {
                "status": "error",
                "error": "Path escapes sandbox: target must be within base_dir",
                "path": target.as_posix(),
                "patched": False,
            }

        # Read original content
        if not target.exists():
            if create_if_missing:
                original = ""
            else:
                return {
                    "status": "error",
                    "error": f"File not found: {rel_path}",
                    "path": target.as_posix(),
                    "patched": False,
                }
        elif not target.is_file():
            return {
                "status": "error",
                "error": f"Not a file: {rel_path}",
                "path": target.as_posix(),
                "patched": False,
            }
        else:
            original = target.read_text(encoding="utf-8")

        # Apply the patch
        patched, errors = _apply_unified_diff(original, diff)

        if errors:
            return {
                "status": "error",
                "error": f"Patch application failed: {len(errors)} error(s)",
                "errors": errors,
                "path": target.as_posix(),
                "patched": False,
            }

        # Count changes
        original_lines = original.splitlines()
        patched_lines = patched.splitlines()
        lines_added = max(0, len(patched_lines) - len(original_lines))
        lines_removed = max(0, len(original_lines) - len(patched_lines))

        # Write if not dry run
        if not dry_run:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(patched, encoding="utf-8")

        return {
            "status": "success",
            "path": target.as_posix(),
            "patched": True,
            "lines_added": lines_added,
            "lines_removed": lines_removed,
            "dry_run": dry_run,
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "patched": False,
        }
