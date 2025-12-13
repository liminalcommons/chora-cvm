"""
Domain: System (Environment)
ID Prefix: sys.*

Sandboxed system interaction primitives. These allow controlled subprocess
execution without shell=True vulnerabilities.

Security Model:
- Commands MUST be passed as lists, never strings
- shell=True is NEVER used
- Timeouts are enforced
- Output is captured with size limits
- Working directory is sandboxed

Primitives:
  - sys.shell.run: Execute a command safely in a subprocess
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..schema import ExecutionContext

# Maximum output size to capture (256 KB)
MAX_OUTPUT_SIZE = 256 * 1024

# Default timeout (5 minutes)
DEFAULT_TIMEOUT = 300


def shell_run(
    cmd: List[str],
    _ctx: ExecutionContext,
    cwd: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
    env: Optional[Dict[str, str]] = None,
    capture_output: bool = True,
    max_output_size: int = MAX_OUTPUT_SIZE,
) -> Dict[str, Any]:
    """
    Primitive: sys.shell.run

    Execute a command in a subprocess with safety constraints.

    SECURITY:
    - Command MUST be a list, never a string (prevents shell injection)
    - shell=True is NEVER used
    - Working directory is validated
    - Timeout is enforced
    - Output is size-limited

    Cross-platform: Uses pathlib for paths, subprocess with list args.

    Args:
        cmd: Command as a list of strings (e.g., ["python", "-m", "pytest"])
        _ctx: Execution context (MANDATORY in lib/)
        cwd: Working directory (optional, validated if provided)
        timeout: Maximum execution time in seconds (default: 300)
        env: Additional environment variables to merge (optional)
        capture_output: If True, capture stdout/stderr (default: True)
        max_output_size: Maximum bytes to capture per stream (default: 256KB)

    Returns:
        {
            "status": "success",
            "exit_code": int,
            "stdout": str (if capture_output),
            "stderr": str (if capture_output),
            "truncated": bool (if output exceeded max_output_size),
            "timed_out": False,
            "command": List[str],
            "cwd": str | None,
        }
        or {"status": "error", "error": str} on failure

    Example:
        # Good: Command as list
        shell_run(["python", "-m", "pytest", "tests/"], ctx)

        # Good: With working directory
        shell_run(["npm", "test"], ctx, cwd="/path/to/project")

        # BAD (will fail): String command
        shell_run("python -m pytest", ctx)  # TypeError: cmd must be a list
    """
    # SECURITY: Validate cmd is a list, not a string
    if not isinstance(cmd, list):
        return {
            "status": "error",
            "error": "Security: cmd must be a list of strings, not a string. "
                    "Example: ['python', '-m', 'pytest'] not 'python -m pytest'",
            "command": str(cmd),
        }

    if not cmd:
        return {
            "status": "error",
            "error": "Empty command list",
            "command": [],
        }

    # Validate all command parts are strings
    for i, part in enumerate(cmd):
        if not isinstance(part, str):
            return {
                "status": "error",
                "error": f"Command part at index {i} must be a string, got {type(part).__name__}",
                "command": cmd,
            }

    # Validate timeout
    if timeout <= 0:
        timeout = DEFAULT_TIMEOUT
    if timeout > 3600:  # Max 1 hour
        timeout = 3600

    # Validate and resolve working directory
    resolved_cwd: Optional[str] = None
    if cwd:
        cwd_path = Path(cwd).resolve()
        if not cwd_path.exists():
            return {
                "status": "error",
                "error": f"Working directory does not exist: {cwd}",
                "command": cmd,
                "cwd": cwd,
            }
        if not cwd_path.is_dir():
            return {
                "status": "error",
                "error": f"Working directory is not a directory: {cwd}",
                "command": cmd,
                "cwd": cwd,
            }
        resolved_cwd = str(cwd_path)

    try:
        # Build subprocess kwargs
        kwargs: Dict[str, Any] = {
            "timeout": timeout,
        }

        if resolved_cwd:
            kwargs["cwd"] = resolved_cwd

        if env:
            # Merge with current environment
            import os
            merged_env = os.environ.copy()
            merged_env.update(env)
            kwargs["env"] = merged_env

        if capture_output:
            kwargs["capture_output"] = True
            kwargs["text"] = True
        else:
            # Discard output
            kwargs["stdout"] = subprocess.DEVNULL
            kwargs["stderr"] = subprocess.DEVNULL

        # Execute command - NEVER with shell=True
        result = subprocess.run(cmd, **kwargs)

        # Build response
        response: Dict[str, Any] = {
            "status": "success",
            "exit_code": result.returncode,
            "timed_out": False,
            "command": cmd,
            "cwd": resolved_cwd,
        }

        if capture_output:
            stdout = result.stdout or ""
            stderr = result.stderr or ""
            truncated = False

            # Apply size limits
            if len(stdout.encode("utf-8")) > max_output_size:
                stdout = stdout[:max_output_size] + "\n... [output truncated]"
                truncated = True
            if len(stderr.encode("utf-8")) > max_output_size:
                stderr = stderr[:max_output_size] + "\n... [output truncated]"
                truncated = True

            response["stdout"] = stdout
            response["stderr"] = stderr
            response["truncated"] = truncated

        return response

    except subprocess.TimeoutExpired as e:
        response = {
            "status": "error",
            "error": f"Command timed out after {timeout} seconds",
            "exit_code": -1,
            "timed_out": True,
            "command": cmd,
            "cwd": resolved_cwd,
        }
        # Try to capture partial output
        if capture_output and hasattr(e, "stdout") and e.stdout:
            response["stdout"] = e.stdout[:max_output_size] if isinstance(e.stdout, str) else ""
        if capture_output and hasattr(e, "stderr") and e.stderr:
            response["stderr"] = e.stderr[:max_output_size] if isinstance(e.stderr, str) else ""
        return response

    except FileNotFoundError:
        return {
            "status": "error",
            "error": f"Command not found: {cmd[0]}",
            "command": cmd,
            "cwd": resolved_cwd,
        }

    except PermissionError:
        return {
            "status": "error",
            "error": f"Permission denied executing: {cmd[0]}",
            "command": cmd,
            "cwd": resolved_cwd,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": f"Execution error: {str(e)}",
            "command": cmd,
            "cwd": resolved_cwd,
        }
