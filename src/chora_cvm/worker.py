"""
Autonomic Worker: Background task execution for the CVM.

This module provides asynchronous protocol execution using Huey's SqliteHuey,
enabling the system to process long-running protocols in the background while
the CLI returns immediately.

Architecture:
    CLI --enqueue--> SqliteHuey (chora-worker.db) --dequeue--> Worker Process
                                                                    |
                                                               runner.py
                                                                    |
                                                            Protocol Execution

Usage:
    # Start the worker (in a terminal):
    python -m chora_cvm.cli worker

    # Enqueue a protocol (from CLI):
    python -m chora_cvm.cli invoke protocol-foo --async

    # Check task status:
    python -m chora_cvm.cli status <task_id>
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from huey import SqliteHuey

from .kernel.runner import execute_protocol


# Worker database location (separate from Loom)
def get_worker_db_path() -> str:
    """Get the path to the worker database."""
    return os.environ.get("CHORA_WORKER_DB", str(Path.cwd() / "chora-worker.db"))


def get_loom_db_path() -> str:
    """Get the path to the main Loom database."""
    return os.environ.get("CHORA_DB", str(Path.cwd() / "chora-cvm.db"))


# Lazy Huey initialization (resolved at runtime, not import time)
_huey_instance: Optional[SqliteHuey] = None


def get_huey() -> SqliteHuey:
    """Get the Huey instance, creating it lazily if needed."""
    global _huey_instance
    if _huey_instance is None:
        _huey_instance = SqliteHuey(
            name="chora-cvm",
            filename=get_worker_db_path(),
            immediate=False,  # Always queue, even in test mode
        )
    return _huey_instance


# Module-level huey for decorator compatibility
# This creates the instance on first access to a decorated function
huey = get_huey()


# =============================================================================
# Task Result Storage
# =============================================================================

def init_results_table(db_path: str) -> None:
    """Initialize the results table in the worker database."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS task_results (
            task_id TEXT PRIMARY KEY,
            protocol_id TEXT NOT NULL,
            status TEXT NOT NULL,
            result_json TEXT,
            error_message TEXT,
            enqueued_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_task_results_status
        ON task_results(status)
    """)
    conn.commit()
    conn.close()


def record_task_enqueued(
    db_path: str,
    task_id: str,
    protocol_id: str,
) -> None:
    """Record that a task has been enqueued."""
    init_results_table(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        INSERT INTO task_results (task_id, protocol_id, status, enqueued_at)
        VALUES (?, ?, 'pending', ?)
    """, (task_id, protocol_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def record_task_started(db_path: str, task_id: str) -> None:
    """Record that a task has started execution."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        UPDATE task_results
        SET status = 'running', started_at = ?
        WHERE task_id = ?
    """, (datetime.now().isoformat(), task_id))
    conn.commit()
    conn.close()


def record_task_completed(
    db_path: str,
    task_id: str,
    result: Dict[str, Any],
) -> None:
    """Record that a task has completed."""
    status = "error" if result.get("status") == "error" else "completed"
    error_message = result.get("error_message") if status == "error" else None

    conn = sqlite3.connect(db_path)
    conn.execute("""
        UPDATE task_results
        SET status = ?, result_json = ?, error_message = ?, completed_at = ?
        WHERE task_id = ?
    """, (status, json.dumps(result), error_message, datetime.now().isoformat(), task_id))
    conn.commit()
    conn.close()


def get_task_status(db_path: str, task_id: str) -> Optional[Dict[str, Any]]:
    """Get the status and result of a task."""
    init_results_table(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("""
        SELECT * FROM task_results WHERE task_id = ?
    """, (task_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    result = {
        "task_id": row["task_id"],
        "protocol_id": row["protocol_id"],
        "status": row["status"],
        "enqueued_at": row["enqueued_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
    }

    if row["result_json"]:
        result["result"] = json.loads(row["result_json"])
    if row["error_message"]:
        result["error"] = row["error_message"]

    return result


# =============================================================================
# Huey Tasks
# =============================================================================

@huey.task()
def execute_protocol_async(
    task_id: str,
    db_path: str,
    protocol_id: str,
    inputs: Optional[Dict[str, Any]] = None,
    persona_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute a protocol asynchronously.

    This is the Huey task that runs in the worker process.
    Wraps execution in try/except to ensure tasks never get stuck in "running" state.
    """
    worker_db = get_worker_db_path()

    # Record start
    record_task_started(worker_db, task_id)

    try:
        # Execute protocol
        result = execute_protocol(
            db_path=db_path,
            protocol_id=protocol_id,
            inputs=inputs,
            persona_id=persona_id,
            state_id=f"async-{task_id}",
        )
    except Exception as e:
        # Ensure we always record completion, even on unexpected errors
        result = {
            "status": "error",
            "error_message": f"Unexpected error: {type(e).__name__}: {str(e)}",
            "protocol_id": protocol_id,
        }

    # Record completion (always reached)
    record_task_completed(worker_db, task_id, result)

    return result


# =============================================================================
# Enqueue Interface
# =============================================================================

def enqueue_protocol(
    db_path: str,
    protocol_id: str,
    inputs: Optional[Dict[str, Any]] = None,
    persona_id: Optional[str] = None,
) -> str:
    """
    Enqueue a protocol for asynchronous execution.

    Returns the task_id for status tracking.
    """
    import uuid
    task_id = f"task-{uuid.uuid4().hex[:12]}"
    worker_db = get_worker_db_path()

    # Record enqueue
    record_task_enqueued(worker_db, task_id, protocol_id)

    # Enqueue the task
    execute_protocol_async(
        task_id=task_id,
        db_path=db_path,
        protocol_id=protocol_id,
        inputs=inputs,
        persona_id=persona_id,
    )

    return task_id


# =============================================================================
# Pulse Logging
# =============================================================================


def init_pulse_log_table(db_path: str) -> None:
    """Initialize the pulse log table in the worker database."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pulse_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pulse_at TEXT NOT NULL,
            signals_found INTEGER DEFAULT 0,
            signals_processed INTEGER DEFAULT 0,
            protocols_triggered INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            errors_json TEXT,
            duration_ms INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pulse_log_at
        ON pulse_log(pulse_at)
    """)
    conn.commit()
    conn.close()


def record_pulse(db_path: str, result: Dict[str, Any], duration_ms: int = 0) -> None:
    """Record a pulse execution in the log."""
    init_pulse_log_table(db_path)

    errors_val = result.get("errors", [])
    # Handle both list (from pulse_check_signals) and int (from tests)
    if isinstance(errors_val, int):
        errors_count = errors_val
        errors_list = []
    else:
        errors_list = errors_val
        errors_count = len(errors_list)

    protocols_val = result.get("protocols_triggered", [])
    protocols_count = protocols_val if isinstance(protocols_val, int) else len(protocols_val)

    conn = sqlite3.connect(db_path)
    conn.execute("""
        INSERT INTO pulse_log (
            pulse_at, signals_found, signals_processed,
            protocols_triggered, errors, errors_json, duration_ms
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now().isoformat(),
        result.get("signals_found", 0),
        result.get("signals_processed", 0),
        protocols_count,
        errors_count,
        json.dumps(errors_list) if errors_list else None,
        duration_ms,
    ))
    conn.commit()
    conn.close()


def get_pulse_status(db_path: str, limit: int = 10) -> Dict[str, Any]:
    """
    Get recent pulse history for status display.

    Args:
        db_path: Path to the worker database
        limit: Number of recent pulses to return

    Returns:
        {
            "pulses": [
                {
                    "pulse_at": str,
                    "signals_found": int,
                    "signals_processed": int,
                    "protocols_triggered": int,
                    "errors": int,
                    "duration_ms": int,
                },
                ...
            ],
            "total_pulses": int,
        }
    """
    init_pulse_log_table(db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get recent pulses
    cur = conn.execute("""
        SELECT pulse_at, signals_found, signals_processed,
               protocols_triggered, errors, duration_ms
        FROM pulse_log
        ORDER BY pulse_at DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()

    # Get total count
    cur = conn.execute("SELECT COUNT(*) FROM pulse_log")
    total = cur.fetchone()[0]

    conn.close()

    pulses = [
        {
            "pulse_at": row["pulse_at"],
            "signals_found": row["signals_found"],
            "signals_processed": row["signals_processed"],
            "protocols_triggered": row["protocols_triggered"],
            "errors": row["errors"],
            "duration_ms": row["duration_ms"],
        }
        for row in rows
    ]

    return {"pulses": pulses, "total_pulses": total}


# =============================================================================
# Worker Runner
# =============================================================================

def run_worker(workers: int = 1, verbose: bool = True) -> None:
    """
    Run the Huey worker.

    This starts a consumer that processes tasks from the queue.
    """
    from huey.consumer import Consumer

    consumer = Consumer(
        huey,
        workers=workers,
        worker_type="thread",  # Use threads for SQLite compatibility
    )

    if verbose:
        print(f"[*] Starting Chora CVM Worker")
        print(f"    Queue DB: {get_worker_db_path()}")
        print(f"    Workers: {workers}")
        print()

    consumer.run()
