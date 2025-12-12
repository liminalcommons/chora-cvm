from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from .store import EventStore


def resolve_circle(
    db_path: str,
    cwd: Optional[str] = None,
) -> Optional[str]:
    """
    Resolve the active circle id for a given working directory.

    Strategy (MVP):
    - Look for .chora/circle.json in cwd or parents.
    - If not found, try to match cwd to an asset.source_uri in the Loom.
    """
    workdir = Path(cwd or os.getcwd()).resolve()

    # 1. Look for marker file
    for parent in [workdir] + list(workdir.parents):
        marker = parent / ".chora" / "circle.json"
        if marker.exists():
            try:
                data = json.loads(marker.read_text())
                circle_id = data.get("circle_id")
                if circle_id:
                    return circle_id
            except Exception:
                continue

    # 2. Fallback: attempt to match cwd to asset.source_uri
    store = EventStore(db_path)
    try:
        rows = store._conn.execute(  # type: ignore[attr-defined]
            "SELECT id, data_json FROM entities WHERE type = 'asset';",
        ).fetchall()
        for row in rows:
            data = json.loads(row["data_json"])
            source_uri = data.get("source_uri")
            circle_id = data.get("circle_id")
            if not source_uri or not circle_id:
                continue
            try:
                if Path(source_uri).resolve() == workdir:
                    return circle_id
            except Exception:
                continue
    finally:
        store.close()

    return None
