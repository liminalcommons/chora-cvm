#!/usr/bin/env python3
"""
Safely merge semantic primitives/protocols from package DB into root Loom.

This script:
1. Identifies entities in package DB that don't exist in root
2. Identifies entities that exist in both (for verification)
3. Copies new entities to root
4. Reports what was done

Safe: Read-only on package DB, only inserts new entities to root.
"""

import json
import sqlite3
import sys
from pathlib import Path

# Paths
ROOT_DB = Path(__file__).parent.parent.parent.parent / "chora-cvm-manifest.db"
PKG_DB = Path(__file__).parent.parent / "chora-cvm-manifest.db"


def get_all_entities(db_path: Path) -> dict[str, dict]:
    """Get all entities from a database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT id, type, data_json FROM entities")
    entities = {}
    for row in cur:
        entities[row["id"]] = {
            "id": row["id"],
            "type": row["type"],
            "data": json.loads(row["data_json"]),
        }
    conn.close()
    return entities


def entities_equal(e1: dict, e2: dict) -> bool:
    """Check if two entities have identical content."""
    return json.dumps(e1["data"], sort_keys=True) == json.dumps(e2["data"], sort_keys=True)


def merge_entity(root_conn: sqlite3.Connection, entity: dict) -> None:
    """Insert an entity into the root database."""
    cur = root_conn.cursor()
    cur.execute(
        """
        INSERT INTO entities (id, type, data_json)
        VALUES (?, ?, json(?))
        ON CONFLICT(id) DO UPDATE SET
            type = excluded.type,
            data_json = excluded.data_json
        """,
        (entity["id"], entity["type"], json.dumps(entity["data"])),
    )


def main():
    print(f"=== Database Merge Tool ===")
    print(f"Source (package): {PKG_DB}")
    print(f"Target (root):    {ROOT_DB}")
    print()

    # Verify paths exist
    if not PKG_DB.exists():
        print(f"ERROR: Package DB not found: {PKG_DB}")
        sys.exit(1)
    if not ROOT_DB.exists():
        print(f"ERROR: Root DB not found: {ROOT_DB}")
        sys.exit(1)

    # Load entities from both databases
    pkg_entities = get_all_entities(PKG_DB)
    root_entities = get_all_entities(ROOT_DB)

    print(f"Package DB entities: {len(pkg_entities)}")
    print(f"Root DB entities:    {len(root_entities)}")
    print()

    # Categorize entities
    new_entities = []  # In package but not root
    identical = []  # In both, same content
    different = []  # In both, different content

    for entity_id, pkg_entity in pkg_entities.items():
        if entity_id not in root_entities:
            new_entities.append(pkg_entity)
        elif entities_equal(pkg_entity, root_entities[entity_id]):
            identical.append(entity_id)
        else:
            different.append({
                "id": entity_id,
                "pkg": pkg_entity,
                "root": root_entities[entity_id],
            })

    # Report
    print("=== Analysis ===")
    print(f"New entities to add:     {len(new_entities)}")
    print(f"Identical (no action):   {len(identical)}")
    print(f"Different (need review): {len(different)}")
    print()

    if new_entities:
        print("NEW ENTITIES:")
        for e in new_entities:
            desc = e["data"].get("description", "")[:50]
            print(f"  + {e['id']} ({e['type']}): {desc}")
        print()

    if identical:
        print("IDENTICAL (skipping):")
        for eid in identical:
            print(f"  = {eid}")
        print()

    if different:
        print("DIFFERENT (need manual review):")
        for d in different:
            print(f"  ! {d['id']}")
            print(f"    PKG:  {json.dumps(d['pkg']['data'])[:80]}...")
            print(f"    ROOT: {json.dumps(d['root']['data'])[:80]}...")
        print()
        print("WARNING: Different entities found. Review before merging.")

    # Confirm merge
    if not new_entities:
        print("Nothing new to merge. Done.")
        return

    print(f"Ready to merge {len(new_entities)} new entities.")

    if "--dry-run" in sys.argv:
        print("DRY RUN: No changes made.")
        return

    if "--yes" not in sys.argv:
        response = input("Proceed? [y/N] ")
        if response.lower() != "y":
            print("Aborted.")
            return

    # Perform merge
    root_conn = sqlite3.connect(ROOT_DB)
    try:
        for entity in new_entities:
            merge_entity(root_conn, entity)
            print(f"  Merged: {entity['id']}")
        root_conn.commit()
        print()
        print(f"SUCCESS: Merged {len(new_entities)} entities into root Loom.")
    except Exception as e:
        root_conn.rollback()
        print(f"ERROR: {e}")
        sys.exit(1)
    finally:
        root_conn.close()


if __name__ == "__main__":
    main()
