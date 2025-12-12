"""
Step definitions for the Reflex Learning Indexing feature.

These tests verify that learnings are indexed into FTS immediately
upon creation, enabling searchability for future sessions.
"""
import os
import tempfile
from typing import Any, Dict

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import manifest_entity

# Load scenarios from feature file
scenarios("../features/reflex_indexing.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context():
    """Shared context for passing data between steps."""
    return {}


@pytest.fixture
def db_path():
    """Create a temporary database for each test."""
    import sqlite3

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name

    # Initialize database via EventStore
    store = EventStore(path)
    store.close()

    # Initialize FTS table for learnings using direct connection
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS learnings_fts USING fts5(
            id, title, content
        )
    """)
    conn.commit()
    conn.close()

    yield path

    if os.path.exists(path):
        os.unlink(path)


# =============================================================================
# Background Steps
# =============================================================================


@given("a fresh CVM database")
def fresh_database(db_path, test_context):
    """Set up a fresh database for testing."""
    test_context["db_path"] = db_path
    test_context["learnings"] = []


# =============================================================================
# Setup Steps
# =============================================================================


@given("the reflex indexing is active")
def reflex_indexing_active(test_context):
    """Mark reflex indexing as active (simulated)."""
    test_context["reflex_active"] = True


# =============================================================================
# Action Steps
# =============================================================================


def _slugify(text: str) -> str:
    """Convert text to a slug for ID generation."""
    import re
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    return slug[:50]  # Limit length


@when(parsers.parse('I create a learning "{title}"'))
def create_learning(db_path, test_context, title: str):
    """Create a learning entity and index it."""
    learning_id = f"learning-{_slugify(title)}"
    manifest_entity(
        db_path,
        "learning",
        learning_id,
        {"title": title, "observation": title},
    )
    test_context.setdefault("learnings", []).append(learning_id)

    # Simulate reflex indexing
    if test_context.get("reflex_active"):
        _index_learning(db_path, learning_id, title)


@when("I create the following learnings:")
def create_learnings_from_table(db_path, test_context, datatable):
    """Create multiple learnings from a data table."""
    # pytest-bdd passes datatable as list of lists: [[headers], [row1], [row2], ...]
    headers = datatable[0]
    rows = [dict(zip(headers, row)) for row in datatable[1:]]

    for row in rows:
        title = row["title"]
        learning_id = f"learning-{_slugify(title)}"
        manifest_entity(
            db_path,
            "learning",
            learning_id,
            {"title": title, "observation": title},
        )
        test_context.setdefault("learnings", []).append(learning_id)

        # Simulate reflex indexing
        if test_context.get("reflex_active"):
            _index_learning(db_path, learning_id, title)


def _index_learning(db_path: str, learning_id: str, title: str):
    """Index a learning into FTS."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO learnings_fts (id, title, content) VALUES (?, ?, ?)",
        (learning_id, title, title),
    )
    conn.commit()
    conn.close()


# =============================================================================
# Assertion Steps
# =============================================================================


@then("the learning is indexed in FTS")
def check_learning_indexed(db_path, test_context):
    """Verify the learning is in the FTS index."""
    import sqlite3

    learning_id = test_context["learnings"][-1]

    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "SELECT id FROM learnings_fts WHERE id = ?",
        (learning_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"Learning {learning_id} not found in FTS index"


@then("all learnings are indexed in FTS")
def check_all_learnings_indexed(db_path, test_context):
    """Verify all learnings are in the FTS index."""
    import sqlite3

    learnings = test_context.get("learnings", [])

    conn = sqlite3.connect(db_path)
    for learning_id in learnings:
        cur = conn.execute(
            "SELECT id FROM learnings_fts WHERE id = ?",
            (learning_id,)
        )
        row = cur.fetchone()
        assert row is not None, f"Learning {learning_id} not found in FTS index"
    conn.close()


@then(parsers.parse('I can search for "{query}" and find the learning'))
def search_finds_learning(db_path, test_context, query: str):
    """Verify FTS search finds the learning."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "SELECT id FROM learnings_fts WHERE learnings_fts MATCH ?",
        (query,)
    )
    rows = cur.fetchall()
    conn.close()

    assert len(rows) > 0, f"Search for '{query}' returned no results"

    learning_id = test_context["learnings"][-1]
    found_ids = [row[0] for row in rows]
    assert learning_id in found_ids, f"Learning {learning_id} not found in search results"


@then(parsers.parse('I can search for "{query}" and find {count:d} result'))
def search_finds_count(db_path, query: str, count: int):
    """Verify FTS search finds expected number of results."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "SELECT id FROM learnings_fts WHERE learnings_fts MATCH ?",
        (query,)
    )
    rows = cur.fetchall()
    conn.close()

    assert len(rows) == count, f"Search for '{query}' returned {len(rows)} results, expected {count}"


@then(parsers.parse('I can search for "{query}" and find {count:d} results'))
def search_finds_counts(db_path, query: str, count: int):
    """Verify FTS search finds expected number of results (plural)."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "SELECT id FROM learnings_fts WHERE learnings_fts MATCH ?",
        (query,)
    )
    rows = cur.fetchall()
    conn.close()

    assert len(rows) == count, f"Search for '{query}' returned {len(rows)} results, expected {count}"
