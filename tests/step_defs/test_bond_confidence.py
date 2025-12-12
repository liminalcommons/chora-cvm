"""
Step definitions for the Bond Confidence feature.

These tests verify the behaviors specified by story-bonds-carry-epistemic-confidence.
Bonds carry confidence values (0.0-1.0) representing epistemic certainty.
"""
import json
import os
import tempfile

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import manifest_entity, manage_bond, update_bond_confidence

# Load scenarios from feature file
scenarios("../features/bond_confidence.feature")


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
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name

    # Initialize the database with required tables
    store = EventStore(path)
    store.close()

    yield path

    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


# =============================================================================
# Background Steps
# =============================================================================


@given("a fresh CVM database")
def fresh_database(db_path, test_context):
    """Set up a fresh database for testing."""
    test_context["db_path"] = db_path
    test_context["bonds_created"] = []
    test_context["signals_emitted"] = []


@given(parsers.parse('a learning "{learning_id}" exists'))
def create_learning(db_path, test_context, learning_id: str):
    """Create a learning entity."""
    manifest_entity(
        db_path,
        "learning",
        learning_id,
        {"title": f"Test learning {learning_id}"},
    )
    test_context["learning_id"] = learning_id


@given(parsers.parse('a principle "{principle_id}" exists'))
def create_principle(db_path, test_context, principle_id: str):
    """Create a principle entity."""
    manifest_entity(
        db_path,
        "principle",
        principle_id,
        {"title": f"Test principle {principle_id}", "statement": "A test statement"},
    )
    test_context["principle_id"] = principle_id


# =============================================================================
# Bond Creation Steps
# =============================================================================


@when(parsers.parse('I create a bond surfaces from "{from_id}" to "{to_id}" with confidence {confidence:f}'))
def create_bond_with_confidence(db_path, test_context, from_id: str, to_id: str, confidence: float):
    """Create a bond with specified confidence."""
    result = manage_bond(
        db_path,
        "surfaces",
        from_id,
        to_id,
        confidence=confidence,
    )
    test_context["last_bond"] = result
    test_context["bonds_created"].append(result)
    if result.get("signal_id"):
        test_context["signals_emitted"].append(result["signal_id"])


@when(parsers.parse("I create a bond with confidence {confidence:f}"))
def create_bond_any_confidence(db_path, test_context, confidence: float):
    """Create a bond with specified confidence using context entities."""
    from_id = test_context.get("learning_id", "learning-test")
    to_id = test_context.get("principle_id", "principle-test")

    result = manage_bond(
        db_path,
        "surfaces",
        from_id,
        to_id,
        confidence=confidence,
    )
    test_context["last_bond"] = result
    test_context["bonds_created"].append(result)
    if result.get("signal_id"):
        test_context["signals_emitted"].append(result["signal_id"])


# =============================================================================
# Bond Update Steps
# =============================================================================


@given(parsers.parse("a bond exists with confidence {confidence:f}"))
def existing_bond_with_confidence(db_path, test_context, confidence: float):
    """Create an existing bond with specified confidence."""
    # Create entities if not already present
    if "learning_id" not in test_context:
        create_learning(db_path, test_context, "learning-test-insight")
    if "principle_id" not in test_context:
        create_principle(db_path, test_context, "principle-test-truth")

    result = manage_bond(
        db_path,
        "surfaces",
        test_context["learning_id"],
        test_context["principle_id"],
        confidence=confidence,
    )
    test_context["existing_bond"] = result
    test_context["last_bond"] = result
    # Clear any signal from creation for update tests
    test_context["signals_emitted"] = []


@when(parsers.parse("I update the bond confidence to {confidence:f}"))
def update_confidence(db_path, test_context, confidence: float):
    """Update confidence on existing bond."""
    bond_id = test_context["existing_bond"]["id"]
    result = update_bond_confidence(db_path, bond_id, confidence)
    test_context["update_result"] = result
    if result.get("signal_id"):
        test_context["signals_emitted"].append(result["signal_id"])


# =============================================================================
# Assertion Steps - Confidence
# =============================================================================


@then(parsers.parse("the bond has confidence {confidence:f}"))
def check_bond_confidence(db_path, test_context, confidence: float):
    """Verify bond has expected confidence."""
    bond_id = test_context["last_bond"]["id"]
    store = EventStore(db_path)
    bond = store.get_bond(bond_id)
    store.close()

    assert bond is not None, f"Bond {bond_id} not found"
    assert abs(bond["confidence"] - confidence) < 0.01, \
        f"Expected confidence {confidence}, got {bond['confidence']}"


# =============================================================================
# Assertion Steps - Signals
# =============================================================================


@then("no signal is emitted")
def check_no_signal(test_context):
    """Verify no signal was emitted."""
    signals = test_context.get("signals_emitted", [])
    assert len(signals) == 0, f"Expected no signals, got {signals}"


@then(parsers.parse('a signal is emitted with title containing "{text}"'))
def check_signal_title(db_path, test_context, text: str):
    """Verify a signal was emitted with expected title."""
    signals = test_context.get("signals_emitted", [])
    assert len(signals) > 0, "No signals were emitted"

    # Check the most recent signal
    signal_id = signals[-1]
    store = EventStore(db_path)
    conn = store._conn
    cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (signal_id,))
    row = cur.fetchone()
    store.close()

    assert row is not None, f"Signal {signal_id} not found"
    data = json.loads(row["data_json"])
    assert text.lower() in data.get("title", "").lower(), \
        f"Expected title containing '{text}', got '{data.get('title')}'"


@then("the signal has source_id equal to the bond id")
def check_signal_source(db_path, test_context):
    """Verify signal source_id matches bond id."""
    signals = test_context.get("signals_emitted", [])
    assert len(signals) > 0, "No signals were emitted"

    signal_id = signals[-1]
    bond_id = test_context["last_bond"]["id"]

    store = EventStore(db_path)
    conn = store._conn
    cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (signal_id,))
    row = cur.fetchone()
    store.close()

    data = json.loads(row["data_json"])
    assert data.get("source_id") == bond_id, \
        f"Expected source_id {bond_id}, got {data.get('source_id')}"


@then(parsers.parse('a signal is emitted with urgency "{urgency}"'))
def check_signal_urgency(db_path, test_context, urgency: str):
    """Verify signal has expected urgency."""
    signals = test_context.get("signals_emitted", [])
    assert len(signals) > 0, "No signals were emitted"

    signal_id = signals[-1]
    store = EventStore(db_path)
    conn = store._conn
    cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (signal_id,))
    row = cur.fetchone()
    store.close()

    data = json.loads(row["data_json"])
    assert data.get("urgency") == urgency, \
        f"Expected urgency {urgency}, got {data.get('urgency')}"


@then("the signal shows the confidence drop")
def check_signal_shows_drop(db_path, test_context):
    """Verify signal description shows confidence change."""
    signals = test_context.get("signals_emitted", [])
    assert len(signals) > 0, "No signals were emitted"

    signal_id = signals[-1]
    store = EventStore(db_path)
    conn = store._conn
    cur = conn.execute("SELECT data_json FROM entities WHERE id = ?", (signal_id,))
    row = cur.fetchone()
    store.close()

    data = json.loads(row["data_json"])
    desc = data.get("description", "")
    assert "changed from" in desc or "->" in data.get("title", ""), \
        f"Signal doesn't show confidence drop: {data}"


# =============================================================================
# Gradient Mapping Steps
# =============================================================================


@then(parsers.parse('the effective certainty level is "{certainty}"'))
def check_certainty_level(test_context, certainty: str):
    """Verify confidence maps to expected certainty level."""
    confidence = test_context["last_bond"]["confidence"]

    # Map confidence to certainty based on plan:
    # 1.0 = certain, 0.8-0.99 = high, 0.5-0.79 = hypothesis, <0.5 = speculation
    if confidence >= 1.0:
        actual = "certain"
    elif confidence >= 0.8:
        actual = "high"
    elif confidence >= 0.5:
        actual = "hypothesis"
    else:
        actual = "speculation"

    assert actual == certainty, f"Expected {certainty}, got {actual} for confidence {confidence}"
