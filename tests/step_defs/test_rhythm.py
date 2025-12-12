"""
Step definitions for Kairotic Rhythm Sensing feature.

These tests verify the behaviors specified by story-system-senses-kairotic-rhythm.

BDD Flow: Feature file -> Step definitions -> Implementation
Tests should FAIL initially until rhythm.py is implemented.

The Kairotic Flow Cycle:
- Orange/Lit side (active, visible): Pioneer → Cultivator → Regulator
- Purple/Shadow side (preparatory, receptive): Steward → Curator → Scout
"""
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from chora_cvm.schema import GenericEntity
from chora_cvm.store import EventStore

# Load scenarios from feature file
scenarios("../features/rhythm.feature")


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


# =============================================================================
# Kairotic State Setup Steps
# =============================================================================


@given("the Loom has 10 entities with varied types")
def setup_varied_entities(db_path, test_context):
    """Create entities of varied types for testing."""
    store = EventStore(db_path)
    types = ["inquiry", "learning", "principle", "pattern", "story",
             "behavior", "tool", "signal", "focus", "inquiry"]

    for i, etype in enumerate(types):
        entity = GenericEntity(
            id=f"test-{etype}-{i}",
            type=etype,
            data={
                "title": f"Test {etype} {i}",
                "status": "active",
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        store.save_entity(entity)

    store.close()
    test_context["entity_count"] = len(types)


@given("recent activity shows high inquiry creation")
def setup_high_inquiry_activity(db_path, test_context):
    """Mark that recent activity shows high inquiry creation."""
    # The entities created in previous step already include inquiries
    test_context["high_inquiry_activity"] = True


@given(parsers.parse("the Loom has {count:d} active inquiries created this week"))
def setup_recent_inquiries(db_path, test_context, count: int):
    """Create recent active inquiries with NO behaviors (so verification rate stays low)."""
    store = EventStore(db_path)
    recent_date = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

    for i in range(count):
        entity = GenericEntity(
            id=f"inquiry-recent-{i}",
            type="inquiry",
            data={
                "title": f"Recent Inquiry {i}",
                "status": "active",
                "created_at": recent_date,
            },
        )
        store.save_entity(entity)

        # Add bonds to inquiries so they're not orphans
        if i > 0:
            store.save_bond(
                bond_id=f"rel-inquiry-chain-{i}",
                bond_type="continues",
                from_id=f"inquiry-recent-{i}",
                to_id=f"inquiry-recent-{i-1}",
                data={"confidence": 1.0},
            )

    store.close()
    test_context["recent_inquiry_count"] = count


@given("verification rate is below 50%")
def setup_low_verification_rate(db_path, test_context):
    """Set up a system with low verification rate."""
    store = EventStore(db_path)

    # Create some behaviors and tools, but few verifies bonds
    for i in range(4):
        behavior = GenericEntity(
            id=f"behavior-test-{i}",
            type="behavior",
            data={"title": f"Behavior {i}", "status": "active"},
        )
        store.save_entity(behavior)

        tool = GenericEntity(
            id=f"tool-test-{i}",
            type="tool",
            data={"title": f"Tool {i}", "handler": f"test.handler_{i}"},
        )
        store.save_entity(tool)

        # implements bond
        store.save_bond(
            bond_id=f"rel-implements-{i}",
            bond_type="implements",
            from_id=f"behavior-test-{i}",
            to_id=f"tool-test-{i}",
            data={"confidence": 1.0},
        )

    # Only 1 verifies bond out of 4 possible (25%)
    store.save_bond(
        bond_id="rel-verifies-0",
        bond_type="verifies",
        from_id="tool-test-0",
        to_id="behavior-test-0",
        data={"confidence": 1.0},
    )

    store.close()
    test_context["verification_rate"] = 0.25


@given("the Loom has integrity_score above 0.9")
def setup_high_integrity(db_path, test_context):
    """Set up a system with high integrity.

    High integrity = behaviors with status='verified' / total behaviors
    For steward phase, we want high integrity but NOT high verification rate.
    So we create verified behaviors WITHOUT verifies bonds.
    """
    store = EventStore(db_path)

    # Create verified behaviors (status=verified) WITHOUT verifies bonds
    # This gives high integrity but low verification rate
    for i in range(5):
        behavior = GenericEntity(
            id=f"behavior-stable-{i}",
            type="behavior",
            data={"title": f"Stable Behavior {i}", "status": "verified"},
        )
        store.save_entity(behavior)

        tool = GenericEntity(
            id=f"tool-stable-{i}",
            type="tool",
            data={"title": f"Stable Tool {i}", "handler": f"stable.handler_{i}"},
        )
        store.save_entity(tool)

        # ONLY implements bond - no verifies bond
        # This keeps integrity high (status=verified) but verification_rate low (no verifies bonds)
        store.save_bond(
            bond_id=f"rel-implements-stable-{i}",
            bond_type="implements",
            from_id=f"behavior-stable-{i}",
            to_id=f"tool-stable-{i}",
            data={"confidence": 1.0},
        )

    store.close()
    test_context["integrity_score"] = 0.95


@given("change rate is below 0.1 entities per day")
def setup_low_change_rate(db_path, test_context):
    """Set up a system with low change rate (entities created long ago)."""
    store = EventStore(db_path)
    old_date = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

    # Update existing entities to have old created_at
    cur = store._conn.cursor()
    cur.execute(
        "UPDATE entities SET data_json = json_set(data_json, '$.created_at', ?)",
        (old_date,)
    )
    store._conn.commit()
    store.close()

    test_context["change_rate"] = 0.05


@given(parsers.parse("the Loom has {count:d} new bonds created this week"))
def setup_recent_bonds(db_path, test_context, count: int):
    """Create recent bonds for cultivator phase detection.

    For cultivator phase to dominate:
    - Need high bond activity (cultivator signal)
    - Need entities to be well-connected (minimize orphans/curator signal)
    - Need some verification happening (so orange side wins)
    """
    store = EventStore(db_path)
    recent_date = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

    # Create a small set of well-connected entities (fewer entities = fewer potential orphans)
    entity_count = min(count + 2, 10)
    for i in range(entity_count):
        entity = GenericEntity(
            id=f"entity-bondable-{i}",
            type="learning",
            data={"title": f"Bondable Entity {i}", "created_at": recent_date},
        )
        store.save_entity(entity)

    # Create bonds in a circular pattern so every entity has at least one bond
    for i in range(count):
        store.save_bond(
            bond_id=f"rel-recent-bond-{i}",
            bond_type="crystallized-from",
            from_id=f"entity-bondable-{i % entity_count}",
            to_id=f"entity-bondable-{(i + 1) % entity_count}",
            data={"confidence": 1.0, "created_at": recent_date},
        )

    # Add some behaviors with verifies bonds to boost orange side (regulator)
    for i in range(2):
        behavior = GenericEntity(
            id=f"behavior-cultivator-{i}",
            type="behavior",
            data={"title": f"Cultivator Behavior {i}", "status": "verified", "created_at": recent_date},
        )
        store.save_entity(behavior)
        tool = GenericEntity(
            id=f"tool-cultivator-{i}",
            type="tool",
            data={"title": f"Cultivator Tool {i}", "handler": f"cultivator.handler_{i}"},
        )
        store.save_entity(tool)
        store.save_bond(
            bond_id=f"rel-cultivator-verifies-{i}",
            bond_type="verifies",
            from_id=f"tool-cultivator-{i}",
            to_id=f"behavior-cultivator-{i}",
            data={"confidence": 1.0, "created_at": recent_date},
        )

    store.close()
    test_context["recent_bond_count"] = count


@given(parsers.parse("{count:d} new learnings captured this week"))
def setup_recent_learnings(db_path, test_context, count: int):
    """Create recent learnings for cultivator phase detection.

    Learnings should be connected to existing entities to avoid orphan detection.
    """
    store = EventStore(db_path)
    recent_date = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

    for i in range(count):
        entity = GenericEntity(
            id=f"learning-recent-{i}",
            type="learning",
            data={
                "title": f"Recent Learning {i}",
                "insight": f"Fresh insight {i}",
                "created_at": recent_date,
            },
        )
        store.save_entity(entity)

        # Connect learnings to existing bondable entities (from the bonds step)
        # or to each other if those don't exist
        if i > 0:
            store.save_bond(
                bond_id=f"rel-learning-chain-{i}",
                bond_type="crystallized-from",
                from_id=f"learning-recent-{i}",
                to_id=f"learning-recent-{i-1}",
                data={"confidence": 1.0, "created_at": recent_date},
            )
        else:
            # Connect first learning to entity-bondable-0 if it exists
            store.save_bond(
                bond_id=f"rel-learning-to-bondable-{i}",
                bond_type="crystallized-from",
                from_id=f"learning-recent-{i}",
                to_id="entity-bondable-0",
                data={"confidence": 1.0, "created_at": recent_date},
            )

    store.close()
    test_context["recent_learning_count"] = count


# =============================================================================
# Temporal Health Setup Steps
# =============================================================================


@given("the Loom has activity over the past 7 days")
def setup_activity_over_week(db_path, test_context):
    """Create varied activity over the past week."""
    store = EventStore(db_path)

    for day in range(7):
        date = (datetime.now(timezone.utc) - timedelta(days=day)).isoformat()

        entity = GenericEntity(
            id=f"entity-day-{day}",
            type="learning",
            data={"title": f"Entity from day {day}", "created_at": date},
        )
        store.save_entity(entity)

    store.close()
    test_context["activity_days"] = 7


@given(parsers.parse("{count:d} entities were created in the last 7 days"))
def setup_created_entities(db_path, test_context, count: int):
    """Create entities with recent timestamps."""
    store = EventStore(db_path)

    for i in range(count):
        date = (datetime.now(timezone.utc) - timedelta(days=i % 7)).isoformat()
        entity = GenericEntity(
            id=f"entity-created-{i}",
            type="learning",
            data={"title": f"Created Entity {i}", "created_at": date},
        )
        store.save_entity(entity)

    store.close()
    test_context["entities_created"] = count


@given(parsers.parse("{count:d} entity was composted in the last 7 days"))
def setup_composted_entity(db_path, test_context, count: int):
    """Create archive entries for composted entities."""
    store = EventStore(db_path)
    recent_date = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

    # Create archive entries for composted entities
    cur = store._conn.cursor()
    for i in range(count):
        cur.execute(
            """
            INSERT INTO archive (id, original_type, original_id, data_json, archived_at, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                f"archive-composted-{i}",
                "learning",
                f"composted-entity-{i}",
                '{"title": "Composted entity"}',
                recent_date,
                "composted",
            ),
        )
    store._conn.commit()
    store.close()
    test_context["entities_composted"] = count


@given(parsers.parse("{count:d} entities were composted in the last 7 days"))
def setup_composted_entities(db_path, test_context, count: int):
    """Create archive entries for composted entities."""
    store = EventStore(db_path)
    recent_date = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

    # Create archive entries for composted entities
    cur = store._conn.cursor()
    for i in range(count):
        cur.execute(
            """
            INSERT INTO archive (id, original_type, original_id, data_json, archived_at, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                f"archive-composted-{i}",
                "learning",
                f"composted-entity-{i}",
                '{"title": "Composted entity"}',
                recent_date,
                "composted",
            ),
        )
    store._conn.commit()
    store.close()
    test_context["entities_composted"] = count


@given(parsers.parse("{entities:d} entities and {bonds:d} bonds were created in the last 7 days"))
def setup_entities_and_bonds(db_path, test_context, entities: int, bonds: int):
    """Create exactly the specified entities and bonds for metabolic balance calculation.

    Important: This step should NOT create any additional entities beyond the count specified.
    The metabolic_balance formula is: (entities_created + bonds_created) / (composted + digested)
    """
    store = EventStore(db_path)
    recent_date = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

    # Create exactly 'entities' entities - no more
    for i in range(entities):
        entity = GenericEntity(
            id=f"entity-mb-{i}",
            type="learning",
            data={"title": f"Entity for MB {i}", "created_at": recent_date},
        )
        store.save_entity(entity)

    # Create exactly 'bonds' bonds
    for i in range(bonds):
        store.save_bond(
            bond_id=f"rel-mb-{i}",
            bond_type="crystallized-from",
            from_id=f"entity-mb-{i % entities}",
            to_id=f"entity-mb-{(i + 1) % entities}",
            data={"confidence": 1.0, "created_at": recent_date},
        )

    store.close()
    test_context["entities_created"] = entities
    test_context["bonds_created"] = bonds


# =============================================================================
# Satiation Setup Steps
# =============================================================================


@given(parsers.parse("the Loom has integrity_score {score:g}"))
def setup_integrity_score(test_context, score: float):
    """Set expected integrity score."""
    test_context["integrity_score"] = score


@given(parsers.parse("entropy_score {score:g}"))
def setup_entropy_score(test_context, score: float):
    """Set expected entropy score."""
    test_context["entropy_score"] = score


@given(parsers.parse("{count:d} active inquiries"))
def setup_active_inquiries(db_path, test_context, count: int):
    """Create active inquiries."""
    store = EventStore(db_path)

    for i in range(count):
        entity = GenericEntity(
            id=f"inquiry-active-{i}",
            type="inquiry",
            data={"title": f"Active Inquiry {i}", "status": "active"},
        )
        store.save_entity(entity)

    store.close()
    test_context["active_inquiries"] = count


@given(parsers.parse("{count:d} unresolved signals"))
def setup_unresolved_signals(db_path, test_context, count: int):
    """Create unresolved signals."""
    store = EventStore(db_path)

    for i in range(count):
        entity = GenericEntity(
            id=f"signal-unresolved-{i}",
            type="signal",
            data={"title": f"Unresolved Signal {i}", "status": "active"},
        )
        store.save_entity(entity)

    store.close()
    test_context["unresolved_signals"] = count


# =============================================================================
# Action Steps
# =============================================================================


@when("sense_kairotic_state is invoked")
def invoke_sense_kairotic_state(db_path, test_context):
    """Call sense_kairotic_state and store result."""
    try:
        from chora_cvm.rhythm import sense_kairotic_state
        result = sense_kairotic_state(db_path)
        test_context["kairotic_result"] = result
    except ImportError:
        pytest.skip("rhythm.py not yet implemented")


@when(parsers.parse("temporal_health is invoked with window_days {days:d}"))
def invoke_temporal_health(db_path, test_context, days: int):
    """Call temporal_health with window."""
    try:
        from chora_cvm.rhythm import temporal_health
        result = temporal_health(db_path, window_days=days)
        test_context["temporal_result"] = result
    except ImportError:
        pytest.skip("rhythm.py not yet implemented")


@when("compute_satiation is invoked")
def invoke_compute_satiation(db_path, test_context):
    """Call compute_satiation and store result."""
    try:
        from chora_cvm.rhythm import compute_satiation
        result = compute_satiation(db_path)
        test_context["satiation_result"] = result
    except ImportError:
        pytest.skip("rhythm.py not yet implemented")


# =============================================================================
# Assertion Steps - Kairotic State
# =============================================================================


@then("the result includes KairoticState with 6 phase weights")
def check_kairotic_phases(test_context):
    """Verify KairoticState structure."""
    result = test_context.get("kairotic_result", {})
    phases = ["pioneer", "cultivator", "regulator", "steward", "curator", "scout"]

    for phase in phases:
        key = f"{phase}_weight"
        assert key in result, f"Missing {key} in KairoticState"


@then("each phase weight is between 0.0 and 1.0")
def check_phase_weight_range(test_context):
    """Verify phase weights are valid."""
    result = test_context.get("kairotic_result", {})
    phases = ["pioneer", "cultivator", "regulator", "steward", "curator", "scout"]

    for phase in phases:
        weight = result.get(f"{phase}_weight", -1)
        assert 0.0 <= weight <= 1.0, f"{phase}_weight {weight} not in [0.0, 1.0]"


@then("the result includes a dominant_phase field")
def check_dominant_phase_exists(test_context):
    """Verify dominant_phase exists."""
    result = test_context.get("kairotic_result", {})
    assert "dominant_phase" in result, "Missing dominant_phase"


@then("the result includes a side field (orange or purple)")
def check_side_field(test_context):
    """Verify side field exists and is valid."""
    result = test_context.get("kairotic_result", {})
    assert "side" in result, "Missing side field"
    assert result["side"] in ("orange", "purple"), f"Invalid side: {result['side']}"


@then(parsers.parse('the dominant_phase is "{phase}"'))
def check_dominant_phase(test_context, phase: str):
    """Verify dominant phase."""
    result = test_context.get("kairotic_result", {})
    assert result.get("dominant_phase") == phase, \
        f"Expected dominant_phase={phase}, got {result.get('dominant_phase')}"


@then(parsers.parse('the side is "{side}"'))
def check_side(test_context, side: str):
    """Verify side."""
    result = test_context.get("kairotic_result", {})
    assert result.get("side") == side, f"Expected side={side}, got {result.get('side')}"


# =============================================================================
# Assertion Steps - Temporal Health
# =============================================================================


@then("the result includes entities_created count")
def check_entities_created(test_context):
    """Verify entities_created in result."""
    result = test_context.get("temporal_result", {})
    assert "entities_created" in result, "Missing entities_created"


@then("the result includes bonds_created count")
def check_bonds_created(test_context):
    """Verify bonds_created in result."""
    result = test_context.get("temporal_result", {})
    assert "bonds_created" in result, "Missing bonds_created"


@then("the result includes growth_rate as float")
def check_growth_rate(test_context):
    """Verify growth_rate in result."""
    result = test_context.get("temporal_result", {})
    assert "growth_rate" in result, "Missing growth_rate"
    assert isinstance(result["growth_rate"], (int, float)), "growth_rate is not numeric"


@then("the result includes metabolic_balance as float")
def check_metabolic_balance(test_context):
    """Verify metabolic_balance in result."""
    result = test_context.get("temporal_result", {})
    assert "metabolic_balance" in result, "Missing metabolic_balance"
    assert isinstance(result["metabolic_balance"], (int, float)), "metabolic_balance is not numeric"


@then(parsers.parse("growth_rate is approximately {rate:g} per day"))
def check_growth_rate_value(test_context, rate: float):
    """Verify growth_rate value (with tolerance)."""
    result = test_context.get("temporal_result", {})
    actual = result.get("growth_rate", 0)
    tolerance = 0.1
    assert abs(actual - rate) < tolerance, f"Expected growth_rate ~{rate}, got {actual}"


@then(parsers.parse("metabolic_balance is {balance:g} (anabolic / catabolic)"))
def check_metabolic_balance_value(test_context, balance: float):
    """Verify metabolic_balance value."""
    result = test_context.get("temporal_result", {})
    actual = result.get("metabolic_balance", 0)
    tolerance = 1.0
    assert abs(actual - balance) < tolerance, f"Expected metabolic_balance ~{balance}, got {actual}"


# =============================================================================
# Assertion Steps - Satiation
# =============================================================================


@then("the satiation score is between 0.0 and 1.0")
def check_satiation_range(test_context):
    """Verify satiation score range."""
    result = test_context.get("satiation_result")
    assert result is not None, "No satiation result"
    assert 0.0 <= result <= 1.0, f"Satiation {result} not in [0.0, 1.0]"


@then(parsers.parse("the satiation score is above {threshold:g}"))
def check_satiation_above(test_context, threshold: float):
    """Verify satiation is above threshold."""
    result = test_context.get("satiation_result")
    assert result is not None, "No satiation result"
    assert result > threshold, f"Satiation {result} not above {threshold}"


@then(parsers.parse("the satiation score is below {threshold:g}"))
def check_satiation_below(test_context, threshold: float):
    """Verify satiation is below threshold."""
    result = test_context.get("satiation_result")
    assert result is not None, "No satiation result"
    assert result < threshold, f"Satiation {result} not below {threshold}"
