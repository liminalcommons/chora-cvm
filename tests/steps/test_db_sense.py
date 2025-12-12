"""Step definitions for database health sensing."""

import tempfile

import pytest
from pytest_bdd import given, when, then, scenarios, parsers

from chora_cvm.store import EventStore
from chora_cvm.kernel.schema import GenericEntity
from chora_cvm.schema import ExecutionContext

# Link to feature file
scenarios("../features/db_sense.feature")


@pytest.fixture
def test_context():
    """Shared test context passed between steps."""
    return {"db_path": None, "sense_result": None}


@given("a database with entities and bonds", target_fixture="test_context")
def given_database(test_context):
    """Set up a database with entities and bonds."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    store = EventStore(db_path)

    # Add some entities
    for i in range(3):
        entity = GenericEntity(
            id=f"test-entity-{i}",
            type="learning",
            data={"title": f"Test Learning {i}"}
        )
        store.save_entity(entity)

    # Add a bond
    store.save_bond(
        bond_id="test-bond-1",
        bond_type="yields",
        from_id="test-entity-0",
        to_id="test-entity-1",
        status="active",
        confidence=1.0,
        data={}
    )

    store.close()
    test_context["db_path"] = db_path
    return test_context


@when("I call graph.db.sense with the database path")
def when_call_sense(test_context):
    """Call the db_sense primitive."""
    from chora_cvm.lib.graph import db_sense

    ctx = ExecutionContext(db_path=test_context["db_path"], persona_id="test")
    result = db_sense(_ctx=ctx)
    test_context["sense_result"] = result


@then("I receive a structured health summary")
def then_receive_summary(test_context):
    """Check that we got a result."""
    assert test_context["sense_result"] is not None
    assert isinstance(test_context["sense_result"], dict)


@then("the summary contains entity counts by type")
def then_entity_counts(test_context):
    """Check entity counts are present."""
    result = test_context["sense_result"]
    assert "entity_counts" in result
    assert isinstance(result["entity_counts"], dict)
    assert result["entity_counts"].get("learning") == 3


@then("the summary contains total bond count")
def then_bond_count(test_context):
    """Check total bond count."""
    result = test_context["sense_result"]
    assert "total_bonds" in result
    assert result["total_bonds"] == 1


@then("the summary contains orphan bond count")
def then_orphan_count(test_context):
    """Check orphan bond count."""
    result = test_context["sense_result"]
    assert "orphan_bonds" in result
    assert result["orphan_bonds"] == 0


@then("the summary contains last modified timestamp")
def then_last_modified(test_context):
    """Check last modified timestamp."""
    result = test_context["sense_result"]
    assert "last_modified" in result
