"""
Step definitions for the Asset Ownership feature.

These tests verify the behaviors specified by story-assets-belong-to-circles.
Assets belong to circles via belongs-to bonds.
"""
import os
import tempfile

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import manifest_entity, manage_bond

# Load scenarios from feature file
scenarios("../features/asset_ownership.feature")


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
    test_context["circles"] = []
    test_context["assets"] = []


# =============================================================================
# Circle and Asset Setup Steps
# =============================================================================


@given(parsers.parse('a circle "{circle_id}" exists'))
def create_circle(db_path, test_context, circle_id: str):
    """Create a circle entity."""
    manifest_entity(
        db_path,
        "circle",
        circle_id,
        {"title": f"Test circle {circle_id}", "sync_policy": "local-only"},
    )
    test_context["circles"].append(circle_id)


@given(parsers.parse('an asset "{asset_id}" of kind "{kind}" exists'))
def create_asset(db_path, test_context, asset_id: str, kind: str):
    """Create an asset entity."""
    manifest_entity(
        db_path,
        "asset",
        asset_id,
        {"title": f"Test asset {asset_id}", "kind": kind},
    )
    test_context["assets"].append(asset_id)
    test_context["last_asset"] = asset_id


@given(parsers.parse('an asset "{asset_id}" of kind "{kind}" with uri "{uri}" exists'))
def create_asset_with_uri(db_path, test_context, asset_id: str, kind: str, uri: str):
    """Create an asset entity with URI."""
    manifest_entity(
        db_path,
        "asset",
        asset_id,
        {"title": f"Test asset {asset_id}", "kind": kind, "uri": uri},
    )
    test_context["assets"].append(asset_id)
    test_context["last_asset"] = asset_id


@given(parsers.parse('all {count:d} assets belong to "{circle_id}"'))
def all_assets_belong(db_path, test_context, count: int, circle_id: str):
    """Bond all assets to a circle."""
    for asset_id in test_context["assets"][-count:]:
        manage_bond(db_path, "belongs-to", asset_id, circle_id)


@given(parsers.parse('"{asset_id}" belongs to both circles'))
def asset_belongs_both(db_path, test_context, asset_id: str):
    """Bond asset to both circles in context."""
    for circle_id in test_context["circles"]:
        manage_bond(db_path, "belongs-to", asset_id, circle_id)


# =============================================================================
# When Steps
# =============================================================================


@when(parsers.parse('I bond belongs-to from "{from_id}" to "{to_id}"'))
def bond_belongs_to(db_path, test_context, from_id: str, to_id: str):
    """Create a belongs-to bond."""
    result = manage_bond(db_path, "belongs-to", from_id, to_id)
    test_context["last_bond"] = result


@when(parsers.parse('I query get_assets for "{circle_id}"'))
def query_assets(db_path, test_context, circle_id: str):
    """Query assets owned by a circle."""
    store = EventStore(db_path)
    assets = store.get_assets(circle_id)
    store.close()
    test_context["query_result"] = assets


@when(parsers.parse('I query get_owner_circles for "{asset_id}"'))
def query_owner_circles(db_path, test_context, asset_id: str):
    """Query circles that own an asset."""
    store = EventStore(db_path)
    circles = store.get_owner_circles(asset_id)
    store.close()
    test_context["query_result"] = circles


# =============================================================================
# Then Steps - Constellation
# =============================================================================


@then(parsers.parse("the asset appears in {circle_id}'s constellation"))
def check_asset_in_constellation(db_path, test_context, circle_id: str):
    """Verify asset appears in circle's constellation."""
    asset_id = test_context.get("last_asset")
    store = EventStore(db_path)
    constellation = store.get_constellation(circle_id)
    store.close()

    incoming_ids = [b["from_id"] for b in constellation["incoming"]]
    assert asset_id in incoming_ids, \
        f"Asset {asset_id} not in {circle_id} constellation: {incoming_ids}"


@then(parsers.parse('get_assets for "{circle_id}" returns the asset'))
def check_get_assets_returns(db_path, test_context, circle_id: str):
    """Verify get_assets returns the asset."""
    asset_id = test_context.get("last_asset")
    store = EventStore(db_path)
    assets = store.get_assets(circle_id)
    store.close()

    asset_ids = [a["id"] for a in assets]
    assert asset_id in asset_ids, f"Asset {asset_id} not in {circle_id} assets: {asset_ids}"


# =============================================================================
# Then Steps - Query Results
# =============================================================================


@then(parsers.parse('the result includes an asset with kind "{kind}"'))
def check_result_has_kind(test_context, kind: str):
    """Verify result includes asset with expected kind."""
    assets = test_context.get("query_result", [])
    kinds = [a.get("data", {}).get("kind") for a in assets]
    assert kind in kinds, f"Expected kind '{kind}' in {kinds}"


@then(parsers.parse("{count:d} assets are returned"))
def check_asset_count(test_context, count: int):
    """Verify number of assets returned."""
    assets = test_context.get("query_result", [])
    assert len(assets) == count, f"Expected {count} assets, got {len(assets)}"


@then("an empty list is returned")
def check_empty_result(test_context):
    """Verify empty result."""
    result = test_context.get("query_result", [])
    assert len(result) == 0, f"Expected empty list, got {result}"


@then(parsers.parse('get_assets for "{circle_id}" includes "{asset_id}"'))
def check_assets_includes(db_path, circle_id: str, asset_id: str):
    """Verify get_assets includes specific asset."""
    store = EventStore(db_path)
    assets = store.get_assets(circle_id)
    store.close()

    asset_ids = [a["id"] for a in assets]
    assert asset_id in asset_ids, f"Asset {asset_id} not in {circle_id} assets: {asset_ids}"


@then("both circles are returned")
def check_both_circles_returned(test_context):
    """Verify both circles in result."""
    circles = test_context.get("query_result", [])
    expected = test_context.get("circles", [])
    for circle_id in expected:
        assert circle_id in circles, f"Expected {circle_id} in {circles}"


@then("each result includes entity id, type, and data")
def check_result_structure(test_context):
    """Verify result structure."""
    result = test_context.get("query_result", [])
    for entity in result:
        assert "id" in entity, f"Missing id in {entity}"
        assert "type" in entity, f"Missing type in {entity}"
        assert "data" in entity, f"Missing data in {entity}"


@then("the data includes uri field")
def check_data_has_uri(test_context):
    """Verify data includes uri field."""
    result = test_context.get("query_result", [])
    assert len(result) > 0, "No results to check"
    for entity in result:
        assert "uri" in entity.get("data", {}), f"Missing uri in {entity}"
