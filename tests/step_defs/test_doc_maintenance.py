"""
Step definitions for the Documentation Self-Maintenance feature.

These tests verify behaviors specified by:
- story-docs-detect-their-own-staleness
- story-docs-auto-repair-syntactic-issues
- story-docs-propose-semantic-changes-for-review

Principle: principle-documentation-emerges-from-the-entity-graph-not-manual-editing
"""
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest
from pytest_bdd import given, scenarios, then, when, parsers

from chora_cvm.store import EventStore
from chora_cvm.std import (
    emit_signal,
    create_focus,
    resolve_focus,
    manifest_entity,
)

# Load scenarios from feature file
scenarios("../features/doc_maintenance.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_context() -> Dict[str, Any]:
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


@pytest.fixture
def workspace_path():
    """Create a temporary workspace directory for testing."""
    workspace = tempfile.mkdtemp(prefix="chora_test_")
    yield workspace
    # Cleanup
    shutil.rmtree(workspace, ignore_errors=True)


# =============================================================================
# Background Steps
# =============================================================================


@given("a fresh CVM database")
def fresh_database(db_path, test_context):
    """Set up a fresh database for testing."""
    test_context["db_path"] = db_path


@given("a temporary workspace directory")
def temp_workspace(workspace_path, test_context):
    """Set up a temporary workspace directory."""
    test_context["workspace"] = workspace_path
    # Create basic structure
    (Path(workspace_path) / "packages").mkdir()
    (Path(workspace_path) / "docs" / "research").mkdir(parents=True)


# =============================================================================
# Detection Steps (Phase 2)
# =============================================================================


@given(parsers.parse('a CLAUDE.md with reference to "{path}"'))
def create_claude_md_with_ref(workspace_path, test_context, path: str):
    """Create a CLAUDE.md file with a reference to a nonexistent path."""
    claude_md = Path(workspace_path) / "CLAUDE.md"
    content = f"""# Test CLAUDE.md

This file references `{path}` which does not exist.

## Files
- `{path}` - a nonexistent file
"""
    claude_md.write_text(content)
    test_context["claude_md_path"] = str(claude_md)
    test_context["stale_path"] = path


@given(parsers.parse('an inquiry file "{filepath}" not mentioned in main docs'))
def create_unsurfaced_inquiry(workspace_path, test_context, filepath: str):
    """Create an inquiry file that is not mentioned in main docs."""
    inquiry_path = Path(workspace_path) / filepath
    inquiry_path.parent.mkdir(parents=True, exist_ok=True)
    inquiry_path.write_text("# Inquiry: Autoevolution\n\nWhat is it like when...")

    # Create main docs that don't mention it
    (Path(workspace_path) / "CLAUDE.md").write_text("# CLAUDE.md\n\nNo references here.")
    (Path(workspace_path) / "AGENTS.md").write_text("# AGENTS.md\n\nNo references here.")

    test_context["inquiry_path"] = str(inquiry_path)
    test_context["inquiry_name"] = inquiry_path.stem


@given(parsers.parse('a package directory "{pkg_path}" without CLAUDE.md'))
def create_package_without_claude(workspace_path, test_context, pkg_path: str):
    """Create a package directory without CLAUDE.md."""
    pkg_dir = Path(workspace_path) / pkg_path
    pkg_dir.mkdir(parents=True, exist_ok=True)
    # Create some content but no CLAUDE.md
    (pkg_dir / "src").mkdir()
    (pkg_dir / "pyproject.toml").write_text("[project]\nname = 'orphan-pkg'\n")
    test_context["orphan_pkg"] = pkg_path.split("/")[-1]


@given(parsers.parse('an AGENTS.md with reference to "{text}"'))
def create_agents_md_with_outdated(workspace_path, test_context, text: str):
    """Create an AGENTS.md with outdated content."""
    agents_md = Path(workspace_path) / "AGENTS.md"
    agents_md.write_text(f"# AGENTS.md\n\nThe system uses {text} for entity types.\n")
    test_context["outdated_text"] = text


@given("a well-formed workspace with no doc issues")
def create_clean_workspace(workspace_path, test_context):
    """Create a workspace with no documentation issues."""
    # Create CLAUDE.md
    claude_md = Path(workspace_path) / "CLAUDE.md"
    claude_md.write_text("# CLAUDE.md\n\nClean documentation.\n")

    # Create AGENTS.md with correct references
    agents_md = Path(workspace_path) / "AGENTS.md"
    agents_md.write_text("# AGENTS.md\n\nThe system uses 10 Nouns (Decemvirate).\n")

    # Create a package with CLAUDE.md
    pkg = Path(workspace_path) / "packages" / "good-pkg"
    pkg.mkdir(parents=True)
    (pkg / "CLAUDE.md").write_text("# Good Package\n\nDocumented.\n")


@when("I run doc detection")
def run_doc_detection(db_path, test_context):
    """Run the doc detection protocol."""
    # Import the audit function (will be implemented)
    from chora_cvm.std import audit_docs

    workspace = test_context.get("workspace")
    result = audit_docs(db_path, workspace_path=workspace, emit_signals=True)
    test_context["detection_result"] = result
    test_context["emitted_signals"] = result.get("signals", [])


@then(parsers.parse('a signal "{signal_type}" should be emitted'))
def check_signal_emitted(db_path, test_context, signal_type: str):
    """Verify a signal of the given type was emitted."""
    signals = test_context.get("emitted_signals", [])
    found = any(s.get("signal_type") == signal_type for s in signals)

    if not found:
        # Also check database
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT data_json FROM entities WHERE type = 'signal'"
        )
        rows = cur.fetchall()
        conn.close()

        for row in rows:
            data = json.loads(row["data_json"])
            if data.get("signal_type") == signal_type:
                found = True
                break

    assert found, f"Signal of type '{signal_type}' not found. Got: {signals}"


@then(parsers.parse('the signal data should contain the stale path "{path}"'))
def check_signal_contains_path(test_context, path: str):
    """Verify signal data contains the stale path."""
    signals = test_context.get("emitted_signals", [])
    found = False
    for s in signals:
        data = s.get("data", {})
        if path in str(data):
            found = True
            break
    assert found, f"Signal data should contain path '{path}'"


@then(parsers.parse('the signal data should reference "{reference}"'))
def check_signal_references(test_context, reference: str):
    """Verify signal data references the given item."""
    signals = test_context.get("emitted_signals", [])
    found = False
    for s in signals:
        data = s.get("data", {})
        if reference in str(data) or reference in s.get("title", ""):
            found = True
            break
    assert found, f"Signal should reference '{reference}'"


@then("the signal data should indicate the stale reference")
def check_signal_indicates_stale(test_context):
    """Verify signal indicates stale reference."""
    signals = test_context.get("emitted_signals", [])
    assert len(signals) > 0, "Expected at least one signal"
    # The signal should have data about what's stale
    for s in signals:
        if s.get("signal_type") == "doc-outdated-count":
            assert s.get("data") is not None, "Signal should have data about stale reference"


@then("no signals should be emitted")
def check_no_signals(test_context):
    """Verify no signals were emitted."""
    signals = test_context.get("emitted_signals", [])
    assert len(signals) == 0, f"Expected no signals, got {len(signals)}: {signals}"


# =============================================================================
# Syntactic Repair Steps (Phase 3 - After Validation Gate)
# =============================================================================


@given(parsers.parse('a signal "doc-stale-ref" for path "{path}"'))
def create_stale_ref_signal(db_path, test_context, path: str):
    """Create a signal for a stale reference."""
    result = emit_signal(
        db_path,
        title=f"Stale reference to {path}",
        signal_type="doc-stale-ref",
        data={"stale_path": path},
    )
    test_context["signal_id"] = result.get("id")
    test_context["stale_path"] = path


@given("a CLAUDE.md containing a reference to that path")
def create_claude_md_with_stale_ref(workspace_path, test_context):
    """Create CLAUDE.md with the stale reference."""
    path = test_context.get("stale_path")
    claude_md = Path(workspace_path) / "CLAUDE.md"
    content = f"""# CLAUDE.md

This references `{path}` which is stale.
"""
    claude_md.write_text(content)
    test_context["target_file"] = str(claude_md)


@when("the repair protocol runs")
def run_repair_protocol(db_path, test_context):
    """Run the syntactic repair protocol."""
    # Import and run repair (will be implemented in Phase 3)
    from chora_cvm.std import repair_syntactic

    signal_id = test_context.get("signal_id")
    target = test_context.get("target_file")
    result = repair_syntactic(db_path, signal_id=signal_id, target_file=target)
    test_context["repair_result"] = result


@when("the repair protocol runs in dry-run mode")
def run_repair_dry_run(db_path, test_context):
    """Run repair in dry-run mode."""
    from chora_cvm.std import repair_syntactic

    signal_id = test_context.get("signal_id")
    target = test_context.get("target_file")
    result = repair_syntactic(db_path, signal_id=signal_id, target_file=target, dry_run=True)
    test_context["repair_result"] = result


@then(parsers.parse('the reference should be commented with "{comment}"'))
def check_reference_commented(test_context, comment: str):
    """Verify the reference was commented."""
    target = test_context.get("target_file")
    content = Path(target).read_text()
    assert comment in content, f"Expected comment '{comment}' not found in file"


@then("the signal should be resolved")
def check_signal_resolved(db_path, test_context):
    """Verify the signal was resolved."""
    signal_id = test_context.get("signal_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (signal_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert data.get("status") == "resolved", f"Signal status should be 'resolved', got {data.get('status')}"


@then("the proposed change should be shown")
def check_proposed_change_shown(test_context):
    """Verify proposed change was shown."""
    result = test_context.get("repair_result", {})
    assert "proposed_change" in result, "Repair result should contain proposed_change"


@then("the file should not be modified")
def check_file_not_modified(test_context):
    """Verify file was not modified in dry-run."""
    target = test_context.get("target_file")
    stale_path = test_context.get("stale_path")
    content = Path(target).read_text()
    # Should still contain the raw reference, not commented
    assert f"`{stale_path}`" in content, "File should not be modified in dry-run"
    assert "<!-- STALE" not in content, "File should not have STALE comment in dry-run"


@then("the signal should remain active")
def check_signal_still_active(db_path, test_context):
    """Verify signal is still active."""
    signal_id = test_context.get("signal_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (signal_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert data.get("status") == "active", f"Signal should still be 'active'"


@then("a backup file should exist with .bak extension")
def check_backup_exists(test_context):
    """Verify backup was created."""
    target = test_context.get("target_file")
    backup = target + ".bak"
    assert Path(backup).exists(), f"Backup file {backup} should exist"


# =============================================================================
# Semantic Proposal Steps (Phase 4)
# =============================================================================


@given(parsers.parse('a signal "doc-unsurfaced-research" for "{research_name}"'))
def create_unsurfaced_research_signal(db_path, test_context, research_name: str):
    """Create a signal for unsurfaced research."""
    result = emit_signal(
        db_path,
        title=f"Unsurfaced research: {research_name}",
        signal_type="doc-unsurfaced-research",
        data={"research_name": research_name},
    )
    test_context["signal_id"] = result.get("id")
    test_context["research_name"] = research_name


@given(parsers.parse('a signal "doc-outdated-count" for "{count_ref}"'))
def create_outdated_count_signal(db_path, test_context, count_ref: str):
    """Create a signal for outdated noun count."""
    result = emit_signal(
        db_path,
        title=f"Outdated reference: {count_ref}",
        signal_type="doc-outdated-count",
        data={"stale_reference": count_ref, "current": "10 Nouns (Decemvirate)"},
    )
    test_context["signal_id"] = result.get("id")


@when("the propose protocol runs")
def run_propose_protocol(db_path, test_context):
    """Run the semantic proposal protocol."""
    from chora_cvm.std import propose_semantic

    signal_id = test_context.get("signal_id")
    result = propose_semantic(db_path, signal_id=signal_id)
    test_context["propose_result"] = result
    test_context["focus_id"] = result.get("focus_id")


@then(parsers.parse('a Focus should be created with title containing "{text}"'))
def check_focus_created_with_title(db_path, test_context, text: str):
    """Verify Focus was created with expected title."""
    focus_id = test_context.get("focus_id")
    assert focus_id is not None, "No focus_id in propose result"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"Focus {focus_id} not found"
    data = json.loads(row["data_json"])
    assert text in data.get("title", ""), f"Focus title should contain '{text}'"


@then("the Focus review_data should contain the proposed integration text")
def check_focus_has_review_data(db_path, test_context):
    """Verify Focus has review_data with proposed changes."""
    focus_id = test_context.get("focus_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    data = json.loads(row["data_json"])
    assert "review_data" in data, "Focus should have review_data"
    assert "proposed" in data["review_data"], "review_data should contain proposed changes"


@then(parsers.parse('the Focus should have status "{status}"'))
def check_focus_status(db_path, test_context, status: str):
    """Verify Focus has expected status."""
    focus_id = test_context.get("focus_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    data = json.loads(row["data_json"])
    assert data.get("status") == status, f"Focus status should be '{status}'"


@then("a Focus should be created for reviewing the noun count update")
def check_focus_for_noun_update(db_path, test_context):
    """Verify Focus was created for noun count update."""
    focus_id = test_context.get("focus_id")
    assert focus_id is not None, "Focus should be created"


@then("the Focus review_data should contain the proposed correction")
def check_focus_has_correction(db_path, test_context):
    """Verify Focus has the proposed correction."""
    focus_id = test_context.get("focus_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    data = json.loads(row["data_json"])
    review = data.get("review_data", {})
    assert "proposed" in review or "correction" in str(review), "review_data should contain correction"


# =============================================================================
# Approval Flow Steps (Phase 4)
# =============================================================================


@given("a Focus for doc change with target file and proposed content")
def create_focus_with_doc_change(db_path, workspace_path, test_context):
    """Create a Focus for doc change."""
    # Create target file
    target = Path(workspace_path) / "CLAUDE.md"
    target.write_text("# Original Content\n\nOld text.\n")
    test_context["target_file"] = str(target)

    # Create Focus with review_data
    result = create_focus(
        db_path,
        title="Review doc change",
        data={
            "review_data": {
                "target_file": str(target),
                "original": "Old text.",
                "proposed": "New text.",
            }
        },
    )
    test_context["focus_id"] = result.get("id")


@given("a Focus for doc change")
def create_basic_focus_for_doc_change(db_path, test_context):
    """Create a basic Focus for doc change."""
    result = create_focus(
        db_path,
        title="Review doc change",
        data={
            "review_data": {
                "proposed": "Some proposed change",
            }
        },
    )
    test_context["focus_id"] = result.get("id")


@when("I approve the Focus")
def approve_focus(db_path, test_context):
    """Approve the Focus and apply the change."""
    from chora_cvm.std import approve_doc_change

    focus_id = test_context.get("focus_id")
    result = approve_doc_change(db_path, focus_id)
    test_context["approve_result"] = result


@when(parsers.parse('I reject the Focus with reason "{reason}"'))
def reject_focus(db_path, test_context, reason: str):
    """Reject the Focus with a reason."""
    from chora_cvm.std import reject_doc_change

    focus_id = test_context.get("focus_id")
    result = reject_doc_change(db_path, focus_id, reason=reason)
    test_context["reject_result"] = result
    test_context["learning_id"] = result.get("learning_id")


@when(parsers.parse('I reject the Focus with reason "{reason}" and suggest "{suggestion}"'))
def reject_focus_with_suggestion(db_path, test_context, reason: str, suggestion: str):
    """Reject the Focus with a reason and suggestion."""
    from chora_cvm.std import reject_doc_change

    focus_id = test_context.get("focus_id")
    result = reject_doc_change(db_path, focus_id, reason=reason, suggestion=suggestion)
    test_context["reject_result"] = result
    test_context["learning_id"] = result.get("learning_id")


@then("the change should be applied to the target file")
def check_change_applied(test_context):
    """Verify the change was applied."""
    target = test_context.get("target_file")
    content = Path(target).read_text()
    assert "New text." in content, "Proposed change should be applied"


@then(parsers.parse('the Focus should be resolved with outcome "{outcome}"'))
def check_focus_resolved_outcome(db_path, test_context, outcome: str):
    """Verify Focus was resolved with expected outcome."""
    focus_id = test_context.get("focus_id")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (focus_id,)
    )
    row = cur.fetchone()
    conn.close()

    data = json.loads(row["data_json"])
    assert data.get("status") == "resolved", "Focus should be resolved"
    assert data.get("outcome") == outcome, f"Focus outcome should be '{outcome}'"


@then("a backup of the original file should exist")
def check_original_backup(test_context):
    """Verify backup of original file exists."""
    target = test_context.get("target_file")
    backup = target + ".bak"
    assert Path(backup).exists(), "Backup should exist"


@then("a learning should be created with the rejection reason")
def check_learning_with_reason(db_path, test_context):
    """Verify learning was created with rejection reason."""
    learning_id = test_context.get("learning_id")
    assert learning_id is not None, "Learning should be created"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (learning_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    assert "reason" in str(data) or "rejection" in str(data).lower(), "Learning should contain rejection reason"


@then("the learning should be bonded to the Focus")
def check_learning_bonded_to_focus(db_path, test_context):
    """Verify learning is bonded to the Focus."""
    learning_id = test_context.get("learning_id")
    focus_id = test_context.get("focus_id")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Check learning references focus in its data
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (learning_id,)
    )
    row = cur.fetchone()
    conn.close()

    if row:
        data = json.loads(row["data_json"])
        # Learning should have focus_id in its data
        assert data.get("focus_id") == focus_id, f"Learning should reference focus_id {focus_id}"
    else:
        assert False, f"Learning {learning_id} not found"


@then("a learning should be created capturing both reason and suggestion")
def check_learning_with_suggestion(db_path, test_context):
    """Verify learning captures both reason and suggestion."""
    learning_id = test_context.get("learning_id")
    assert learning_id is not None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (learning_id,)
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    data = json.loads(row["data_json"])
    data_str = str(data)
    assert "suggestion" in data_str.lower() or "pattern" in data_str.lower(), \
        "Learning should contain suggestion"
