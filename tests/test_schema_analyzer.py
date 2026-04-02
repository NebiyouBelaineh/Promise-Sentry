"""Tests for SchemaEvolutionAnalyzer."""
import json
import pytest

from contracts.schema_analyzer import (
    diff_columns,
    diff_single_column,
    diff_constraints,
    classify_evolution,
    generate_rollback_plan,
    build_report,
    find_snapshot_pair,
    load_snapshot,
    _summarize_clause,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BEFORE_SCHEMA = {
    "user_id": {"type": "string", "required": True, "format": "uuid"},
    "score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
    "status": {"type": "string", "enum": ["active", "inactive"]},
    "old_field": {"type": "string", "required": False},
}

AFTER_SCHEMA = {
    "user_id": {"type": "string", "required": True, "format": "uuid"},
    "score": {"type": "integer", "minimum": 0, "maximum": 100},
    "status": {"type": "string", "enum": ["active", "inactive", "suspended"]},
    "new_field": {"type": "string", "required": False},
}


# ---------------------------------------------------------------------------
# Column diff tests
# ---------------------------------------------------------------------------

class TestDiffColumns:
    def test_detects_added_column(self):
        changes = diff_columns(BEFORE_SCHEMA, AFTER_SCHEMA)
        added = [c for c in changes if c["change_type"] == "column_added"]
        assert len(added) == 1
        assert added[0]["column"] == "new_field"
        assert added[0]["compatibility"] == "backward"

    def test_detects_removed_column(self):
        changes = diff_columns(BEFORE_SCHEMA, AFTER_SCHEMA)
        removed = [c for c in changes if c["change_type"] == "column_removed"]
        assert len(removed) == 1
        assert removed[0]["column"] == "old_field"
        assert removed[0]["compatibility"] == "breaking"

    def test_detects_type_change(self):
        changes = diff_columns(BEFORE_SCHEMA, AFTER_SCHEMA)
        type_changes = [c for c in changes if c["change_type"] == "type_changed"]
        assert len(type_changes) == 1
        assert type_changes[0]["column"] == "score"
        assert type_changes[0]["compatibility"] == "breaking"

    def test_detects_enum_addition(self):
        changes = diff_columns(BEFORE_SCHEMA, AFTER_SCHEMA)
        enum_added = [c for c in changes if c["change_type"] == "enum_values_added"]
        assert len(enum_added) == 1
        assert enum_added[0]["compatibility"] == "backward"

    def test_identical_schemas_produce_no_changes(self):
        changes = diff_columns(BEFORE_SCHEMA, BEFORE_SCHEMA)
        assert changes == []

    def test_empty_schemas(self):
        changes = diff_columns({}, {})
        assert changes == []


class TestDiffSingleColumn:
    def test_type_change_breaking(self):
        before = {"type": "string"}
        after = {"type": "number"}
        changes = diff_single_column("col", before, after)
        assert any(c["compatibility"] == "breaking" for c in changes)

    def test_required_optional_to_required_is_breaking(self):
        before = {"type": "string", "required": False}
        after = {"type": "string", "required": True}
        changes = diff_single_column("col", before, after)
        assert any(
            c["change_type"] == "required_changed" and c["compatibility"] == "breaking"
            for c in changes
        )

    def test_required_to_optional_is_backward(self):
        before = {"type": "string", "required": True}
        after = {"type": "string", "required": False}
        changes = diff_single_column("col", before, after)
        assert any(
            c["change_type"] == "required_changed" and c["compatibility"] == "backward"
            for c in changes
        )

    def test_enum_removal_is_breaking(self):
        before = {"type": "string", "enum": ["a", "b", "c"]}
        after = {"type": "string", "enum": ["a", "b"]}
        changes = diff_single_column("col", before, after)
        assert any(
            c["change_type"] == "enum_values_removed" and c["compatibility"] == "breaking"
            for c in changes
        )

    def test_minimum_increase_is_breaking(self):
        before = {"type": "number", "minimum": 0.0}
        after = {"type": "number", "minimum": 10.0}
        changes = diff_single_column("col", before, after)
        assert any(c["compatibility"] == "breaking" for c in changes)

    def test_maximum_decrease_is_breaking(self):
        before = {"type": "number", "maximum": 100.0}
        after = {"type": "number", "maximum": 50.0}
        changes = diff_single_column("col", before, after)
        assert any(c["compatibility"] == "breaking" for c in changes)

    def test_pattern_change_is_breaking(self):
        before = {"type": "string", "pattern": "^[a-z]+$"}
        after = {"type": "string", "pattern": "^[A-Z]+$"}
        changes = diff_single_column("col", before, after)
        assert any(c["change_type"] == "pattern_changed" for c in changes)

    def test_no_change_produces_empty(self):
        clause = {"type": "string", "required": True}
        changes = diff_single_column("col", clause, clause)
        assert changes == []


# ---------------------------------------------------------------------------
# Constraint diff tests
# ---------------------------------------------------------------------------

class TestDiffConstraints:
    def test_detects_added_constraint(self):
        before = []
        after = [{"name": "temporal_order", "type": "structural"}]
        changes = diff_constraints(before, after)
        assert len(changes) == 1
        assert changes[0]["change_type"] == "constraint_added"
        assert changes[0]["compatibility"] == "breaking"

    def test_detects_removed_constraint(self):
        before = [{"name": "temporal_order", "type": "structural"}]
        after = []
        changes = diff_constraints(before, after)
        assert len(changes) == 1
        assert changes[0]["change_type"] == "constraint_removed"
        assert changes[0]["compatibility"] == "backward"


# ---------------------------------------------------------------------------
# Classification tests
# ---------------------------------------------------------------------------

class TestClassifyEvolution:
    def test_no_changes(self):
        assert classify_evolution([]) == "none"

    def test_all_backward(self):
        changes = [{"compatibility": "backward"}, {"compatibility": "backward"}]
        assert classify_evolution(changes) == "backward"

    def test_any_breaking(self):
        changes = [{"compatibility": "backward"}, {"compatibility": "breaking"}]
        assert classify_evolution(changes) == "breaking"

    def test_all_forward(self):
        changes = [{"compatibility": "forward"}]
        assert classify_evolution(changes) == "forward"

    def test_mixed_backward_forward(self):
        changes = [{"compatibility": "backward"}, {"compatibility": "forward"}]
        assert classify_evolution(changes) == "full"


# ---------------------------------------------------------------------------
# Rollback plan tests
# ---------------------------------------------------------------------------

class TestGenerateRollbackPlan:
    def test_no_rollback_when_safe(self):
        changes = [{"compatibility": "backward", "change_type": "column_added", "column": "x", "detail": ""}]
        plan = generate_rollback_plan(changes, {"path": "old.yaml"})
        assert plan["needed"] is False

    def test_rollback_for_breaking(self):
        changes = [{
            "compatibility": "breaking",
            "change_type": "column_removed",
            "column": "important",
            "detail": "Column removed",
            "before": {"type": "string"},
            "after": None,
        }]
        plan = generate_rollback_plan(changes, {"path": "old.yaml"})
        assert plan["needed"] is True
        assert len(plan["steps"]) >= 1
        assert "old.yaml" in plan["steps"][-1]

    def test_type_revert_step(self):
        changes = [{
            "compatibility": "breaking",
            "change_type": "type_changed",
            "column": "score",
            "detail": "type changed",
            "before": "number",
            "after": "integer",
        }]
        plan = generate_rollback_plan(changes, {"path": "snap.yaml"})
        assert any("score" in s and "number" in s for s in plan["steps"])


# ---------------------------------------------------------------------------
# Report structure test
# ---------------------------------------------------------------------------

class TestBuildReport:
    def test_report_has_required_fields(self):
        before = {"path": "a.yaml", "id": "test", "version": "1.0", "schema": {}}
        after = {"path": "b.yaml", "id": "test", "version": "2.0", "schema": {}}
        report = build_report(before, after, [], "none", {"needed": False, "steps": []})
        assert "analyzed_at" in report
        assert "classification" in report
        assert "verdict" in report
        assert "changes" in report
        assert "rollback_plan" in report
        assert "summary" in report


# ---------------------------------------------------------------------------
# Snapshot loading tests
# ---------------------------------------------------------------------------

class TestFindSnapshotPair:
    def test_finds_two_most_recent(self, tmp_path):
        (tmp_path / "20260101_000000.yaml").write_text("schema: {}")
        (tmp_path / "20260102_000000.yaml").write_text("schema: {}")
        (tmp_path / "20260103_000000.yaml").write_text("schema: {}")
        before, after = find_snapshot_pair(str(tmp_path))
        assert "20260102" in str(before)
        assert "20260103" in str(after)

    def test_raises_on_single_snapshot(self, tmp_path):
        (tmp_path / "20260101_000000.yaml").write_text("schema: {}")
        with pytest.raises(ValueError):
            find_snapshot_pair(str(tmp_path))


class TestSummarizeClause:
    def test_includes_type(self):
        result = _summarize_clause({"type": "string", "required": True})
        assert result["type"] == "string"
        assert result["required"] is True

    def test_excludes_unset_fields(self):
        result = _summarize_clause({"type": "number"})
        assert "format" not in result
        assert "enum" not in result
