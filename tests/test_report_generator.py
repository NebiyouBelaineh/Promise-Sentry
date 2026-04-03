"""Tests for ReportGenerator."""
import json
import pytest

from contracts.report_generator import (
    compute_health_score,
    top_violations,
    _violation_to_plain,
    summarize_schema_evolution,
    summarize_ai_extensions,
    generate_recommendations,
    build_report,
    load_validation_reports,
    load_violations,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_REPORTS = [
    {
        "report_id": "r1",
        "contract_id": "week5-events",
        "total_checks": 100,
        "passed": 90,
        "failed": 10,
        "warned": 0,
        "errored": 0,
        "results": [],
    },
    {
        "report_id": "r2",
        "contract_id": "week3-extractions",
        "total_checks": 50,
        "passed": 50,
        "failed": 0,
        "warned": 0,
        "errored": 0,
        "results": [],
    },
]

SAMPLE_VIOLATIONS = [
    {
        "violation_id": "v1",
        "check_id": "payload_app_id.uuid",
        "column_name": "payload_application_id",
        "check_type": "uuid_format",
        "severity": "CRITICAL",
        "message": "43 values not matching UUID pattern.",
        "records_failing": 43,
        "blame_chain": [],
        "blast_radius": {"downstream_nodes": 5},
    },
    {
        "violation_id": "v2",
        "check_id": "payload_hash.pattern",
        "column_name": "payload_file_hash",
        "check_type": "pattern",
        "severity": "HIGH",
        "message": "184 values not matching SHA-256 pattern.",
        "records_failing": 184,
        "blame_chain": [],
        "blast_radius": {"downstream_nodes": 2},
    },
    {
        "violation_id": "v3",
        "check_id": "payload_bool.enum",
        "column_name": "payload_is_coherent",
        "check_type": "enum",
        "severity": "CRITICAL",
        "message": "18 unexpected values.",
        "records_failing": 18,
        "blame_chain": [],
        "blast_radius": {"downstream_nodes": 0},
    },
]

SAMPLE_EVOLUTION = {
    "classification": "breaking",
    "verdict": "BREAKING",
    "total_changes": 6,
    "breaking_changes": 1,
    "summary": "1 breaking change detected.",
    "before_snapshot": "schema_snapshots/week5/20260401.yaml",
    "changes": [
        {"column": "metadata_user_id", "change_type": "enum_values_removed",
         "compatibility": "breaking", "detail": "Enum values removed: ['applicant']"},
    ],
    "rollback_plan": {"needed": True, "steps": ["Revert enum"]},
}

SAMPLE_AI_RESULTS = {
    "results": [
        {"check_id": "ai.embedding_drift", "status": "PASS", "message": "Stable", "drift_score": 0.01},
        {"check_id": "ai.prompt_input_schema", "status": "PASS", "message": "0 violations"},
        {"check_id": "ai.output_schema_violation_rate", "status": "PASS", "message": "0%", "violation_rate": 0.0},
    ]
}


# ---------------------------------------------------------------------------
# Health score tests
# ---------------------------------------------------------------------------

class TestComputeHealthScore:
    def test_perfect_score(self):
        reports = [{"total_checks": 100, "passed": 100}]
        score = compute_health_score(reports, [], None, SAMPLE_AI_RESULTS)
        assert score == 100.0

    def test_pass_rate_formula(self):
        # (140/150)*100 = 93.3, no critical violations
        score = compute_health_score(SAMPLE_REPORTS, [], None, None)
        assert round(score, 1) == 93.3

    def test_critical_violations_deduct_20_each(self):
        reports = [{"total_checks": 100, "passed": 100}]
        # 100% pass rate = 100, minus 2 critical * 20 = 60
        violations = [{"severity": "CRITICAL"}, {"severity": "CRITICAL"}]
        score = compute_health_score(reports, violations, None, None)
        assert score == 60.0

    def test_non_critical_violations_no_deduction(self):
        reports = [{"total_checks": 100, "passed": 100}]
        violations = [{"severity": "HIGH"}, {"severity": "MEDIUM"}]
        score = compute_health_score(reports, violations, None, None)
        assert score == 100.0

    def test_score_clamped_0_to_100(self):
        many_violations = [{"severity": "CRITICAL"}] * 100
        score = compute_health_score([], many_violations, None, None)
        assert score == 0

    def test_empty_inputs(self):
        score = compute_health_score([], [], None, None)
        assert score == 100.0


# ---------------------------------------------------------------------------
# Top violations tests
# ---------------------------------------------------------------------------

class TestTopViolations:
    def test_returns_top_n(self):
        result = top_violations(SAMPLE_VIOLATIONS, n=2)
        assert len(result) == 2

    def test_sorted_by_severity(self):
        result = top_violations(SAMPLE_VIOLATIONS, n=3)
        assert result[0]["severity"] == "CRITICAL"
        assert result[1]["severity"] == "CRITICAL"

    def test_includes_plain_language(self):
        result = top_violations(SAMPLE_VIOLATIONS, n=1)
        assert "plain_language" in result[0]
        assert len(result[0]["plain_language"]) > 20

    def test_empty_violations(self):
        result = top_violations([], n=3)
        assert result == []


# ---------------------------------------------------------------------------
# Plain language tests
# ---------------------------------------------------------------------------

class TestViolationToPlain:
    def test_uuid_format(self):
        v = {"column_name": "payload_app_id", "check_type": "uuid_format",
             "records_failing": 10, "severity": "CRITICAL"}
        text = _violation_to_plain(v)
        assert "identifier format" in text
        assert "10" in text

    def test_enum(self):
        v = {"column_name": "payload_status", "check_type": "enum",
             "records_failing": 5, "severity": "HIGH"}
        text = _violation_to_plain(v)
        assert "unexpected values" in text

    def test_pattern(self):
        v = {"column_name": "payload_hash", "check_type": "pattern_match",
             "records_failing": 20, "severity": "HIGH"}
        text = _violation_to_plain(v)
        assert "pattern" in text

    def test_drift(self):
        v = {"column_name": "confidence", "check_type": "statistical_drift",
             "records_failing": 100, "severity": "MEDIUM"}
        text = _violation_to_plain(v)
        assert "distribution" in text or "shifted" in text


# ---------------------------------------------------------------------------
# Summary tests
# ---------------------------------------------------------------------------

class TestSummarizeSchemaEvolution:
    def test_with_evolution_data(self):
        result = summarize_schema_evolution(SAMPLE_EVOLUTION)
        assert result["classification"] == "breaking"
        assert result["rollback_needed"] is True

    def test_without_evolution_data(self):
        result = summarize_schema_evolution(None)
        assert "status" in result


class TestSummarizeAiExtensions:
    def test_with_results(self):
        result = summarize_ai_extensions(SAMPLE_AI_RESULTS)
        assert len(result["extensions"]) == 3

    def test_without_results(self):
        result = summarize_ai_extensions(None)
        assert "status" in result


# ---------------------------------------------------------------------------
# Recommendations tests
# ---------------------------------------------------------------------------

class TestGenerateRecommendations:
    def test_low_score_recommendation(self):
        recs = generate_recommendations(30, [], None, None)
        assert any("below 50" in r for r in recs)

    def test_critical_violations_produce_specific_recs(self):
        recs = generate_recommendations(80, SAMPLE_VIOLATIONS, None, None)
        # Should reference specific file paths and contract clauses
        assert any("clause" in r.lower() or "contract" in r.lower() for r in recs)

    def test_breaking_evolution_recommendation(self):
        recs = generate_recommendations(80, [], SAMPLE_EVOLUTION, None)
        assert any("Breaking" in r or "breaking" in r for r in recs)

    def test_all_clear(self):
        recs = generate_recommendations(95, [], None, SAMPLE_AI_RESULTS)
        assert any("passing" in r.lower() for r in recs)

    def test_ai_failure_recommendation(self):
        ai = {"results": [{"check_id": "ai.drift", "status": "FAIL", "message": "Big drift"}]}
        recs = generate_recommendations(80, [], None, ai)
        assert any("drift" in r.lower() for r in recs)

    def test_uuid_violation_names_file_and_clause(self):
        violations = [{
            "check_id": "payload_app_id.uuid",
            "column_name": "payload_application_id",
            "check_type": "uuid_format",
            "severity": "CRITICAL",
            "contract_id": "week5-event-records",
            "source_files": ["ledger/event_store.py"],
        }]
        recs = generate_recommendations(80, violations, None, None)
        assert any("event_store.py" in r and "payload_app_id.uuid" in r for r in recs)


# ---------------------------------------------------------------------------
# Build report tests
# ---------------------------------------------------------------------------

class TestBuildReport:
    def test_report_has_required_fields(self):
        report = build_report(SAMPLE_REPORTS, SAMPLE_VIOLATIONS, SAMPLE_EVOLUTION, SAMPLE_AI_RESULTS)
        assert "report_id" in report
        assert "generated_at" in report
        assert "data_health_score" in report
        assert "validation_summary" in report
        assert "top_violations" in report
        assert "schema_evolution" in report
        assert "ai_extensions" in report
        assert "recommendations" in report

    def test_health_score_is_number(self):
        report = build_report(SAMPLE_REPORTS, SAMPLE_VIOLATIONS, None, None)
        assert isinstance(report["data_health_score"], (int, float))
        assert 0 <= report["data_health_score"] <= 100

    def test_validation_summary_aggregates(self):
        report = build_report(SAMPLE_REPORTS, [], None, None)
        assert report["validation_summary"]["total_checks"] == 150
        assert report["validation_summary"]["passed"] == 140

    def test_contracts_evaluated_listed(self):
        report = build_report(SAMPLE_REPORTS, [], None, None)
        assert "week5-events" in report["contracts_evaluated"]
        assert "week3-extractions" in report["contracts_evaluated"]


# ---------------------------------------------------------------------------
# File loading tests
# ---------------------------------------------------------------------------

class TestLoadValidationReports:
    def test_loads_from_directory(self, tmp_path):
        report = {"report_id": "x", "total_checks": 10, "passed": 10, "results": []}
        (tmp_path / "test.json").write_text(json.dumps(report))
        reports = load_validation_reports(str(tmp_path))
        assert len(reports) == 1

    def test_skips_invalid_json(self, tmp_path):
        (tmp_path / "bad.json").write_text("not json")
        reports = load_validation_reports(str(tmp_path))
        assert len(reports) == 0

    def test_skips_ai_extensions(self, tmp_path):
        report = {"report_id": "x", "total_checks": 10, "passed": 10, "results": []}
        (tmp_path / "ai_extensions.json").write_text(json.dumps(report))
        reports = load_validation_reports(str(tmp_path))
        assert len(reports) == 0

    def test_missing_directory(self):
        reports = load_validation_reports("/nonexistent/path")
        assert reports == []


class TestLoadViolations:
    def test_loads_jsonl(self, tmp_path):
        p = tmp_path / "violations.jsonl"
        p.write_text('{"id": "v1"}\n{"id": "v2"}\n')
        violations = load_violations(str(p))
        assert len(violations) == 2

    def test_missing_file(self):
        violations = load_violations("/nonexistent/path.jsonl")
        assert violations == []
