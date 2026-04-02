"""Tests for ValidationRunner enforcement modes."""
from contracts.runner import apply_mode


SAMPLE_RESULTS = [
    {"check_id": "col.required", "status": "FAIL", "severity": "CRITICAL"},
    {"check_id": "col.type", "status": "FAIL", "severity": "HIGH"},
    {"check_id": "col.drift", "status": "FAIL", "severity": "MEDIUM"},
    {"check_id": "col.ok", "status": "PASS", "severity": "LOW"},
    {"check_id": "col.warn", "status": "WARN", "severity": "MEDIUM"},
]


class TestApplyMode:
    def test_enforce_keeps_all_fails(self):
        results = apply_mode(SAMPLE_RESULTS, "ENFORCE")
        fails = [r for r in results if r["status"] == "FAIL"]
        assert len(fails) == 3

    def test_audit_downgrades_all_fails_to_warn(self):
        results = apply_mode(SAMPLE_RESULTS, "AUDIT")
        fails = [r for r in results if r["status"] == "FAIL"]
        warns = [r for r in results if r["status"] == "WARN"]
        assert len(fails) == 0
        # 3 downgraded + 1 original WARN = 4
        assert len(warns) == 4

    def test_warn_mode_keeps_critical_fails(self):
        results = apply_mode(SAMPLE_RESULTS, "WARN")
        fails = [r for r in results if r["status"] == "FAIL"]
        assert len(fails) == 1
        assert fails[0]["severity"] == "CRITICAL"

    def test_warn_mode_downgrades_non_critical(self):
        results = apply_mode(SAMPLE_RESULTS, "WARN")
        warns = [r for r in results if r["status"] == "WARN"]
        # 2 downgraded HIGH+MEDIUM + 1 original WARN = 3
        assert len(warns) == 3

    def test_pass_results_unchanged(self):
        for mode in ["AUDIT", "WARN", "ENFORCE"]:
            results = apply_mode(SAMPLE_RESULTS, mode)
            passes = [r for r in results if r["status"] == "PASS"]
            assert len(passes) == 1

    def test_mode_note_added_on_downgrade(self):
        results = apply_mode(SAMPLE_RESULTS, "AUDIT")
        downgraded = [r for r in results if "mode_note" in r]
        assert len(downgraded) == 3
