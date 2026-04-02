"""ReportGenerator — Auto-generates the Enforcer Report from validation data.

Aggregates validation reports, violation log, schema evolution, and AI
extension results into a single machine-readable report with a data
health score (0-100) and plain-language summaries.

Usage:
    python contracts/report_generator.py \
        --validation-reports validation_reports/ \
        --violations violation_log/violations.jsonl \
        --evolution schema_snapshots/week5-event-records/evolution_report.json \
        --ai-extensions validation_reports/ai_extensions.json \
        --output enforcer_report/report_data.json
"""
import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_validation_reports(directory):
    """Load all JSON validation reports from a directory."""
    reports = []
    report_dir = Path(directory)
    if not report_dir.exists():
        return reports
    for f in sorted(report_dir.glob("*.json")):
        if f.name == "ai_extensions.json":
            continue
        try:
            with open(f) as fh:
                data = json.load(fh)
                if "results" in data and "total_checks" in data:
                    data["_source_file"] = f.name
                    reports.append(data)
        except (json.JSONDecodeError, KeyError):
            continue
    return reports


def load_violations(path):
    """Load violation log JSONL."""
    violations = []
    p = Path(path)
    if not p.exists():
        return violations
    with open(p) as f:
        for line in f:
            if line.strip():
                violations.append(json.loads(line))
    return violations


def load_json_file(path):
    """Load a single JSON file, returning None if missing."""
    p = Path(path)
    if not p.exists():
        return None
    with open(p) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Health score computation
# ---------------------------------------------------------------------------

def compute_health_score(reports, violations, evolution, ai_results):
    """Compute a 0-100 data health score.

    Scoring breakdown:
    - Validation pass rate: 0-40 points
    - Violation severity: 0-20 points (deductions for critical/high)
    - Schema stability: 0-20 points
    - AI extension results: 0-20 points
    """
    score = 0.0

    # Validation pass rate (40 points)
    total_checks = sum(r.get("total_checks", 0) for r in reports)
    total_passed = sum(r.get("passed", 0) for r in reports)
    if total_checks > 0:
        pass_rate = total_passed / total_checks
        score += pass_rate * 40
    else:
        score += 40  # No checks = no failures

    # Violation severity (20 points, deductions)
    violation_score = 20.0
    for v in violations:
        sev = v.get("severity", "")
        if sev == "CRITICAL":
            violation_score -= 1.0
        elif sev == "HIGH":
            violation_score -= 0.5
        elif sev == "MEDIUM":
            violation_score -= 0.25
    score += max(violation_score, 0)

    # Schema stability (20 points)
    if evolution:
        if evolution.get("classification") == "breaking":
            score += 5
        elif evolution.get("classification") == "none":
            score += 20
        else:
            score += 15
    else:
        score += 20  # No evolution data = stable

    # AI extensions (20 points)
    if ai_results and "results" in ai_results:
        ai_checks = ai_results["results"]
        ai_total = len(ai_checks)
        ai_passed = sum(1 for r in ai_checks if r.get("status") in ("PASS", "BASELINE_SET"))
        if ai_total > 0:
            score += (ai_passed / ai_total) * 20
        else:
            score += 20
    else:
        score += 10  # Partial credit if no AI data

    return round(min(max(score, 0), 100), 1)


# ---------------------------------------------------------------------------
# Plain-language summaries
# ---------------------------------------------------------------------------

def top_violations(violations, n=3):
    """Extract the top N violations by severity for plain-language reporting."""
    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    sorted_v = sorted(violations, key=lambda v: severity_order.get(v.get("severity", "LOW"), 4))

    summaries = []
    for v in sorted_v[:n]:
        summaries.append({
            "check_id": v.get("check_id"),
            "severity": v.get("severity"),
            "message": v.get("message"),
            "records_affected": v.get("records_failing", 0),
            "plain_language": _violation_to_plain(v),
        })
    return summaries


def _violation_to_plain(violation):
    """Convert a violation to a plain-language description a non-engineer can read."""
    col = violation.get("column_name", "unknown field")
    check = violation.get("check_type", "")
    failing = violation.get("records_failing", 0)
    severity = violation.get("severity", "unknown")

    col_readable = col.replace("payload_", "").replace("metadata_", "").replace("_", " ")

    if check == "uuid_format":
        return (
            f"The '{col_readable}' field contains {failing} values that don't follow "
            f"the expected identifier format. Systems that rely on this format for "
            f"lookups or joins will fail silently."
        )
    elif check == "enum":
        return (
            f"The '{col_readable}' field has {failing} records with unexpected values. "
            f"Any filtering or routing logic based on this field may miss these records."
        )
    elif "pattern" in check:
        return (
            f"The '{col_readable}' field has {failing} values that don't match the "
            f"expected pattern. Data integrity checks downstream may produce false results."
        )
    elif "drift" in check:
        return (
            f"The statistical distribution of '{col_readable}' has shifted from its "
            f"baseline. This may indicate a model change, data pipeline issue, or "
            f"intentional update that should be investigated."
        )
    elif check == "structural":
        return (
            f"A structural constraint failed on {failing} records. "
            f"This indicates a schema-level issue affecting data completeness."
        )
    else:
        return (
            f"{severity} issue found in '{col_readable}': {failing} records affected. "
            f"Review the violation details for corrective action."
        )


def summarize_schema_evolution(evolution):
    """Summarize schema evolution for the report."""
    if not evolution:
        return {"status": "No evolution data available."}

    return {
        "classification": evolution.get("classification", "unknown"),
        "verdict": evolution.get("verdict", "UNKNOWN"),
        "total_changes": evolution.get("total_changes", 0),
        "breaking_changes": evolution.get("breaking_changes", 0),
        "summary": evolution.get("summary", ""),
        "rollback_needed": evolution.get("rollback_plan", {}).get("needed", False),
    }


def summarize_ai_extensions(ai_results):
    """Summarize AI extension results for the report."""
    if not ai_results or "results" not in ai_results:
        return {"status": "AI extensions not run."}

    summaries = []
    for r in ai_results["results"]:
        summaries.append({
            "check": r.get("check_id"),
            "status": r.get("status"),
            "message": r.get("message"),
            "drift_score": r.get("drift_score"),
            "violation_rate": r.get("violation_rate"),
        })
    return {"extensions": summaries}


def generate_recommendations(health_score, violations, evolution, ai_results):
    """Generate actionable recommendations based on all findings."""
    recs = []

    if health_score < 50:
        recs.append(
            "Data health is below 50%. Prioritize fixing CRITICAL violations before "
            "any new feature work."
        )

    # Check for critical violations
    critical_count = sum(1 for v in violations if v.get("severity") == "CRITICAL")
    if critical_count > 0:
        recs.append(
            f"{critical_count} CRITICAL violations found. Review identifier formats "
            f"and enum values across event types to prevent silent downstream failures."
        )

    # Schema evolution
    if evolution and evolution.get("classification") == "breaking":
        recs.append(
            "Breaking schema changes detected. Coordinate with downstream consumers "
            "before deploying. See rollback plan in evolution report."
        )

    # AI extensions
    if ai_results and "results" in ai_results:
        for r in ai_results["results"]:
            if r.get("status") == "FAIL":
                recs.append(
                    f"AI extension '{r.get('check_id')}' failed: {r.get('message', '')}. "
                    f"Investigate the root cause."
                )
            elif r.get("status") == "WARN":
                recs.append(
                    f"AI extension '{r.get('check_id')}' warning: {r.get('message', '')}."
                )

    if not recs:
        recs.append("All checks passing. Continue monitoring for drift.")

    return recs


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_report(reports, violations, evolution, ai_results):
    """Build the full enforcer report."""
    health_score = compute_health_score(reports, violations, evolution, ai_results)
    top_v = top_violations(violations)
    schema_summary = summarize_schema_evolution(evolution)
    ai_summary = summarize_ai_extensions(ai_results)
    recommendations = generate_recommendations(health_score, violations, evolution, ai_results)

    # Aggregate validation stats
    total_checks = sum(r.get("total_checks", 0) for r in reports)
    total_passed = sum(r.get("passed", 0) for r in reports)
    total_failed = sum(r.get("failed", 0) for r in reports)
    total_warned = sum(r.get("warned", 0) for r in reports)

    return {
        "report_id": str(uuid.uuid4()),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_health_score": health_score,
        "validation_summary": {
            "reports_analyzed": len(reports),
            "total_checks": total_checks,
            "passed": total_passed,
            "failed": total_failed,
            "warned": total_warned,
            "pass_rate": round(total_passed / max(total_checks, 1), 4),
        },
        "top_violations": top_v,
        "total_violations": len(violations),
        "schema_evolution": schema_summary,
        "ai_extensions": ai_summary,
        "recommendations": recommendations,
        "contracts_evaluated": [r.get("contract_id", "unknown") for r in reports],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate the Enforcer Report from validation data"
    )
    parser.add_argument(
        "--validation-reports", default="validation_reports/",
        help="Directory containing validation report JSONs"
    )
    parser.add_argument(
        "--violations", default="violation_log/violations.jsonl",
        help="Path to violation log JSONL"
    )
    parser.add_argument(
        "--evolution", default=None,
        help="Path to schema evolution report JSON"
    )
    parser.add_argument(
        "--ai-extensions", default=None,
        help="Path to AI extensions report JSON"
    )
    parser.add_argument(
        "--output", required=True,
        help="Output path for enforcer report JSON"
    )
    args = parser.parse_args()

    print("Loading validation data...")
    reports = load_validation_reports(args.validation_reports)
    print(f"  {len(reports)} validation reports loaded")

    violations = load_violations(args.violations)
    print(f"  {len(violations)} violations loaded")

    evolution = load_json_file(args.evolution) if args.evolution else None
    if evolution:
        print(f"  Schema evolution report loaded")

    ai_results = load_json_file(args.ai_extensions) if args.ai_extensions else None
    if ai_results:
        print(f"  AI extensions report loaded ({len(ai_results.get('results', []))} checks)")

    print("\nGenerating enforcer report...")
    report = build_report(reports, violations, evolution, ai_results)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nEnforcer Report written to {output_path}")
    print(f"  Data Health Score: {report['data_health_score']}/100")
    print(f"  Validation: {report['validation_summary']['passed']}/{report['validation_summary']['total_checks']} checks passed")
    print(f"  Top violations: {len(report['top_violations'])}")
    print(f"  Recommendations: {len(report['recommendations'])}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
