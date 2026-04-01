"""Migrate LangGraph Automation Auditor evidence to canonical verdict_record JSONL.

Source: ~/tenx/week2/LangGraph-Automation-Auditor/output/evidence*.json
Target: outputs/week2/verdicts.jsonl
"""
import json
import uuid
import hashlib
import re
from pathlib import Path
from datetime import datetime, timezone

WEEK2_ROOT = Path.home() / "tenx" / "week2" / "LangGraph-Automation-Auditor" / "output"
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "week2" / "verdicts.jsonl"


def score_to_verdict(overall_score):
    """Convert numeric score (1-5) to PASS/FAIL/WARN."""
    if overall_score >= 3.5:
        return "PASS"
    elif overall_score >= 2.5:
        return "WARN"
    else:
        return "FAIL"


def extract_rubric_id(evidence_data):
    """Generate a rubric_id from the criteria structure."""
    criteria = evidence_data.get("final_report", {}).get("criteria", [])
    criterion_ids = sorted([c.get("dimension_id", "") for c in criteria])
    rubric_str = json.dumps(criterion_ids, sort_keys=True)
    return hashlib.sha256(rubric_str.encode()).hexdigest()


def extract_timestamp(thread_id):
    """Extract timestamp from thread_id like 'audit-20260228-143022-xxxx'."""
    match = re.search(r"(\d{8})-(\d{6})", thread_id)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        try:
            dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).isoformat()


def build_verdict(evidence_data):
    """Build a canonical verdict_record from auditor evidence."""
    thread_id = evidence_data.get("thread_id", str(uuid.uuid4()))
    final_report = evidence_data.get("final_report", {})
    criteria = final_report.get("criteria", [])
    overall_score = final_report.get("overall_score", 0)
    target_ref = final_report.get("repo_url", "unknown")

    # Build scores dict from criteria
    scores = {}
    for criterion in criteria:
        dim_id = criterion.get("dimension_id", criterion.get("dimension_name", "unknown"))
        score_val = criterion.get("final_score", 3)
        evidence_list = []
        for judge in criterion.get("judge_opinions", []):
            if judge.get("argument"):
                evidence_list.append(judge["argument"][:200])

        scores[dim_id] = {
            "score": max(1, min(5, int(score_val))),
            "evidence": evidence_list[:3],
            "notes": criterion.get("remediation", ""),
        }

    # Compute weighted mean for overall_score
    if scores:
        computed_overall = sum(s["score"] for s in scores.values()) / len(scores)
    else:
        computed_overall = overall_score

    # Compute overall confidence from evidence confidence scores
    all_confidences = []
    for cat in evidence_data.get("evidences", {}).values():
        if isinstance(cat, list):
            for ev in cat:
                if isinstance(ev, dict) and "confidence" in ev:
                    all_confidences.append(ev["confidence"])
    avg_confidence = sum(all_confidences) / len(all_confidences) if all_confidences else 0.85

    rubric_id = extract_rubric_id(evidence_data)
    evaluated_at = extract_timestamp(thread_id)

    return {
        "verdict_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"auditor:{thread_id}")),
        "target_ref": target_ref,
        "rubric_id": rubric_id,
        "rubric_version": "1.0.0",
        "scores": scores,
        "overall_verdict": score_to_verdict(computed_overall),
        "overall_score": round(computed_overall, 2),
        "confidence": round(min(1.0, max(0.0, avg_confidence)), 4),
        "evaluated_at": evaluated_at,
    }


def main():
    print(f"Scanning evidence files in {WEEK2_ROOT}...")

    evidence_files = sorted(WEEK2_ROOT.glob("evidence*.json"))
    print(f"  Found {len(evidence_files)} evidence files")

    records = []
    seen_thread_ids = set()

    for path in evidence_files:
        with open(path) as f:
            data = json.load(f)

        thread_id = data.get("thread_id", "")
        if thread_id in seen_thread_ids:
            continue
        seen_thread_ids.add(thread_id)

        if not data.get("final_report"):
            print(f"  Skipping {path.name} (no final_report)")
            continue

        verdict = build_verdict(data)
        records.append(verdict)
        print(f"  {path.name}: {verdict['overall_verdict']} (score={verdict['overall_score']})")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as out:
        for rec in records:
            out.write(json.dumps(rec) + "\n")

    print(f"\nWrote {len(records)} verdict records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
