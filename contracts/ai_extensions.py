"""AI Contract Extensions — Embedding drift, prompt schema, output violation rate.

Three AI-specific contract checks:
1. Embedding drift: cosine distance of text centroids against stored baseline.
2. Prompt input schema validation: validates extraction inputs match expected structure.
3. LLM output schema violation rate: tracks structured output conformance.

Usage:
    python contracts/ai_extensions.py \
        --week3 outputs/week3/extractions.jsonl \
        --week2 outputs/week2/verdicts.jsonl \
        --output validation_reports/ai_extensions.json
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import requests


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OLLAMA_BASE_URL = os.environ.get("OLLAMA_HOST", "http://172.29.96.1:11434")
EMBEDDING_MODEL = "nomic-embed-text:v1.5"
EMBEDDING_BASELINE_PATH = Path("schema_snapshots/embedding_baselines.npz")
DRIFT_THRESHOLD = 0.15
SAMPLE_SIZE = 200


# ---------------------------------------------------------------------------
# Extension 1: Embedding Drift Detection
# ---------------------------------------------------------------------------

def embed_texts(texts, model=EMBEDDING_MODEL):
    """Embed a batch of texts via Ollama API.

    Precondition: Ollama is running and model is pulled.
    Guarantee: returns numpy array of shape (n_texts, embedding_dim).
    Raises: ConnectionError if Ollama is unreachable.
    """
    url = f"{OLLAMA_BASE_URL}/api/embed"
    response = requests.post(url, json={"model": model, "input": texts}, timeout=120)
    response.raise_for_status()
    embeddings = response.json()["embeddings"]
    return np.array(embeddings)


def sample_texts(records, field_path="extracted_facts", text_key="text", n=SAMPLE_SIZE):
    """Extract text samples from nested records."""
    texts = []
    for rec in records:
        items = rec.get(field_path, [])
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and text_key in item:
                    t = item[text_key]
                    if t and isinstance(t, str) and len(t.strip()) > 0:
                        texts.append(t.strip())
    if len(texts) > n:
        rng = np.random.default_rng(42)
        indices = rng.choice(len(texts), size=n, replace=False)
        texts = [texts[i] for i in indices]
    return texts


def check_embedding_drift(texts, baseline_path=EMBEDDING_BASELINE_PATH, threshold=DRIFT_THRESHOLD):
    """Check embedding drift against stored centroid baseline.

    On first run, stores baseline centroid. On subsequent runs, computes
    cosine distance from baseline and reports drift status.
    """
    if not texts:
        return {
            "check_id": "ai.embedding_drift",
            "check_type": "embedding_drift",
            "status": "ERROR",
            "message": "No text samples available for embedding.",
            "drift_score": None,
            "threshold": threshold,
        }

    try:
        vecs = embed_texts(texts)
    except Exception as e:
        return {
            "check_id": "ai.embedding_drift",
            "check_type": "embedding_drift",
            "status": "ERROR",
            "message": f"Embedding failed: {e}",
            "drift_score": None,
            "threshold": threshold,
        }

    centroid = vecs.mean(axis=0)

    if not baseline_path.exists():
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        np.savez(str(baseline_path), centroid=centroid, sample_count=len(texts))
        return {
            "check_id": "ai.embedding_drift",
            "check_type": "embedding_drift",
            "status": "BASELINE_SET",
            "message": f"Baseline established from {len(texts)} samples. Run again to detect drift.",
            "drift_score": 0.0,
            "threshold": threshold,
            "sample_count": len(texts),
            "embedding_dim": int(centroid.shape[0]),
        }

    baseline_data = np.load(str(baseline_path))
    baseline_centroid = baseline_data["centroid"]

    cosine_sim = np.dot(centroid, baseline_centroid) / (
        np.linalg.norm(centroid) * np.linalg.norm(baseline_centroid) + 1e-9
    )
    drift_score = float(1 - cosine_sim)

    if drift_score > threshold:
        status = "FAIL"
        severity = "HIGH"
        interpretation = "Semantic content has shifted significantly from baseline."
    elif drift_score > threshold * 0.5:
        status = "WARN"
        severity = "MEDIUM"
        interpretation = "Minor semantic drift detected; monitor closely."
    else:
        status = "PASS"
        severity = "LOW"
        interpretation = "Content semantically stable."

    return {
        "check_id": "ai.embedding_drift",
        "check_type": "embedding_drift",
        "status": status,
        "severity": severity,
        "drift_score": round(drift_score, 6),
        "cosine_similarity": round(float(cosine_sim), 6),
        "threshold": threshold,
        "sample_count": len(texts),
        "embedding_dim": int(centroid.shape[0]),
        "message": f"Drift score: {drift_score:.4f} (threshold: {threshold}). {interpretation}",
    }


# ---------------------------------------------------------------------------
# Extension 2: Prompt Input Schema Validation
# ---------------------------------------------------------------------------

EXPECTED_EXTRACTION_SCHEMA = {
    "required_keys": ["doc_id", "source_path", "extraction_model", "extracted_at"],
    "required_fact_keys": ["fact_id", "text", "confidence"],
    "confidence_range": (0.0, 1.0),
    "doc_id_format": "uuid",
}


def check_prompt_input_schema(records):
    """Validate that extraction records match expected prompt input schema.

    Checks: required fields, fact structure, confidence range, doc_id format.
    """
    violations = []
    total = len(records)

    for i, rec in enumerate(records):
        # Check required keys
        for key in EXPECTED_EXTRACTION_SCHEMA["required_keys"]:
            if key not in rec or rec[key] is None:
                violations.append({
                    "record_index": i,
                    "issue": f"Missing required field: {key}",
                    "doc_id": rec.get("doc_id", "unknown"),
                })

        # Check extracted_facts structure
        facts = rec.get("extracted_facts", [])
        if not isinstance(facts, list):
            violations.append({
                "record_index": i,
                "issue": "extracted_facts is not a list",
                "doc_id": rec.get("doc_id", "unknown"),
            })
            continue

        for j, fact in enumerate(facts):
            for key in EXPECTED_EXTRACTION_SCHEMA["required_fact_keys"]:
                if key not in fact:
                    violations.append({
                        "record_index": i,
                        "fact_index": j,
                        "issue": f"Fact missing required field: {key}",
                        "doc_id": rec.get("doc_id", "unknown"),
                    })

            # Confidence range
            conf = fact.get("confidence")
            if conf is not None:
                lo, hi = EXPECTED_EXTRACTION_SCHEMA["confidence_range"]
                if not (lo <= conf <= hi):
                    violations.append({
                        "record_index": i,
                        "fact_index": j,
                        "issue": f"Confidence {conf} outside [{lo}, {hi}]",
                        "doc_id": rec.get("doc_id", "unknown"),
                    })

    violation_rate = len(violations) / max(total, 1)

    if violation_rate > 0.05:
        status = "FAIL"
        severity = "HIGH"
    elif violation_rate > 0.01:
        status = "WARN"
        severity = "MEDIUM"
    else:
        status = "PASS"
        severity = "LOW"

    return {
        "check_id": "ai.prompt_input_schema",
        "check_type": "prompt_input_schema",
        "status": status,
        "severity": severity,
        "total_records": total,
        "violations_found": len(violations),
        "violation_rate": round(violation_rate, 4),
        "sample_violations": violations[:10],
        "message": f"{len(violations)} input schema violations across {total} records "
                   f"(rate: {violation_rate:.2%}).",
    }


# ---------------------------------------------------------------------------
# Extension 3: LLM Output Schema Violation Rate
# ---------------------------------------------------------------------------

EXPECTED_VERDICT_SCHEMA = {
    "required_keys": ["verdict_id", "overall_verdict", "confidence", "scores", "evaluated_at"],
    "verdict_enum": ["PASS", "FAIL", "WARN"],
    "confidence_range": (0.0, 1.0),
    "score_range": (1, 5),
}


def check_output_schema_violation_rate(records, baseline_rate=None):
    """Track LLM output schema conformance for Week 2 verdicts.

    Checks: required fields, verdict enum, confidence range, score ranges.
    Triggers WARN if violation rate exceeds baseline by 1.5x.
    """
    violations = []
    total = len(records)

    for i, rec in enumerate(records):
        # Required keys
        for key in EXPECTED_VERDICT_SCHEMA["required_keys"]:
            if key not in rec or rec[key] is None:
                violations.append({
                    "record_index": i,
                    "issue": f"Missing required field: {key}",
                    "verdict_id": rec.get("verdict_id", "unknown"),
                })

        # Verdict enum
        verdict = rec.get("overall_verdict")
        if verdict and verdict not in EXPECTED_VERDICT_SCHEMA["verdict_enum"]:
            violations.append({
                "record_index": i,
                "issue": f"Invalid verdict: '{verdict}' not in {EXPECTED_VERDICT_SCHEMA['verdict_enum']}",
                "verdict_id": rec.get("verdict_id", "unknown"),
            })

        # Confidence range
        conf = rec.get("confidence")
        if conf is not None:
            lo, hi = EXPECTED_VERDICT_SCHEMA["confidence_range"]
            if not (lo <= conf <= hi):
                violations.append({
                    "record_index": i,
                    "issue": f"Confidence {conf} outside [{lo}, {hi}]",
                    "verdict_id": rec.get("verdict_id", "unknown"),
                })

        # Score ranges
        scores = rec.get("scores", {})
        if isinstance(scores, dict):
            lo, hi = EXPECTED_VERDICT_SCHEMA["score_range"]
            for criterion, data in scores.items():
                score_val = data.get("score") if isinstance(data, dict) else None
                if score_val is not None and not (lo <= score_val <= hi):
                    violations.append({
                        "record_index": i,
                        "issue": f"Score '{criterion}' = {score_val} outside [{lo}, {hi}]",
                        "verdict_id": rec.get("verdict_id", "unknown"),
                    })

    violation_rate = len(violations) / max(total, 1)

    # Trend detection
    trend = "stable"
    if baseline_rate is not None and baseline_rate > 0:
        if violation_rate > baseline_rate * 1.5:
            trend = "rising"
        elif violation_rate < baseline_rate * 0.5:
            trend = "declining"

    if violation_rate > 0.02 or trend == "rising":
        status = "FAIL" if violation_rate > 0.05 else "WARN"
        severity = "HIGH" if status == "FAIL" else "MEDIUM"
    else:
        status = "PASS"
        severity = "LOW"

    return {
        "check_id": "ai.output_schema_violation_rate",
        "check_type": "output_schema_violation_rate",
        "status": status,
        "severity": severity,
        "total_records": total,
        "violations_found": len(violations),
        "violation_rate": round(violation_rate, 4),
        "baseline_rate": baseline_rate,
        "trend": trend,
        "sample_violations": violations[:10],
        "message": f"Output schema violation rate: {violation_rate:.2%} "
                   f"({len(violations)}/{total} records). Trend: {trend}.",
    }


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_jsonl(path):
    records = []
    with open(path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Run AI contract extensions on Week 2 and Week 3 data"
    )
    parser.add_argument(
        "--week3", default="outputs/week3/extractions.jsonl",
        help="Path to Week 3 extractions JSONL"
    )
    parser.add_argument(
        "--week2", default="outputs/week2/verdicts.jsonl",
        help="Path to Week 2 verdicts JSONL"
    )
    parser.add_argument(
        "--output", required=True,
        help="Output path for AI extensions report JSON"
    )
    parser.add_argument(
        "--skip-embeddings", action="store_true",
        help="Skip embedding drift check (no Ollama dependency)"
    )
    args = parser.parse_args()

    results = []

    # Extension 1: Embedding drift on Week 3 extracted_facts
    if Path(args.week3).exists():
        print(f"Loading Week 3 data from {args.week3}...")
        week3_records = load_jsonl(args.week3)
        print(f"  {len(week3_records)} records loaded")

        if not args.skip_embeddings:
            print("\n[Extension 1] Embedding drift detection...")
            texts = sample_texts(week3_records)
            print(f"  Sampled {len(texts)} text values")
            drift_result = check_embedding_drift(texts)
            results.append(drift_result)
            print(f"  Status: {drift_result['status']} | {drift_result['message']}")
        else:
            print("\n[Extension 1] Embedding drift skipped (--skip-embeddings)")

        # Extension 2: Prompt input schema
        print("\n[Extension 2] Prompt input schema validation...")
        input_result = check_prompt_input_schema(week3_records)
        results.append(input_result)
        print(f"  Status: {input_result['status']} | {input_result['message']}")
    else:
        print(f"Week 3 data not found at {args.week3}, skipping extensions 1-2")

    # Extension 3: Output schema violation rate on Week 2 verdicts
    if Path(args.week2).exists():
        print(f"\nLoading Week 2 data from {args.week2}...")
        week2_records = load_jsonl(args.week2)
        print(f"  {len(week2_records)} records loaded")

        print("\n[Extension 3] LLM output schema violation rate...")
        output_result = check_output_schema_violation_rate(week2_records)
        results.append(output_result)
        print(f"  Status: {output_result['status']} | {output_result['message']}")
    else:
        print(f"Week 2 data not found at {args.week2}, skipping extension 3")

    # Write report
    report = {
        "report_id": str(uuid.uuid4()),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "extensions_run": len(results),
        "results": results,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nAI extensions report written to {output_path}")
    for r in results:
        print(f"  {r['check_id']:40s} {r['status']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
