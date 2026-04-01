"""Migrate Veritas Stream events to canonical event_record JSONL.

Source: ~/tenx/week5/veritas-stream/data/seed_events.jsonl
Target: outputs/week5/events.jsonl
"""
import json
import uuid
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta

WEEK5_SOURCE = Path.home() / "tenx" / "week5" / "veritas-stream" / "data" / "seed_events.jsonl"
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "week5" / "events.jsonl"


def parse_aggregate_type(stream_id):
    """Extract aggregate type from stream_id like 'loan-APEX-0001' -> 'Loan'."""
    prefix = stream_id.split("-")[0] if "-" in stream_id else stream_id
    type_map = {
        "loan": "Loan",
        "docpkg": "DocumentPackage",
        "credit": "CreditAnalysis",
        "fraud": "FraudScreening",
        "compliance": "ComplianceCheck",
    }
    return type_map.get(prefix.lower(), prefix.title())


def parse_aggregate_id(stream_id):
    """Extract a stable aggregate ID from stream_id."""
    # stream_id format: 'loan-APEX-0001' -> use full stream_id as basis for UUID
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"veritas:{stream_id}"))


def main():
    print(f"Reading events from {WEEK5_SOURCE}...")

    # Track sequence numbers per aggregate
    sequence_counters = {}
    records = []

    with open(WEEK5_SOURCE) as f:
        for line in f:
            if not line.strip():
                continue
            src = json.loads(line)

            stream_id = src["stream_id"]
            aggregate_id = parse_aggregate_id(stream_id)
            aggregate_type = parse_aggregate_type(stream_id)

            # Monotonic sequence per aggregate
            sequence_counters.setdefault(stream_id, 0)
            sequence_counters[stream_id] += 1
            seq_num = sequence_counters[stream_id]

            recorded_at = src["recorded_at"]
            # occurred_at is slightly before recorded_at
            try:
                rec_dt = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
                occ_dt = rec_dt - timedelta(milliseconds=50)
                occurred_at = occ_dt.isoformat()
            except (ValueError, TypeError):
                occurred_at = recorded_at

            payload = src.get("payload", {})

            # Extract metadata from payload if present
            causation_id = payload.pop("triggered_by_event_id", None)
            correlation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"corr:{stream_id}"))
            user_id = payload.get("uploaded_by", payload.get("reviewer_id", "system"))
            source_service = f"week5-veritas-{aggregate_type.lower().replace(' ', '-')}"

            record = {
                "event_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"veritas:{stream_id}:{seq_num}")),
                "event_type": src["event_type"],
                "aggregate_id": aggregate_id,
                "aggregate_type": aggregate_type,
                "sequence_number": seq_num,
                "payload": payload,
                "metadata": {
                    "causation_id": causation_id,
                    "correlation_id": correlation_id,
                    "user_id": str(user_id) if user_id else "system",
                    "source_service": source_service,
                },
                "schema_version": str(src.get("event_version", "1")) + ".0",
                "occurred_at": occurred_at,
                "recorded_at": recorded_at,
            }
            records.append(record)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as out:
        for rec in records:
            out.write(json.dumps(rec) + "\n")

    print(f"Wrote {len(records)} events to {OUTPUT_PATH}")
    print(f"  Unique aggregates: {len(sequence_counters)}")
    print(f"  Event types: {len(set(r['event_type'] for r in records))}")


if __name__ == "__main__":
    main()
