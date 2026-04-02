"""Migrate Veritas Stream events to canonical event_record JSONL.

Source: PostgreSQL events table (veritas-stream apex_ledger DB)
Target: outputs/week5/events.jsonl
"""
import json
import uuid
import os
from pathlib import Path
from datetime import timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path.home() / "tenx" / "week5" / "veritas-stream" / ".env")

DATABASE_URL = os.environ["DATABASE_URL"]
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "week5" / "events.jsonl"

AGGREGATE_TYPE_MAP = {
    "loan": "Loan",
    "docpkg": "DocumentPackage",
    "credit": "CreditAnalysis",
    "fraud": "FraudScreening",
    "compliance": "ComplianceCheck",
}


def parse_aggregate_type(stream_id: str) -> str:
    prefix = stream_id.split("-")[0] if "-" in stream_id else stream_id
    return AGGREGATE_TYPE_MAP.get(prefix.lower(), prefix.title())


def parse_aggregate_id(stream_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"veritas:{stream_id}"))


def fetch_events(dsn: str) -> list[dict]:
    query = """
        SELECT event_id, stream_id, stream_position, event_type,
               event_version, payload, metadata, recorded_at
        FROM events
        ORDER BY global_position ASC
    """
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def to_canonical(row: dict) -> dict:
    stream_id = row["stream_id"]
    aggregate_type = parse_aggregate_type(stream_id)
    recorded_at = row["recorded_at"].isoformat()
    occurred_at = (row["recorded_at"] - timedelta(milliseconds=50)).isoformat()

    payload = row["payload"] if isinstance(row["payload"], dict) else {}
    metadata = row["metadata"] if isinstance(row["metadata"], dict) else {}

    return {
        "event_id": str(row["event_id"]),
        "event_type": row["event_type"],
        "aggregate_id": parse_aggregate_id(stream_id),
        "aggregate_type": aggregate_type,
        "sequence_number": row["stream_position"],
        "payload": payload,
        "metadata": {
            "causation_id": metadata.get("causation_id"),
            "correlation_id": metadata.get("correlation_id"),
            "user_id": metadata.get("user_id", "system"),
            "source_service": f"week5-veritas-{aggregate_type.lower().replace(' ', '-')}",
        },
        "schema_version": f"{row['event_version']}.0",
        "occurred_at": occurred_at,
        "recorded_at": recorded_at,
    }


def main():
    print(f"Querying events from {DATABASE_URL.split('@')[1]}...")
    rows = fetch_events(DATABASE_URL)
    print(f"  Fetched {len(rows)} events from DB")

    records = [to_canonical(row) for row in rows]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as out:
        for rec in records:
            out.write(json.dumps(rec, default=str) + "\n")

    stream_ids = {r["stream_id"] for r in rows}
    event_types = {r["event_type"] for r in records}
    print(f"Wrote {len(records)} events to {OUTPUT_PATH}")
    print(f"  Unique aggregates: {len(stream_ids)}")
    print(f"  Event types: {len(event_types)}")


if __name__ == "__main__":
    main()
