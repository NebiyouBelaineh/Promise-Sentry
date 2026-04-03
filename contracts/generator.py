"""ContractGenerator — Auto-generates baseline data contracts from JSONL outputs.

Reads a JSONL data file + optional Week 4 lineage graph, profiles the data,
and produces a Bitol-compatible YAML contract + dbt schema.yml.

Usage:
    python contracts/generator.py \
        --source outputs/week3/extractions.jsonl \
        --contract-id week3-document-refinery-extractions \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output generated_contracts/
"""
import argparse
import json
import hashlib
import shutil
import uuid
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np
import yaml


# ---------------------------------------------------------------------------
# Stage 1: Load and profile data
# ---------------------------------------------------------------------------

def load_jsonl(path):
    """Load JSONL file into list of dicts."""
    records = []
    with open(path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def flatten_for_profile(records):
    """Flatten nested JSONL to a flat DataFrame for profiling.
    For arrays like extracted_facts[], explode to one row per item.
    For dicts, prefix keys with parent name."""
    rows = []
    for r in records:
        base = {}
        nested_arrays = {}
        nested_dicts = {}

        for k, v in r.items():
            if isinstance(v, list):
                nested_arrays[k] = v
            elif isinstance(v, dict):
                nested_dicts[k] = v
            else:
                base[k] = v

        # Flatten dicts by prefixing
        for dict_key, dict_val in nested_dicts.items():
            for dk, dv in dict_val.items():
                if not isinstance(dv, (list, dict)):
                    base[f"{dict_key}_{dk}"] = dv

        # Explode arrays
        if nested_arrays:
            # Use the largest array as the primary explode target
            primary_array_key = max(nested_arrays, key=lambda k: len(nested_arrays[k]))
            primary_array = nested_arrays[primary_array_key]

            if primary_array and isinstance(primary_array[0], dict):
                for item in primary_array:
                    row = dict(base)
                    for ik, iv in item.items():
                        if not isinstance(iv, (list, dict)):
                            row[f"{primary_array_key}_{ik}"] = iv
                        elif isinstance(iv, list):
                            row[f"{primary_array_key}_{ik}_count"] = len(iv)
                    rows.append(row)
            else:
                rows.append(base)
        else:
            rows.append(base)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Stage 2: Structural + statistical profiling per column
# ---------------------------------------------------------------------------

def profile_column(series, col_name):
    """Profile a single column: type, nulls, cardinality, stats."""
    result = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": int(series.nunique()),
        "sample_values": [str(v) for v in series.dropna().unique()[:5]],
    }

    if pd.api.types.is_numeric_dtype(series):
        s = series.dropna()
        if len(s) > 0:
            result["stats"] = {
                "min": float(s.min()),
                "max": float(s.max()),
                "mean": float(s.mean()),
                "p25": float(s.quantile(0.25)),
                "p50": float(s.quantile(0.50)),
                "p75": float(s.quantile(0.75)),
                "p95": float(s.quantile(0.95)),
                "p99": float(s.quantile(0.99)),
                "stddev": float(s.std()) if len(s) > 1 else 0.0,
            }
    return result


def profile_all_columns(df):
    """Profile every column in the DataFrame."""
    return {col: profile_column(df[col], col) for col in df.columns}


# ---------------------------------------------------------------------------
# Stage 3: Translate profiles to Bitol YAML clauses
# ---------------------------------------------------------------------------

def infer_type(dtype_str):
    """Map pandas dtype to JSON Schema type."""
    mapping = {
        "float64": "number",
        "float32": "number",
        "int64": "integer",
        "int32": "integer",
        "bool": "boolean",
        "object": "string",
    }
    return mapping.get(dtype_str, "string")


def column_to_clause(profile):
    """Convert a column profile to a Bitol contract clause."""
    clause = {
        "type": infer_type(profile["dtype"]),
        "required": profile["null_fraction"] == 0.0,
    }

    name = profile["name"]

    # Confidence fields: must be 0.0-1.0
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = (
            "Confidence score. Must remain 0.0-1.0 float. "
            "BREAKING if changed to 0-100."
        )

    # UUID fields
    if name.endswith("_id") and clause["type"] == "string":
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-f-]{36}$"

    # Timestamp fields
    if name.endswith("_at") and clause["type"] == "string":
        clause["format"] = "date-time"

    # Hash fields
    if "hash" in name and clause["type"] == "string":
        clause["pattern"] = "^[a-f0-9]{64}$"
        clause["description"] = "SHA-256 hash. Changes iff source content changes."

    # Enum detection: low cardinality string columns
    if (clause["type"] == "string"
            and profile["cardinality_estimate"] <= 10
            and profile["cardinality_estimate"] > 0
            and len(profile["sample_values"]) == profile["cardinality_estimate"]):
        clause["enum"] = profile["sample_values"]

    # PascalCase pattern detection
    if clause["type"] == "string" and profile["sample_values"]:
        pascal_re = re.compile(r"^[A-Z][a-zA-Z0-9]+$")
        if all(pascal_re.match(v) for v in profile["sample_values"] if v):
            clause["pattern"] = "^[A-Z][a-zA-Z0-9]+$"
            clause.setdefault("description", "")
            clause["description"] = (clause["description"] + " PascalCase identifier.").strip()

    # Numeric range from stats
    if "stats" in profile:
        stats = profile["stats"]
        if "minimum" not in clause:
            if stats["min"] >= 0:
                clause["minimum"] = 0

    # Auto-generate descriptions for fields that lack one
    if not clause.get("description"):
        clause["description"] = _auto_describe(name, clause, profile)

    # Uniqueness hint
    if (name.endswith("_id")
            and profile["null_fraction"] == 0.0
            and profile["cardinality_estimate"] > 0):
        clause["unique"] = True

    return clause


def _auto_describe(name, clause, profile):
    """Generate a human-readable description for a column."""
    parts = []
    col_type = clause.get("type", "unknown")

    if name.endswith("_id"):
        parts.append(f"Identifier field ({col_type}).")
    elif name.endswith("_at"):
        parts.append("Timestamp in ISO 8601 format.")
    elif "count" in name:
        parts.append(f"Count metric ({col_type}).")
    elif "token" in name:
        parts.append("Token usage metric.")
    elif "path" in name:
        parts.append("File or resource path.")
    elif "model" in name:
        parts.append("Model identifier string.")
    elif "text" in name or "excerpt" in name:
        parts.append("Free-text content field.")
    elif "score" in name:
        parts.append("Numeric score.")
    elif "version" in name:
        parts.append("Version identifier.")
    elif "type" in name:
        parts.append("Type classifier.")
    else:
        parts.append(f"{name.replace('_', ' ').title()} field.")

    if clause.get("required"):
        parts.append("Required — must not be null.")
    if clause.get("unique"):
        parts.append("Must be unique across all records.")
    if "stats" in profile:
        s = profile["stats"]
        parts.append(f"Observed range: [{s['min']:.2f}, {s['max']:.2f}], mean={s['mean']:.2f}.")

    return " ".join(parts)


def infer_cross_column_constraints(records, column_profiles):
    """Infer constraints that span multiple columns."""
    constraints = []

    col_names = set(column_profiles.keys())

    # Temporal ordering: recorded_at >= occurred_at
    if "recorded_at" in col_names and "occurred_at" in col_names:
        constraints.append({
            "id": "temporal_ordering",
            "type": "cross_column",
            "rule": "recorded_at >= occurred_at",
            "description": "Recording timestamp must be at or after the occurrence timestamp.",
            "severity": "CRITICAL",
        })

    # Token sum: total_tokens = prompt_tokens + completion_tokens
    if "total_tokens" in col_names and "prompt_tokens" in col_names and "completion_tokens" in col_names:
        constraints.append({
            "id": "token_sum",
            "type": "cross_column",
            "rule": "total_tokens == prompt_tokens + completion_tokens",
            "description": "Total token count must equal the sum of prompt and completion tokens.",
            "severity": "CRITICAL",
        })

    # Sequence monotonicity per aggregate
    if "sequence_number" in col_names and "aggregate_id" in col_names:
        constraints.append({
            "id": "sequence_monotonicity",
            "type": "cross_column",
            "rule": "sequence_number is monotonically increasing per aggregate_id",
            "description": "Sequence numbers must increase without gaps or duplicates within each aggregate.",
            "severity": "CRITICAL",
        })

    # entity_refs referential integrity (Week 3)
    for r in records[:1]:
        if "extracted_facts" in r and isinstance(r.get("extracted_facts"), list):
            if "entities" in r:
                constraints.append({
                    "id": "entity_refs_integrity",
                    "type": "referential",
                    "rule": "extracted_facts[*].entity_refs[] must reference entity_ids in entities[] of the same record",
                    "description": "Every entity reference in extracted facts must exist in the record's entities array.",
                    "severity": "CRITICAL",
                })
            # Non-empty array
            constraints.append({
                "id": "extracted_facts_non_empty",
                "type": "structural",
                "rule": "len(extracted_facts) >= 1",
                "description": "Each extraction record must contain at least one extracted fact.",
                "severity": "HIGH",
            })
            break

    # Non-empty array for events payload
    for r in records[:1]:
        if "payload" in r and isinstance(r.get("payload"), dict):
            constraints.append({
                "id": "payload_non_empty",
                "type": "structural",
                "rule": "len(payload) >= 1",
                "description": "Event payload must contain at least one field.",
                "severity": "HIGH",
            })
            break

    return constraints


# ---------------------------------------------------------------------------
# Stage 3B: LLM annotation for ambiguous columns
# ---------------------------------------------------------------------------

def annotate_ambiguous_columns(contract, column_profiles, df):
    """Annotate ambiguous columns with LLM-generated descriptions.

    For columns whose business meaning is unclear from name and sample
    values alone, invoke an LLM with the column name, table name, five
    sample values, and adjacent column names for:
    (a) a plain-English description
    (b) a business rule as a validation expression
    (c) any cross-column relationship

    Requires ANTHROPIC_API_KEY in environment. Skips gracefully if unavailable.
    """
    import os

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return contract

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
    except (ImportError, Exception):
        return contract

    schema = contract.get("schema", {})
    ambiguous = []
    for col_name, clause in schema.items():
        desc = clause.get("description", "")
        # Columns with generic auto-generated descriptions are ambiguous
        if "field" in desc.lower() and ("required" in desc.lower() or "identifier" in desc.lower()):
            if col_name in column_profiles:
                ambiguous.append(col_name)

    # Limit to 10 most ambiguous to control cost
    for col_name in ambiguous[:10]:
        profile = column_profiles.get(col_name, {})
        samples = profile.get("sample_values", [])[:5]
        adjacent = list(schema.keys())[:10]

        prompt = (
            f"Column: {col_name}\n"
            f"Adjacent columns: {', '.join(adjacent)}\n"
            f"Sample values: {samples}\n"
            f"Type: {profile.get('dtype', 'unknown')}\n\n"
            f"Provide:\n"
            f"1. A plain-English description of this column's business meaning\n"
            f"2. A business rule as a validation expression\n"
            f"3. Any cross-column relationship\n"
            f"Reply in JSON: {{\"description\": ..., \"business_rule\": ..., \"cross_column\": ...}}"
        )

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text
            annotation = json.loads(text)
            schema[col_name]["llm_annotations"] = annotation
        except Exception:
            continue

    return contract


# ---------------------------------------------------------------------------
# Stage 4: Lineage injection + write YAML
# ---------------------------------------------------------------------------

def inject_lineage(contract, lineage_path, contract_id):
    """Inject lineage context from Week 4 snapshot into the contract."""
    if not lineage_path or not Path(lineage_path).exists():
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract

    with open(lineage_path) as f:
        lines = f.readlines()
    if not lines:
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract

    # Use the latest (last) snapshot
    snapshot = json.loads(lines[-1])

    # Find nodes that might consume this contract's data
    contract_week = contract_id.split("-")[0] if "-" in contract_id else contract_id
    consumers = []
    for edge in snapshot.get("edges", []):
        src = edge.get("source", "").lower()
        tgt = edge.get("target", "").lower()
        if contract_week in src or contract_id.replace("-", "_") in src:
            consumers.append({
                "id": edge["target"],
                "fields_consumed": ["doc_id", "extracted_facts"],
                "breaking_if_changed": ["confidence"],
            })

    # Deduplicate
    seen = set()
    unique_consumers = []
    for c in consumers[:10]:
        if c["id"] not in seen:
            seen.add(c["id"])
            unique_consumers.append(c)

    contract["lineage"] = {
        "upstream": [],
        "downstream": unique_consumers,
    }
    return contract


def build_contract(contract_id, source_path, column_profiles, lineage_path=None,
                   records=None):
    """Assemble the full Bitol-compatible contract YAML."""
    # Build schema section
    schema = {}
    for col_name, profile in column_profiles.items():
        clause = column_to_clause(profile)
        schema[col_name] = clause

    source_name = Path(source_path).stem
    week_match = re.search(r"week(\d+)", source_path)
    week_num = week_match.group(1) if week_match else "unknown"

    # Infer cross-column constraints from raw records
    cross_column = []
    if records:
        cross_column = infer_cross_column_constraints(records, column_profiles)

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": f"Week {week_num} — {source_name.replace('_', ' ').title()}",
            "version": "1.0.0",
            "owner": f"week{week_num}-team",
            "description": f"Auto-generated contract for {source_name}. "
                           f"Each record represents one unit of output from the Week {week_num} system.",
        },
        "servers": {
            "local": {
                "type": "local",
                "path": source_path,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": "Confidence fields must remain in 0.0-1.0 float range.",
        },
        "schema": schema,
        "constraints": cross_column,
        "quality": build_quality_section(column_profiles, source_name, cross_column),
    }

    # Inject lineage
    contract = inject_lineage(contract, lineage_path, contract_id)

    return contract


def build_quality_section(column_profiles, table_name, cross_column=None):
    """Build Soda-compatible quality checks."""
    checks = []
    for col_name, profile in column_profiles.items():
        if profile["null_fraction"] == 0.0:
            checks.append(f"missing_count({col_name}) = 0")
        if col_name.endswith("_id") and profile["null_fraction"] == 0.0:
            checks.append(f"duplicate_count({col_name}) = 0")
        if "confidence" in col_name and "stats" in profile:
            checks.append(f"min({col_name}) >= 0.0")
            checks.append(f"max({col_name}) <= 1.0")

    # Add cross-column constraint checks
    for cc in (cross_column or []):
        checks.append(f"# {cc['id']}: {cc['rule']}")

    checks.append("row_count >= 1")

    return {
        "type": "SodaChecks",
        "specification": {
            f"checks for {table_name}": checks,
        },
    }


def build_dbt_schema(contract_id, column_profiles, source_path, cross_column=None):
    """Generate dbt-compatible schema.yml from the contract."""
    source_name = Path(source_path).stem
    columns = []

    for col_name, profile in column_profiles.items():
        col_def = {"name": col_name}
        tests = []

        if profile["null_fraction"] == 0.0:
            tests.append("not_null")
        if col_name.endswith("_id") and profile["null_fraction"] == 0.0:
            tests.append("unique")
        if (profile.get("cardinality_estimate", 0) <= 10
                and len(profile["sample_values"]) == profile.get("cardinality_estimate", 0)
                and profile["sample_values"]):
            tests.append({"accepted_values": {"values": profile["sample_values"]}})

        # Range tests for confidence fields
        if "confidence" in col_name and "stats" in profile:
            tests.append({"dbt_utils.expression_is_true": {
                "expression": f"{col_name} >= 0.0 AND {col_name} <= 1.0"
            }})

        # Positive integer checks for count/token/ms fields
        if profile.get("stats") and profile["stats"]["min"] >= 0:
            if any(kw in col_name for kw in ["_ms", "_count", "token", "processing_time"]):
                tests.append({"dbt_utils.expression_is_true": {
                    "expression": f"{col_name} >= 0"
                }})

        if tests:
            col_def["tests"] = tests
        columns.append(col_def)

    # Model-level tests from cross-column constraints
    model_tests = []
    for cc in (cross_column or []):
        if cc["id"] == "temporal_ordering":
            model_tests.append({"dbt_utils.expression_is_true": {
                "expression": "recorded_at >= occurred_at",
                "config": {"severity": "error"},
            }})
        elif cc["id"] == "token_sum":
            model_tests.append({"dbt_utils.expression_is_true": {
                "expression": "total_tokens = prompt_tokens + completion_tokens",
                "config": {"severity": "error"},
            }})
        elif cc["id"] == "extracted_facts_non_empty":
            model_tests.append({"dbt_utils.expression_is_true": {
                "expression": "json_array_length(extracted_facts) >= 1",
                "config": {"severity": "error"},
            }})

    model_def = {
        "name": source_name,
        "description": f"dbt schema for {contract_id}",
        "columns": columns,
    }
    if model_tests:
        model_def["tests"] = model_tests

    dbt_schema = {
        "version": 2,
        "models": [model_def],
    }
    return dbt_schema


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate data contracts from JSONL outputs")
    parser.add_argument("--source", required=True, help="Path to JSONL data file")
    parser.add_argument("--contract-id", required=True, help="Contract identifier")
    parser.add_argument("--lineage", default=None, help="Path to Week 4 lineage JSONL")
    parser.add_argument("--annotate", action="store_true",
                        help="Use LLM to annotate ambiguous columns (requires ANTHROPIC_API_KEY)")
    parser.add_argument("--output", required=True, help="Output directory for contracts")
    args = parser.parse_args()

    source_path = args.source
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading data from {source_path}...")
    records = load_jsonl(source_path)
    print(f"  {len(records)} records loaded")

    print("Flattening and profiling...")
    df = flatten_for_profile(records)
    print(f"  {len(df)} rows, {len(df.columns)} columns after flattening")
    print(f"  Columns: {list(df.columns)}")

    column_profiles = profile_all_columns(df)
    print(f"  Profiled {len(column_profiles)} columns")

    # Flag confidence issues
    for col_name, profile in column_profiles.items():
        if "confidence" in col_name and "stats" in profile:
            stats = profile["stats"]
            if stats["max"] > 1.0:
                print(f"  WARNING: {col_name} has max={stats['max']:.3f} (>1.0) — possible scale issue!")
            if stats["mean"] > 0.99:
                print(f"  WARNING: {col_name} mean={stats['mean']:.3f} — almost certainly clamped")
            if stats["mean"] < 0.01:
                print(f"  WARNING: {col_name} mean={stats['mean']:.3f} — almost certainly broken")

    print("Building contract...")
    contract = build_contract(args.contract_id, source_path, column_profiles, args.lineage,
                              records=records)

    if args.annotate:
        print("Annotating ambiguous columns with LLM...")
        contract = annotate_ambiguous_columns(contract, column_profiles, df)

    # Derive output filename from contract-id
    safe_name = args.contract_id.replace("-", "_").split("_", 1)[-1] if "_" in args.contract_id.replace("-", "_") else args.contract_id.replace("-", "_")
    # Use a simpler naming: extract week info
    week_match = re.search(r"week(\d+)", args.contract_id)
    if week_match:
        # Find the data type from the source filename
        source_stem = Path(source_path).stem
        yaml_name = f"week{week_match.group(1)}_{source_stem}"
    else:
        yaml_name = args.contract_id.replace("-", "_")

    # Write Bitol YAML
    yaml_path = output_dir / f"{yaml_name}.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"  Contract written to {yaml_path}")
    print(f"  Schema clauses: {len(contract['schema'])}")

    # Write dbt schema.yml
    dbt_schema = build_dbt_schema(args.contract_id, column_profiles, source_path,
                                  cross_column=contract.get("constraints", []))
    dbt_path = output_dir / f"{yaml_name}_dbt.yml"
    with open(dbt_path, "w") as f:
        yaml.dump(dbt_schema, f, default_flow_style=False, sort_keys=False)
    print(f"  dbt schema written to {dbt_path}")

    # Write schema snapshot
    snapshot_dir = Path("schema_snapshots") / args.contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"{ts}.yaml"
    shutil.copy(yaml_path, snapshot_path)
    print(f"  Schema snapshot written to {snapshot_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
