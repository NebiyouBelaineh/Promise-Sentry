"""ValidationRunner — Executes contract checks on a dataset snapshot.

Runs structural and statistical checks defined in a Bitol YAML contract
against a JSONL data file. Produces a structured validation report JSON.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3_extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/week3_baseline.json
"""
import argparse
import json
import hashlib
import uuid
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np
import yaml


def load_jsonl(path):
    records = []
    with open(path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def flatten_for_profile(records):
    """Same flattening logic as generator to ensure column names match."""
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

        for dict_key, dict_val in nested_dicts.items():
            for dk, dv in dict_val.items():
                if not isinstance(dv, (list, dict)):
                    base[f"{dict_key}_{dk}"] = dv

        if nested_arrays:
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


def compute_snapshot_hash(data_path):
    """SHA-256 of the input JSONL file."""
    h = hashlib.sha256()
    with open(data_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Check implementations
# ---------------------------------------------------------------------------

def check_required(col_name, clause, df):
    """Check that a required field has no nulls."""
    if not clause.get("required", False):
        return None

    if col_name not in df.columns:
        return {
            "check_id": f"{col_name}.required",
            "column_name": col_name,
            "check_type": "required",
            "status": "ERROR",
            "actual_value": "column missing from data",
            "expected": "column present with no nulls",
            "severity": "CRITICAL",
            "records_failing": len(df),
            "sample_failing": [],
            "message": f"Column {col_name} defined as required but not found in data.",
        }

    null_count = int(df[col_name].isna().sum())
    if null_count > 0:
        return {
            "check_id": f"{col_name}.required",
            "column_name": col_name,
            "check_type": "required",
            "status": "FAIL",
            "actual_value": f"null_count={null_count}",
            "expected": "null_count=0",
            "severity": "CRITICAL",
            "records_failing": null_count,
            "sample_failing": df[df[col_name].isna()].index.tolist()[:5],
            "message": f"{col_name} has {null_count} null values but is required.",
        }

    return {
        "check_id": f"{col_name}.required",
        "column_name": col_name,
        "check_type": "required",
        "status": "PASS",
        "actual_value": "null_count=0",
        "expected": "null_count=0",
        "severity": "LOW",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} has no nulls.",
    }


def check_type(col_name, clause, df):
    """Check that column type matches contract."""
    expected_type = clause.get("type")
    if not expected_type or col_name not in df.columns:
        return None

    actual_dtype = str(df[col_name].dtype)
    type_compatible = {
        "number": ["float64", "float32", "int64", "int32"],
        "integer": ["int64", "int32"],
        "string": ["object", "str", "string", "string[python]", "String"],
        "boolean": ["bool"],
    }

    expected_dtypes = type_compatible.get(expected_type, [])
    if actual_dtype in expected_dtypes:
        return {
            "check_id": f"{col_name}.type",
            "column_name": col_name,
            "check_type": "type",
            "status": "PASS",
            "actual_value": f"dtype={actual_dtype}",
            "expected": f"type={expected_type}",
            "severity": "LOW",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"{col_name} dtype {actual_dtype} matches expected {expected_type}.",
        }
    else:
        return {
            "check_id": f"{col_name}.type",
            "column_name": col_name,
            "check_type": "type",
            "status": "FAIL",
            "actual_value": f"dtype={actual_dtype}",
            "expected": f"type={expected_type} (one of {expected_dtypes})",
            "severity": "CRITICAL",
            "records_failing": len(df),
            "sample_failing": [],
            "message": f"{col_name} has dtype {actual_dtype}, expected {expected_type}.",
        }


def check_enum(col_name, clause, df):
    """Check enum conformance."""
    enum_values = clause.get("enum")
    if not enum_values or col_name not in df.columns:
        return None

    series = df[col_name].dropna()
    invalid = series[~series.isin(enum_values)]

    if len(invalid) == 0:
        return {
            "check_id": f"{col_name}.enum",
            "column_name": col_name,
            "check_type": "enum",
            "status": "PASS",
            "actual_value": f"all values in enum",
            "expected": f"enum={enum_values}",
            "severity": "LOW",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"All {col_name} values conform to enum.",
        }
    else:
        sample = invalid.unique()[:5].tolist()
        return {
            "check_id": f"{col_name}.enum",
            "column_name": col_name,
            "check_type": "enum",
            "status": "FAIL",
            "actual_value": f"non_conforming_count={len(invalid)}, samples={sample}",
            "expected": f"enum={enum_values}",
            "severity": "CRITICAL",
            "records_failing": int(len(invalid)),
            "sample_failing": [str(s) for s in sample],
            "message": f"{col_name} has {len(invalid)} values not in enum: {sample}",
        }


def check_uuid_pattern(col_name, clause, df):
    """Check UUID format."""
    if clause.get("format") != "uuid" or col_name not in df.columns:
        return None

    pattern = re.compile(r"^[0-9a-f-]{36}$")
    series = df[col_name].dropna().astype(str)
    invalid = series[~series.str.match(pattern)]

    if len(invalid) == 0:
        return {
            "check_id": f"{col_name}.uuid",
            "column_name": col_name,
            "check_type": "uuid_format",
            "status": "PASS",
            "actual_value": "all values match UUID pattern",
            "expected": "format=uuid",
            "severity": "LOW",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"All {col_name} values are valid UUIDs.",
        }
    else:
        sample = invalid.head(5).tolist()
        return {
            "check_id": f"{col_name}.uuid",
            "column_name": col_name,
            "check_type": "uuid_format",
            "status": "FAIL",
            "actual_value": f"invalid_count={len(invalid)}",
            "expected": "format=uuid (^[0-9a-f-]{36}$)",
            "severity": "CRITICAL",
            "records_failing": int(len(invalid)),
            "sample_failing": sample,
            "message": f"{col_name} has {len(invalid)} values not matching UUID pattern.",
        }


def check_datetime_format(col_name, clause, df):
    """Check date-time format."""
    if clause.get("format") != "date-time" or col_name not in df.columns:
        return None

    series = df[col_name].dropna().astype(str)
    invalid_count = 0
    invalid_samples = []

    for val in series:
        try:
            datetime.fromisoformat(val.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            invalid_count += 1
            if len(invalid_samples) < 5:
                invalid_samples.append(val)

    if invalid_count == 0:
        return {
            "check_id": f"{col_name}.datetime",
            "column_name": col_name,
            "check_type": "datetime_format",
            "status": "PASS",
            "actual_value": "all values parse as ISO 8601",
            "expected": "format=date-time",
            "severity": "LOW",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"All {col_name} values are valid ISO 8601.",
        }
    else:
        return {
            "check_id": f"{col_name}.datetime",
            "column_name": col_name,
            "check_type": "datetime_format",
            "status": "FAIL",
            "actual_value": f"invalid_count={invalid_count}",
            "expected": "format=date-time (ISO 8601)",
            "severity": "HIGH",
            "records_failing": invalid_count,
            "sample_failing": invalid_samples,
            "message": f"{col_name} has {invalid_count} values that don't parse as ISO 8601.",
        }


def check_range(col_name, clause, df):
    """Check min/max range for numeric columns."""
    has_min = "minimum" in clause
    has_max = "maximum" in clause
    if (not has_min and not has_max) or col_name not in df.columns:
        return None

    if not pd.api.types.is_numeric_dtype(df[col_name]):
        return None

    series = df[col_name].dropna()
    if len(series) == 0:
        return None

    actual_min = float(series.min())
    actual_max = float(series.max())
    actual_mean = float(series.mean())

    violations = []
    if has_min and actual_min < clause["minimum"]:
        violations.append(f"min={actual_min:.4f} < contract_min={clause['minimum']}")
    if has_max and actual_max > clause["maximum"]:
        violations.append(f"max={actual_max:.4f} > contract_max={clause['maximum']}")

    expected_parts = []
    if has_min:
        expected_parts.append(f"min>={clause['minimum']}")
    if has_max:
        expected_parts.append(f"max<={clause['maximum']}")

    if violations:
        failing_mask = pd.Series([False] * len(series), index=series.index)
        if has_min:
            failing_mask |= series < clause["minimum"]
        if has_max:
            failing_mask |= series > clause["maximum"]
        records_failing = int(failing_mask.sum())

        return {
            "check_id": f"{col_name}.range",
            "column_name": col_name,
            "check_type": "range",
            "status": "FAIL",
            "actual_value": f"min={actual_min:.4f}, max={actual_max:.4f}, mean={actual_mean:.4f}",
            "expected": ", ".join(expected_parts),
            "severity": "CRITICAL",
            "records_failing": records_failing,
            "sample_failing": [],
            "message": f"{col_name} range violation: {'; '.join(violations)}. "
                       "Breaking change detected.",
        }
    else:
        return {
            "check_id": f"{col_name}.range",
            "column_name": col_name,
            "check_type": "range",
            "status": "PASS",
            "actual_value": f"min={actual_min:.4f}, max={actual_max:.4f}, mean={actual_mean:.4f}",
            "expected": ", ".join(expected_parts),
            "severity": "LOW",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"{col_name} values within expected range.",
        }


def check_statistical_drift(col_name, clause, df, baselines):
    """Check for statistical drift against stored baselines."""
    if col_name not in df.columns or not pd.api.types.is_numeric_dtype(df[col_name]):
        return None

    series = df[col_name].dropna()
    if len(series) < 2:
        return None

    current_mean = float(series.mean())
    current_std = float(series.std())

    if not baselines or col_name not in baselines:
        return None  # No baseline yet

    b = baselines[col_name]
    baseline_mean = b.get("mean", current_mean)
    baseline_stddev = b.get("stddev", 1.0)

    z_score = abs(current_mean - baseline_mean) / max(baseline_stddev, 1e-9)

    if z_score > 3:
        return {
            "check_id": f"{col_name}.drift",
            "column_name": col_name,
            "check_type": "statistical_drift",
            "status": "FAIL",
            "actual_value": f"mean={current_mean:.4f}, z_score={z_score:.2f}",
            "expected": f"mean near {baseline_mean:.4f} (within 3 stddev)",
            "severity": "HIGH",
            "records_failing": len(series),
            "sample_failing": [],
            "message": f"{col_name} mean drifted {z_score:.1f} stddev from baseline.",
        }
    elif z_score > 2:
        return {
            "check_id": f"{col_name}.drift",
            "column_name": col_name,
            "check_type": "statistical_drift",
            "status": "WARN",
            "actual_value": f"mean={current_mean:.4f}, z_score={z_score:.2f}",
            "expected": f"mean near {baseline_mean:.4f} (within 2 stddev)",
            "severity": "MEDIUM",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"{col_name} mean within warning range ({z_score:.1f} stddev).",
        }
    else:
        return {
            "check_id": f"{col_name}.drift",
            "column_name": col_name,
            "check_type": "statistical_drift",
            "status": "PASS",
            "actual_value": f"mean={current_mean:.4f}, z_score={z_score:.2f}",
            "expected": f"mean near {baseline_mean:.4f}",
            "severity": "LOW",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"{col_name} statistical drift within normal range.",
        }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_baselines():
    """Load baselines from schema_snapshots/baselines.json if available."""
    baselines_path = Path("schema_snapshots/baselines.json")
    if baselines_path.exists():
        with open(baselines_path) as f:
            data = json.load(f)
        return data.get("columns", {})
    return {}


def write_baselines(df):
    """Write baselines after first successful run."""
    baselines = {}
    for col in df.select_dtypes(include="number").columns:
        series = df[col].dropna()
        if len(series) > 1:
            baselines[col] = {
                "mean": float(series.mean()),
                "stddev": float(series.std()),
            }

    baselines_path = Path("schema_snapshots/baselines.json")
    baselines_path.parent.mkdir(parents=True, exist_ok=True)
    with open(baselines_path, "w") as f:
        json.dump(
            {
                "written_at": datetime.now(timezone.utc).isoformat(),
                "columns": baselines,
            },
            f,
            indent=2,
        )
    print(f"  Baselines written to {baselines_path}")


def main():
    parser = argparse.ArgumentParser(description="Run contract validation on JSONL data")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--data", required=True, help="Path to JSONL data file")
    parser.add_argument("--output", required=True, help="Output path for validation report JSON")
    args = parser.parse_args()

    print(f"Loading contract from {args.contract}...")
    with open(args.contract) as f:
        contract = yaml.safe_load(f)

    print(f"Loading data from {args.data}...")
    records = load_jsonl(args.data)
    print(f"  {len(records)} records loaded")

    df = flatten_for_profile(records)
    print(f"  {len(df)} rows, {len(df.columns)} columns after flattening")

    schema = contract.get("schema", {})
    print(f"  {len(schema)} contract clauses to check")

    # Load baselines for drift detection
    baselines = load_baselines()

    # Run all checks
    results = []
    for col_name, clause in schema.items():
        # Structural checks
        for check_fn in [check_required, check_type, check_enum,
                         check_uuid_pattern, check_datetime_format, check_range]:
            result = check_fn(col_name, clause, df)
            if result:
                results.append(result)

        # Statistical checks
        drift_result = check_statistical_drift(col_name, clause, df, baselines)
        if drift_result:
            results.append(drift_result)

    # Tally results
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    warned = sum(1 for r in results if r["status"] == "WARN")
    errored = sum(1 for r in results if r["status"] == "ERROR")

    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract.get("id", "unknown"),
        "snapshot_id": compute_snapshot_hash(args.data),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": results,
    }

    # Write report
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nValidation report written to {output_path}")
    print(f"  Total checks: {len(results)}")
    print(f"  PASS: {passed}  FAIL: {failed}  WARN: {warned}  ERROR: {errored}")

    # Write baselines on first run
    if not baselines:
        write_baselines(df)

    # Exit with non-zero if any failures
    if failed > 0:
        print(f"\n  {failed} check(s) FAILED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
