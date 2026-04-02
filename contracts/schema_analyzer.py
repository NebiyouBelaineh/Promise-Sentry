"""SchemaEvolutionAnalyzer — Diffs schema snapshots and classifies changes.

Compares two timestamped schema snapshots to detect additions, removals,
type changes, and constraint modifications. Classifies each change as
backward-compatible, forward-compatible, or breaking. Produces a
migration impact report with a rollback plan.

Usage:
    python contracts/schema_analyzer.py \
        --snapshots schema_snapshots/week5-event-records/ \
        --output schema_snapshots/week5-event-records/evolution_report.json

    python contracts/schema_analyzer.py \
        --before schema_snapshots/week5-event-records/20260401_190655.yaml \
        --after schema_snapshots/week5-event-records/20260402_104232.yaml \
        --output schema_snapshots/evolution_report.json
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def load_snapshot(path):
    """Load a YAML schema snapshot and return its schema dict + metadata."""
    with open(path) as f:
        doc = yaml.safe_load(f)
    return {
        "path": str(path),
        "id": doc.get("id", "unknown"),
        "version": doc.get("info", {}).get("version", "0.0.0"),
        "schema": doc.get("schema", {}),
        "constraints": doc.get("constraints", []),
    }


def find_snapshot_pair(snapshot_dir):
    """Find the two most recent snapshots in a directory.

    Precondition: directory contains at least 2 YAML files.
    Guarantee: returns (older, newer) paths sorted by filename timestamp.
    Raises: ValueError if fewer than 2 snapshots exist.
    """
    snapshots = sorted(Path(snapshot_dir).glob("*.yaml"))
    if len(snapshots) < 2:
        raise ValueError(
            f"Need at least 2 snapshots in {snapshot_dir}, found {len(snapshots)}"
        )
    return snapshots[-2], snapshots[-1]


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------

BREAKING_TYPE_CHANGES = {
    ("string", "number"), ("string", "integer"), ("string", "boolean"),
    ("number", "string"), ("number", "integer"),
    ("integer", "string"), ("integer", "number"),
    ("boolean", "string"), ("boolean", "number"), ("boolean", "integer"),
}


def diff_columns(before_schema, after_schema):
    """Compute column-level diffs between two schema versions."""
    before_cols = set(before_schema.keys())
    after_cols = set(after_schema.keys())

    added = after_cols - before_cols
    removed = before_cols - after_cols
    common = before_cols & after_cols

    changes = []

    for col in sorted(added):
        clause = after_schema[col]
        is_required = clause.get("required", False)
        changes.append({
            "column": col,
            "change_type": "column_added",
            "compatibility": "backward" if not is_required else "breaking",
            "detail": f"New column '{col}' added"
                + (" (required; breaks old producers)" if is_required else " (optional; safe)"),
            "before": None,
            "after": _summarize_clause(clause),
        })

    for col in sorted(removed):
        clause = before_schema[col]
        changes.append({
            "column": col,
            "change_type": "column_removed",
            "compatibility": "breaking",
            "detail": f"Column '{col}' removed (consumers depending on it will fail)",
            "before": _summarize_clause(clause),
            "after": None,
        })

    for col in sorted(common):
        col_changes = diff_single_column(col, before_schema[col], after_schema[col])
        changes.extend(col_changes)

    return changes


def diff_single_column(col_name, before, after):
    """Diff a single column's clause between two versions."""
    changes = []

    # Type change
    b_type = before.get("type")
    a_type = after.get("type")
    if b_type != a_type:
        is_breaking = (b_type, a_type) in BREAKING_TYPE_CHANGES
        changes.append({
            "column": col_name,
            "change_type": "type_changed",
            "compatibility": "breaking" if is_breaking else "forward",
            "detail": f"Type changed from '{b_type}' to '{a_type}'",
            "before": b_type,
            "after": a_type,
        })

    # Required change
    b_req = before.get("required", False)
    a_req = after.get("required", False)
    if b_req != a_req:
        if a_req and not b_req:
            compat = "breaking"
            detail = f"'{col_name}' changed from optional to required"
        else:
            compat = "backward"
            detail = f"'{col_name}' changed from required to optional"
        changes.append({
            "column": col_name,
            "change_type": "required_changed",
            "compatibility": compat,
            "detail": detail,
            "before": b_req,
            "after": a_req,
        })

    # Enum change
    b_enum = set(before.get("enum", []))
    a_enum = set(after.get("enum", []))
    if b_enum and a_enum and b_enum != a_enum:
        removed_vals = b_enum - a_enum
        added_vals = a_enum - b_enum
        if removed_vals:
            changes.append({
                "column": col_name,
                "change_type": "enum_values_removed",
                "compatibility": "breaking",
                "detail": f"Enum values removed: {sorted(removed_vals)}",
                "before": sorted(b_enum),
                "after": sorted(a_enum),
            })
        if added_vals:
            changes.append({
                "column": col_name,
                "change_type": "enum_values_added",
                "compatibility": "backward",
                "detail": f"Enum values added: {sorted(added_vals)}",
                "before": sorted(b_enum),
                "after": sorted(a_enum),
            })

    # Range change
    for bound in ("minimum", "maximum"):
        b_val = before.get(bound)
        a_val = after.get(bound)
        if b_val is not None and a_val is not None and b_val != a_val:
            if bound == "minimum" and a_val > b_val:
                compat = "breaking"
            elif bound == "maximum" and a_val < b_val:
                compat = "breaking"
            else:
                compat = "backward"
            changes.append({
                "column": col_name,
                "change_type": f"{bound}_changed",
                "compatibility": compat,
                "detail": f"{bound} changed from {b_val} to {a_val}",
                "before": b_val,
                "after": a_val,
            })

    # Pattern change
    b_pat = before.get("pattern")
    a_pat = after.get("pattern")
    if b_pat and a_pat and b_pat != a_pat:
        changes.append({
            "column": col_name,
            "change_type": "pattern_changed",
            "compatibility": "breaking",
            "detail": f"Regex pattern changed from '{b_pat}' to '{a_pat}'",
            "before": b_pat,
            "after": a_pat,
        })

    # Format change
    b_fmt = before.get("format")
    a_fmt = after.get("format")
    if b_fmt and a_fmt and b_fmt != a_fmt:
        changes.append({
            "column": col_name,
            "change_type": "format_changed",
            "compatibility": "breaking",
            "detail": f"Format changed from '{b_fmt}' to '{a_fmt}'",
            "before": b_fmt,
            "after": a_fmt,
        })

    return changes


def diff_constraints(before_constraints, after_constraints):
    """Diff cross-column constraints between two versions."""
    changes = []
    b_names = {c.get("name", str(i)): c for i, c in enumerate(before_constraints)}
    a_names = {c.get("name", str(i)): c for i, c in enumerate(after_constraints)}

    for name in sorted(set(a_names) - set(b_names)):
        changes.append({
            "column": f"constraint.{name}",
            "change_type": "constraint_added",
            "compatibility": "breaking",
            "detail": f"New constraint '{name}' added",
            "before": None,
            "after": a_names[name],
        })

    for name in sorted(set(b_names) - set(a_names)):
        changes.append({
            "column": f"constraint.{name}",
            "change_type": "constraint_removed",
            "compatibility": "backward",
            "detail": f"Constraint '{name}' removed",
            "before": b_names[name],
            "after": None,
        })

    return changes


def _summarize_clause(clause):
    """Create a compact summary of a schema clause."""
    summary = {"type": clause.get("type")}
    if clause.get("required"):
        summary["required"] = True
    if clause.get("format"):
        summary["format"] = clause["format"]
    if clause.get("enum"):
        summary["enum"] = clause["enum"]
    if clause.get("minimum") is not None:
        summary["minimum"] = clause["minimum"]
    if clause.get("maximum") is not None:
        summary["maximum"] = clause["maximum"]
    return summary


# ---------------------------------------------------------------------------
# Classification and reporting
# ---------------------------------------------------------------------------

def classify_evolution(changes):
    """Classify the overall schema evolution.

    Returns: 'backward', 'forward', 'full', or 'breaking'.
    """
    if not changes:
        return "none"

    compatibilities = {c["compatibility"] for c in changes}

    if "breaking" in compatibilities:
        return "breaking"
    if compatibilities == {"backward"}:
        return "backward"
    if compatibilities == {"forward"}:
        return "forward"
    if compatibilities == {"backward", "forward"}:
        return "full"
    return "backward"


def generate_rollback_plan(changes, before_snapshot):
    """Generate a rollback plan for breaking changes."""
    breaking = [c for c in changes if c["compatibility"] == "breaking"]
    if not breaking:
        return {"needed": False, "steps": []}

    steps = []
    for change in breaking:
        ct = change["change_type"]
        col = change["column"]

        if ct == "column_removed":
            steps.append(f"Re-add column '{col}' with original schema: {change['before']}")
        elif ct == "column_added" and change.get("after", {}).get("required"):
            steps.append(f"Remove required column '{col}' or make it optional")
        elif ct == "type_changed":
            steps.append(f"Revert '{col}' type from {change['after']} to {change['before']}")
        elif ct == "required_changed":
            steps.append(f"Revert '{col}' required status to {change['before']}")
        elif ct == "enum_values_removed":
            steps.append(f"Restore removed enum values for '{col}': {change['before']}")
        elif "minimum" in ct or "maximum" in ct:
            steps.append(f"Revert '{col}' {ct.replace('_changed','')} to {change['before']}")
        elif ct == "pattern_changed":
            steps.append(f"Revert '{col}' pattern to '{change['before']}'")
        elif ct == "format_changed":
            steps.append(f"Revert '{col}' format to '{change['before']}'")
        elif ct == "constraint_added":
            steps.append(f"Remove new constraint '{col}'")
        else:
            steps.append(f"Revert change to '{col}': {change['detail']}")

    steps.append(f"Revert to snapshot: {before_snapshot['path']}")

    return {"needed": True, "steps": steps}


def build_report(before, after, changes, classification, rollback):
    """Build the full evolution report."""
    breaking_count = sum(1 for c in changes if c["compatibility"] == "breaking")
    backward_count = sum(1 for c in changes if c["compatibility"] == "backward")
    forward_count = sum(1 for c in changes if c["compatibility"] == "forward")

    return {
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "contract_id": after["id"],
        "before_snapshot": before["path"],
        "after_snapshot": after["path"],
        "before_version": before["version"],
        "after_version": after["version"],
        "total_changes": len(changes),
        "breaking_changes": breaking_count,
        "backward_compatible_changes": backward_count,
        "forward_compatible_changes": forward_count,
        "classification": classification,
        "verdict": "SAFE" if classification != "breaking" else "BREAKING",
        "changes": changes,
        "rollback_plan": rollback,
        "summary": _build_summary(changes, classification),
    }


def _build_summary(changes, classification):
    """Build a plain-language summary of the evolution."""
    if not changes:
        return "No schema changes detected between snapshots."

    parts = []
    breaking = [c for c in changes if c["compatibility"] == "breaking"]
    safe = [c for c in changes if c["compatibility"] != "breaking"]

    if breaking:
        parts.append(
            f"{len(breaking)} breaking change(s) detected: "
            + "; ".join(c["detail"] for c in breaking[:5])
        )
    if safe:
        parts.append(f"{len(safe)} backward-compatible change(s).")

    if classification == "breaking":
        parts.append("Deployment requires coordinated migration of all consumers.")
    else:
        parts.append("Safe to deploy without consumer changes.")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Analyze schema evolution between snapshots"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--snapshots",
        help="Directory containing timestamped schema snapshots (uses two most recent)"
    )
    group.add_argument(
        "--before",
        help="Path to the older schema snapshot YAML"
    )
    parser.add_argument("--after", help="Path to the newer schema snapshot YAML")
    parser.add_argument(
        "--output", required=True,
        help="Output path for evolution report JSON"
    )
    args = parser.parse_args()

    if args.snapshots:
        print(f"Finding latest snapshot pair in {args.snapshots}...")
        before_path, after_path = find_snapshot_pair(args.snapshots)
    else:
        if not args.after:
            parser.error("--after is required when using --before")
        before_path, after_path = Path(args.before), Path(args.after)

    print(f"  Before: {before_path}")
    print(f"  After:  {after_path}")

    before = load_snapshot(before_path)
    after = load_snapshot(after_path)

    print(f"\nDiffing schemas ({len(before['schema'])} -> {len(after['schema'])} columns)...")
    changes = diff_columns(before["schema"], after["schema"])
    constraint_changes = diff_constraints(
        before.get("constraints", []), after.get("constraints", [])
    )
    changes.extend(constraint_changes)

    classification = classify_evolution(changes)
    rollback = generate_rollback_plan(changes, before)

    report = build_report(before, after, changes, classification, rollback)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n  Classification: {classification.upper()}")
    print(f"  Total changes: {len(changes)}")
    print(f"  Breaking: {report['breaking_changes']}")
    print(f"  Backward-compatible: {report['backward_compatible_changes']}")
    print(f"  Verdict: {report['verdict']}")
    print(f"\nEvolution report written to {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
