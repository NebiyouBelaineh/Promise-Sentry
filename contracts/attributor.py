"""ViolationAttributor — Traces contract violations to root causes.

Reads validation report failures, queries the contract registry for
subscriber blast radius, traverses the Week 4 lineage graph for
enrichment, and runs git blame against source repositories to
identify the commit that introduced the violation.

Usage:
    python contracts/attributor.py \
        --report validation_reports/week5_db.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --registry contract_registry/subscriptions.yaml \
        --repo-map week5=~/tenx/week5/veritas-stream \
        --output violation_log/violations.jsonl
"""
import argparse
import json
import subprocess
import uuid
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml


# ---------------------------------------------------------------------------
# Lineage graph loading
# ---------------------------------------------------------------------------

def load_lineage(path):
    """Load all lineage snapshots and build a unified graph."""
    nodes = {}
    edges = []
    if not path or not Path(path).exists():
        return nodes, edges

    with open(path) as f:
        for line in f:
            if not line.strip():
                continue
            snap = json.loads(line)
            for node in snap.get("nodes", []):
                nodes[node["node_id"]] = node
            edges.extend(snap.get("edges", []))
    return nodes, edges


def find_downstream(node_id, edges, max_depth=5):
    """BFS traversal to find all downstream nodes from a given node."""
    visited = set()
    queue = [node_id]
    depth = 0
    downstream = []

    while queue and depth < max_depth:
        next_queue = []
        for current in queue:
            if current in visited:
                continue
            visited.add(current)
            for edge in edges:
                if edge["source"] == current and edge["target"] not in visited:
                    downstream.append({
                        "node_id": edge["target"],
                        "relationship": edge.get("relationship", "UNKNOWN"),
                        "confidence": edge.get("confidence", 0.0),
                        "depth": depth + 1,
                    })
                    next_queue.append(edge["target"])
        queue = next_queue
        depth += 1

    return downstream


def find_upstream(node_id, edges, max_depth=5):
    """BFS traversal to find all upstream nodes (producers) of a given node."""
    visited = set()
    queue = [node_id]
    depth = 0
    upstream = []

    while queue and depth < max_depth:
        next_queue = []
        for current in queue:
            if current in visited:
                continue
            visited.add(current)
            for edge in edges:
                if edge["target"] == current and edge["source"] not in visited:
                    upstream.append({
                        "node_id": edge["source"],
                        "relationship": edge.get("relationship", "UNKNOWN"),
                        "confidence": edge.get("confidence", 0.0),
                        "depth": depth + 1,
                    })
                    next_queue.append(edge["source"])
        queue = next_queue
        depth += 1

    return upstream


def compute_blast_radius(node_id, nodes, edges):
    """Compute blast radius: how many downstream nodes are affected."""
    downstream = find_downstream(node_id, edges)
    upstream = find_upstream(node_id, edges)

    downstream_count = len(downstream)
    total_nodes = len(nodes) if nodes else 1

    return {
        "downstream_nodes": downstream_count,
        "upstream_nodes": len(upstream),
        "total_graph_nodes": total_nodes,
        "impact_ratio": round(downstream_count / max(total_nodes, 1), 4),
        "downstream": downstream[:10],
        "upstream": upstream[:10],
    }


# ---------------------------------------------------------------------------
# Contract registry
# ---------------------------------------------------------------------------

def load_registry(path):
    """Load the contract registry subscriptions YAML."""
    if not path or not Path(path).exists():
        return []
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("subscriptions", [])


def registry_blast_radius(contract_id, failing_field, subscriptions):
    """Query registry for subscribers affected by a breaking field change.

    This is the primary blast radius source. The lineage graph is
    enrichment only, not a replacement.
    """
    affected = []
    for sub in subscriptions:
        if sub.get("contract_id") != contract_id:
            continue
        breaking = sub.get("breaking_fields", [])
        # Match failing field against breaking_fields and fields_consumed.
        # Both use underscore notation matching runner check column names.
        stripped = failing_field.replace("payload_", "").replace("metadata_", "")
        field_match = any(
            failing_field == bf.get("field", "")
            or stripped == bf.get("field", "")
            or bf.get("field", "") in failing_field
            or failing_field in bf.get("field", "")
            for bf in breaking
        )
        field_consumed = any(
            failing_field == fc or stripped == fc
            or fc in failing_field or failing_field in fc
            for fc in sub.get("fields_consumed", [])
        )
        if field_match or field_consumed:
            affected.append({
                "subscriber_id": sub.get("subscriber_id"),
                "subscriber_team": sub.get("subscriber_team"),
                "fields_consumed": sub.get("fields_consumed"),
                "breaking_reason": next(
                    (bf.get("reason") for bf in breaking
                     if failing_field in bf.get("field", "") or bf.get("field", "") in failing_field),
                    None
                ),
                "validation_mode": sub.get("validation_mode"),
                "contact": sub.get("contact"),
            })
    return affected


# ---------------------------------------------------------------------------
# Blame confidence scoring
# ---------------------------------------------------------------------------

def compute_blame_confidence(days_since_commit, lineage_hops=0):
    """Compute blame confidence per spec formula.

    base = 1.0 - (days_since_commit * 0.1)
    Reduce by 0.2 for each lineage hop.
    Clamp to [0.05, 1.0].
    """
    base = 1.0 - (days_since_commit * 0.1)
    score = base - (lineage_hops * 0.2)
    return round(max(min(score, 1.0), 0.05), 2)


# ---------------------------------------------------------------------------
# Git blame
# ---------------------------------------------------------------------------

def parse_repo_map(repo_map_args):
    """Parse repo-map args like 'week5=~/tenx/week5/veritas-stream'."""
    mapping = {}
    if not repo_map_args:
        return mapping
    for item in repo_map_args:
        if "=" in item:
            key, path = item.split("=", 1)
            mapping[key.strip()] = str(Path(path.strip()).expanduser())
    return mapping


def resolve_repo_for_contract(contract_id, repo_map):
    """Determine which repo to blame based on contract_id."""
    for key, path in repo_map.items():
        if key in contract_id:
            return path
    return None


def git_blame_file(repo_path, file_path, line_num=None):
    """Run git blame on a file and return structured results.

    Precondition: repo_path is a valid git repository.
    Guarantee: returns a list of blame entries, or empty list on failure.
    Raises: nothing (errors are caught and logged).
    """
    try:
        cmd = ["git", "-C", repo_path, "blame", "--porcelain"]
        if line_num:
            cmd.extend(["-L", f"{line_num},{line_num}"])
        cmd.append(file_path)

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return []

        entries = []
        current = {}
        for raw_line in result.stdout.splitlines():
            if raw_line.startswith("author "):
                current["author"] = raw_line[7:]
            elif raw_line.startswith("author-mail "):
                current["author_email"] = raw_line[12:].strip("<>")
            elif raw_line.startswith("author-time "):
                current["author_time"] = raw_line[12:]
            elif raw_line.startswith("summary "):
                current["summary"] = raw_line[8:]
            elif len(raw_line) == 40 or (len(raw_line) > 40 and raw_line[40] == " "):
                if current:
                    entries.append(current)
                commit_hash = raw_line[:40]
                current = {"commit": commit_hash}

        if current and "commit" in current:
            entries.append(current)

        return entries
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


def git_log_search(repo_path, search_term, max_results=5):
    """Search git log for commits mentioning a term."""
    try:
        cmd = [
            "git", "-C", repo_path, "log",
            f"--grep={search_term}", "--oneline",
            f"-{max_results}",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return []

        commits = []
        for raw_line in result.stdout.strip().splitlines():
            if raw_line:
                parts = raw_line.split(" ", 1)
                commits.append({
                    "commit": parts[0],
                    "message": parts[1] if len(parts) > 1 else "",
                })
        return commits
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


def git_recent_commits(repo_path, file_pattern=None, max_results=10):
    """Get recent commits, optionally filtered by file pattern."""
    try:
        cmd = ["git", "-C", repo_path, "log", "--oneline", f"-{max_results}"]
        if file_pattern:
            cmd.extend(["--", file_pattern])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return []

        commits = []
        for raw_line in result.stdout.strip().splitlines():
            if raw_line:
                parts = raw_line.split(" ", 1)
                commits.append({
                    "commit": parts[0],
                    "message": parts[1] if len(parts) > 1 else "",
                })
        return commits
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


# ---------------------------------------------------------------------------
# Violation attribution
# ---------------------------------------------------------------------------

def infer_source_files(check_result, contract_id):
    """Infer which source files likely relate to a violation."""
    col = check_result.get("column_name", "")
    check_type = check_result.get("check_type", "")
    files = []

    # Map column names to likely source files
    if "week5" in contract_id or "event" in contract_id:
        if "payload" in col:
            files.append("ledger/event_store.py")
            files.append("datagen/generate_all.py")
        if "agent" in col:
            files.append("agents/")
        if "metadata" in col:
            files.append("ledger/event_store.py")
    elif "week3" in contract_id or "extraction" in contract_id:
        if "confidence" in col:
            files.append("extraction/")
            files.append("pipeline/")
        if "extracted_facts" in col:
            files.append("extraction/")

    return files


def attribute_violation(check_result, contract_id, repo_path, nodes, edges,
                        subscriptions=None):
    """Attribute a single violation to its root cause.

    Precondition: check_result has status == 'FAIL'.
    Guarantee: returns a violation record with blame chain and blast radius.

    Blast radius is computed in two steps:
    1. Registry query (primary): find subscribers of this contract whose
       breaking_fields include the failing column.
    2. Lineage traversal (enrichment): find transitive downstream nodes
       from the lineage graph to add contamination depth.
    """
    col_name = check_result.get("column_name", "unknown")
    check_id = check_result.get("check_id", "unknown")
    now = datetime.now(timezone.utc)

    # Step 1: Registry blast radius (primary)
    registry_affected = []
    if subscriptions:
        registry_affected = registry_blast_radius(
            contract_id, col_name, subscriptions
        )

    # Step 2: Lineage traversal (enrichment)
    matching_nodes = []
    for nid, node in nodes.items():
        node_label = node.get("label", "").lower()
        node_path = node.get("metadata", {}).get("path", "").lower()
        if col_name.lower().replace("payload_", "").replace("metadata_", "") in node_label:
            matching_nodes.append(nid)
        elif col_name.lower().replace("payload_", "").replace("metadata_", "") in node_path:
            matching_nodes.append(nid)

    lineage_blast = None
    if matching_nodes:
        lineage_blast = compute_blast_radius(matching_nodes[0], nodes, edges)
    else:
        lineage_blast = {
            "downstream_nodes": 0,
            "upstream_nodes": 0,
            "total_graph_nodes": len(nodes),
            "impact_ratio": 0.0,
            "downstream": [],
            "upstream": [],
        }

    # Merge blast radius: registry is authoritative, lineage enriches
    # Derive affected_pipelines from subscriber IDs
    affected_pipelines = [
        f"{s['subscriber_id']}-pipeline" for s in registry_affected
    ]
    # Derive affected_nodes from lineage downstream
    affected_nodes = [
        d["node_id"] for d in lineage_blast.get("downstream", [])[:10]
    ]
    # Add contamination_depth from lineage traversal.
    # Each registry subscriber gets contamination_depth = max lineage hop
    # depth from the matched node. Direct subscribers = 1, transitive = 2+.
    # If no lineage match, depth = 1 (direct subscriber by registry).
    max_lineage_depth = 0
    downstream_list = lineage_blast.get("downstream", [])
    if downstream_list:
        max_lineage_depth = max(d.get("depth", 1) for d in downstream_list)
    for i, sub in enumerate(registry_affected):
        # Direct registry subscribers get depth 1; if lineage shows
        # transitive contamination beyond direct, add that depth.
        sub["contamination_depth"] = max(1, max_lineage_depth) if downstream_list else 1

    blast = {
        "registry_subscribers": registry_affected,
        "registry_subscriber_count": len(registry_affected),
        "affected_nodes": affected_nodes,
        "affected_pipelines": affected_pipelines,
        "lineage_downstream_nodes": lineage_blast["downstream_nodes"],
        "lineage_upstream_nodes": lineage_blast["upstream_nodes"],
        "total_graph_nodes": lineage_blast["total_graph_nodes"],
        "impact_ratio": lineage_blast["impact_ratio"],
        "estimated_records": check_result.get("records_failing", 0),
    }

    # Step 3: Git blame for cause attribution
    blame_chain = []
    source_files = infer_source_files(check_result, contract_id)

    if repo_path:
        search_terms = [
            col_name.replace("payload_", "").replace("metadata_", ""),
            check_result.get("check_type", ""),
        ]
        for term in search_terms:
            commits = git_log_search(repo_path, term, max_results=3)
            for c in commits:
                # Compute days since commit for confidence scoring
                blame_chain.append({
                    "commit": c["commit"],
                    "message": c["message"],
                    "source": "git_log_search",
                    "search_term": term,
                })

        for fpath in source_files:
            entries = git_blame_file(repo_path, fpath)
            for entry in entries[:3]:
                blame_chain.append({
                    "commit": entry.get("commit", "unknown"),
                    "author": entry.get("author", "unknown"),
                    "author_email": entry.get("author_email", ""),
                    "summary": entry.get("summary", ""),
                    "source": "git_blame",
                    "file": fpath,
                })

        if not blame_chain:
            for fpath in source_files:
                commits = git_recent_commits(repo_path, fpath, max_results=3)
                for c in commits:
                    blame_chain.append({
                        "commit": c["commit"],
                        "message": c["message"],
                        "source": "recent_commits",
                        "file": fpath,
                    })

    # Deduplicate and add confidence scores + rank
    seen = set()
    unique_blame = []
    for entry in blame_chain:
        commit = entry.get("commit", "")
        if commit and commit not in seen:
            seen.add(commit)
            # Parse author_time for confidence calculation
            days_ago = 7  # default assumption
            author_time = entry.get("author_time")
            if author_time:
                try:
                    commit_dt = datetime.fromtimestamp(int(author_time), tz=timezone.utc)
                    days_ago = (now - commit_dt).days
                except (ValueError, OSError):
                    pass
            entry["confidence_score"] = compute_blame_confidence(days_ago)
            unique_blame.append(entry)

    # Sort by confidence descending, limit to 5
    unique_blame.sort(key=lambda e: e.get("confidence_score", 0), reverse=True)
    for rank, entry in enumerate(unique_blame[:5], 1):
        entry["rank"] = rank

    return {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_id,
        "column_name": col_name,
        "check_type": check_result.get("check_type", "unknown"),
        "severity": check_result.get("severity", "UNKNOWN"),
        "message": check_result.get("message", ""),
        "records_failing": check_result.get("records_failing", 0),
        "sample_failing": check_result.get("sample_failing", [])[:5],
        "contract_id": contract_id,
        "detected_at": now.isoformat(),
        "blame_chain": unique_blame[:5],
        "blast_radius": blast,
        "source_files": source_files,
        "lineage_nodes_matched": matching_nodes[:5],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_validation_report(path):
    with open(path) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Attribute contract violations to root causes"
    )
    parser.add_argument(
        "--report", required=True,
        help="Path to validation report JSON"
    )
    parser.add_argument(
        "--lineage", default=None,
        help="Path to Week 4 lineage_snapshots.jsonl"
    )
    parser.add_argument(
        "--registry", default="contract_registry/subscriptions.yaml",
        help="Path to contract registry subscriptions YAML"
    )
    parser.add_argument(
        "--repo-map", nargs="*", default=[],
        help="Mapping of data source to repo path (e.g. week5=~/tenx/week5/veritas-stream)"
    )
    parser.add_argument(
        "--output", required=True,
        help="Output path for violation log JSONL"
    )
    args = parser.parse_args()

    print(f"Loading validation report from {args.report}...")
    report = load_validation_report(args.report)
    contract_id = report.get("contract_id", "unknown")

    failures = [r for r in report["results"] if r["status"] == "FAIL"]
    print(f"  {len(failures)} failing checks to attribute")

    if not failures:
        print("  No failures to attribute. Exiting.")
        return 0

    print("Loading lineage graph...")
    nodes, edges = load_lineage(args.lineage)
    print(f"  {len(nodes)} nodes, {len(edges)} edges loaded")

    print("Loading contract registry...")
    subscriptions = load_registry(args.registry)
    print(f"  {len(subscriptions)} subscriptions loaded")

    repo_map = parse_repo_map(args.repo_map)
    repo_path = resolve_repo_for_contract(contract_id, repo_map)
    if repo_path:
        print(f"  Blame target: {repo_path}")
    else:
        print("  No repo mapping found; git blame disabled")

    print("\nAttributing violations...")
    violations = []
    for fail in failures:
        v = attribute_violation(fail, contract_id, repo_path, nodes, edges,
                                subscriptions=subscriptions)
        violations.append(v)
        blame_count = len(v["blame_chain"])
        reg_subs = v["blast_radius"]["registry_subscriber_count"]
        lineage_dn = v["blast_radius"]["lineage_downstream_nodes"]
        print(
            f"  {v['check_id']:50s} "
            f"blame={blame_count:2d} commits  "
            f"registry={reg_subs:d} subs  "
            f"lineage={lineage_dn:3d} downstream"
        )

    # Write violation log
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Append mode: preserve existing violations
    existing = []
    if output_path.exists():
        with open(output_path) as f:
            for line in f:
                if line.strip():
                    existing.append(json.loads(line))

    with open(output_path, "w") as f:
        for v in existing:
            f.write(json.dumps(v) + "\n")
        for v in violations:
            f.write(json.dumps(v, default=str) + "\n")

    total = len(existing) + len(violations)
    print(f"\nWrote {len(violations)} violations to {output_path}")
    print(f"  Total violations in log: {total}")

    # Summary
    severities = {}
    for v in violations:
        sev = v["severity"]
        severities[sev] = severities.get(sev, 0) + 1
    for sev, count in sorted(severities.items()):
        print(f"  {sev}: {count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
