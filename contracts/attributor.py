"""ViolationAttributor — Traces contract violations to root causes.

Reads validation report failures, traverses the Week 4 lineage graph
for blast radius, and runs git blame against source repositories to
identify the commit that introduced the violation.

Usage:
    python contracts/attributor.py \
        --report validation_reports/week5_db.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
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


def attribute_violation(check_result, contract_id, repo_path, nodes, edges):
    """Attribute a single violation to its root cause.

    Precondition: check_result has status == 'FAIL'.
    Guarantee: returns a violation record with blame chain and blast radius.
    """
    col_name = check_result.get("column_name", "unknown")
    check_id = check_result.get("check_id", "unknown")

    # Find relevant lineage nodes
    matching_nodes = []
    for nid, node in nodes.items():
        node_label = node.get("label", "").lower()
        node_path = node.get("metadata", {}).get("path", "").lower()
        if col_name.lower().replace("payload_", "").replace("metadata_", "") in node_label:
            matching_nodes.append(nid)
        elif col_name.lower().replace("payload_", "").replace("metadata_", "") in node_path:
            matching_nodes.append(nid)

    # Compute blast radius from first matching node (or use contract-level estimate)
    blast = None
    if matching_nodes:
        blast = compute_blast_radius(matching_nodes[0], nodes, edges)
    else:
        blast = {
            "downstream_nodes": 0,
            "upstream_nodes": 0,
            "total_graph_nodes": len(nodes),
            "impact_ratio": 0.0,
            "downstream": [],
            "upstream": [],
        }

    # Git blame
    blame_chain = []
    source_files = infer_source_files(check_result, contract_id)

    if repo_path:
        # Search git log for commits related to the column
        search_terms = [
            col_name.replace("payload_", "").replace("metadata_", ""),
            check_result.get("check_type", ""),
        ]
        for term in search_terms:
            commits = git_log_search(repo_path, term, max_results=3)
            for c in commits:
                blame_chain.append({
                    "commit": c["commit"],
                    "message": c["message"],
                    "source": "git_log_search",
                    "search_term": term,
                })

        # Blame specific files
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

        # Recent commits on related files as fallback
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

    # Deduplicate blame chain by commit hash
    seen = set()
    unique_blame = []
    for entry in blame_chain:
        commit = entry.get("commit", "")
        if commit and commit not in seen:
            seen.add(commit)
            unique_blame.append(entry)

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
        "attributed_at": datetime.now(timezone.utc).isoformat(),
        "blame_chain": unique_blame[:10],
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

    repo_map = parse_repo_map(args.repo_map)
    repo_path = resolve_repo_for_contract(contract_id, repo_map)
    if repo_path:
        print(f"  Blame target: {repo_path}")
    else:
        print("  No repo mapping found; git blame disabled")

    print("\nAttributing violations...")
    violations = []
    for fail in failures:
        v = attribute_violation(fail, contract_id, repo_path, nodes, edges)
        violations.append(v)
        blame_count = len(v["blame_chain"])
        downstream = v["blast_radius"]["downstream_nodes"]
        print(
            f"  {v['check_id']:50s} "
            f"blame={blame_count:2d} commits  "
            f"blast={downstream:3d} downstream"
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
