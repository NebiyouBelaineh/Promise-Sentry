"""Tests for ViolationAttributor."""
import json
import tempfile
from pathlib import Path

import pytest

from contracts.attributor import (
    load_lineage,
    load_registry,
    find_downstream,
    find_upstream,
    compute_blast_radius,
    compute_blame_confidence,
    registry_blast_radius,
    parse_repo_map,
    resolve_repo_for_contract,
    infer_source_files,
    attribute_violation,
    git_recent_commits,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_EDGES = [
    {"source": "A", "target": "B", "relationship": "PRODUCES", "confidence": 0.9},
    {"source": "B", "target": "C", "relationship": "CONSUMES", "confidence": 0.8},
    {"source": "B", "target": "D", "relationship": "CONSUMES", "confidence": 0.7},
    {"source": "C", "target": "E", "relationship": "PRODUCES", "confidence": 0.6},
]

SAMPLE_NODES = {
    "A": {"node_id": "A", "label": "source_a", "metadata": {"path": "a.py"}},
    "B": {"node_id": "B", "label": "transform_b", "metadata": {"path": "b.py"}},
    "C": {"node_id": "C", "label": "output_c", "metadata": {"path": "c.py"}},
    "D": {"node_id": "D", "label": "output_d", "metadata": {"path": "d.py"}},
    "E": {"node_id": "E", "label": "final_e", "metadata": {"path": "e.py"}},
}

SAMPLE_FAILURE = {
    "check_id": "payload_application_id.uuid",
    "column_name": "payload_application_id",
    "check_type": "uuid_format",
    "status": "FAIL",
    "severity": "CRITICAL",
    "actual_value": "invalid_count=43",
    "expected": "format=uuid",
    "records_failing": 43,
    "sample_failing": ["APEX-0001", "APEX-0002"],
    "message": "payload_application_id has 43 values not matching UUID pattern.",
}


# ---------------------------------------------------------------------------
# Graph traversal tests
# ---------------------------------------------------------------------------

class TestFindDownstream:
    def test_finds_direct_children(self):
        result = find_downstream("A", SAMPLE_EDGES)
        target_ids = {r["node_id"] for r in result}
        assert "B" in target_ids

    def test_finds_transitive_descendants(self):
        result = find_downstream("A", SAMPLE_EDGES, max_depth=5)
        target_ids = {r["node_id"] for r in result}
        assert "C" in target_ids
        assert "D" in target_ids
        assert "E" in target_ids

    def test_empty_on_leaf_node(self):
        result = find_downstream("E", SAMPLE_EDGES)
        assert result == []

    def test_respects_max_depth(self):
        result = find_downstream("A", SAMPLE_EDGES, max_depth=1)
        target_ids = {r["node_id"] for r in result}
        assert "B" in target_ids
        assert "E" not in target_ids

    def test_includes_depth_field(self):
        result = find_downstream("A", SAMPLE_EDGES, max_depth=3)
        for entry in result:
            assert "depth" in entry
            assert entry["depth"] >= 1


class TestFindUpstream:
    def test_finds_direct_parents(self):
        result = find_upstream("B", SAMPLE_EDGES)
        source_ids = {r["node_id"] for r in result}
        assert "A" in source_ids

    def test_finds_transitive_ancestors(self):
        result = find_upstream("E", SAMPLE_EDGES, max_depth=5)
        source_ids = {r["node_id"] for r in result}
        assert "C" in source_ids
        assert "B" in source_ids
        assert "A" in source_ids

    def test_empty_on_root_node(self):
        result = find_upstream("A", SAMPLE_EDGES)
        assert result == []


class TestComputeBlastRadius:
    def test_returns_expected_structure(self):
        result = compute_blast_radius("A", SAMPLE_NODES, SAMPLE_EDGES)
        assert "downstream_nodes" in result
        assert "upstream_nodes" in result
        assert "total_graph_nodes" in result
        assert "impact_ratio" in result

    def test_root_has_all_downstream(self):
        result = compute_blast_radius("A", SAMPLE_NODES, SAMPLE_EDGES)
        assert result["downstream_nodes"] == 4
        assert result["upstream_nodes"] == 0

    def test_leaf_has_zero_downstream(self):
        result = compute_blast_radius("E", SAMPLE_NODES, SAMPLE_EDGES)
        assert result["downstream_nodes"] == 0

    def test_impact_ratio_calculated(self):
        result = compute_blast_radius("A", SAMPLE_NODES, SAMPLE_EDGES)
        assert result["impact_ratio"] == round(4 / 5, 4)


# ---------------------------------------------------------------------------
# Repo map and resolution tests
# ---------------------------------------------------------------------------

class TestParseRepoMap:
    def test_parses_single_mapping(self):
        result = parse_repo_map(["week5=/home/user/veritas"])
        assert result["week5"] == "/home/user/veritas"

    def test_parses_multiple_mappings(self):
        result = parse_repo_map(["week5=/a", "week3=/b"])
        assert len(result) == 2
        assert result["week3"] == "/b"

    def test_empty_input(self):
        assert parse_repo_map([]) == {}
        assert parse_repo_map(None) == {}

    def test_expands_home(self):
        result = parse_repo_map(["x=~/foo"])
        assert "~" not in result["x"]


class TestResolveRepo:
    def test_matches_contract_id(self):
        repo_map = {"week5": "/path/to/veritas", "week3": "/path/to/papermind"}
        assert resolve_repo_for_contract("week5-event-records", repo_map) == "/path/to/veritas"

    def test_returns_none_when_no_match(self):
        repo_map = {"week5": "/path"}
        assert resolve_repo_for_contract("week99-unknown", repo_map) is None


# ---------------------------------------------------------------------------
# Source file inference tests
# ---------------------------------------------------------------------------

class TestInferSourceFiles:
    def test_week5_payload_column(self):
        fail = {"column_name": "payload_application_id", "check_type": "uuid_format"}
        files = infer_source_files(fail, "week5-event-records")
        assert any("event_store" in f for f in files)

    def test_week5_agent_column(self):
        fail = {"column_name": "payload_agent_id", "check_type": "uuid_format"}
        files = infer_source_files(fail, "week5-event-records")
        assert any("agent" in f for f in files)

    def test_week3_confidence_column(self):
        fail = {"column_name": "extracted_facts_confidence", "check_type": "range"}
        files = infer_source_files(fail, "week3-extractions")
        assert any("extraction" in f for f in files)


# ---------------------------------------------------------------------------
# Lineage loading tests
# ---------------------------------------------------------------------------

class TestLoadLineage:
    def test_loads_from_jsonl(self, tmp_path):
        snap = {
            "snapshot_id": "test",
            "nodes": [{"node_id": "n1", "label": "x", "metadata": {}}],
            "edges": [{"source": "n1", "target": "n2", "relationship": "R", "confidence": 1.0}],
        }
        p = tmp_path / "lineage.jsonl"
        p.write_text(json.dumps(snap) + "\n")
        nodes, edges = load_lineage(str(p))
        assert "n1" in nodes
        assert len(edges) == 1

    def test_returns_empty_on_missing_file(self):
        nodes, edges = load_lineage("/nonexistent/path.jsonl")
        assert nodes == {}
        assert edges == []

    def test_returns_empty_on_none(self):
        nodes, edges = load_lineage(None)
        assert nodes == {}
        assert edges == []


# ---------------------------------------------------------------------------
# Attribution integration test
# ---------------------------------------------------------------------------

class TestAttributeViolation:
    def test_returns_expected_structure(self):
        result = attribute_violation(
            SAMPLE_FAILURE, "week5-event-records", None, SAMPLE_NODES, SAMPLE_EDGES
        )
        assert "violation_id" in result
        assert "check_id" in result
        assert "blame_chain" in result
        assert "blast_radius" in result
        assert result["severity"] == "CRITICAL"
        assert result["contract_id"] == "week5-event-records"

    def test_handles_no_repo(self):
        result = attribute_violation(
            SAMPLE_FAILURE, "week5-event-records", None, {}, []
        )
        assert result["blame_chain"] == []
        assert result["blast_radius"]["lineage_downstream_nodes"] == 0

    def test_includes_registry_subscribers(self):
        subs = [
            {
                "contract_id": "week5-event-records",
                "subscriber_id": "week7-enforcer",
                "subscriber_team": "week7",
                "fields_consumed": ["application_id"],
                "breaking_fields": [
                    {"field": "application_id", "reason": "join key"}
                ],
                "validation_mode": "WARN",
                "contact": "team@org.com",
            }
        ]
        result = attribute_violation(
            SAMPLE_FAILURE, "week5-event-records", None, {}, [],
            subscriptions=subs,
        )
        assert result["blast_radius"]["registry_subscriber_count"] == 1
        assert result["blast_radius"]["registry_subscribers"][0]["subscriber_id"] == "week7-enforcer"
        assert "affected_pipelines" in result["blast_radius"]
        assert "affected_nodes" in result["blast_radius"]
        assert "contamination_depth" in result["blast_radius"]["registry_subscribers"][0]


class TestGitRecentCommits:
    def test_returns_list_on_invalid_repo(self):
        result = git_recent_commits("/nonexistent/repo")
        assert result == []


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

SAMPLE_SUBSCRIPTIONS = [
    {
        "contract_id": "week5-event-records",
        "subscriber_id": "week7-enforcer",
        "subscriber_team": "week7",
        "fields_consumed": ["event_id", "event_type", "payload"],
        "breaking_fields": [
            {"field": "event_type", "reason": "routing"},
            {"field": "payload", "reason": "parsing"},
        ],
        "validation_mode": "WARN",
        "contact": "team@org.com",
    },
    {
        "contract_id": "week3-document-refinery-extractions",
        "subscriber_id": "week4-cartographer",
        "subscriber_team": "week4",
        "fields_consumed": ["doc_id", "extracted_facts"],
        "breaking_fields": [
            {"field": "extracted_facts.confidence", "reason": "ranking"},
        ],
        "validation_mode": "ENFORCE",
        "contact": "team@org.com",
    },
]


class TestRegistryBlastRadius:
    def test_finds_matching_subscribers(self):
        result = registry_blast_radius(
            "week5-event-records", "payload_event_type", SAMPLE_SUBSCRIPTIONS
        )
        assert len(result) == 1
        assert result[0]["subscriber_id"] == "week7-enforcer"

    def test_no_match_returns_empty(self):
        result = registry_blast_radius(
            "week99-unknown", "some_field", SAMPLE_SUBSCRIPTIONS
        )
        assert result == []

    def test_matches_on_breaking_field(self):
        result = registry_blast_radius(
            "week3-document-refinery-extractions",
            "extracted_facts_confidence",
            SAMPLE_SUBSCRIPTIONS,
        )
        assert len(result) == 1
        assert result[0]["subscriber_id"] == "week4-cartographer"


class TestLoadRegistry:
    def test_loads_yaml(self, tmp_path):
        p = tmp_path / "subs.yaml"
        p.write_text("subscriptions:\n  - contract_id: test\n    subscriber_id: s1\n")
        result = load_registry(str(p))
        assert len(result) == 1

    def test_missing_file(self):
        result = load_registry("/nonexistent.yaml")
        assert result == []

    def test_none_path(self):
        result = load_registry(None)
        assert result == []


# ---------------------------------------------------------------------------
# Blame confidence tests
# ---------------------------------------------------------------------------

class TestComputeBlameConfidence:
    def test_recent_commit_high_confidence(self):
        assert compute_blame_confidence(0) == 1.0

    def test_old_commit_low_confidence(self):
        assert compute_blame_confidence(10) == 0.05

    def test_lineage_hops_reduce(self):
        no_hops = compute_blame_confidence(2)
        with_hops = compute_blame_confidence(2, lineage_hops=2)
        assert with_hops < no_hops

    def test_never_below_minimum(self):
        assert compute_blame_confidence(100, lineage_hops=10) == 0.05

    def test_never_above_one(self):
        assert compute_blame_confidence(-5) == 1.0
