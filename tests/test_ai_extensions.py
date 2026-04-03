"""Tests for AI contract extensions."""
import json
import numpy as np
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from contracts.ai_extensions import (
    sample_texts,
    check_embedding_drift,
    check_prompt_input_schema,
    check_output_schema_violation_rate,
    EXPECTED_EXTRACTION_SCHEMA,
    EXPECTED_VERDICT_SCHEMA,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_EXTRACTION_RECORDS = [
    {
        "doc_id": "abc-123",
        "source_path": "/docs/file.pdf",
        "extraction_model": "gpt-4",
        "extracted_at": "2026-03-01T00:00:00Z",
        "extracted_facts": [
            {"fact_id": "f1", "text": "Revenue was $1M", "confidence": 0.85, "page_ref": 1},
            {"fact_id": "f2", "text": "EBITDA margin 15%", "confidence": 0.72, "page_ref": 2},
        ],
    },
    {
        "doc_id": "def-456",
        "source_path": "/docs/file2.pdf",
        "extraction_model": "gpt-4",
        "extracted_at": "2026-03-02T00:00:00Z",
        "extracted_facts": [
            {"fact_id": "f3", "text": "Total assets $5M", "confidence": 0.91, "page_ref": 1},
        ],
    },
]

SAMPLE_VERDICT_RECORDS = [
    {
        "verdict_id": "v1",
        "overall_verdict": "PASS",
        "confidence": 0.85,
        "scores": {"criterion_a": {"score": 4, "evidence": []}},
        "evaluated_at": "2026-03-01T00:00:00Z",
    },
    {
        "verdict_id": "v2",
        "overall_verdict": "FAIL",
        "confidence": 0.60,
        "scores": {"criterion_a": {"score": 2, "evidence": []}},
        "evaluated_at": "2026-03-02T00:00:00Z",
    },
]


# ---------------------------------------------------------------------------
# Extension 1: Embedding drift tests
# ---------------------------------------------------------------------------

class TestSampleTexts:
    def test_extracts_texts_from_facts(self):
        texts = sample_texts(SAMPLE_EXTRACTION_RECORDS)
        assert len(texts) == 3
        assert "Revenue was $1M" in texts

    def test_limits_to_sample_size(self):
        # Create records with many facts
        big_records = [{
            "extracted_facts": [
                {"text": f"fact {i}", "confidence": 0.5} for i in range(500)
            ]
        }]
        texts = sample_texts(big_records, n=50)
        assert len(texts) == 50

    def test_skips_empty_text(self):
        records = [{"extracted_facts": [{"text": "", "confidence": 0.5}, {"text": "valid", "confidence": 0.8}]}]
        texts = sample_texts(records)
        assert len(texts) == 1
        assert texts[0] == "valid"

    def test_handles_missing_facts(self):
        records = [{"extracted_facts": []}, {"no_facts": True}]
        texts = sample_texts(records)
        assert texts == []


class TestCheckEmbeddingDrift:
    def test_error_on_empty_texts(self):
        result = check_embedding_drift([])
        assert result["status"] == "ERROR"

    @patch("contracts.ai_extensions.embed_texts")
    def test_baseline_set_on_first_run(self, mock_embed, tmp_path):
        mock_embed.return_value = np.random.rand(5, 768)
        baseline_path = tmp_path / "baseline.npz"
        result = check_embedding_drift(
            ["text1", "text2", "text3", "text4", "text5"],
            baseline_path=baseline_path,
        )
        assert result["status"] == "BASELINE_SET"
        assert baseline_path.exists()
        assert result["sample_count"] == 5

    @patch("contracts.ai_extensions.embed_texts")
    def test_pass_when_no_drift(self, mock_embed, tmp_path):
        centroid = np.random.rand(768)
        centroid = centroid / np.linalg.norm(centroid)

        # Store baseline
        baseline_path = tmp_path / "baseline.npz"
        np.savez(str(baseline_path), centroid=centroid, sample_count=5)

        # Return nearly identical embeddings
        vecs = np.tile(centroid, (5, 1)) + np.random.rand(5, 768) * 0.001
        mock_embed.return_value = vecs

        result = check_embedding_drift(
            ["t1", "t2", "t3", "t4", "t5"],
            baseline_path=baseline_path,
        )
        assert result["status"] == "PASS"
        assert result["drift_score"] < 0.15

    @patch("contracts.ai_extensions.embed_texts")
    def test_fail_when_large_drift(self, mock_embed, tmp_path):
        # Store baseline pointing in one direction
        baseline_centroid = np.zeros(768)
        baseline_centroid[0] = 1.0
        baseline_path = tmp_path / "baseline.npz"
        np.savez(str(baseline_path), centroid=baseline_centroid)

        # Return embeddings pointing in opposite direction
        opposite = np.zeros((5, 768))
        opposite[:, 1] = 1.0
        mock_embed.return_value = opposite

        result = check_embedding_drift(
            ["t1", "t2", "t3", "t4", "t5"],
            baseline_path=baseline_path,
        )
        assert result["status"] == "FAIL"
        assert result["drift_score"] > 0.15

    @patch("contracts.ai_extensions.embed_texts")
    def test_handles_embedding_error(self, mock_embed):
        mock_embed.side_effect = ConnectionError("Ollama down")
        result = check_embedding_drift(["text"])
        assert result["status"] == "ERROR"
        assert "Ollama down" in result["message"]


# ---------------------------------------------------------------------------
# Extension 2: Prompt input schema tests
# ---------------------------------------------------------------------------

class TestCheckPromptInputSchema:
    def test_pass_on_valid_records(self):
        result = check_prompt_input_schema(SAMPLE_EXTRACTION_RECORDS)
        assert result["status"] == "PASS"
        assert result["violations_found"] == 0

    def test_fails_on_missing_required_field(self):
        bad_records = [{"source_path": "/x", "extraction_model": "m", "extracted_at": "t", "extracted_facts": []}]
        result = check_prompt_input_schema(bad_records)
        assert result["violations_found"] >= 1
        assert any("doc_id" in v["issue"] for v in result["sample_violations"])

    def test_fails_on_bad_confidence(self):
        bad_records = [{
            "doc_id": "x",
            "source_path": "/x",
            "extraction_model": "m",
            "extracted_at": "t",
            "extracted_facts": [{"fact_id": "f1", "text": "t", "confidence": 85.0}],
        }]
        result = check_prompt_input_schema(bad_records, quarantine_path=None)
        assert result["violations_found"] >= 1
        assert any("Confidence" in v["issue"] for v in result["sample_violations"])

    def test_quarantines_bad_records(self, tmp_path):
        bad_records = [
            {"doc_id": "x", "source_path": "/x", "extraction_model": "m",
             "extracted_at": "t", "extracted_facts": [{"fact_id": "f1", "text": "t", "confidence": 85.0}]},
            {"doc_id": "y", "source_path": "/y", "extraction_model": "m",
             "extracted_at": "t", "extracted_facts": [{"fact_id": "f2", "text": "t", "confidence": 0.9}]},
        ]
        q_path = tmp_path / "quarantine.jsonl"
        result = check_prompt_input_schema(bad_records, quarantine_path=str(q_path))
        assert result["quarantined_count"] == 1
        assert q_path.exists()
        with open(q_path) as f:
            lines = f.readlines()
        assert len(lines) == 1

    def test_fails_on_non_list_facts(self):
        bad_records = [{
            "doc_id": "x",
            "source_path": "/x",
            "extraction_model": "m",
            "extracted_at": "t",
            "extracted_facts": "not a list",
        }]
        result = check_prompt_input_schema(bad_records)
        assert result["violations_found"] >= 1

    def test_empty_records(self):
        result = check_prompt_input_schema([])
        assert result["status"] == "PASS"
        assert result["total_records"] == 0


# ---------------------------------------------------------------------------
# Extension 3: Output violation rate tests
# ---------------------------------------------------------------------------

class TestCheckOutputSchemaViolationRate:
    def test_pass_on_valid_verdicts(self):
        result = check_output_schema_violation_rate(SAMPLE_VERDICT_RECORDS)
        assert result["status"] == "PASS"
        assert result["violations_found"] == 0

    def test_fails_on_invalid_verdict_enum(self):
        bad = [{"verdict_id": "v1", "overall_verdict": "MAYBE", "confidence": 0.5,
                "scores": {}, "evaluated_at": "t"}]
        result = check_output_schema_violation_rate(bad)
        assert result["violations_found"] >= 1
        assert any("Invalid verdict" in v["issue"] for v in result["sample_violations"])

    def test_fails_on_bad_confidence(self):
        bad = [{"verdict_id": "v1", "overall_verdict": "PASS", "confidence": 1.5,
                "scores": {}, "evaluated_at": "t"}]
        result = check_output_schema_violation_rate(bad)
        assert result["violations_found"] >= 1

    def test_fails_on_bad_score_range(self):
        bad = [{"verdict_id": "v1", "overall_verdict": "PASS", "confidence": 0.8,
                "scores": {"x": {"score": 10}}, "evaluated_at": "t"}]
        result = check_output_schema_violation_rate(bad)
        assert result["violations_found"] >= 1

    def test_declining_trend_when_rate_drops(self):
        # 0 violations with a positive baseline means rate dropped
        result = check_output_schema_violation_rate(
            SAMPLE_VERDICT_RECORDS, baseline_rate=0.0001
        )
        assert result["trend"] == "declining"

    def test_detects_rising_trend(self):
        # Create records with some violations
        bad = [
            {"verdict_id": "v1", "overall_verdict": "INVALID", "confidence": 0.5,
             "scores": {}, "evaluated_at": "t"},
            {"verdict_id": "v2", "overall_verdict": "PASS", "confidence": 0.8,
             "scores": {}, "evaluated_at": "t"},
        ]
        result = check_output_schema_violation_rate(bad, baseline_rate=0.01)
        assert result["trend"] == "rising"

    def test_empty_records(self):
        result = check_output_schema_violation_rate([])
        assert result["status"] == "PASS"
        assert result["total_records"] == 0

    def test_missing_required_fields(self):
        bad = [{"verdict_id": "v1"}]
        result = check_output_schema_violation_rate(bad)
        assert result["violations_found"] >= 1
