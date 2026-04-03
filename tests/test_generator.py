"""Tests for ContractGenerator."""
import json
import sys
import pytest
from unittest.mock import patch, MagicMock

from contracts.generator import annotate_ambiguous_columns, write_baselines


class TestAnnotateAmbiguousColumns:
    def test_skips_when_no_api_key(self):
        contract = {"schema": {"col_a": {"description": "Identifier field (string). Required."}}}
        with patch.dict("os.environ", {}, clear=True):
            result = annotate_ambiguous_columns(contract, {"col_a": {}}, None)
        assert "llm_annotations" not in result["schema"]["col_a"]

    def test_skips_when_anthropic_import_fails(self):
        contract = {"schema": {"col_a": {"description": "Identifier field (string). Required."}}}
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            # Temporarily remove anthropic from sys.modules to force ImportError
            saved = sys.modules.pop("anthropic", None)
            with patch.dict("sys.modules", {"anthropic": None}):
                result = annotate_ambiguous_columns(contract, {"col_a": {}}, None)
            if saved:
                sys.modules["anthropic"] = saved
        assert "llm_annotations" not in result["schema"]["col_a"]

    def test_returns_contract_unchanged_on_no_ambiguous(self):
        contract = {"schema": {"revenue": {"description": "Total annual revenue in USD."}}}
        with patch.dict("os.environ", {}, clear=True):
            result = annotate_ambiguous_columns(contract, {}, None)
        assert result == contract

    def test_annotates_ambiguous_column(self):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(
            text='{"description": "Application ID", "business_rule": "non-null", "cross_column": "none"}'
        )]
        mock_client.messages.create.return_value = mock_response

        mock_anthropic = MagicMock()
        mock_anthropic.Anthropic.return_value = mock_client

        contract = {"schema": {
            "payload_app_id": {
                "description": "Identifier field (string). Required — must not be null.",
                "type": "string",
            }
        }}
        profiles = {"payload_app_id": {"dtype": "object", "sample_values": ["APEX-001"]}}

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
                result = annotate_ambiguous_columns(contract, profiles, None)

        assert "llm_annotations" in result["schema"]["payload_app_id"]
        assert result["schema"]["payload_app_id"]["llm_annotations"]["description"] == "Application ID"


class TestWriteBaselines:
    def test_writes_baselines_file(self, tmp_path):
        profiles = {
            "score": {"stats": {"mean": 0.83, "stddev": 0.05}},
            "name": {"dtype": "object"},
        }
        path = str(tmp_path / "baselines.json")
        count = write_baselines(profiles, baselines_path=path)
        assert count == 1

        with open(path) as f:
            data = json.load(f)
        assert "score" in data["columns"]
        assert data["columns"]["score"]["mean"] == 0.83
        assert data["columns"]["score"]["stddev"] == 0.05

    def test_preserves_existing_baselines(self, tmp_path):
        path = str(tmp_path / "baselines.json")
        # Write initial baseline
        with open(path, "w") as f:
            json.dump({"columns": {"old_col": {"mean": 1.0, "stddev": 0.1}}}, f)

        profiles = {"new_col": {"stats": {"mean": 2.0, "stddev": 0.2}}}
        count = write_baselines(profiles, baselines_path=path)
        assert count == 2

        with open(path) as f:
            data = json.load(f)
        assert "old_col" in data["columns"]
        assert "new_col" in data["columns"]

    def test_skips_non_numeric_columns(self, tmp_path):
        profiles = {"text_col": {"dtype": "object"}}
        path = str(tmp_path / "baselines.json")
        count = write_baselines(profiles, baselines_path=path)
        assert count == 0
