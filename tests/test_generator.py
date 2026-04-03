"""Tests for ContractGenerator."""
import json
import sys
import pytest
from unittest.mock import patch, MagicMock

from contracts.generator import annotate_ambiguous_columns


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
