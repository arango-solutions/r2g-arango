"""Unit tests for the LLM provider factory and proposal models (Phase 10a)."""
from __future__ import annotations

import sys

import pytest

from r2g.llm.base import (
    SUPPORTED_LLM_TYPES,
    LLMProvider,
    OntologyProposal,
    OntologyRequest,
    ProposedEdge,
    create_llm_provider,
    normalize_llm_type,
)
from r2g.llm.openai_provider import OpenAIProvider


class TestNormalizeAndFactory:
    def test_supported_types(self):
        assert "openai" in SUPPORTED_LLM_TYPES

    @pytest.mark.parametrize("alias", ["openai", "OpenAI", " gpt ", "oai", "open-ai"])
    def test_aliases_normalize_to_openai(self, alias):
        assert normalize_llm_type(alias) == "openai"

    def test_factory_returns_openai_provider(self):
        provider = create_llm_provider("openai", model="gpt-4o-mini", api_key="sk-test")
        assert isinstance(provider, OpenAIProvider)
        assert provider.provider_type == "openai"
        assert provider.model == "gpt-4o-mini"
        # Satisfies the structural protocol.
        assert isinstance(provider, LLMProvider)

    def test_factory_unknown_type_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unsupported LLM provider type"):
            create_llm_provider("ollama-local")


class TestProviderKeyAndDeps:
    def test_missing_key_raises_on_call(self, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        provider = create_llm_provider("openai")
        with pytest.raises(ValueError, match="No OpenAI API key"):
            provider.propose_ontology(OntologyRequest(schema_digest="x"))

    def test_missing_httpx_extra_raises_importerror(self, monkeypatch):
        # Simulate the `llm` extra not being installed: `import httpx` fails.
        monkeypatch.setitem(sys.modules, "httpx", None)
        provider = create_llm_provider("openai", api_key="sk-test")
        with pytest.raises(ImportError, match=r"r2g-arango\[llm\]"):
            provider.propose_ontology(OntologyRequest(schema_digest="x"))


class TestModels:
    def test_request_defaults(self):
        req = OntologyRequest(schema_digest="digest")
        assert req.domain_hint == ""
        assert req.options == {}

    def test_proposal_validation_coerces(self):
        proposal = OntologyProposal.model_validate(
            {
                "edges": [
                    {
                        "edge_collection": "o_to_c",
                        "from_collection": "orders",
                        "to_collection": "customer",
                        "from_fields": ["customer_id"],
                        "to_fields": ["id"],
                        "confidence": 0.9,
                    }
                ]
            }
        )
        assert len(proposal.edges) == 1
        assert isinstance(proposal.edges[0], ProposedEdge)
        assert proposal.collections == []

    def test_parse_response_extracts_and_validates(self):
        data = {
            "choices": [{"message": {"content": '{"notes": ["hi"]}'}}],
            "usage": {"total_tokens": 123},
        }
        proposal = OpenAIProvider._parse_response(data)
        assert "hi" in proposal.notes
        assert any("token_usage: 123" in n for n in proposal.notes)

    def test_parse_response_bad_json_raises(self):
        data = {"choices": [{"message": {"content": "not json"}}]}
        with pytest.raises(ValueError, match="not valid JSON"):
            OpenAIProvider._parse_response(data)

    def test_parse_response_malformed_raises(self):
        with pytest.raises(ValueError, match="Malformed OpenAI response"):
            OpenAIProvider._parse_response({"choices": []})
