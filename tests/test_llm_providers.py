"""Unit tests for the additional LLM providers (PRD Phase 10c).

Covers the factory dispatch to Anthropic and OpenAI-compatible/local providers,
their key/base-url requirements, and the Anthropic response parser. No network
calls are made — parsing is exercised directly on canned payloads.
"""
from __future__ import annotations

import pytest

from r2g.llm.anthropic_provider import AnthropicProvider
from r2g.llm.base import (
    SUPPORTED_LLM_TYPES,
    LLMProvider,
    OntologyRequest,
    create_llm_provider,
    normalize_llm_type,
)
from r2g.llm.openai_provider import OpenAIProvider


class TestProviderTypes:
    def test_all_three_types_supported(self):
        for t in ("openai", "anthropic", "openai-compatible"):
            assert t in SUPPORTED_LLM_TYPES

    @pytest.mark.parametrize("alias", ["anthropic", "Claude", " CLAUDE "])
    def test_anthropic_aliases(self, alias):
        assert normalize_llm_type(alias) == "anthropic"

    @pytest.mark.parametrize(
        "alias", ["openai-compatible", "openai_compatible", "local", "ollama", "vllm", "lmstudio"]
    )
    def test_compatible_aliases(self, alias):
        assert normalize_llm_type(alias) == "openai-compatible"


class TestAnthropicProvider:
    def test_factory_returns_anthropic(self):
        provider = create_llm_provider("claude", model="claude-x", api_key="sk-ant")
        assert isinstance(provider, AnthropicProvider)
        assert provider.provider_type == "anthropic"
        assert provider.model == "claude-x"
        assert isinstance(provider, LLMProvider)

    def test_missing_key_raises_on_call(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        provider = create_llm_provider("anthropic")
        with pytest.raises(ValueError, match="No Anthropic API key"):
            provider.propose_ontology(OntologyRequest(schema_digest="x"))

    def test_parse_response_plain_json(self):
        data = {
            "content": [{"type": "text", "text": '{"notes": ["hi"]}'}],
            "usage": {"input_tokens": 10, "output_tokens": 5},
        }
        proposal = AnthropicProvider._parse_response(data)
        assert "hi" in proposal.notes
        assert any("token_usage: 15" in n for n in proposal.notes)

    def test_parse_response_strips_code_fence(self):
        data = {"content": [{"type": "text", "text": '```json\n{"notes": ["x"]}\n```'}]}
        proposal = AnthropicProvider._parse_response(data)
        assert "x" in proposal.notes

    def test_parse_response_bad_json_raises(self):
        data = {"content": [{"type": "text", "text": "not json"}]}
        with pytest.raises(ValueError, match="not valid JSON"):
            AnthropicProvider._parse_response(data)

    def test_parse_response_empty_raises(self):
        with pytest.raises(ValueError, match="no text content"):
            AnthropicProvider._parse_response({"content": []})


class TestOpenAICompatibleProvider:
    def test_requires_base_url(self, monkeypatch):
        monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
        with pytest.raises(ValueError, match="needs a base URL"):
            create_llm_provider("local")

    def test_base_url_from_params(self):
        provider = create_llm_provider(
            "ollama", params={"base_url": "http://localhost:11434/v1/"}
        )
        assert isinstance(provider, OpenAIProvider)
        assert provider.provider_type == "openai-compatible"
        assert provider.base_url == "http://localhost:11434/v1"

    def test_no_api_key_required(self, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        # Should not raise on call for a missing key; it fails later at httpx.
        provider = create_llm_provider("local", params={"base_url": "http://localhost:1234/v1"})
        assert provider.require_key is False

    def test_base_url_from_env(self, monkeypatch):
        monkeypatch.setenv("OPENAI_BASE_URL", "http://host:9/v1")
        provider = create_llm_provider("vllm")
        assert provider.base_url == "http://host:9/v1"
