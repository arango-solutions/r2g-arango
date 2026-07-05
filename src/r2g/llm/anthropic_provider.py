"""Anthropic (Claude) ontology provider — REST over ``httpx`` (PRD Phase 10c).

Mirrors :class:`r2g.llm.openai_provider.OpenAIProvider` but speaks the Anthropic
Messages API. Anthropic has no ``response_format=json_object`` switch, so the
fixed system prompt instructs the model to return a single JSON object and the
parser strips any Markdown code fences before validating against
:class:`~r2g.llm.base.OntologyProposal` — the structured shape is enforced
regardless of what the model returns.

The key follows the project convention: read from ``ANTHROPIC_API_KEY`` unless an
already-resolved key is passed in, and never persisted.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

from r2g.llm.base import OntologyProposal, OntologyRequest
from r2g.llm.prompt import SYSTEM_PROMPT, build_user_prompt

DEFAULT_MODEL = "claude-3-5-sonnet-latest"
DEFAULT_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_API_VERSION = "2023-06-01"
DEFAULT_TIMEOUT = 60.0
DEFAULT_MAX_TOKENS = 4096

_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.IGNORECASE)


class AnthropicProvider:
    """Propose an ontology via the Anthropic Messages API."""

    provider_type = "anthropic"

    def __init__(
        self,
        *,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> None:
        self.params = params or {}
        self.model = model or self.params.get("model") or DEFAULT_MODEL
        self.base_url = str(self.params.get("base_url", DEFAULT_BASE_URL)).rstrip("/")
        self.api_version = str(self.params.get("api_version", DEFAULT_API_VERSION))
        self.timeout = float(self.params.get("timeout", DEFAULT_TIMEOUT))
        self.max_tokens = int(self.params.get("max_tokens", DEFAULT_MAX_TOKENS))
        self.temperature = float(self.params.get("temperature", 0.0))
        self._api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")

    def propose_ontology(self, request: OntologyRequest) -> OntologyProposal:
        if not self._api_key:
            raise ValueError(
                "No Anthropic API key. Set $ANTHROPIC_API_KEY (or pass --api-key) — "
                "the key is read from the environment and never stored."
            )
        try:
            import httpx
        except ImportError as err:  # pragma: no cover - exercised via base test
            raise ImportError(
                "The Anthropic provider needs httpx. Install with: "
                "pip install 'r2g-arango[llm]'"
            ) from err

        payload = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "system": SYSTEM_PROMPT,
            "messages": [
                {
                    "role": "user",
                    "content": build_user_prompt(
                        request.schema_digest, request.domain_hint, request.grounding
                    ),
                },
            ],
        }
        headers = {
            "x-api-key": self._api_key,
            "anthropic-version": self.api_version,
            "content-type": "application/json",
        }
        resp = httpx.post(
            f"{self.base_url}/messages",
            headers=headers,
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return self._parse_response(resp.json())

    @staticmethod
    def _parse_response(data: dict[str, Any]) -> OntologyProposal:
        """Extract and validate the structured proposal from a Messages response."""
        try:
            blocks = data["content"]
            text = "".join(b.get("text", "") for b in blocks if isinstance(b, dict))
        except (KeyError, TypeError) as err:
            raise ValueError(f"Malformed Anthropic response: {err}") from err
        if not text.strip():
            raise ValueError("Anthropic response contained no text content")
        cleaned = _FENCE_RE.sub("", text.strip())
        try:
            obj = json.loads(cleaned)
        except json.JSONDecodeError as err:
            raise ValueError(f"Anthropic response was not valid JSON: {err}") from err
        proposal = OntologyProposal.model_validate(obj)
        usage = data.get("usage")
        if isinstance(usage, dict):
            total = (usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0)
            if total:
                proposal.notes.append(f"token_usage: {total}")
        return proposal
