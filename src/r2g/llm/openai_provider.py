"""OpenAI ontology provider — REST over ``httpx`` (PRD Phase 10a).

REST (rather than the official SDK) keeps the optional dependency tiny and makes
the call trivial to mock in tests — the same rationale as the OpenMetadata
catalog provider. The chat-completions endpoint is called in JSON-object mode
with ``temperature=0`` for determinism; the response is parsed and validated
against :class:`~r2g.llm.base.OntologyProposal`, so the structured shape is
enforced regardless of what the model returns.

The API key follows the project convention: it is read from the environment
(``OPENAI_API_KEY``) unless an already-resolved key is passed in, and never
persisted. ``base_url`` may be overridden via ``params`` to target an
OpenAI-compatible local endpoint.
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

from r2g.llm.base import OntologyProposal, OntologyRequest
from r2g.llm.prompt import SYSTEM_PROMPT, build_user_prompt

DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_TIMEOUT = 60.0
DEFAULT_MAX_TOKENS = 4096


class OpenAIProvider:
    """Propose an ontology via the OpenAI chat-completions API."""

    provider_type = "openai"

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
        self.timeout = float(self.params.get("timeout", DEFAULT_TIMEOUT))
        self.max_tokens = int(self.params.get("max_tokens", DEFAULT_MAX_TOKENS))
        self.temperature = float(self.params.get("temperature", 0.0))
        # Resolve the key lazily so constructing a provider never requires one
        # (e.g. listing supported types); calls without a key fail clearly.
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY")

    def propose_ontology(self, request: OntologyRequest) -> OntologyProposal:
        if not self._api_key:
            raise ValueError(
                "No OpenAI API key. Set $OPENAI_API_KEY (or pass --api-key) — "
                "the key is read from the environment and never stored."
            )
        try:
            import httpx
        except ImportError as err:  # pragma: no cover - exercised via base test
            raise ImportError(
                "The OpenAI provider needs httpx. Install with: "
                "pip install 'r2g-arango[llm]'"
            ) from err

        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": build_user_prompt(request.schema_digest, request.domain_hint),
                },
            ],
        }
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        resp = httpx.post(
            f"{self.base_url}/chat/completions",
            headers=headers,
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        return self._parse_response(data)

    @staticmethod
    def _parse_response(data: dict[str, Any]) -> OntologyProposal:
        """Extract and validate the structured proposal from a chat response."""
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as err:
            raise ValueError(f"Malformed OpenAI response: {err}") from err
        try:
            obj = json.loads(content)
        except json.JSONDecodeError as err:
            raise ValueError(f"OpenAI response was not valid JSON: {err}") from err
        proposal = OntologyProposal.model_validate(obj)
        usage = data.get("usage")
        if isinstance(usage, dict):
            total = usage.get("total_tokens")
            if total is not None:
                proposal.notes.append(f"token_usage: {total}")
        return proposal
