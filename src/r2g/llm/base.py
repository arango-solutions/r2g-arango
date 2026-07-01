"""``LLMProvider`` abstraction for ontology proposals (PRD Phase 10a).

Mirrors :mod:`r2g.catalogs.base`:

- :class:`LLMProvider` is a structural ``Protocol`` describing the single
  read-only operation r2g performs against a model: turn an
  :class:`OntologyRequest` (a schema digest + optional domain hint) into a
  structured :class:`OntologyProposal`.
- The proposal is a *candidate*, not a decision. It carries vertex/edge
  designations, implicit relationships, rename suggestions and embed hints —
  each with a short ``rationale`` and a ``confidence`` — which
  :func:`r2g.llm.ontology.proposal_to_mapping` validates and repairs against the
  real schema before anything downstream sees it.
- :func:`create_llm_provider` is the thin factory the CLI / UI call. It
  lazy-imports the concrete provider so optional dependencies (``httpx``) load
  only when a provider of that type is actually used.

Adding a provider is a single edit here plus a concrete implementation, exactly
like adding a catalog provider.
"""

from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field


class ProposedCollection(BaseModel):
    """A table the model proposes to map to a collection (vertex or edge)."""

    source_table: str
    target_collection: str = ""
    collection_type: str = "document"  # "document" or "edge"
    is_join_table: bool = False
    rationale: str = ""
    confidence: float = 0.5


class ProposedEdge(BaseModel):
    """A relationship the model proposes (declared *or* implicit/undeclared).

    ``from_collection`` / ``to_collection`` name **source tables** (the same
    convention as :class:`~r2g.types.EdgeDefinition`, which validates against the
    schema's tables, not target collection names).
    """

    edge_collection: str
    from_collection: str
    to_collection: str
    from_fields: list[str] = Field(default_factory=list)
    to_fields: list[str] = Field(default_factory=list)
    rationale: str = ""
    confidence: float = 0.5


class ProposedRename(BaseModel):
    """A column → target-property name improvement."""

    source_table: str
    column: str
    target_property: str
    rationale: str = ""
    confidence: float = 0.5


class ProposedEmbed(BaseModel):
    """An embed-vs-link hint (advisory only in V1).

    The deterministic ``MappingConfig`` shape cannot mechanically represent
    document embedding yet (see Phase 11), so embed proposals surface as review
    *notes* rather than being applied automatically.
    """

    parent_table: str
    child_table: str
    as_property: str = ""
    rationale: str = ""
    confidence: float = 0.5


class OntologyProposal(BaseModel):
    """The structured ontology a provider returns. Always schema-validated and
    repaired by :func:`r2g.llm.ontology.proposal_to_mapping` before use."""

    collections: list[ProposedCollection] = Field(default_factory=list)
    edges: list[ProposedEdge] = Field(default_factory=list)
    renames: list[ProposedRename] = Field(default_factory=list)
    embeds: list[ProposedEmbed] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class OntologyRequest(BaseModel):
    """Everything a provider needs to produce a proposal, with no schema object.

    ``schema_digest`` is the compact, classification-redacted, injection-hardened
    text built by :func:`r2g.llm.prompt.build_schema_digest`. Keeping the request
    text-only (rather than a live :class:`~r2g.types.Schema`) keeps providers
    trivially mockable and makes the egress surface explicit and auditable.
    """

    schema_digest: str
    domain_hint: str = ""
    table_count: int = 0
    options: dict[str, Any] = Field(default_factory=dict)


@runtime_checkable
class LLMProvider(Protocol):
    """Structural interface every ontology-proposing provider must satisfy."""

    provider_type: str

    def propose_ontology(self, request: OntologyRequest) -> OntologyProposal:
        """Return a structured ontology proposal for ``request`` (no side effects)."""
        ...


SUPPORTED_LLM_TYPES: tuple[str, ...] = ("openai",)

_LLM_ALIASES: dict[str, str] = {
    "openai": "openai",
    "open-ai": "openai",
    "gpt": "openai",
    "oai": "openai",
}


def normalize_llm_type(provider_type: str | None) -> str:
    """Canonicalize an LLM provider-type string."""
    key = (provider_type or "").strip().lower()
    return _LLM_ALIASES.get(key, key)


def create_llm_provider(
    provider_type: str,
    *,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    params: Optional[dict[str, Any]] = None,
) -> LLMProvider:
    """Return an LLM provider matching ``provider_type``.

    Concrete classes are lazy-imported so optional dependencies load only when a
    provider of that type is used. Unknown types raise :class:`ValueError`;
    missing optional deps raise :class:`ImportError` with a pip-install hint.
    """
    key = normalize_llm_type(provider_type)
    if key == "openai":
        from r2g.llm.openai_provider import OpenAIProvider

        return OpenAIProvider(model=model, api_key=api_key, params=params or {})
    raise ValueError(
        f"Unsupported LLM provider type '{provider_type}'. "
        f"Expected one of: {', '.join(SUPPORTED_LLM_TYPES)}."
    )
