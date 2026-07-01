"""LLM-assisted ontology derivation (PRD Phase 10).

An *optional* path where a domain-aware model **proposes** a richer target
ontology from an introspected schema. The model never touches the graph: its
output is a candidate :class:`~r2g.types.MappingConfig` that flows through the
**same** ``validate_config`` → ``diff_mappings`` review → loader path as every
other mapping. The LLM proposes; the deterministic pipeline disposes.

This package mirrors :mod:`r2g.catalogs`: a structural ``LLMProvider`` Protocol,
a thin lazy-importing factory (so optional deps load only when used), and
concrete providers behind it.
"""

from __future__ import annotations

from r2g.llm.base import (
    SUPPORTED_LLM_TYPES,
    LLMProvider,
    OntologyProposal,
    OntologyRequest,
    ProposedCollection,
    ProposedEdge,
    ProposedEmbed,
    ProposedRename,
    create_llm_provider,
    normalize_llm_type,
)
from r2g.llm.ontology import proposal_to_mapping

__all__ = [
    "SUPPORTED_LLM_TYPES",
    "LLMProvider",
    "OntologyProposal",
    "OntologyRequest",
    "ProposedCollection",
    "ProposedEdge",
    "ProposedEmbed",
    "ProposedRename",
    "create_llm_provider",
    "normalize_llm_type",
    "proposal_to_mapping",
]
