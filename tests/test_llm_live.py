"""Gated live smoke test for the LLM ontology round-trip (Phase 10c).

Skipped unless ``R2G_LLM_LIVE=1`` is set, so the default suite never makes a
network call or needs a key. When enabled it exercises the *real* provider end
to end and asserts only that the round-trip yields a **valid** MappingConfig
(never specific content — models are non-deterministic).

Configure via env:
  R2G_LLM_LIVE=1            enable the test
  R2G_LLM_PROVIDER=openai   provider type (openai | anthropic | openai-compatible)
  R2G_LLM_MODEL=...         optional model override
  R2G_LLM_BASE_URL=...      base URL (required for openai-compatible)
  plus the provider's key env ($OPENAI_API_KEY / $ANTHROPIC_API_KEY)

Run one with, e.g.::

  R2G_LLM_LIVE=1 R2G_LLM_PROVIDER=openai pytest tests/test_llm_live.py -q
"""
from __future__ import annotations

import os

import pytest

from r2g.config import validate_config
from r2g.llm import create_llm_provider, proposal_to_mapping
from r2g.llm.base import OntologyRequest
from r2g.llm.prompt import build_schema_digest
from r2g.types import Column, ForeignKey, Schema, Table

pytestmark = pytest.mark.skipif(
    os.environ.get("R2G_LLM_LIVE") != "1",
    reason="live LLM test; set R2G_LLM_LIVE=1 to run",
)


def _schema() -> Schema:
    return Schema(
        tables={
            "customer": Table(
                name="customer",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="name", data_type="varchar"),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="customer_id", data_type="integer"),
                    Column(name="status", data_type="varchar"),
                ],
                primary_key=["id"],
                foreign_keys=[
                    ForeignKey(
                        columns=["customer_id"],
                        foreign_table="customer",
                        foreign_columns=["id"],
                    )
                ],
            ),
        }
    )


def test_live_round_trip_yields_valid_mapping():
    provider_type = os.environ.get("R2G_LLM_PROVIDER", "openai")
    model = os.environ.get("R2G_LLM_MODEL") or None
    base_url = os.environ.get("R2G_LLM_BASE_URL") or None
    params = {"base_url": base_url} if base_url else None

    schema = _schema()
    digest = build_schema_digest(schema, domain_hint="e-commerce orders")
    provider = create_llm_provider(provider_type, model=model, params=params)
    proposal = provider.propose_ontology(
        OntologyRequest(schema_digest=digest, domain_hint="e-commerce orders",
                        table_count=len(schema.tables))
    )
    mapping, _notes = proposal_to_mapping(proposal, schema)
    # The hallucination gate guarantees validity regardless of model output.
    assert validate_config(schema, mapping) == []
    assert mapping.collections
