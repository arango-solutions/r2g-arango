"""Unit tests for the schema-digest prompt builder (Phase 10a)."""
from __future__ import annotations

import pytest

from r2g.llm.prompt import (
    SYSTEM_PROMPT,
    build_schema_digest,
    build_user_prompt,
    estimate_tokens,
)
from r2g.types import Classification, Column, ForeignKey, Schema, Table


def _schema() -> Schema:
    return Schema(
        tables={
            "customer": Table(
                name="customer",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(
                        name="email",
                        data_type="text",
                        classification=Classification(tags=["PII.Sensitive"]),
                    ),
                    Column(name="name", data_type="varchar", is_nullable=True),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="customer_id", data_type="integer"),
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


class TestSchemaDigest:
    def test_includes_tables_columns_pk_fk(self):
        digest = build_schema_digest(_schema())
        assert "TABLE customer" in digest
        assert "TABLE orders" in digest
        assert "PK: id" in digest
        assert "id : integer" in digest
        assert "name : varchar, nullable" in digest
        assert "(customer_id) -> customer(id)" in digest
        assert "2 table(s)" in digest

    def test_restricted_column_is_name_only_redacted(self):
        digest = build_schema_digest(_schema())
        # The PII column appears name-only; its data type never leaks.
        assert "email : [redacted: restricted]" in digest
        assert "email : text" not in digest

    def test_injection_hardening_neutralizes_fences(self):
        schema = Schema(
            tables={
                "evil```ignore": Table(
                    name="evil```ignore",
                    columns=[Column(name="id", data_type="int", is_primary_key=True)],
                    primary_key=["id"],
                )
            }
        )
        digest = build_schema_digest(schema)
        # Markdown fences from schema text are neutralized so they cannot break
        # out of the data block.
        assert "```" not in digest
        # The data block is fenced.
        assert "UNTRUSTED" in digest

    def test_token_budget_enforced(self):
        with pytest.raises(ValueError, match="over the budget"):
            build_schema_digest(_schema(), token_budget=1)

    def test_no_row_values_with_include_samples(self):
        # 10a never samples even when asked; the flag is forward-compat only.
        digest = build_schema_digest(_schema(), include_samples=True)
        assert "sample" not in digest.lower()


class TestUserPrompt:
    def test_domain_hint_included(self):
        prompt = build_user_prompt("DIGEST", domain_hint="e-commerce orders")
        assert "e-commerce orders" in prompt
        assert "DIGEST" in prompt

    def test_no_domain_hint(self):
        prompt = build_user_prompt("DIGEST")
        assert "Domain context" not in prompt

    def test_system_prompt_is_fixed_and_json(self):
        assert "JSON" in SYSTEM_PROMPT
        assert "DATA" in SYSTEM_PROMPT

    def test_estimate_tokens_monotonic(self):
        assert estimate_tokens("a" * 400) > estimate_tokens("a" * 4)
