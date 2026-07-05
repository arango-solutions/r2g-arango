"""Unit tests for deterministic prompt grounding (PRD P11.10 → Phase 10)."""
from __future__ import annotations

from r2g.llm.grounding import _restricted_columns, build_grounding
from r2g.types import Classification, Column, Schema, Table


def _contact_schema() -> Schema:
    # A numbered family (phone1..3) is a structural repeating-group finding that
    # the analyzer reports with no sampler.
    return Schema(
        tables={
            "contact": Table(
                name="contact",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="phone1", data_type="text"),
                    Column(name="phone2", data_type="text"),
                    Column(name="phone3", data_type="text"),
                ],
                primary_key=["id"],
            )
        }
    )


class TestBuildGrounding:
    def test_structural_finding_without_sampler(self):
        digest = build_grounding(_contact_schema())
        assert "denormalization findings" in digest
        assert "contact" in digest
        assert "repeating_group" in digest

    def test_empty_when_no_findings(self):
        schema = Schema(
            tables={
                "t": Table(
                    name="t",
                    columns=[Column(name="id", data_type="integer", is_primary_key=True)],
                    primary_key=["id"],
                )
            }
        )
        assert build_grounding(schema) == ""

    def test_restricted_columns_excluded_from_sampling(self):
        # A restricted column must never be value-sampled while grounding.
        schema = Schema(
            tables={
                "person": Table(
                    name="person",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        Column(
                            name="ssn",
                            data_type="text",
                            classification=Classification(tags=["PII.Sensitive"]),
                        ),
                        Column(name="city", data_type="text"),
                    ],
                    primary_key=["id"],
                )
            }
        )

        class _RecordingSampler:
            def __init__(self):
                self.seen: list[tuple[str, str]] = []

            def distinct_ratio(self, table, column):
                self.seen.append((table, column))
                return 0.05

            def group_single_valued(self, table, det, dep):
                return None

            def delimiter_rate(self, table, column, delimiter):
                return None

        sampler = _RecordingSampler()
        build_grounding(schema, sampler=sampler)
        assert ("person", "ssn") not in sampler.seen


class TestRestrictedColumns:
    def test_collects_qualified_names(self):
        schema = Schema(
            tables={
                "c": Table(
                    name="c",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        Column(
                            name="email",
                            data_type="text",
                            classification=Classification(tags=["PII.Sensitive"]),
                        ),
                        Column(name="name", data_type="text"),
                    ],
                    primary_key=["id"],
                )
            }
        )
        restricted = _restricted_columns(schema, "restricted")
        assert "c.email" in restricted
        assert "c.name" not in restricted
