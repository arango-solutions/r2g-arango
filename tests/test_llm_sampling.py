"""Unit tests for opt-in, classification-filtered value sampling (Phase 10c)."""
from __future__ import annotations

from r2g.llm.sampling import collect_samples
from r2g.types import Classification, Column, Schema, Table


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
                    Column(name="status", data_type="varchar"),
                ],
                primary_key=["id"],
            ),
        }
    )


class _FakeSampler:
    """Records which columns were probed and returns canned values."""

    def __init__(self, values):
        self.values = values
        self.probed: list[tuple[str, str]] = []

    def sample_values(self, table: str, column: str, limit: int = 5):
        self.probed.append((table, column))
        return list(self.values.get((table, column), []))[:limit]


class TestCollectSamples:
    def test_skips_restricted_columns(self):
        sampler = _FakeSampler(
            {
                ("customer", "id"): [1, 2, 3],
                ("customer", "status"): ["active", "closed"],
                ("customer", "email"): ["a@x.com"],
            }
        )
        out = collect_samples(sampler, _schema())
        # The PII/restricted column is never even probed.
        assert ("customer", "email") not in sampler.probed
        assert "email" not in out.get("customer", {})
        assert out["customer"]["status"] == ["active", "closed"]

    def test_per_column_bound_forwarded(self):
        sampler = _FakeSampler({("customer", "status"): ["a", "b", "c", "d"]})
        out = collect_samples(sampler, _schema(), per_column=2)
        assert out["customer"]["status"] == ["a", "b"]

    def test_max_columns_caps_probes(self):
        sampler = _FakeSampler({("customer", "id"): [1]})
        collect_samples(sampler, _schema(), max_columns=1)
        # Only the first non-redacted column is probed.
        assert len(sampler.probed) == 1

    def test_sampler_failure_is_swallowed(self):
        class Boom:
            def sample_values(self, *a, **k):
                raise RuntimeError("db down")

        out = collect_samples(Boom(), _schema())
        assert out == {}

    def test_empty_values_omitted(self):
        sampler = _FakeSampler({})
        out = collect_samples(sampler, _schema())
        assert out == {}
