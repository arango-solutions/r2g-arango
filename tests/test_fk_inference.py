from __future__ import annotations

import pytest

from r2g.fk_inference import (
    InferenceOptions,
    InferredForeignKey,
    PostgresValueSampler,
    infer_foreign_keys,
)
from r2g.types import Column, EdgeDefinition, ForeignKey, Schema, Table

# ── Helpers ─────────────────────────────────────────────────────────


def _tbl(name: str, cols: list[tuple[str, str, bool, bool]], pk: list[str] | None = None,
         fks: list[ForeignKey] | None = None) -> Table:
    """`cols` entries: (name, data_type, is_nullable, is_primary_key)."""
    return Table(
        name=name,
        columns=[
            Column(name=n, data_type=t, is_nullable=nul, is_primary_key=pkf)
            for (n, t, nul, pkf) in cols
        ],
        primary_key=pk or [],
        foreign_keys=fks or [],
    )


def _schema(*tables: Table) -> Schema:
    return Schema(tables={t.name: t for t in tables})


# ── Name-based inference ────────────────────────────────────────────


class TestNameHeuristic:
    def test_single_column_user_id_points_to_users_id(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True), ("name", "text", True, False)], pk=["id"]),
            _tbl("orders", [("id", "integer", False, True), ("user_id", "integer", False, False)], pk=["id"]),
        )
        out = infer_foreign_keys(s)
        assert len(out) == 1
        c = out[0]
        assert c.table == "orders"
        assert c.columns == ["user_id"]
        assert c.foreign_table == "users"
        assert c.foreign_columns == ["id"]
        assert c.method == "name_suffix"
        assert c.confidence >= 0.75

    def test_plural_and_singular_table_names_both_match(self):
        s_plural = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl("orders", [("id", "integer", False, True), ("user_id", "integer", False, False)], pk=["id"]),
        )
        s_singular = _schema(
            _tbl("user", [("id", "integer", False, True)], pk=["id"]),
            _tbl("orders", [("id", "integer", False, True), ("user_id", "integer", False, False)], pk=["id"]),
        )
        out1 = infer_foreign_keys(s_plural)
        out2 = infer_foreign_keys(s_singular)
        assert any(c.foreign_table == "users" for c in out1)
        assert any(c.foreign_table == "user" for c in out2)

    def test_declared_fk_suppresses_suggestion_for_that_column(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "integer", False, False)],
                pk=["id"],
                fks=[ForeignKey(columns=["user_id"], foreign_table="users", foreign_columns=["id"])],
            ),
        )
        out = infer_foreign_keys(s)
        assert out == []

    def test_type_incompatibility_rejects_candidate(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "flags",
                [("id", "integer", False, True), ("user_id", "boolean", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        assert out == []

    def test_integer_and_float_are_considered_compatible(self):
        # Snowflake NUMBER(38,0) ends up as "number" → float in the map,
        # but joining against integer PKs is a very real workflow.
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "number", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        assert any(c.columns == ["user_id"] for c in out)

    def test_bare_id_column_does_not_match_random_pks(self):
        # Direct pk_name_match should skip generic names like `id`.
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl("sessions", [("id", "integer", False, True)], pk=["id"]),
        )
        out = infer_foreign_keys(s)
        assert out == []

    def test_non_generic_pk_name_triggers_direct_match(self):
        s = _schema(
            _tbl("products", [("sku", "text", False, True), ("name", "text", True, False)], pk=["sku"]),
            _tbl(
                "line_items",
                [("id", "integer", False, True), ("sku", "text", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        match = [c for c in out if c.foreign_table == "products" and c.columns == ["sku"]]
        assert len(match) == 1
        assert match[0].method in ("pk_name_match", "name_suffix")

    def test_no_underscore_id_gets_lower_confidence(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("userid", "integer", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s, options=InferenceOptions(min_confidence=0.3))
        # The "userid" → "users.id" suggestion should exist but be scored
        # below the clean "_id" form.
        assert any(c.columns == ["userid"] for c in out)
        conf = next(c for c in out if c.columns == ["userid"]).confidence
        assert conf < 0.6

    def test_min_confidence_filters_weak_candidates(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("userid", "integer", False, False)],
                pk=["id"],
            ),
        )
        strict = infer_foreign_keys(s, options=InferenceOptions(min_confidence=0.9))
        assert strict == []

    def test_nullable_target_with_non_nullable_source_is_penalized(self):
        s = _schema(
            _tbl("users", [("id", "integer", True, True)], pk=["id"]),  # nullable PK (weird!)
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "integer", False, False)],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        conf = next(c for c in out if c.columns == ["user_id"]).confidence
        assert conf < 0.8

    def test_results_are_sorted_by_confidence_desc(self):
        s = _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl("customers", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [
                    ("id", "integer", False, True),
                    ("user_id", "integer", False, False),
                    ("customerid", "integer", False, False),
                ],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s, options=InferenceOptions(min_confidence=0.3))
        confs = [c.confidence for c in out]
        assert confs == sorted(confs, reverse=True)


# ── Composite inference ────────────────────────────────────────────


class TestCompositeInference:
    def test_composite_pk_gets_grouped_suggestion(self):
        s = _schema(
            _tbl(
                "order_products",
                [("order_id", "integer", False, True), ("product_id", "integer", False, True)],
                pk=["order_id", "product_id"],
            ),
            _tbl(
                "order_lines",
                [
                    ("id", "integer", False, True),
                    ("order_id", "integer", False, False),
                    ("product_id", "integer", False, False),
                ],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(s)
        composite = [c for c in out if c.method == "composite"]
        assert len(composite) == 1
        c = composite[0]
        assert c.table == "order_lines"
        assert c.foreign_table == "order_products"
        assert c.columns == ["order_id", "product_id"]
        assert c.foreign_columns == ["order_id", "product_id"]

    def test_composite_disabled_returns_only_singles(self):
        s = _schema(
            _tbl(
                "order_products",
                [("order_id", "integer", False, True), ("product_id", "integer", False, True)],
                pk=["order_id", "product_id"],
            ),
            _tbl(
                "order_lines",
                [
                    ("id", "integer", False, True),
                    ("order_id", "integer", False, False),
                    ("product_id", "integer", False, False),
                ],
                pk=["id"],
            ),
        )
        out = infer_foreign_keys(
            s,
            options=InferenceOptions(allow_composite=False, min_confidence=0.0),
        )
        assert all(c.method != "composite" for c in out)


# ── Sampler integration ────────────────────────────────────────────


class TestSamplerIntegration:
    def _schema(self) -> Schema:
        return _schema(
            _tbl("users", [("id", "integer", False, True)], pk=["id"]),
            _tbl(
                "orders",
                [("id", "integer", False, True), ("user_id", "integer", False, False)],
                pk=["id"],
            ),
        )

    def test_high_overlap_boosts_confidence(self):
        s = self._schema()

        def sampler(lt, lc, ft, fc):
            return 1.0

        out_no = infer_foreign_keys(s)
        out_yes = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True),
            sampler=sampler,
        )
        assert out_yes[0].confidence > out_no[0].confidence

    def test_zero_overlap_vetoes_when_enabled(self):
        s = self._schema()
        out = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True, overlap_veto_on_zero=True),
            sampler=lambda *args: 0.0,
        )
        assert out == []

    def test_zero_overlap_without_veto_still_lowers_score(self):
        s = self._schema()
        out = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True, overlap_veto_on_zero=False),
            sampler=lambda *args: 0.0,
        )
        assert len(out) == 1
        # 0.85 base minus 0.25 penalty, rounded = 0.6 — still above the default floor.
        assert out[0].confidence < 0.85

    def test_sampler_exception_keeps_candidate(self):
        s = self._schema()

        def angry(*args):
            raise RuntimeError("boom")

        out = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True),
            sampler=angry,
        )
        assert len(out) == 1

    def test_sampler_none_is_neutral(self):
        s = self._schema()
        out_no = infer_foreign_keys(s)
        out_neutral = infer_foreign_keys(
            s,
            options=InferenceOptions(sample_overlap=True),
            sampler=lambda *args: None,
        )
        assert out_neutral[0].confidence == out_no[0].confidence


# ── to_edge_definition ─────────────────────────────────────────────


class TestEdgeDefinitionConversion:
    def test_single_column_edge_round_trip(self):
        c = InferredForeignKey(
            table="orders",
            columns=["user_id"],
            foreign_table="users",
            foreign_columns=["id"],
            confidence=0.85,
            method="name_suffix",
        )
        ed = c.to_edge_definition()
        assert isinstance(ed, EdgeDefinition)
        assert ed.edge_collection == "orders_to_users"
        assert ed.from_collection == "orders"
        assert ed.to_collection == "users"
        assert ed.from_fields == ["user_id"]
        assert ed.to_fields == ["id"]

    def test_composite_edge_keeps_column_order(self):
        c = InferredForeignKey(
            table="order_lines",
            columns=["order_id", "product_id"],
            foreign_table="order_products",
            foreign_columns=["order_id", "product_id"],
            confidence=0.82,
            method="composite",
        )
        ed = c.to_edge_definition(edge_collection="lines_to_order_products")
        assert ed.edge_collection == "lines_to_order_products"
        assert ed.from_fields == ["order_id", "product_id"]
        assert ed.to_fields == ["order_id", "product_id"]


# ── PostgresValueSampler plumbing ──────────────────────────────────


class TestPostgresValueSampler:
    def test_query_failure_is_swallowed_and_returns_none(self, monkeypatch):
        sampler = PostgresValueSampler("postgresql://bogus@localhost/none")

        class _FakeCur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def execute(self, *a, **kw):
                raise RuntimeError("boom")

            def fetchone(self):
                return None

        class _FakeConn:
            def cursor(self):
                return _FakeCur()

            def rollback(self):
                self.rolled_back = True

            def close(self):
                pass

        sampler._conn = _FakeConn()
        res = sampler("orders", "user_id", "users", "id")
        assert res is None
        assert sampler._conn.rolled_back is True

    def test_empty_result_returns_none(self):
        sampler = PostgresValueSampler("postgresql://bogus/none")

        class _FakeCur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def execute(self, *a, **kw):
                pass

            def fetchone(self):
                return None

        class _FakeConn:
            def cursor(self):
                return _FakeCur()

            def close(self):
                pass

        sampler._conn = _FakeConn()
        assert sampler("a", "b", "c", "d") is None

    def test_successful_result_is_coerced_to_float(self):
        sampler = PostgresValueSampler("postgresql://bogus/none")

        class _FakeCur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def execute(self, *a, **kw):
                pass

            def fetchone(self):
                return (0.75,)

        class _FakeConn:
            def cursor(self):
                return _FakeCur()

            def close(self):
                pass

        sampler._conn = _FakeConn()
        assert sampler("a", "b", "c", "d") == 0.75

    def test_limit_is_clamped_to_minimum(self):
        sampler = PostgresValueSampler("postgresql://x/y", limit=10)
        assert sampler.limit >= 100


# ── Parametric coverage ────────────────────────────────────────────


@pytest.mark.parametrize(
    "col_name,expected_prefix",
    [
        ("user_id", "user"),
        ("customer_id", "customer"),
        ("customerid", "customer"),
        ("uuid", None),  # generic UUID column should not auto-match
    ],
)
def test_prefix_extraction_produces_expected_tables(col_name, expected_prefix):
    parent = _tbl(
        expected_prefix or "unrelated",
        [("id", "integer", False, True)],
        pk=["id"],
    ) if expected_prefix else _tbl("foo", [("id", "integer", False, True)], pk=["id"])
    s = _schema(
        parent,
        _tbl(
            "child",
            [("id", "integer", False, True), (col_name, "integer", False, False)],
            pk=["id"],
        ),
    )
    out = infer_foreign_keys(s, options=InferenceOptions(min_confidence=0.3))
    if expected_prefix:
        assert any(c.foreign_table == expected_prefix for c in out)
    else:
        assert all(c.columns != [col_name] for c in out)
