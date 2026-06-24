"""Unit tests for the deterministic denormalization analyzer (PRD Phase 11a).

Structural detectors run with no sampler. The functional-dependency detector is
driven by a *fake* sampler returning canned probe values, so the tests are fully
deterministic and need no live database.
"""

from __future__ import annotations

from typing import Optional

import pytest

from r2g.denorm import (
    AnalyzeOptions,
    DenormFinding,
    analyze_denormalization,
    remediation_hint,
    summarize_findings_for_prompt,
    with_hints,
)
from r2g.types import Column, ForeignKey, Schema, Table


def _col(name: str, data_type: str = "text", pk: bool = False) -> Column:
    return Column(name=name, data_type=data_type, is_nullable=not pk, is_primary_key=pk)


def _table(name: str, columns: list[Column], pk: list[str] | None = None) -> Table:
    return Table(name=name, columns=columns, primary_key=pk or [])


def _schema(*tables: Table) -> Schema:
    return Schema(tables={t.name: t for t in tables})


class FakeSampler:
    """Canned probe values keyed by table/columns.

    ``distinct`` maps ``(table, column) -> ratio``; ``fd`` maps
    ``(table, determinant, dependent) -> single-valued fraction``. Anything not
    present returns ``None`` (the "couldn't evaluate" signal).
    """

    def __init__(self, distinct=None, fd=None, delim=None, *, raise_on=None):
        self.distinct = distinct or {}
        self.fd = fd or {}
        self.delim = delim or {}
        self.raise_on = raise_on or set()
        self.sampled_columns: list[tuple[str, str]] = []

    def distinct_ratio(self, table: str, column: str) -> Optional[float]:
        self.sampled_columns.append((table, column))
        if (table, column) in self.raise_on:
            raise RuntimeError("boom")
        return self.distinct.get((table, column))

    def group_single_valued(self, table, determinant_columns, dependent_column) -> Optional[float]:
        det = determinant_columns[0]
        return self.fd.get((table, det, dependent_column))

    def delimiter_rate(self, table: str, column: str, delimiter: str) -> Optional[float]:
        return self.delim.get((table, column, delimiter))


# ── Repeating groups (structural) ───────────────────────────────────


class TestRepeatingGroup:
    def test_numbered_family_detected(self):
        t = _table(
            "contact",
            [_col("id", "integer", pk=True), _col("phone1"), _col("phone2"), _col("phone3")],
            pk=["id"],
        )
        findings = analyze_denormalization(_schema(t))
        rg = [f for f in findings if f.kind == "repeating_group"]
        assert len(rg) == 1
        assert rg[0].table == "contact"
        assert rg[0].columns == ["phone1", "phone2", "phone3"]
        assert rg[0].recommended_action == "embed_array"
        # More members → higher confidence than a bare pair.
        assert rg[0].confidence > 0.6

    def test_underscore_separated_family(self):
        t = _table(
            "addr",
            [_col("addr_line_1"), _col("addr_line_2"), _col("addr_line_3")],
        )
        findings = analyze_denormalization(_schema(t))
        assert any(f.columns == ["addr_line_1", "addr_line_2", "addr_line_3"] for f in findings)

    def test_single_digit_suffixed_names_do_not_misfire(self):
        # md5 / sha256 each have a unique stem → no family of >= 2.
        t = _table("hashes", [_col("md5"), _col("sha256"), _col("crc32")])
        findings = analyze_denormalization(_schema(t))
        assert [f for f in findings if f.kind == "repeating_group"] == []

    def test_no_sampler_needed_for_structural(self):
        t = _table("c", [_col("x1"), _col("x2")])
        # x has stem length 1 → below the min-stem guard → not a family.
        findings = analyze_denormalization(_schema(t))
        assert findings == []


# ── Embedded lookup / functional dependency (sampled) ───────────────


def _customers_table() -> Table:
    return _table(
        "customers",
        [
            _col("id", "integer", pk=True),
            _col("email"),
            _col("zip"),
            _col("city"),
            _col("state"),
        ],
        pk=["id"],
    )


class TestEmbeddedLookup:
    def test_fd_detected(self):
        sampler = FakeSampler(
            distinct={
                ("customers", "email"): 1.0,  # unique → not a determinant
                ("customers", "zip"): 0.1,    # repeats → candidate determinant
                ("customers", "city"): 0.15,
                ("customers", "state"): 0.05,
            },
            fd={
                ("customers", "zip", "city"): 1.0,
                ("customers", "zip", "state"): 1.0,
                ("customers", "zip", "email"): 0.0,
                # city does not determine zip (a city spans many zips)
                ("customers", "city", "zip"): 0.2,
                ("customers", "city", "state"): 1.0,
                ("customers", "state", "zip"): 0.0,
                ("customers", "state", "city"): 0.0,
            },
        )
        findings = analyze_denormalization(
            _schema(_customers_table()),
            options=AnalyzeOptions(sample=True),
            sampler=sampler,
        )
        lookups = [f for f in findings if f.kind == "embedded_lookup"]
        zip_finding = next(f for f in lookups if f.determinant == ["zip"])
        assert set(zip_finding.dependents) == {"city", "state"}
        assert zip_finding.recommended_action == "extract_vertex"
        assert zip_finding.confidence >= 0.7
        # email (unique) is never a determinant.
        assert all(f.determinant != ["email"] for f in lookups)

    def test_no_fd_when_weak(self):
        sampler = FakeSampler(
            distinct={("customers", "zip"): 0.1, ("customers", "city"): 0.15},
            fd={("customers", "zip", "city"): 0.3, ("customers", "city", "zip"): 0.2},
        )
        findings = analyze_denormalization(
            _schema(_customers_table()),
            options=AnalyzeOptions(sample=True),
            sampler=sampler,
        )
        assert [f for f in findings if f.kind == "embedded_lookup"] == []

    def test_not_run_without_sample_flag(self):
        sampler = FakeSampler(
            distinct={("customers", "zip"): 0.1},
            fd={("customers", "zip", "city"): 1.0},
        )
        # sample defaults to False → sampler ignored.
        findings = analyze_denormalization(_schema(_customers_table()), sampler=sampler)
        assert [f for f in findings if f.kind == "embedded_lookup"] == []
        assert sampler.sampled_columns == []

    def test_min_confidence_filters(self):
        sampler = FakeSampler(
            distinct={("customers", "zip"): 0.4, ("customers", "city"): 0.4},
            fd={("customers", "zip", "city"): 1.0, ("customers", "city", "zip"): 0.0},
        )
        findings = analyze_denormalization(
            _schema(_customers_table()),
            options=AnalyzeOptions(sample=True, min_confidence=0.9),
            sampler=sampler,
        )
        # Single dependent, ratio not low enough for bonuses → conf ~0.6 < 0.9.
        assert [f for f in findings if f.kind == "embedded_lookup"] == []

    def test_classification_gate_excludes_column(self):
        sampler = FakeSampler(
            distinct={
                ("customers", "zip"): 0.1,
                ("customers", "city"): 0.15,
                ("customers", "state"): 0.05,
            },
            fd={
                ("customers", "zip", "city"): 1.0,
                ("customers", "zip", "state"): 1.0,
            },
        )
        analyze_denormalization(
            _schema(_customers_table()),
            options=AnalyzeOptions(
                sample=True, no_sample_columns=frozenset({"customers.zip"})
            ),
            sampler=sampler,
        )
        # zip was excluded → never passed to the sampler at all.
        assert ("customers", "zip") not in sampler.sampled_columns

    def test_sampler_failure_is_resilient(self):
        sampler = FakeSampler(
            distinct={("customers", "zip"): 0.1, ("customers", "city"): 0.15},
            fd={("customers", "zip", "city"): 1.0, ("customers", "city", "zip"): 0.0},
            raise_on={("customers", "city")},  # distinct_ratio raises for city
        )
        # Structural still returns; no crash; zip finding may or may not include city.
        findings = analyze_denormalization(
            _schema(_customers_table()),
            options=AnalyzeOptions(sample=True),
            sampler=sampler,
        )
        assert isinstance(findings, list)


class TestMultiValued:
    def _table(self):
        return _table(
            "post",
            [_col("id", "integer", pk=True), _col("tags", "text"), _col("title", "text")],
            pk=["id"],
        )

    def test_delimited_column_detected(self):
        sampler = FakeSampler(delim={("post", "tags", ","): 0.95, ("post", "title", ","): 0.0})
        findings = analyze_denormalization(
            _schema(self._table()), options=AnalyzeOptions(sample=True), sampler=sampler
        )
        mv = [f for f in findings if f.kind == "multi_valued"]
        assert len(mv) == 1
        assert mv[0].columns == ["tags"]
        assert mv[0].recommended_action == "split_column"
        assert "comma" in mv[0].evidence[0]

    def test_below_threshold_not_detected(self):
        sampler = FakeSampler(delim={("post", "tags", ","): 0.3})
        findings = analyze_denormalization(
            _schema(self._table()), options=AnalyzeOptions(sample=True), sampler=sampler
        )
        assert [f for f in findings if f.kind == "multi_valued"] == []

    def test_not_run_without_sampler(self):
        findings = analyze_denormalization(_schema(self._table()))
        assert [f for f in findings if f.kind == "multi_valued"] == []


class TestOneToOne:
    def test_pk_equals_fk_detected(self):
        users = _table("users", [_col("id", "integer", pk=True)], pk=["id"])
        profile = _table(
            "user_profile",
            [_col("user_id", "integer", pk=True), _col("bio", "text")],
            pk=["user_id"],
        )
        profile.foreign_keys = [
            ForeignKey(columns=["user_id"], foreign_table="users", foreign_columns=["id"])
        ]
        # Structural — runs with no sampler.
        findings = analyze_denormalization(_schema(users, profile))
        oto = [f for f in findings if f.kind == "one_to_one"]
        assert len(oto) == 1
        assert oto[0].table == "user_profile"
        assert oto[0].columns == ["user_id"]
        assert oto[0].recommended_action == "merge"
        assert "users" in oto[0].evidence[0]

    def test_partial_pk_fk_not_one_to_one(self):
        # A FK that covers only part of the PK is a normal child, not 1:1.
        parent = _table("parent", [_col("id", "integer", pk=True)], pk=["id"])
        child = _table(
            "child",
            [_col("parent_id", "integer", pk=True), _col("seq", "integer", pk=True)],
            pk=["parent_id", "seq"],
        )
        child.foreign_keys = [
            ForeignKey(columns=["parent_id"], foreign_table="parent", foreign_columns=["id"])
        ]
        findings = analyze_denormalization(_schema(parent, child))
        assert [f for f in findings if f.kind == "one_to_one"] == []


class TestRedundantReference:
    def test_low_cardinality_label_detected(self):
        t = _table(
            "orders",
            [_col("id", "integer", pk=True), _col("status_label", "text")],
            pk=["id"],
        )
        sampler = FakeSampler(distinct={("orders", "status_label"): 0.004})
        findings = analyze_denormalization(
            _schema(t), options=AnalyzeOptions(sample=True), sampler=sampler
        )
        rr = [f for f in findings if f.kind == "redundant_reference"]
        assert len(rr) == 1
        assert rr[0].columns == ["status_label"]
        assert rr[0].recommended_action == "extract_vertex"

    def test_high_cardinality_not_flagged(self):
        t = _table(
            "orders",
            [_col("id", "integer", pk=True), _col("note", "text")],
            pk=["id"],
        )
        sampler = FakeSampler(distinct={("orders", "note"): 0.9})
        findings = analyze_denormalization(
            _schema(t), options=AnalyzeOptions(sample=True), sampler=sampler
        )
        assert [f for f in findings if f.kind == "redundant_reference"] == []

    def test_suppressed_when_part_of_embedded_lookup(self):
        # city/state are dependents of zip → must not also be reported as
        # standalone redundant_reference findings.
        sampler = FakeSampler(
            distinct={
                ("customers", "zip"): 0.05,
                ("customers", "city"): 0.05,
                ("customers", "state"): 0.02,
            },
            fd={
                ("customers", "zip", "city"): 1.0,
                ("customers", "zip", "state"): 1.0,
                ("customers", "city", "state"): 0.0,
                ("customers", "city", "zip"): 0.2,
                ("customers", "state", "zip"): 0.0,
                ("customers", "state", "city"): 0.0,
            },
        )
        findings = analyze_denormalization(
            _schema(_customers_table()),
            options=AnalyzeOptions(sample=True),
            sampler=sampler,
        )
        rr_cols = {tuple(f.columns) for f in findings if f.kind == "redundant_reference"}
        assert ("city",) not in rr_cols
        assert ("state",) not in rr_cols


class TestModel:
    def test_finding_round_trips(self):
        f = DenormFinding(
            kind="embedded_lookup",
            table="customers",
            columns=["zip", "city"],
            recommended_action="extract_vertex",
            confidence=0.8,
            determinant=["zip"],
            dependents=["city"],
        )
        assert f.model_dump(mode="json")["confidence"] == 0.8

    def test_confidence_bounds_enforced(self):
        with pytest.raises(ValueError):
            DenormFinding(
                kind="x", table="t", columns=["a"], recommended_action="merge", confidence=1.5
            )


class TestRemediationGuidance:
    def _finding(self, kind, **kw):
        base = dict(
            kind=kind,
            table="t",
            columns=kw.pop("columns", ["a"]),
            recommended_action=kw.pop("action", "merge"),
            confidence=0.8,
        )
        base.update(kw)
        return DenormFinding(**base)

    def test_hint_per_kind_is_nonempty(self):
        for kind, action in [
            ("repeating_group", "embed_array"),
            ("embedded_lookup", "extract_vertex"),
            ("redundant_reference", "extract_vertex"),
            ("multi_valued", "split_column"),
            ("one_to_one", "merge"),
        ]:
            hint = remediation_hint(self._finding(kind, action=action))
            assert isinstance(hint, str) and len(hint) > 10

    def test_multi_valued_hint_mentions_delimiter_label(self):
        f = self._finding(
            "multi_valued", action="split_column", params={"delimiter_label": "comma"}
        )
        assert "comma" in remediation_hint(f)

    def test_with_hints_attaches_hint(self):
        f = self._finding("one_to_one", action="merge", dependents=["parent"])
        dicts = with_hints([f])
        assert dicts[0]["kind"] == "one_to_one"
        assert "hint" in dicts[0]
        assert "parent" in dicts[0]["hint"]


class TestGrounding:
    def test_empty(self):
        assert summarize_findings_for_prompt([]) == ""

    def test_orders_by_confidence_and_caps(self):
        findings = [
            DenormFinding(
                kind="multi_valued",
                table="t",
                columns=[f"c{i}"],
                recommended_action="split_column",
                confidence=0.5 + i * 0.01,
            )
            for i in range(5)
        ]
        digest = summarize_findings_for_prompt(findings, max_items=3)
        lines = digest.splitlines()
        # header + 3 capped items
        assert len(lines) == 4
        # highest confidence (c4) listed before lower ones
        assert lines[1].index("c4") >= 0
        assert "grounding" in lines[0].lower()
