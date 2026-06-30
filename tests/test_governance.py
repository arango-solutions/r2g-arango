"""Unit tests for the entitlement report, gate, masking, and lineage (Phase 9b)."""
from __future__ import annotations

import json

from r2g.governance import (
    apply_sensitivity_gate,
    build_entitlement_report,
    lineage_manifest,
    write_lineage_manifest,
)
from r2g.masking import (
    MASK_KINDS,
    is_masking_expression,
    make_mask_expression,
    mask_kind_of,
)
from r2g.types import (
    Classification,
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    MappingConfig,
    Schema,
    Table,
)


def _col(name, clf=None, pk=False):
    return Column(name=name, data_type="text", is_primary_key=pk, classification=clf)


def _pii():
    return Classification(tags=["PII.Sensitive"])


def _schema():
    return Schema(tables={
        "customer": Table(
            name="customer",
            columns=[
                _col("id", pk=True),
                _col("email", _pii()),                       # restricted
                _col("first_name"),                          # public
                _col("loyalty_tier", Classification(tier="Tier.Tier1")),  # confidential
            ],
            primary_key=["id"],
        ),
        "order": Table(
            name="order",
            columns=[_col("id", pk=True), _col("customer_id"), _col("total")],
            primary_key=["id"],
        ),
    })


def _config():
    return MappingConfig(
        collections={
            "customer": CollectionMapping(source_table="customer", target_collection="Customer"),
            "order": CollectionMapping(source_table="order", target_collection="Order"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="placed",
                from_collection="order",
                to_collection="customer",
                from_fields=["customer_id"],
                to_fields=["id"],
            )
        ],
    )


class TestMaskingHelpers:
    def test_all_kinds_build_tagged_expressions(self):
        for kind in MASK_KINDS:
            fx = make_mask_expression("email", kind)
            assert fx.target == "email"
            assert is_masking_expression(fx)
            assert mask_kind_of(fx) == kind

    def test_hash_and_tokenize_reference_column(self):
        assert "@email" in make_mask_expression("email", "hash").expression
        assert make_mask_expression("email", "redact").expression == '"***"'
        assert make_mask_expression("email", "nullify").expression == "null"

    def test_unknown_kind_rejected(self):
        try:
            make_mask_expression("x", "scramble")
        except ValueError as e:
            assert "scramble" in str(e)
        else:
            raise AssertionError("expected ValueError")

    def test_non_masking_expression_detected(self):
        fx = FieldExpression(target="x", expression="@a + @b", description="sum")
        assert not is_masking_expression(fx)
        assert mask_kind_of(fx) is None


class TestEntitlementReport:
    def test_levels_and_lineage(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        by_target = {f"{f.target_collection}.{f.target_property}": f for f in report.fields}
        assert by_target["Customer.email"].level == "restricted"
        assert by_target["Customer.email"].source_columns == ["email"]
        assert by_target["Customer.loyalty_tier"].level == "confidential"
        assert by_target["Customer.first_name"].level == "public"
        # collection + edge rollup come from the mosaic recompute
        assert report.collection_levels["customer"] == "restricted"
        assert report.edge_levels["placed"] == "restricted"

    def test_above_threshold_filters_by_level(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        flagged = {f.target_property for f in report.above_threshold}
        assert flagged == {"email", "loyalty_tier"}
        # raising the threshold to restricted drops loyalty_tier (confidential)
        report2 = build_entitlement_report(_config(), _schema(), threshold="restricted")
        assert {f.target_property for f in report2.above_threshold} == {"email"}

    def test_masked_field_excluded_from_above_threshold(self):
        config = _config()
        config.collections["customer"].field_expressions = [
            make_mask_expression("email", "hash")
        ]
        report = build_entitlement_report(config, _schema(), threshold="confidential")
        email = next(f for f in report.fields if f.target_property == "email")
        assert email.masked and email.mask_kind == "hash"
        # masked => not in the gate's above-threshold set, even though restricted
        assert "email" not in {f.target_property for f in report.above_threshold}


class TestSensitivityGate:
    def test_excludes_above_threshold_columns_by_default(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        gated, excluded = apply_sensitivity_gate(_config(), report, allow_sensitive=False)
        assert set(gated.collections["customer"].exclude_fields) == {"email", "loyalty_tier"}
        assert {f.target_property for f in excluded} == {"email", "loyalty_tier"}
        assert all(f.excluded for f in excluded)

    def test_allow_sensitive_is_a_noop(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        gated, excluded = apply_sensitivity_gate(_config(), report, allow_sensitive=True)
        assert gated.collections["customer"].exclude_fields == []
        assert excluded == []

    def test_gate_does_not_mutate_input_config(self):
        config = _config()
        report = build_entitlement_report(config, _schema(), threshold="confidential")
        apply_sensitivity_gate(config, report, allow_sensitive=False)
        assert config.collections["customer"].exclude_fields == []

    def test_masked_field_passes_gate(self):
        config = _config()
        config.collections["customer"].field_expressions = [
            make_mask_expression("email", "hash")
        ]
        report = build_entitlement_report(config, _schema(), threshold="confidential")
        gated, excluded = apply_sensitivity_gate(config, report, allow_sensitive=False)
        # email is masked -> not excluded; loyalty_tier still excluded
        assert "email" not in gated.collections["customer"].exclude_fields
        assert "loyalty_tier" in gated.collections["customer"].exclude_fields


class TestLineageManifest:
    def test_manifest_records_handling(self):
        config = _config()
        config.collections["customer"].field_expressions = [
            make_mask_expression("email", "nullify")
        ]
        report = build_entitlement_report(config, _schema(), threshold="confidential", project="p")
        gated, excluded = apply_sensitivity_gate(config, report, allow_sensitive=False)
        # reflect exclusion back onto the report fields for the manifest
        excl_targets = {f.target_property for f in excluded}
        for f in report.fields:
            if f.target_property in excl_targets:
                f.excluded = True
        manifest = lineage_manifest(report)
        handling = {
            e["target"]: e["handling"] for e in manifest["fields"]
        }
        assert handling["Customer.email"].startswith("masked")
        assert handling["Customer.loyalty_tier"] == "excluded"
        assert handling["Customer.first_name"] == "loaded"
        assert manifest["project"] == "p"
        assert manifest["summary"]["total_fields"] == len(report.fields)

    def test_write_manifest_to_disk(self, tmp_path):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        path = write_lineage_manifest(report, tmp_path)
        assert path.exists() and path.name == "lineage.json"
        data = json.loads(path.read_text())
        assert "fields" in data and data["threshold"] == "confidential"
