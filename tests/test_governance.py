"""Unit tests for the entitlement report, gate, masking, and lineage (Phase 9b)."""
from __future__ import annotations

import json

from r2g.governance import (
    apply_sensitivity_gate,
    build_entitlement_report,
    classification_manifest,
    lineage_manifest,
    policy_rego,
    suggested_rbac,
    tier_layout_recommendation,
    write_governance_artifacts,
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


# ── Phase 9c: enforcement artifacts ───────────────────────────────────────


class TestClassificationManifest:
    def test_groups_fields_under_collections_with_owners(self):
        report = build_entitlement_report(
            _config(), _schema(), threshold="confidential", project="p"
        )
        manifest = classification_manifest(
            report, owners=["data-team@x.io"], synced_at="2026-06-30T00:00:00+00:00"
        )
        assert manifest["kind"] == "r2g.classification-manifest/v1"
        assert manifest["owners"] == ["data-team@x.io"]
        assert manifest["classifications_synced_at"] == "2026-06-30T00:00:00+00:00"
        cust = manifest["collections"]["Customer"]
        assert cust["level"] == "restricted"
        props = {f["property"]: f for f in cust["fields"]}
        assert props["email"]["level"] == "restricted"
        assert props["email"]["sources"] == ["customer.email"]
        # superset of lineage
        assert "lineage" in manifest and manifest["summary"]["total_fields"]


class TestSuggestedRbac:
    def test_cumulative_roles_by_clearance(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        rbac = suggested_rbac(report, database="graph_db")
        roles = {r["role"]: r for r in rbac["roles"]}
        # restricted clearance reads everything; confidential clearance must NOT
        # include the restricted Customer collection. Keyed by target names.
        assert "r2g_clearance_restricted" in roles
        restricted = set(roles["r2g_clearance_restricted"]["collections"])
        assert {"Customer", "Order", "placed"} <= restricted
        if "r2g_clearance_confidential" in roles:
            assert "Customer" not in roles["r2g_clearance_confidential"]["collections"]
        assert rbac["database"] == "graph_db"

    def test_redundant_clearances_deduped(self):
        # A graph with only public collections collapses to a single role.
        schema = Schema(tables={
            "t": Table(name="t", columns=[_col("id", pk=True), _col("name")], primary_key=["id"])
        })
        config = MappingConfig(collections={
            "t": CollectionMapping(source_table="t", target_collection="T")
        })
        report = build_entitlement_report(config, schema, threshold="confidential")
        rbac = suggested_rbac(report)
        assert len(rbac["roles"]) == 1


class TestPolicyRego:
    def test_rego_is_default_deny_and_lists_levels(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        rego = policy_rego(report)
        assert "package r2g.authz" in rego
        assert "default allow := false" in rego
        assert '"Customer": "restricted"' in rego
        assert "sensitivity_rank" in rego


class TestTierLayout:
    def test_groups_collections_by_tier(self):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        layout = tier_layout_recommendation(report, database="graph_db")
        assert layout["strategy"] == "separate-database"
        assert "Customer" in layout["tiers"]["restricted"]["collections"]
        assert layout["tiers"]["restricted"]["suggested_database"] == "graph_db_restricted"


class TestWriteGovernanceArtifacts:
    def test_writes_full_artifact_set(self, tmp_path):
        report = build_entitlement_report(
            _config(), _schema(), threshold="confidential", project="p"
        )
        written = write_governance_artifacts(
            report, tmp_path, owners=["o@x.io"], database="db", tier_layout=True
        )
        names = set(written)
        assert names == {
            "lineage.json",
            "classification-manifest.json",
            "suggested-rbac.json",
            "policy.rego",
            "tier-layout.json",
        }
        for p in written.values():
            assert p.exists()
        gov = tmp_path / "governance"
        manifest = json.loads((gov / "classification-manifest.json").read_text())
        assert manifest["project"] == "p"

    def test_rego_and_tier_layout_optional(self, tmp_path):
        report = build_entitlement_report(_config(), _schema(), threshold="confidential")
        written = write_governance_artifacts(
            report, tmp_path, tier_layout=False, emit_rego=False
        )
        assert "policy.rego" not in written
        assert "tier-layout.json" not in written
        assert "classification-manifest.json" in written


class TestCdcGovernanceGate:
    """Phase 9c: the CDC/temporal gate helper carries classification on changes."""

    def test_no_govern_returns_input_unchanged(self):
        from r2g.main import _govern_cdc_mapping

        config = _config()
        out = _govern_cdc_mapping(
            _schema(), config, govern=False, allow_sensitive=False, threshold="confidential"
        )
        assert out is config

    def test_govern_excludes_above_threshold(self):
        from r2g.main import _govern_cdc_mapping

        config = _config()
        out = _govern_cdc_mapping(
            _schema(), config, govern=True, allow_sensitive=False, threshold="confidential"
        )
        # email (restricted) + loyalty_tier (confidential) excluded on the copy
        assert set(out.collections["customer"].exclude_fields) == {"email", "loyalty_tier"}
        # input not mutated
        assert config.collections["customer"].exclude_fields == []

    def test_allow_sensitive_keeps_fields(self):
        from r2g.main import _govern_cdc_mapping

        out = _govern_cdc_mapping(
            _schema(), _config(), govern=True, allow_sensitive=True, threshold="confidential"
        )
        assert out.collections["customer"].exclude_fields == []

    def test_invalid_threshold_exits(self):
        import typer

        from r2g.main import _govern_cdc_mapping

        try:
            _govern_cdc_mapping(
                _schema(), _config(), govern=True, allow_sensitive=False, threshold="secret"
            )
        except typer.Exit as e:
            assert e.exit_code == 1
        else:
            raise AssertionError("expected typer.Exit")
