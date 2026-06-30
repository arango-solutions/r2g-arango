"""Unit tests for the sensitivity lattice + mosaic recomputation (PRD Phase 9a).

Pure-logic tests over hand-built ``Schema`` / ``MappingConfig`` — no network, no
catalog. These pin the data-model thread everything else in Phase 9 depends on.
"""
from __future__ import annotations

from r2g.classification import (
    DEFAULT_TAG_LEVELS,
    PUBLIC,
    SENSITIVITY_ORDER,
    annotate_schema,
    diff_classifications,
    exceeds_threshold,
    max_sensitivity,
    recompute_mosaic,
    sensitivity_rank,
    tier_of,
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


def _col(name: str, clf: Classification | None = None, pk: bool = False) -> Column:
    return Column(name=name, data_type="text", is_primary_key=pk, classification=clf)


def _pii() -> Classification:
    return Classification(tags=["PII.Sensitive"])


def _tier1() -> Classification:
    return Classification(tags=["PersonalData.Personal"], tier="Tier.Tier1")


class TestLattice:
    def test_order_is_low_to_high(self):
        assert SENSITIVITY_ORDER == ("public", "internal", "confidential", "restricted")
        ranks = [sensitivity_rank(lv) for lv in SENSITIVITY_ORDER]
        assert ranks == sorted(ranks) == [0, 1, 2, 3]

    def test_unknown_level_ranks_as_public(self):
        assert sensitivity_rank("bogus") == 0
        assert sensitivity_rank("RESTRICTED") == 3  # case-insensitive

    def test_max_sensitivity(self):
        assert max_sensitivity([]) == PUBLIC
        assert max_sensitivity(["public", "internal", "confidential"]) == "confidential"
        assert max_sensitivity(["internal", "restricted", "public"]) == "restricted"

    def test_exceeds_threshold_is_inclusive(self):
        assert exceeds_threshold("confidential", "confidential") is True
        assert exceeds_threshold("restricted", "confidential") is True
        assert exceeds_threshold("internal", "confidential") is False


class TestTierOf:
    def test_none_is_public(self):
        assert tier_of(None) == PUBLIC

    def test_empty_classification_is_public(self):
        assert tier_of(Classification()) == PUBLIC

    def test_pii_maps_restricted(self):
        assert tier_of(_pii()) == "restricted"

    def test_tier_fqn_maps_confidential(self):
        assert tier_of(Classification(tier="Tier.Tier1")) == "confidential"

    def test_max_over_tags_and_tier(self):
        # Tier.Tier2 -> internal, PII.* -> restricted; max wins.
        clf = Classification(tags=["PII.Sensitive"], tier="Tier.Tier2")
        assert tier_of(clf) == "restricted"

    def test_unmapped_tag_does_not_escalate(self):
        assert tier_of(Classification(tags=["Project.Skunkworks"])) == PUBLIC

    def test_longest_prefix_wins(self):
        # personaldata.sensitivepersonal -> restricted (more specific than personaldata)
        clf = Classification(tags=["PersonalData.SensitivePersonal.Health"])
        assert tier_of(clf) == "restricted"

    def test_override_map(self):
        override = {"project.skunkworks": "restricted"}
        assert tier_of(Classification(tags=["Project.Skunkworks"]), tag_levels=override) == "restricted"
        # default map untouched
        assert "project.skunkworks" not in DEFAULT_TAG_LEVELS


class TestAnnotateSchema:
    def test_stamps_matching_columns(self):
        schema = Schema(tables={
            "customer": Table(
                name="customer",
                columns=[_col("id", pk=True), _col("email"), _col("name")],
                primary_key=["id"],
            )
        })
        n = annotate_schema(schema, {"customer": {"email": _pii()}})
        assert n == 1
        cols = {c.name: c for c in schema.tables["customer"].columns}
        assert cols["email"].classification is not None
        assert cols["email"].classification.tags == ["PII.Sensitive"]
        assert cols["name"].classification is None

    def test_ignores_unknown_table_and_column(self):
        schema = Schema(tables={"t": Table(name="t", columns=[_col("a")])})
        n = annotate_schema(schema, {"missing": {"x": _pii()}, "t": {"nope": _pii()}})
        assert n == 0
        assert schema.tables["t"].columns[0].classification is None

    def test_empty_classification_not_stamped(self):
        schema = Schema(tables={"t": Table(name="t", columns=[_col("a")])})
        n = annotate_schema(schema, {"t": {"a": Classification()}})
        assert n == 0


class TestMosaicRecompute:
    def _schema(self) -> Schema:
        return Schema(tables={
            "customer": Table(
                name="customer",
                columns=[
                    _col("id", pk=True),
                    _col("email", _pii()),          # restricted
                    _col("first_name"),             # public
                    _col("last_name"),              # public
                    _col("loyalty_tier", _tier1()), # confidential
                ],
                primary_key=["id"],
            ),
            "order": Table(
                name="order",
                columns=[_col("id", pk=True), _col("customer_id"), _col("total")],
                primary_key=["id"],
            ),
        })

    def test_collection_level_is_max_of_kept_columns(self):
        schema = self._schema()
        config = MappingConfig(collections={
            "customer": CollectionMapping(source_table="customer", target_collection="Customer"),
            "order": CollectionMapping(source_table="order", target_collection="Order"),
        })
        m = recompute_mosaic(config, schema)
        assert m.collections["customer"] == "restricted"
        assert m.collections["order"] == PUBLIC
        assert m.fields["customer.email"] == "restricted"
        assert m.fields["customer.loyalty_tier"] == "confidential"
        assert m.fields["customer.first_name"] == PUBLIC

    def test_excluded_sensitive_column_drops_collection_level(self):
        schema = self._schema()
        config = MappingConfig(collections={
            "customer": CollectionMapping(
                source_table="customer",
                target_collection="Customer",
                exclude_fields=["email"],
            ),
        })
        m = recompute_mosaic(config, schema)
        # loyalty_tier (confidential) remains; email (restricted) excluded.
        assert m.collections["customer"] == "confidential"
        assert "customer.email" not in m.fields

    def test_fan_in_property_takes_max_of_sources(self):
        schema = self._schema()
        config = MappingConfig(collections={
            "customer": CollectionMapping(
                source_table="customer",
                target_collection="Customer",
                field_expressions=[
                    FieldExpression(
                        target="full_name",
                        sources=["first_name", "last_name", "email"],
                        expression="CONCAT(first_name, last_name)",
                    )
                ],
            ),
        })
        m = recompute_mosaic(config, schema)
        # full_name fans in email (restricted) -> property is restricted (mosaic).
        assert m.fields["customer.full_name"] == "restricted"

    def test_edge_level_is_max_of_endpoints(self):
        schema = self._schema()
        config = MappingConfig(
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
        m = recompute_mosaic(config, schema)
        # customer endpoint is restricted -> edge inherits the max.
        assert m.edges["placed"] == "restricted"

    def test_above_threshold_filter(self):
        schema = self._schema()
        config = MappingConfig(collections={
            "customer": CollectionMapping(source_table="customer", target_collection="Customer"),
        })
        m = recompute_mosaic(config, schema)
        above = m.above("confidential")
        assert above["customer.email"] == "restricted"
        assert above["customer.loyalty_tier"] == "confidential"
        assert "customer.first_name" not in above


class TestDiffClassifications:
    def test_detects_escalation(self):
        old = {"customer": {"email": Classification()}}  # public
        new = {"customer": {"email": _pii()}}            # restricted
        deltas = diff_classifications(old, new)
        assert len(deltas) == 1
        d = deltas[0]
        assert (d.table, d.column) == ("customer", "email")
        assert d.old_level == "public" and d.new_level == "restricted"
        assert d.escalated and d.direction == "escalated"

    def test_detects_de_escalation(self):
        old = {"customer": {"email": _pii()}}
        new = {"customer": {"email": Classification()}}
        deltas = diff_classifications(old, new)
        assert deltas[0].direction == "de-escalated"
        assert not deltas[0].escalated

    def test_no_change_when_level_stable(self):
        # tag churn that does not move the lattice level is ignored
        old = {"customer": {"email": Classification(tags=["PII.Sensitive"])}}
        new = {"customer": {"email": Classification(tags=["PII.Sensitive", "Extra.Tag"])}}
        assert diff_classifications(old, new) == []

    def test_new_and_removed_columns(self):
        old = {}
        new = {"customer": {"ssn": _pii()}}
        deltas = diff_classifications(old, new)
        assert deltas[0].old_level == "public" and deltas[0].new_level == "restricted"
        # removed (present old, gone new) de-escalates to public
        deltas2 = diff_classifications(new, old)
        assert deltas2[0].direction == "de-escalated"

    def test_sorted_output(self):
        old = {}
        new = {
            "b": {"y": _pii()},
            "a": {"z": _tier1(), "a": _pii()},
        }
        deltas = diff_classifications(old, new)
        keys = [(d.table, d.column) for d in deltas]
        assert keys == sorted(keys)
