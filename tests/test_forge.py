"""Unit tests for the Federation Forge walking skeleton (``r2g.forge``).

The roundtrip *property* (PLAN F-2/F-3) is asserted here through the real
normalizers — ``csi.owl_entity_name`` / ``csi.owl_property_name`` and
``config.pg_type_to_json_type`` — without a live database; the end-to-end
introspection roundtrip lives in ``tests/integration/test_forge_roundtrip.py``.
"""

import pytest

from r2g.config import pg_type_to_json_type
from r2g.csi import owl_entity_name, owl_property_name
from r2g.forge import (
    PG_TYPE_FOR_JSON_TYPE,
    ForgeError,
    ForgeOntology,
    column_name,
    expected_relationship_type,
    foreign_key_column,
    generate,
    load_ontology_file,
    table_name,
)


def sample_conceptual() -> dict:
    """A three-entity DAG exercising every skeleton feature: all four types,
    a two-level relationship chain, and collision-free labels."""
    return {
        "entities": [
            {
                "name": "Account",
                "properties": [
                    {"name": "accountName", "type": "string"},
                    {"name": "healthScore", "type": "float"},
                    {"name": "seatsSold", "type": "integer"},
                ],
            },
            {
                "name": "Contact",
                "properties": [
                    {"name": "fullName", "type": "string"},
                    {"name": "isPrimary", "type": "boolean"},
                ],
            },
            {
                "name": "Ticket",
                "properties": [
                    {"name": "severity", "type": "integer"},
                ],
            },
        ],
        "relationships": [
            {"type": "contactsToAccounts", "fromEntity": "Contact", "toEntity": "Account"},
            {"type": "ticketsToContacts", "fromEntity": "Ticket", "toEntity": "Contact"},
        ],
    }


def sample_ontology() -> ForgeOntology:
    return ForgeOntology.from_conceptual(sample_conceptual())


class TestNamingInverse:
    def test_table_name_survives_the_real_normalizer(self):
        for entity in ("Account", "Contact", "UsageMetric", "NpsSurvey"):
            assert owl_entity_name(table_name(entity)) == entity

    def test_column_name_survives_the_real_normalizer(self):
        for prop in ("accountName", "healthScore", "isPrimary", "seatsSold"):
            assert owl_property_name(column_name(prop)) == prop

    def test_expected_relationship_type_matches_forward_derivation(self):
        # Auto-Map names the edge "<from_table>_to_<to_table>" and the CSI
        # emitter lowerCamels it; the forge must predict that exactly.
        assert expected_relationship_type("Contact", "Account") == "contactsToAccounts"
        assert expected_relationship_type("UsageMetric", "Account") == "usageMetricsToAccounts"

    def test_foreign_key_column(self):
        assert foreign_key_column("Account") == "account_id"
        assert foreign_key_column("UsageMetric") == "usage_metric_id"


class TestTypeInverse:
    def test_every_generated_pg_type_roundtrips_through_the_real_map(self):
        for json_type, pg_type in PG_TYPE_FOR_JSON_TYPE.items():
            assert pg_type_to_json_type(pg_type) == json_type


class TestOntologyValidation:
    def test_accepts_full_csi_document(self):
        document = {"csiVersion": "1", "conceptualModel": sample_conceptual()}
        ontology = ForgeOntology.from_conceptual(document)
        assert {e.name for e in ontology.entities} == {"Account", "Contact", "Ticket"}

    def test_rejects_empty(self):
        with pytest.raises(ForgeError, match="no entities"):
            ForgeOntology.from_conceptual({"entities": []})

    def test_rejects_unsupported_type(self):
        doc = sample_conceptual()
        doc["entities"][0]["properties"][0]["type"] = "temporal"
        with pytest.raises(ForgeError, match="unsupported type"):
            ForgeOntology.from_conceptual(doc)

    def test_rejects_missing_type(self):
        doc = sample_conceptual()
        del doc["entities"][0]["properties"][0]["type"]
        with pytest.raises(ForgeError, match="unsupported type"):
            ForgeOntology.from_conceptual(doc)

    def test_rejects_cross_entity_label_collision(self):
        doc = sample_conceptual()
        doc["entities"][1]["properties"].append({"name": "accountName", "type": "string"})
        with pytest.raises(ForgeError, match="collision-free"):
            ForgeOntology.from_conceptual(doc)

    def test_rejects_id_property(self):
        doc = sample_conceptual()
        doc["entities"][0]["properties"].append({"name": "id", "type": "integer"})
        with pytest.raises(ForgeError, match="surrogate"):
            ForgeOntology.from_conceptual(doc)

    def test_rejects_name_that_does_not_survive_naming_roundtrip(self):
        # "Goose" -> table "gooses" -> owl_entity_name gives "Goos": the naive
        # pluralize/singularize pair is not an inverse here, so the forge must
        # refuse at generate time (PLAN F-2), never fail at compare time.
        doc = {"entities": [{"name": "Goose", "properties": [{"name": "label", "type": "string"}]}]}
        with pytest.raises(ForgeError, match="naming"):
            ForgeOntology.from_conceptual(doc)

    def test_rejects_wrongly_named_relationship(self):
        doc = sample_conceptual()
        doc["relationships"][0]["type"] = "worksFor"
        with pytest.raises(ForgeError, match="contactsToAccounts"):
            ForgeOntology.from_conceptual(doc)

    def test_rejects_unknown_relationship_endpoint(self):
        doc = sample_conceptual()
        doc["relationships"][0]["fromEntity"] = "Ghost"
        with pytest.raises(ForgeError, match="unknown fromEntity"):
            ForgeOntology.from_conceptual(doc)

    def test_rejects_property_colliding_with_fk_column(self):
        doc = sample_conceptual()
        doc["entities"][1]["properties"].append({"name": "accountId", "type": "string"})
        with pytest.raises(ForgeError, match="foreign-key column"):
            ForgeOntology.from_conceptual(doc)

    def test_rejects_relationship_cycle(self):
        doc = sample_conceptual()
        doc["relationships"].append(
            {"type": "accountsToTickets", "fromEntity": "Account", "toEntity": "Ticket"}
        )
        with pytest.raises(ForgeError, match="cycle"):
            generate(ForgeOntology.from_conceptual(doc), seed=1)


class TestGenerate:
    def test_deterministic_byte_identical_rerun(self):
        first = generate(sample_ontology(), seed=421)
        second = generate(sample_ontology(), seed=421)
        assert first.ddl == second.ddl
        assert first.load_sql == second.load_sql
        assert first.rows == second.rows

    def test_different_seed_different_data_same_schema(self):
        a = generate(sample_ontology(), seed=1)
        b = generate(sample_ontology(), seed=2)
        assert a.ddl == b.ddl
        assert a.rows != b.rows

    def test_rejects_unsupported_dialect(self):
        with pytest.raises(ForgeError, match="dialect"):
            generate(sample_ontology(), dialect="duckdb", seed=1)

    def test_rejects_bad_rows_per_entity(self):
        with pytest.raises(ForgeError, match="rows_per_entity"):
            generate(sample_ontology(), seed=1, rows_per_entity=0)

    def test_ddl_orders_parents_before_children(self):
        ddl = generate(sample_ontology(), seed=1).ddl
        assert ddl.index("CREATE TABLE accounts") < ddl.index("CREATE TABLE contacts")
        assert ddl.index("CREATE TABLE contacts") < ddl.index("CREATE TABLE tickets")

    def test_ddl_declares_pk_and_fk(self):
        ddl = generate(sample_ontology(), seed=1).ddl
        assert "PRIMARY KEY (id)" in ddl
        assert "FOREIGN KEY (account_id) REFERENCES accounts (id)" in ddl
        assert "FOREIGN KEY (contact_id) REFERENCES contacts (id)" in ddl

    def test_join_spine_agrees_across_tables(self):
        artifacts = generate(sample_ontology(), seed=7, rows_per_entity=25)
        account_ids = {row["id"] for row in artifacts.rows["accounts"]}
        for contact in artifacts.rows["contacts"]:
            assert contact["account_id"] in account_ids
        contact_ids = {row["id"] for row in artifacts.rows["contacts"]}
        for ticket in artifacts.rows["tickets"]:
            assert ticket["contact_id"] in contact_ids

    def test_row_values_match_declared_types(self):
        artifacts = generate(sample_ontology(), seed=3)
        for row in artifacts.rows["accounts"]:
            assert isinstance(row["account_name"], str)
            assert isinstance(row["health_score"], float)
            assert isinstance(row["seats_sold"], int)
        for row in artifacts.rows["contacts"]:
            assert isinstance(row["is_primary"], bool)

    def test_load_sql_escapes_and_renders_literals(self):
        artifacts = generate(sample_ontology(), seed=3)
        assert artifacts.load_sql.count("INSERT INTO accounts") == 10
        # Booleans render as SQL keywords, not Python reprs.
        assert " True" not in artifacts.load_sql
        assert "TRUE" in artifacts.load_sql or "FALSE" in artifacts.load_sql

    def test_write_to_writes_three_files(self, tmp_path):
        artifacts = generate(sample_ontology(), seed=5)
        paths = artifacts.write_to(str(tmp_path))
        names = sorted(p.rsplit("/", 1)[-1] for p in paths)
        assert names == ["forge.load.sql", "forge.rows.json", "forge.sql"]
        assert (tmp_path / "forge.sql").read_text().startswith("-- Federation Forge")


class TestLoadOntologyFile:
    def test_loads_json_file(self, tmp_path):
        import json

        path = tmp_path / "ontology.json"
        path.write_text(json.dumps(sample_conceptual()))
        ontology = load_ontology_file(str(path))
        assert {e.name for e in ontology.entities} == {"Account", "Contact", "Ticket"}

    def test_missing_file_is_a_forge_error(self, tmp_path):
        with pytest.raises(ForgeError, match="cannot read ontology file"):
            load_ontology_file(str(tmp_path / "absent.json"))

    def test_rejects_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{not json")
        with pytest.raises(ForgeError, match="not valid JSON"):
            load_ontology_file(str(path))

    def test_rejects_non_object(self, tmp_path):
        path = tmp_path / "list.json"
        path.write_text("[1, 2]")
        with pytest.raises(ForgeError, match="JSON object"):
            load_ontology_file(str(path))
