"""Unit tests for the Federation Forge dialect seam (``r2g.forge.dialects``).

No live database here: these pin the *shape* of each dialect's DDL and loader,
the registry contract every dialect must satisfy, and ADR-0006 D-2 — data is
synthesized once and only projected per system, so ``rows`` are byte-identical
across dialects. The live roundtrips live in ``tests/integration/``.
"""

from __future__ import annotations

import json

import pytest
from test_forge import sample_conceptual, sample_ontology

from r2g.config import pg_type_to_json_type
from r2g.connectors.clickhouse import _clean_type
from r2g.forge import (
    DIALECTS,
    JSON_TYPES,
    SUPPORTED_DIALECTS,
    ForgeError,
    ForgeOntology,
    generate,
    get_dialect,
    plan_schema,
    split_sql_statements,
)
from r2g.forge.dialects.arango import (
    GRAPH_NAME,
    MANIFEST_VERSION,
    project_documents,
    render_manifest,
)
from r2g.forge.dialects.base import check_type_table, sql_literal
from r2g.forge.dialects.clickhouse import CLICKHOUSE_TYPE_FOR_JSON_TYPE, fk_intent_comment
from r2g.forge.dialects.snowflake import SNOWFLAKE_TYPE_FOR_JSON_TYPE

SQL_DIALECTS = ("postgres", "snowflake", "clickhouse")


class TestRegistry:
    def test_launch_set_registered_postgres_first(self):
        # ADR-0006 D-4 launch set; postgres stays first (the CLI default).
        assert SUPPORTED_DIALECTS == ("postgres", "snowflake", "clickhouse", "arango")
        assert set(DIALECTS) == set(SUPPORTED_DIALECTS)

    @pytest.mark.parametrize("name", SUPPORTED_DIALECTS)
    def test_every_dialect_covers_every_json_type(self, name):
        dialect = get_dialect(name)
        assert dialect.name == name
        assert set(dialect.type_for_json) >= set(JSON_TYPES)
        check_type_table(dialect)  # does not raise

    def test_check_type_table_refuses_incomplete_dialect(self):
        class Half(type(get_dialect("postgres"))):  # type: ignore[misc]
            name = "half"
            type_for_json = {"integer": "bigint"}

        with pytest.raises(ForgeError, match="no physical type"):
            check_type_table(Half())

    def test_unknown_dialect_lists_supported(self):
        with pytest.raises(ForgeError, match="duckdb.*postgres"):
            get_dialect("duckdb")

    def test_generate_stamps_registry_name(self):
        for name in SUPPORTED_DIALECTS:
            assert generate(sample_ontology(), dialect=name, seed=1).dialect == name


class TestRowsAreDialectIndependent:
    """ADR-0006 D-2: synthesized once, projected per system."""

    @pytest.mark.parametrize("seed", [0, 7, 421])
    def test_rows_byte_identical_across_all_dialects(self, seed):
        rendered = {
            name: json.dumps(
                generate(sample_ontology(), dialect=name, seed=seed, rows_per_entity=13).rows,
                sort_keys=True,
            )
            for name in SUPPORTED_DIALECTS
        }
        assert len(set(rendered.values())) == 1, "rows diverge across dialects"

    def test_rows_keyed_by_canonical_names_not_physical_spelling(self):
        snowflake = generate(sample_ontology(), dialect="snowflake", seed=1)
        assert set(snowflake.rows) == {"accounts", "contacts", "tickets"}
        assert "account_name" in snowflake.rows["accounts"][0]
        # ...while the loader speaks the dialect's spelling.
        assert "INSERT INTO ACCOUNTS (ID, ACCOUNT_NAME" in snowflake.load_sql

    def test_ddl_is_seed_independent_per_dialect(self):
        for name in SUPPORTED_DIALECTS:
            a = generate(sample_ontology(), dialect=name, seed=1)
            b = generate(sample_ontology(), dialect=name, seed=2)
            assert a.ddl == b.ddl, name
            assert a.rows != b.rows, name

    def test_rerun_is_byte_identical_per_dialect(self):
        for name in SUPPORTED_DIALECTS:
            a = generate(sample_ontology(), dialect=name, seed=99)
            b = generate(sample_ontology(), dialect=name, seed=99)
            assert (a.ddl, a.load_sql, a.rows) == (b.ddl, b.load_sql, b.rows), name


class TestSchemaPlan:
    def test_plan_orders_parents_first_and_columns_by_role(self):
        plan = plan_schema(sample_ontology())
        assert [t.table for t in plan.tables] == ["accounts", "contacts", "tickets"]
        contacts = plan.table("contacts")
        assert [c.name for c in contacts.columns] == ["id", "account_id", "full_name", "is_primary"]
        assert contacts.primary_key.role == "pk" and not contacts.primary_key.nullable
        (fk,) = contacts.foreign_keys
        assert fk.references == "accounts" and not fk.nullable
        assert all(c.nullable for c in contacts.properties)

    def test_plan_edges_carry_forward_pipeline_names(self):
        plan = plan_schema(sample_ontology())
        assert [(e.edge_collection, e.relationship) for e in plan.edges] == [
            ("contacts_to_accounts", "contactsToAccounts"),
            ("tickets_to_contacts", "ticketsToContacts"),
        ]

    def test_plan_is_a_pure_function_of_the_ontology(self):
        assert plan_schema(sample_ontology()) == plan_schema(sample_ontology())

    def test_unknown_table_lookup_is_forge_error(self):
        with pytest.raises(ForgeError, match="unknown planned table"):
            plan_schema(sample_ontology()).table("ghosts")


class TestSqlHelpers:
    def test_sql_literal_renders_each_type(self):
        assert sql_literal(None) == "NULL"
        assert sql_literal(True) == "TRUE" and sql_literal(False) == "FALSE"
        assert sql_literal(True, true="true", false="false") == "true"
        assert sql_literal(42) == "42" and sql_literal(1.5) == "1.5"
        assert sql_literal("it's") == "'it''s'"

    def test_split_sql_statements_skips_comments_and_respects_strings(self):
        sql = "-- header\nINSERT INTO t (s) VALUES ('a;b');\n\n-- more\nCREATE TABLE x (id int)"
        assert split_sql_statements(sql) == [
            "INSERT INTO t (s) VALUES ('a;b')",
            "CREATE TABLE x (id int)",
        ]

    @pytest.mark.parametrize("name", SQL_DIALECTS)
    def test_generated_scripts_split_into_one_statement_per_table_or_row(self, name):
        artifacts = generate(sample_ontology(), dialect=name, seed=1, rows_per_entity=4)
        assert len(split_sql_statements(artifacts.ddl)) == 3
        loader_statements = split_sql_statements(artifacts.load_sql)
        expected = 3 * 4 if name == "postgres" else 3
        assert len(loader_statements) == expected
        assert all(s.startswith("INSERT INTO") for s in loader_statements)


class TestSnowflakeDialect:
    def test_types_roundtrip_through_the_forward_map_except_the_known_number_gap(self):
        # INFORMATION_SCHEMA reports NUMBER(38,0) as bare "number", which the
        # forward map calls float — the one documented fidelity gap
        # (tests/integration/test_forge_roundtrip_snowflake.py pins it).
        introspects_as = {"integer": "number", "float": "float", "boolean": "boolean", "string": "text"}
        for json_type, physical in SNOWFLAKE_TYPE_FOR_JSON_TYPE.items():
            assert physical.split("(")[0].lower() in {introspects_as[json_type], "varchar"}
        assert pg_type_to_json_type("float") == "float"
        assert pg_type_to_json_type("boolean") == "boolean"
        assert pg_type_to_json_type("text") == "string"
        assert pg_type_to_json_type("number") == "float"  # the gap, pinned

    def test_ddl_uses_uppercase_unquoted_identifiers_and_declared_keys(self):
        ddl = generate(sample_ontology(), dialect="snowflake", seed=1).ddl
        assert "CREATE TABLE ACCOUNTS (" in ddl
        assert "    ID NUMBER(38,0) NOT NULL," in ddl
        assert "    ACCOUNT_NAME VARCHAR," in ddl
        assert "    HEALTH_SCORE FLOAT," in ddl
        assert "    IS_PRIMARY BOOLEAN," in ddl
        assert "PRIMARY KEY (ID)" in ddl
        assert "FOREIGN KEY (ACCOUNT_ID) REFERENCES ACCOUNTS (ID)" in ddl
        assert '"' not in ddl, "identifiers are unquoted so folding applies"
        assert "not enforced" in ddl

    def test_ddl_orders_parents_before_children(self):
        ddl = generate(sample_ontology(), dialect="snowflake", seed=1).ddl
        accounts, contacts, tickets = (ddl.index(f"CREATE TABLE {t}") for t in ("ACCOUNTS", "CONTACTS", "TICKETS"))
        assert accounts < contacts < tickets

    def test_loader_is_one_multirow_insert_per_table(self):
        load = generate(sample_ontology(), dialect="snowflake", seed=1, rows_per_entity=5).load_sql
        assert load.count("INSERT INTO") == 3
        assert "INSERT INTO CONTACTS (ID, ACCOUNT_ID, FULL_NAME, IS_PRIMARY) VALUES\n" in load
        assert load.count("\n    (") == 15
        assert "TRUE" in load or "FALSE" in load


class TestClickHouseDialect:
    def test_types_roundtrip_through_the_real_connector_and_forward_map(self):
        # ClickHouseConnector._clean_type lowercases the system.columns type;
        # pg_type_to_json_type must land back on the declared JSON type.
        for json_type, physical in CLICKHOUSE_TYPE_FOR_JSON_TYPE.items():
            assert pg_type_to_json_type(_clean_type(physical)) == json_type
            assert pg_type_to_json_type(_clean_type(f"Nullable({physical})")) == json_type

    def test_ddl_is_mergetree_ordered_by_spine_with_nullable_properties(self):
        ddl = generate(sample_ontology(), dialect="clickhouse", seed=1).ddl
        assert "CREATE TABLE contacts (\n    id Int64,\n" in ddl
        assert (
            "    full_name Nullable(String),\n    is_primary Nullable(Bool)\n) ENGINE = MergeTree ORDER BY (id);"
        ) in ddl
        assert "PRIMARY KEY" not in ddl and "FOREIGN KEY (" not in ddl

    def test_fk_intent_recorded_as_column_comment_and_header(self):
        ddl = generate(sample_ontology(), dialect="clickhouse", seed=1).ddl
        assert f"account_id Int64 COMMENT '{fk_intent_comment('accounts', 'id')}'" in ddl
        assert "-- FOREIGN KEY intent: contacts.account_id -> accounts(id)  [contactsToAccounts]" in ddl
        load = generate(sample_ontology(), dialect="clickhouse", seed=1).load_sql
        lines = load.splitlines()
        assert lines[0].startswith("-- Federation Forge") and lines[1].startswith("-- dialect: clickhouse")
        assert lines[2] == "-- FOREIGN KEY intent (not enforceable in ClickHouse): contacts.account_id -> accounts(id)"

    def test_loader_uses_lowercase_boolean_literals(self):
        load = generate(sample_ontology(), dialect="clickhouse", seed=3).load_sql
        assert "TRUE" not in load and "FALSE" not in load
        assert " true" in load or " false" in load
        assert load.count("INSERT INTO") == 3


class TestArangoDialect:
    def test_manifest_shape(self):
        artifacts = generate(sample_ontology(), dialect="arango", seed=1)
        manifest = json.loads(artifacts.ddl)
        assert manifest["forgeManifestVersion"] == MANIFEST_VERSION
        assert manifest["keyField"] == "id"
        assert [c["name"] for c in manifest["collections"]] == ["accounts", "contacts", "tickets"]
        assert {c["entity"] for c in manifest["collections"]} == {"Account", "Contact", "Ticket"}
        edges = {e["name"]: e for e in manifest["edgeCollections"]}
        assert set(edges) == {"contacts_to_accounts", "tickets_to_contacts"}
        assert edges["contacts_to_accounts"]["from"] == {"collection": "contacts", "field": "id"}
        assert edges["contacts_to_accounts"]["to"] == {"collection": "accounts", "field": "account_id"}
        assert edges["contacts_to_accounts"]["relationship"] == "contactsToAccounts"
        assert manifest["graph"]["name"] == GRAPH_NAME
        assert manifest["graph"]["edgeDefinitions"][0] == {
            "collection": "contacts_to_accounts",
            "from": ["contacts"],
            "to": ["accounts"],
        }

    def test_manifest_matches_render_manifest_and_is_sorted(self):
        artifacts = generate(sample_ontology(), dialect="arango", seed=1)
        assert json.loads(artifacts.ddl) == render_manifest(plan_schema(sample_ontology()))
        assert artifacts.ddl == json.dumps(json.loads(artifacts.ddl), indent=2, sort_keys=True) + "\n"

    def test_project_documents_keys_and_edges_on_the_spine(self):
        artifacts = generate(sample_ontology(), dialect="arango", seed=7, rows_per_entity=6)
        docs = project_documents(json.loads(artifacts.ddl), artifacts.rows)
        assert set(docs) == {"accounts", "contacts", "tickets", "contacts_to_accounts", "tickets_to_contacts"}
        for row, doc in zip(artifacts.rows["contacts"], docs["contacts"]):
            assert doc["_key"] == str(row["id"])
            assert {k: v for k, v in doc.items() if k != "_key"} == row
        account_keys = {d["_key"] for d in docs["accounts"]}
        for edge, row in zip(docs["contacts_to_accounts"], artifacts.rows["contacts"]):
            assert set(edge) == {"_key", "_from", "_to"}, "edges carry no relationship properties"
            assert edge["_from"] == f"contacts/{row['id']}"
            assert edge["_to"] == f"accounts/{row['account_id']}"
            assert edge["_to"].split("/")[1] in account_keys

    def test_rows_are_untouched_by_projection(self):
        artifacts = generate(sample_ontology(), dialect="arango", seed=7)
        before = json.dumps(artifacts.rows, sort_keys=True)
        project_documents(json.loads(artifacts.ddl), artifacts.rows)
        assert json.dumps(artifacts.rows, sort_keys=True) == before
        assert "_key" not in artifacts.rows["accounts"][0]

    def test_loader_script_is_valid_standalone_python_with_seed_stamp(self):
        artifacts = generate(sample_ontology(), dialect="arango", seed=421)
        compile(artifacts.load_sql, "forge.load.py", "exec")
        assert "seed: 421" in artifacts.load_sql
        assert "import r2g" not in artifacts.load_sql, "the loader must not depend on r2g"
        assert "from arango import ArangoClient" in artifacts.load_sql
        assert "on_duplicate=\"replace\"" in artifacts.load_sql

    def test_loader_script_projection_agrees_with_library(self):
        # The script embeds its own project_documents; execute it in isolation
        # and compare with the library function so the two cannot drift.
        artifacts = generate(sample_ontology(), dialect="arango", seed=5, rows_per_entity=4)
        namespace: dict = {"__name__": "forge_loader_under_test"}
        exec(compile(artifacts.load_sql, "forge.load.py", "exec"), namespace)
        manifest = json.loads(artifacts.ddl)
        assert namespace["project_documents"](manifest, artifacts.rows) == project_documents(manifest, artifacts.rows)

    def test_write_to_uses_arango_file_names(self, tmp_path):
        artifacts = generate(sample_ontology(), dialect="arango", seed=5)
        names = sorted(p.rsplit("/", 1)[-1] for p in artifacts.write_to(str(tmp_path)))
        assert names == ["forge.collections.json", "forge.load.py", "forge.rows.json"]
        assert json.loads((tmp_path / "forge.collections.json").read_text())["dialect"] == "arango"

    @pytest.mark.parametrize("name", SQL_DIALECTS)
    def test_write_to_keeps_sql_file_names_for_sql_dialects(self, name, tmp_path):
        paths = generate(sample_ontology(), dialect=name, seed=5).write_to(str(tmp_path))
        names = sorted(p.rsplit("/", 1)[-1] for p in paths)
        assert names == ["forge.load.sql", "forge.rows.json", "forge.sql"]


def test_sample_conceptual_still_refused_when_type_missing_for_any_dialect():
    doc = sample_conceptual()
    doc["entities"][0]["properties"][0]["type"] = "temporal"
    for name in SUPPORTED_DIALECTS:
        with pytest.raises(ForgeError, match="unsupported type"):
            generate(ForgeOntology.from_conceptual(doc), dialect=name, seed=1)
