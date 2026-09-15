"""Federation Forge roundtrip — ``clickhouse`` dialect (ADR-0006 D-3, S2).

**Introspection is by system tables, through r2g's own ``ClickHouseConnector``,
because RSA has no ClickHouse connector.** r2g's connector is the real forward
leg the fabric uses for ClickHouse (``system.tables`` / ``system.columns``
into RSA's ``PhysicalSchema`` shape — r2g's ``Schema`` *is* that type), so the
estate stays in the loop as D-3 demands; nothing forge-internal introspects.

ClickHouse declares no foreign keys, so this is the first live
**constraint-stripped** shape and D-3's two-branch contract applies: the
analyzers must either *recover* the keys by inference or *report them
absent* — a wrong key, or silence, is the failure. Concretely:

- declared branch: ``get_schema()`` reports every ``foreign_keys == []``
  (honestly absent) while the PK comes back from ``is_in_primary_key``;
- inference branch: r2g's ``infer_foreign_keys`` and RSA's baseline
  (``propose_ontology_from_schema``) both recover exactly the planned edges,
  and Auto-Map + the CSI emitter over the inferred keys reproduce ``O``.

Runs against the compose stack's ``clickhouse`` service (``CLICKHOUSE_DSN``,
default ``clickhouse://r2g:r2g_test_2026@localhost:8124/forge``); skipped
cleanly when unreachable. One throwaway database per run, dropped in
``finally``.
"""

from __future__ import annotations

import uuid
from typing import Any, Iterator, Tuple
from urllib.parse import urlparse, urlunparse

import pytest

from r2g.config import ConfigManager, pg_type_to_json_type
from r2g.connectors.clickhouse import ClickHouseConnector
from r2g.csi import mapping_to_csi, validate_csi
from r2g.fk_inference import infer_foreign_keys
from r2g.forge import (
    ForgeArtifacts,
    ForgeOntology,
    column_name,
    foreign_key_column,
    generate,
    split_sql_statements,
    table_name,
)
from r2g.forge.dialects.clickhouse import fk_intent_comment
from r2g.rsa_ontology import propose_ontology_from_schema
from r2g.types import ForeignKey, Schema

from .conftest import CLICKHOUSE_DSN, requires_clickhouse
from .forge_support import (
    ROWS_PER_ENTITY,
    SEED,
    assert_business_collisions_are_plumbing_only,
    assert_conceptual_model_matches,
    expected_fk_pairs,
    expected_tables,
    forge_ontology,
)

pytestmark = requires_clickhouse


def _dsn_for_database(dsn: str, database: str) -> str:
    parts = urlparse(dsn)
    return urlunparse(parts._replace(path=f"/{database}"))


@pytest.fixture
def forge_database() -> Iterator[Tuple[Any, str, str]]:
    """A throwaway ClickHouse database; yields ``(admin_client, dsn, name)``."""
    import clickhouse_connect

    name = f"forge_rt_{uuid.uuid4().hex[:8]}"
    admin = clickhouse_connect.get_client(dsn=CLICKHOUSE_DSN)
    admin.command(f"CREATE DATABASE {name}")
    try:
        yield admin, _dsn_for_database(CLICKHOUSE_DSN, name), name
    finally:
        try:
            admin.command(f"DROP DATABASE IF EXISTS {name}")
        finally:
            admin.close()


@pytest.fixture
def loaded_federation(forge_database) -> Tuple[ForgeOntology, ForgeArtifacts, str, str]:
    """generate(O) replayed statement by statement (ClickHouse HTTP executes
    one statement per call); yields ``(ontology, artifacts, dsn, database)``."""
    import clickhouse_connect

    _admin, dsn, name = forge_database
    ontology = forge_ontology()
    artifacts = generate(ontology, dialect="clickhouse", seed=SEED, rows_per_entity=ROWS_PER_ENTITY)
    client = clickhouse_connect.get_client(dsn=dsn)
    try:
        for statement in split_sql_statements(artifacts.ddl) + split_sql_statements(artifacts.load_sql):
            client.command(statement)
    finally:
        client.close()
    return ontology, artifacts, dsn, name


def _with_inferred_keys(schema: Schema) -> Schema:
    """Apply r2g's FK inference to the introspected schema as declared keys —
    the step the fabric's onboarding takes for a keyless source."""
    for inferred in infer_foreign_keys(schema):
        schema.tables[inferred.table].foreign_keys.append(
            ForeignKey(
                columns=list(inferred.columns),
                foreign_table=inferred.foreign_table,
                foreign_columns=list(inferred.foreign_columns),
                constraint_name=f"inferred_{inferred.table}_{'_'.join(inferred.columns)}",
            )
        )
    return schema


def test_declared_branch_reports_keys_absent_and_pk_present(loaded_federation):
    """D-3 two-branch, declared side: PK recovered from the sorting key, FKs
    honestly reported absent (never invented from the column comment)."""
    ontology, _artifacts, dsn, _name = loaded_federation
    schema = ClickHouseConnector(dsn).get_schema()
    assert set(schema.tables) == expected_tables(ontology)
    for table in schema.tables.values():
        assert table.primary_key == ["id"], f"{table.name}: PK not recovered from ORDER BY"
        assert table.foreign_keys == [], f"{table.name}: ClickHouse cannot declare FKs; got {table.foreign_keys}"


def test_fk_intent_survives_as_column_comment(loaded_federation):
    """The recorded intent is in the database, where a human (or a future
    comment-aware inference) can read it."""
    import clickhouse_connect

    ontology, _artifacts, dsn, name = loaded_federation
    client = clickhouse_connect.get_client(dsn=dsn)
    try:
        rows = client.query(
            "SELECT table, name, comment FROM system.columns WHERE database = {db:String} AND comment != ''",
            parameters={"db": name},
        ).result_rows
    finally:
        client.close()
    assert {(t, c, comment) for t, c, comment in rows} == {
        (table_name(r.from_entity), foreign_key_column(r.to_entity), fk_intent_comment(table_name(r.to_entity), "id"))
        for r in ontology.relationships
    }


def test_types_roundtrip_through_the_real_connector(loaded_federation):
    """F-3 on ClickHouse: ``Nullable(...)`` stripped by the connector, base
    types map back to the declared JSON types exactly."""
    ontology, _artifacts, dsn, _name = loaded_federation
    schema = ClickHouseConnector(dsn).get_schema()
    for entity in ontology.entities:
        table = schema.tables[table_name(entity.name)]
        columns = {c.name: c for c in table.columns}
        for prop in entity.properties:
            column = columns[column_name(prop.name)]
            got = pg_type_to_json_type(column.data_type)
            assert got == prop.type, f"{entity.name}.{prop.name}: {column.data_type} -> {got} != {prop.type}"
            assert column.is_nullable, f"{entity.name}.{prop.name}: property columns are Nullable"
        assert not columns["id"].is_nullable


def test_inference_branch_recovers_exactly_the_planned_edges(loaded_federation):
    """D-3 two-branch, inference side: both real inference engines recover the
    spine — no extra, no missing, no wrong direction."""
    ontology, _artifacts, dsn, _name = loaded_federation
    schema = ClickHouseConnector(dsn).get_schema()

    inferred = {(i.table, i.foreign_table) for i in infer_foreign_keys(schema)}
    assert inferred == expected_fk_pairs(ontology)
    for candidate in infer_foreign_keys(schema):
        assert list(candidate.foreign_columns) == ["id"]

    proposal, meta = propose_ontology_from_schema(schema)
    assert {c.source_table for c in proposal.collections} == expected_tables(ontology)
    assert {(e.from_collection, e.to_collection) for e in proposal.edges} == expected_fk_pairs(ontology)
    assert "inferred_foreign_keys" in (meta.get("detectedPatterns") or meta.get("detected_patterns") or [])


def test_roundtrip_reproduces_the_ontology_over_inferred_keys(loaded_federation):
    """The D-3 property: REAL Auto-Map + REAL CSI emitter over the introspected
    schema with the inferred keys applied reproduce ``O``."""
    ontology, _artifacts, dsn, _name = loaded_federation
    schema = _with_inferred_keys(ClickHouseConnector(dsn).get_schema())
    mapping = ConfigManager.generate_default_config(schema)
    csi = mapping_to_csi(mapping, schema, source_type="clickhouse", source_ref="forge", label_policy="warn")
    validate_csi(csi)
    conceptual = csi["conceptualModel"]
    assert_conceptual_model_matches(ontology, conceptual["entities"], conceptual["relationships"])
    assert_business_collisions_are_plumbing_only(ontology, csi)


def test_without_inference_relationships_are_absent_not_wrong(loaded_federation):
    """Silence is only acceptable when it is *reported* silence: with no keys
    applied, Auto-Map derives no edges at all rather than a wrong one."""
    ontology, _artifacts, dsn, _name = loaded_federation
    schema = ClickHouseConnector(dsn).get_schema()
    mapping = ConfigManager.generate_default_config(schema)
    assert mapping.edges == []
    csi = mapping_to_csi(mapping, schema, source_type="clickhouse", source_ref="forge", label_policy="warn")
    assert csi["conceptualModel"]["relationships"] == []
    assert {e["name"] for e in csi["conceptualModel"]["entities"]} == {e.name for e in ontology.entities}


def test_loaded_data_matches_artifacts_and_spine_joins(loaded_federation):
    """The rows landed and the join spine holds in ClickHouse SQL (F-5) — with
    no engine enforcement anywhere, only synthesis order guarantees it."""
    import clickhouse_connect

    _ontology, artifacts, dsn, _name = loaded_federation
    client = clickhouse_connect.get_client(dsn=dsn)
    try:
        for table, rows in artifacts.rows.items():
            assert client.command(f"SELECT count() FROM {table}") == ROWS_PER_ENTITY == len(rows)
        orphans = client.command(
            "SELECT count() FROM contacts c LEFT JOIN accounts a ON c.account_id = a.id WHERE a.id = 0"
        )
        assert orphans == 0
    finally:
        client.close()
