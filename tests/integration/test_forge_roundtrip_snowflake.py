"""Federation Forge roundtrip — ``snowflake`` dialect (ADR-0006 D-3, S2).

``introspect(generate(O)) ≡ O`` through the REAL forward pipeline against a
real Snowflake account:

    forge.generate(O, "snowflake") -> throwaway schema (DDL + INSERTs)
                                   -> RSA SnowflakeConnector.get_schema()     (real introspection)
                                   -> ConfigManager.generate_default_config() (real Auto-Map)
                                   -> mapping_to_csi() / rsa_ontology         (real emitters)
                                   -> compare to O (tests/integration/forge_support.py)

Configuration (skipped cleanly when absent): ``SNOWFLAKE_ACCOUNT``,
``SNOWFLAKE_USER``, ``SNOWFLAKE_WAREHOUSE``, ``SNOWFLAKE_DATABASE`` and either
``SNOWFLAKE_PASSWORD`` or ``SNOWFLAKE_PRIVATE_KEY_FILE`` (+ optional
``SNOWFLAKE_PRIVATE_KEY_FILE_PWD``). ``SNOWFLAKE_FORGE_ROLE`` (default:
``SNOWFLAKE_ROLE``) is the role used for the whole test and must be allowed to
``CREATE SCHEMA`` on the database — the fabric's read-only role is not, so a
privilege refusal is a *skip* with the reason, not a failure. One unique
schema per run (``FORGE_RT_<seed>_<pid>_<hex>``), always dropped in ``finally``.

Key-pair auth note: RSA's connector builds ``snowflake.connector.connect``
kwargs from a URL, which cannot carry a private-key path, so the fixture wraps
``connect`` to add the auth kwargs. Only authentication is shimmed — every
introspection statement is RSA's own.
"""

from __future__ import annotations

import os
import uuid
from typing import Any, Dict, Iterator, Tuple

import pytest

from r2g.config import ConfigManager, pg_type_to_json_type
from r2g.csi import mapping_to_csi, validate_csi
from r2g.forge import ForgeArtifacts, ForgeOntology, generate, get_dialect, split_sql_statements
from r2g.rsa_ontology import propose_ontology_from_schema
from r2g.types import Schema

from .conftest import requires_snowflake
from .forge_support import (
    ROWS_PER_ENTITY,
    SEED,
    assert_business_collisions_are_plumbing_only,
    assert_conceptual_model_matches,
    expected_fk_pairs,
    expected_tables,
    forge_ontology,
)

pytestmark = requires_snowflake

DIALECT = get_dialect("snowflake")

#: Type roundtrips the forward pipeline cannot make today, pinned so the gap is
#: *named* rather than silently tolerated (D-3: never silently wrong). Snowflake
#: has one integer type, ``NUMBER(38,0)``; ``INFORMATION_SCHEMA.COLUMNS`` reports
#: it as bare ``NUMBER`` and ``pg_type_to_json_type("number")`` is ``float``
#: because the connector does not carry ``NUMERIC_SCALE``. Closing the gap
#: (RSA reporting scale + a scale-aware map) makes this test fail here, which is
#: the prompt to delete the entry.
KNOWN_TYPE_GAPS: Dict[Tuple[str, str], str] = {("integer", "number"): "float"}


def _connect_kwargs() -> Dict[str, Any]:
    env = os.environ
    kwargs: Dict[str, Any] = {
        "account": env["SNOWFLAKE_ACCOUNT"],
        "user": env["SNOWFLAKE_USER"],
        "warehouse": env["SNOWFLAKE_WAREHOUSE"],
        "database": env["SNOWFLAKE_DATABASE"],
    }
    role = env.get("SNOWFLAKE_FORGE_ROLE") or env.get("SNOWFLAKE_ROLE")
    if role:
        kwargs["role"] = role
    if env.get("SNOWFLAKE_PRIVATE_KEY_FILE"):
        kwargs["private_key_file"] = env["SNOWFLAKE_PRIVATE_KEY_FILE"]
        if env.get("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"):
            kwargs["private_key_file_pwd"] = env["SNOWFLAKE_PRIVATE_KEY_FILE_PWD"]
    else:
        kwargs["password"] = env["SNOWFLAKE_PASSWORD"]
    return kwargs


@pytest.fixture(scope="module")
def forge_schema() -> Iterator[Tuple[Any, str, str]]:
    """A throwaway Snowflake schema; yields ``(connection, database, schema)``.
    Skips (not fails) when the role may not create schemas."""
    snowflake = pytest.importorskip("snowflake.connector")
    kwargs = _connect_kwargs()
    database = kwargs["database"]
    schema = f"FORGE_RT_{SEED}_{os.getpid()}_{uuid.uuid4().hex[:4].upper()}"
    try:
        conn = snowflake.connect(**kwargs)
    except Exception as exc:  # noqa: BLE001 - unreachable account is a skip, not a failure
        pytest.skip(f"Snowflake unreachable: {exc}")
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA {database}.{schema}")
    except snowflake.errors.ProgrammingError as exc:
        conn.close()
        if "Insufficient privileges" in str(exc):
            pytest.skip(
                f"role {kwargs.get('role')!r} may not CREATE SCHEMA on {database}; "
                "set SNOWFLAKE_FORGE_ROLE to a role that can"
            )
        raise
    try:
        yield conn, database, schema
    finally:
        try:
            cur.execute(f"DROP SCHEMA IF EXISTS {database}.{schema} CASCADE")
        finally:
            conn.close()


@pytest.fixture(scope="module")
def loaded_federation(forge_schema) -> Tuple[ForgeOntology, ForgeArtifacts, Schema]:
    """generate(O) loaded into the throwaway schema, then introspected once
    through RSA's connector; yields ``(ontology, artifacts, schema)``."""
    import snowflake.connector as sf
    from relational_schema_analyzer.connectors.snowflake import SnowflakeConnector

    conn, database, schema_name = forge_schema
    ontology = forge_ontology()
    artifacts = generate(ontology, dialect="snowflake", seed=SEED, rows_per_entity=ROWS_PER_ENTITY)

    cur = conn.cursor()
    cur.execute(f"USE SCHEMA {database}.{schema_name}")
    for statement in split_sql_statements(artifacts.ddl) + split_sql_statements(artifacts.load_sql):
        cur.execute(statement)

    kwargs = _connect_kwargs()
    real_connect = sf.connect

    def connect_with_forge_auth(**url_kwargs: Any) -> Any:
        merged = {**url_kwargs, **kwargs}
        merged.pop("password", None) if "private_key_file" in kwargs else None
        merged["schema"] = url_kwargs.get("schema", schema_name)
        return real_connect(**merged)

    sf.connect = connect_with_forge_auth  # RSA imports the module and calls connect on it
    try:
        role = kwargs.get("role", "")
        url = (
            f"snowflake://{kwargs['user']}:forge@{kwargs['account']}/{database}/{schema_name}"
            f"?warehouse={kwargs['warehouse']}&role={role}"
        )
        introspected = SnowflakeConnector(url).get_schema()
    finally:
        sf.connect = real_connect
    # r2g's Schema is RSA's PhysicalSchema narrowed to r2g's Column (drops the
    # provenance envelope) — the shape Auto-Map and the CSI emitter take.
    return ontology, artifacts, Schema.model_validate(introspected.model_dump())


def test_declared_keys_come_back_declared(loaded_federation):
    """F-4 on Snowflake: recorded-but-unenforced PK/FK constraints are exactly
    what ``SHOW PRIMARY KEYS`` / ``SHOW IMPORTED KEYS`` return."""
    ontology, _artifacts, schema = loaded_federation
    assert set(schema.tables) == expected_tables(ontology, DIALECT.physical_table)
    for table in schema.tables.values():
        assert table.primary_key == [DIALECT.physical_column("id")], f"{table.name}: PK not recovered"
    fk_pairs = {(name, fk.foreign_table) for name, t in schema.tables.items() for fk in t.foreign_keys}
    assert fk_pairs == expected_fk_pairs(ontology, DIALECT.physical_table)


def test_types_roundtrip_except_the_pinned_number_gap(loaded_federation, record_property):
    """F-3 on Snowflake, honestly: every column type either maps back to the
    declared JSON type or hits a *named* gap in ``KNOWN_TYPE_GAPS`` — and every
    named gap is actually observed, so a silent upstream fix cannot hide."""
    ontology, _artifacts, schema = loaded_federation
    observed_gaps = set()
    for entity in ontology.entities:
        from r2g.forge import column_name, table_name

        columns = {c.name: c.data_type for c in schema.tables[DIALECT.physical_table(table_name(entity.name))].columns}
        for prop in entity.properties:
            physical_type = columns[DIALECT.physical_column(column_name(prop.name))]
            got = pg_type_to_json_type(physical_type)
            if got == prop.type:
                continue
            gap = (prop.type, physical_type)
            assert KNOWN_TYPE_GAPS.get(gap) == got, (
                f"{entity.name}.{prop.name}: {physical_type!r} introspects as {got!r}, "
                f"declared {prop.type!r} — not a known gap"
            )
            observed_gaps.add(gap)
    assert observed_gaps == set(KNOWN_TYPE_GAPS), (
        f"known type gaps no longer observed: {set(KNOWN_TYPE_GAPS) - observed_gaps} — "
        "the upstream fix landed; remove the pin"
    )
    record_property("forge_snowflake_type_gaps", sorted(map(str, observed_gaps)))


def test_roundtrip_reproduces_the_ontology(loaded_federation):
    """The D-3 property through REAL Auto-Map + REAL CSI emitter."""
    ontology, _artifacts, schema = loaded_federation
    mapping = ConfigManager.generate_default_config(schema)
    csi = mapping_to_csi(mapping, schema, source_type="snowflake", source_ref="forge", label_policy="warn")
    validate_csi(csi)
    conceptual = csi["conceptualModel"]
    assert_conceptual_model_matches(ontology, conceptual["entities"], conceptual["relationships"])
    assert_business_collisions_are_plumbing_only(ontology, csi)


def test_rsa_leg_agrees_on_the_conceptual_shape(loaded_federation):
    ontology, _artifacts, schema = loaded_federation
    proposal, _meta = propose_ontology_from_schema(schema)
    assert {c.source_table for c in proposal.collections} == expected_tables(ontology, DIALECT.physical_table)
    assert {(e.from_collection, e.to_collection) for e in proposal.edges} == expected_fk_pairs(
        ontology, DIALECT.physical_table
    )


def test_loaded_data_matches_artifacts_and_spine_joins(forge_schema, loaded_federation):
    """The rows landed and the join spine holds in Snowflake SQL (F-5) — even
    though Snowflake would not have enforced the FK."""
    conn, database, schema_name = forge_schema
    _ontology, artifacts, _schema = loaded_federation
    cur = conn.cursor()
    for table, rows in artifacts.rows.items():
        cur.execute(f"SELECT count(*) FROM {database}.{schema_name}.{DIALECT.physical_table(table)}")
        assert cur.fetchone()[0] == ROWS_PER_ENTITY == len(rows)
    cur.execute(
        f"SELECT count(*) FROM {database}.{schema_name}.CONTACTS c "
        f"LEFT JOIN {database}.{schema_name}.ACCOUNTS a ON c.ACCOUNT_ID = a.ID WHERE a.ID IS NULL"
    )
    assert cur.fetchone()[0] == 0
