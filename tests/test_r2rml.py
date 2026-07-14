"""Tests for the R2RML emitter (src/r2g/r2rml.py).

Parses the emitted Turtle with rdflib and asserts the R2RML triple structure —
so the tests fail on both syntax errors and wrong mappings, not just string
drift.
"""

from __future__ import annotations

import sys

import pytest

rdflib = pytest.importorskip("rdflib")
from rdflib import RDF, Namespace  # noqa: E402

from r2g.r2rml import (  # noqa: E402
    DEFAULT_CONCEPT_BASE,
    R2RMLError,
    mapping_to_r2rml,
)
from r2g.types import (  # noqa: E402
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    MappingConfig,
    Schema,
    Table,
)

RR = Namespace("http://www.w3.org/ns/r2rml#")
CONCEPT = Namespace(DEFAULT_CONCEPT_BASE)
BASE = "http://test.example/m"


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Restore structlog to real stderr after a CliRunner invocation."""
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _config() -> MappingConfig:
    return MappingConfig(
        source_schema="shop",
        collections={
            "users": CollectionMapping(
                source_table="users",
                target_collection="User",
                field_mappings={"full_name": "name"},
            ),
            "orders": CollectionMapping(
                source_table="orders",
                target_collection="Order",
                field_expressions=[
                    FieldExpression(target="total_cents", sources=["total"], expression="total*100"),
                ],
            ),
            "user_orders": CollectionMapping(
                source_table="user_orders",
                target_collection="placed",
                collection_type="edge",
                is_join_table=True,
            ),
        },
        edges=[
            EdgeDefinition(
                edge_collection="placed_by",
                from_collection="orders",
                to_collection="users",
                from_fields=["user_id"],
                to_fields=["id"],
            ),
        ],
    )


def _schema() -> Schema:
    return Schema(
        tables={
            "users": Table(
                name="users",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="full_name", data_type="text"),
                    Column(name="email", data_type="varchar", is_nullable=True),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="user_id", data_type="integer"),
                    Column(name="total", data_type="numeric"),
                    Column(name="created", data_type="timestamp"),
                ],
                primary_key=["id"],
            ),
        }
    )


def _parse(ttl: str):
    g = rdflib.Graph()
    g.parse(data=ttl, format="turtle", publicID=BASE)
    return g


def _triples_maps(g) -> dict:
    """entity local-name -> TriplesMap node, keyed by subjectMap rr:class."""
    out = {}
    for tm in g.subjects(RDF.type, RR.TriplesMap):
        sm = g.value(tm, RR.subjectMap)
        cls = g.value(sm, RR["class"])
        out[str(cls).rsplit("#", 1)[-1]] = tm
    return out


def _poms(g, tm) -> dict:
    """predicate local-name -> objectMap node for a TriplesMap."""
    out = {}
    for pom in g.objects(tm, RR.predicateObjectMap):
        pred = g.value(pom, RR.predicate)
        out[str(pred).rsplit("#", 1)[-1]] = g.value(pom, RR.objectMap)
    return out


def test_parses_as_valid_turtle():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    assert len(g) > 0


def test_one_triplesmap_per_document_entity():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    tms = _triples_maps(g)
    # Join table 'placed' must NOT get a TriplesMap.
    assert set(tms) == {"User", "Order"}


def test_logical_table_names():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    tms = _triples_maps(g)
    user_lt = g.value(tms["User"], RR.logicalTable)
    assert str(g.value(user_lt, RR.tableName)) == "users"
    order_lt = g.value(tms["Order"], RR.logicalTable)
    assert str(g.value(order_lt, RR.tableName)) == "orders"


def test_subject_template_and_class():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    tms = _triples_maps(g)
    sm = g.value(tms["User"], RR.subjectMap)
    assert str(g.value(sm, RR["class"])) == str(CONCEPT.User)
    assert "{id}" in str(g.value(sm, RR.template))


def test_column_rename_maps_property_to_source_column():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    poms = _poms(g, _triples_maps(g)["User"])
    # conceptual property 'name' <- SQL column 'full_name'.
    assert str(g.value(poms["name"], RR.column)) == "full_name"
    # 'email' passes through unchanged.
    assert str(g.value(poms["email"], RR.column)) == "email"


def test_datatype_mapping():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    poms = _poms(g, _triples_maps(g)["Order"])
    assert str(g.value(poms["id"], RR.datatype)).endswith("integer")
    assert str(g.value(poms["total"], RR.datatype)).endswith("decimal")
    assert str(g.value(poms["created"], RR.datatype)).endswith("dateTime")
    # varchar -> string-like -> no explicit datatype.
    user_poms = _poms(g, _triples_maps(g)["User"])
    assert g.value(user_poms["email"], RR.datatype) is None


def test_fk_referencing_object_map():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    tms = _triples_maps(g)
    om = _poms(g, tms["Order"])["placed_by"]
    # The referencing object map points at the User TriplesMap...
    assert g.value(om, RR.parentTriplesMap) == tms["User"]
    # ...and joins child user_id (orders) to parent id (users).
    jc = g.value(om, RR.joinCondition)
    assert str(g.value(jc, RR.child)) == "user_id"
    assert str(g.value(jc, RR.parent)) == "id"


def test_missing_source_table_raises():
    schema = _schema()
    del schema.tables["users"]
    with pytest.raises(R2RMLError):
        mapping_to_r2rml(_config(), schema)


def test_no_primary_key_falls_back_to_all_columns():
    config = MappingConfig(
        collections={
            "t": CollectionMapping(source_table="t", target_collection="T"),
        },
    )
    schema = Schema(
        tables={
            "t": Table(
                name="t",
                columns=[
                    Column(name="a", data_type="text"),
                    Column(name="b", data_type="text"),
                ],
                primary_key=[],
            )
        }
    )
    ttl = mapping_to_r2rml(config, schema)
    g = _parse(ttl)
    sm = g.value(_triples_maps(g)["T"], RR.subjectMap)
    template = str(g.value(sm, RR.template))
    assert "{a}" in template and "{b}" in template
    assert "no primary key" in ttl  # the explanatory note


def test_concept_base_default_matches_aql_leg():
    # The AQL leg (arango-sparql-py) synthesizes concepts under this namespace;
    # both legs must agree so a federated SPARQL query means the same thing.
    assert DEFAULT_CONCEPT_BASE == "urn:arango-sparql:concept#"


def test_join_table_edge_endpoint_is_skipped_with_note():
    config = _config()
    config.edges.append(
        EdgeDefinition(
            edge_collection="tagged",
            from_collection="user_orders",  # a join table, not a document entity
            to_collection="users",
            from_fields=["x"],
            to_fields=["id"],
        )
    )
    ttl = mapping_to_r2rml(config, _schema())
    assert "Skipped edges" in ttl and "tagged" in ttl
    # Still valid Turtle.
    assert len(_parse(ttl)) > 0


def test_export_r2rml_cli(tmp_path):
    from typer.testing import CliRunner

    from r2g.main import app

    schema_path = tmp_path / "schema.json"
    _schema().save_to_file(str(schema_path))
    config_path = tmp_path / "mapping.yaml"
    from r2g.config import ConfigManager

    ConfigManager.save_config(_config(), str(config_path))

    out = tmp_path / "m.ttl"
    result = CliRunner().invoke(
        app,
        ["export-r2rml", "--config", str(config_path), "--schema", str(schema_path), "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    g = _parse(out.read_text(encoding="utf-8"))
    assert set(_triples_maps(g)) == {"User", "Order"}
