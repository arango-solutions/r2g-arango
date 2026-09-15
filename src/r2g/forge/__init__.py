"""Federation Forge — the reverse generator (contextual-data-fabric ADR-0006, D-4).

Public seam (the only thing the fabric's orchestration may depend on)::

    generate(ontology, dialect, seed) -> ForgeArtifacts(ddl, load_sql, rows)

Layout: :mod:`.core` holds the ontology contract, the dialect-independent
schema plan, and seeded synthesis; :mod:`.dialects` holds the per-system
projections behind one registry; :mod:`.generate` composes them.
"""

from .core import (
    JSON_TYPES,
    SURROGATE_KEY,
    ColumnPlan,
    EdgePlan,
    ForgeArtifacts,
    ForgeEntity,
    ForgeError,
    ForgeOntology,
    ForgeProperty,
    ForgeRelationship,
    SchemaPlan,
    TablePlan,
    column_name,
    edge_collection_name,
    expected_relationship_type,
    foreign_key_column,
    load_ontology_file,
    plan_schema,
    synthesize_rows,
    table_name,
)
from .dialects import DIALECTS, SUPPORTED_DIALECTS, ForgeDialect, get_dialect, split_sql_statements
from .dialects.postgres import PG_TYPE_FOR_JSON_TYPE
from .generate import generate

__all__ = [
    "DIALECTS",
    "JSON_TYPES",
    "PG_TYPE_FOR_JSON_TYPE",
    "SUPPORTED_DIALECTS",
    "SURROGATE_KEY",
    "ColumnPlan",
    "EdgePlan",
    "ForgeArtifacts",
    "ForgeDialect",
    "ForgeEntity",
    "ForgeError",
    "ForgeOntology",
    "ForgeProperty",
    "ForgeRelationship",
    "SchemaPlan",
    "TablePlan",
    "column_name",
    "edge_collection_name",
    "expected_relationship_type",
    "foreign_key_column",
    "generate",
    "get_dialect",
    "load_ontology_file",
    "plan_schema",
    "split_sql_statements",
    "synthesize_rows",
    "table_name",
]
