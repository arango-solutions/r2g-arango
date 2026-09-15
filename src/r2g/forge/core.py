"""Federation Forge core — the ontology contract, the dialect-independent
schema plan, and seeded data synthesis.

Commissioned by contextual-data-fabric ADR-0006 (D-4) and specified in
``docs/internal/PLAN-federation-forge.md``: from a conceptual ontology,
*generate* a physical schema plus seeded synthetic data, such that running the
REAL forward pipeline over the loaded result reproduces the ontology::

    introspect(generate(O)) == O      (up to CC-12 normalization and plumbing)

Everything in this module is dialect-independent. :func:`plan_schema` turns an
ontology into a :class:`SchemaPlan` — canonical snake_case tables and columns
with their roles (surrogate PK, FK spine, conceptual property) — and
:func:`synthesize_rows` fills that plan with seeded data **once** (ADR-0006
D-2). The per-system projections live in :mod:`r2g.forge.dialects`; the seam
that composes the two is :func:`r2g.forge.generate.generate`.

Declared PK/FK constraints are always emitted where the target records them
(the constraint-stripped variant is the S3 denormalizer's job); input
ontologies are collision-free by construction (F-6) and are *refused*
otherwise — the forge fails loudly at generate time, never at compare time.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from ..csi import owl_entity_name, owl_property_name
from ..naming import convert_identifier, pluralize

#: Conceptual JSON types the forge synthesizes (PLAN F-1/F-3). Every dialect
#: must declare one roundtrip-stable physical type per entry; the forward
#: pipeline's ``config.pg_type_to_json_type`` must map that physical type back
#: to the same JSON type. Temporal/decimal/uuid are deliberately absent: the
#: forward map collapses them to ``string``, so no honest roundtrip exists yet.
JSON_TYPES: Tuple[str, ...] = ("integer", "float", "boolean", "string")

#: Name of the surrogate primary key every generated table carries (F-4).
SURROGATE_KEY = "id"

#: Roles a planned column can play; the plumbing roles are the only extras the
#: roundtrip tolerates over the conceptual properties.
ROLE_PRIMARY_KEY = "pk"
ROLE_FOREIGN_KEY = "fk"
ROLE_PROPERTY = "property"

Rows = Dict[str, List[Dict[str, Any]]]


class ForgeError(ValueError):
    """A refused ontology or unsupported request — always at generate time."""


class ForgeProperty(BaseModel):
    """One conceptual property: lowerCamel name + JSON type (PLAN F-1)."""

    name: str
    type: str


class ForgeRelationship(BaseModel):
    """One conceptual relationship; ``type`` must equal the name the forward
    pipeline will re-derive (``<from_table>_to_<to_table>`` in lowerCamel), so
    the roundtrip comparison stays exact."""

    type: str
    from_entity: str = Field(alias="fromEntity")
    to_entity: str = Field(alias="toEntity")

    model_config = {"populate_by_name": True}


class ForgeEntity(BaseModel):
    """One conceptual class: singular PascalCase name + typed properties."""

    name: str
    properties: List[ForgeProperty]


class ForgeOntology(BaseModel):
    """A validated conceptual ontology — the forge's only input contract.

    Build one with :meth:`from_conceptual`, which accepts either a bare
    ``{"entities": …, "relationships": …}`` object or a full CSI v1 document
    (the ``conceptualModel`` is taken) and refuses anything the skeleton
    cannot roundtrip byte-honestly.
    """

    entities: List[ForgeEntity]
    relationships: List[ForgeRelationship] = Field(default_factory=list)

    @classmethod
    def from_conceptual(cls, document: Dict[str, Any]) -> "ForgeOntology":
        conceptual = document.get("conceptualModel", document)
        entities = [
            ForgeEntity(
                name=e["name"],
                properties=[
                    ForgeProperty(name=p["name"], type=p.get("type", ""))
                    for p in e.get("properties", [])
                ],
            )
            for e in conceptual.get("entities", [])
        ]
        relationships = [
            ForgeRelationship.model_validate(r)
            for r in conceptual.get("relationships", [])
        ]
        ontology = cls(entities=entities, relationships=relationships)
        ontology.validate_for_forge()
        return ontology

    def entity(self, name: str) -> ForgeEntity:
        for e in self.entities:
            if e.name == name:
                return e
        raise ForgeError(f"unknown entity {name!r}")

    def validate_for_forge(self) -> None:
        """Refuse anything that cannot survive the roundtrip (PLAN F-2/F-6)."""
        if not self.entities:
            raise ForgeError("ontology declares no entities")

        seen_entities: set[str] = set()
        label_owner: Dict[str, str] = {}
        for e in self.entities:
            if e.name in seen_entities:
                raise ForgeError(f"duplicate entity {e.name!r}")
            seen_entities.add(e.name)

            table = table_name(e.name)
            if owl_entity_name(table) != e.name:
                raise ForgeError(
                    f"entity {e.name!r} does not survive the CC-12 naming "
                    f"roundtrip: table {table!r} normalizes back to "
                    f"{owl_entity_name(table)!r}. Rename the class so that "
                    "generate/introspect agree (PLAN F-2)."
                )

            seen_props: set[str] = set()
            for p in e.properties:
                if p.type not in JSON_TYPES:
                    raise ForgeError(
                        f"{e.name}.{p.name}: unsupported type {p.type!r} "
                        f"(the forge supports {sorted(JSON_TYPES)})"
                    )
                if p.name in seen_props:
                    raise ForgeError(f"duplicate property {e.name}.{p.name}")
                seen_props.add(p.name)
                column = column_name(p.name)
                if owl_property_name(column) != p.name:
                    raise ForgeError(
                        f"property {e.name}.{p.name!r} does not survive the "
                        f"CC-12 naming roundtrip: column {column!r} normalizes "
                        f"back to {owl_property_name(column)!r} (PLAN F-2)."
                    )
                if p.name == SURROGATE_KEY:
                    raise ForgeError(
                        f"{e.name}.id collides with the generated surrogate "
                        "primary key; declare a domain identifier instead"
                    )
                # Collision-free by construction (F-6): deliberate collisions
                # are an S3 denormalizer feature, not a skeleton input.
                owner = label_owner.get(p.name)
                if owner is not None:
                    raise ForgeError(
                        f"property label {p.name!r} appears on both {owner} "
                        f"and {e.name}; skeleton ontologies must be "
                        "collision-free (PLAN F-6)"
                    )
                label_owner[p.name] = e.name

        seen_edges: set[tuple[str, str]] = set()
        for r in self.relationships:
            if r.from_entity not in seen_entities:
                raise ForgeError(f"relationship {r.type!r}: unknown fromEntity {r.from_entity!r}")
            if r.to_entity not in seen_entities:
                raise ForgeError(f"relationship {r.type!r}: unknown toEntity {r.to_entity!r}")
            if (r.from_entity, r.to_entity) in seen_edges:
                raise ForgeError(
                    f"duplicate relationship {r.from_entity} -> {r.to_entity}; "
                    "the skeleton supports one relationship per entity pair"
                )
            seen_edges.add((r.from_entity, r.to_entity))

            expected = expected_relationship_type(r.from_entity, r.to_entity)
            if r.type != expected:
                raise ForgeError(
                    f"relationship {r.from_entity} -> {r.to_entity} must be "
                    f"named {expected!r} (the name the forward pipeline "
                    f"re-derives), got {r.type!r}"
                )

            fk_column = foreign_key_column(r.to_entity)
            for p in self.entity(r.from_entity).properties:
                if column_name(p.name) == fk_column:
                    raise ForgeError(
                        f"{r.from_entity}.{p.name} collides with the "
                        f"generated foreign-key column {fk_column!r} for "
                        f"relationship {r.type!r}"
                    )


class ForgeArtifacts(BaseModel):
    """What ``generate`` returns: a schema definition, a loader, and the
    synthesized rows (the pre-partition dataset expected answers are computed
    on).

    ``ddl`` / ``load_sql`` keep their skeleton names for backward compatibility
    but are *dialect-shaped*: SQL dialects put DDL and INSERT statements there;
    the ``arango`` dialect puts a JSON collection manifest in ``ddl`` and a
    standalone Python loader script in ``load_sql``. ``rows`` is always the
    same canonical dataset regardless of dialect (ADR-0006 D-2) — keyed by
    canonical snake_case table and column names, never by the dialect's
    physical spelling.
    """

    dialect: str
    seed: int
    ddl: str
    load_sql: str
    rows: Rows

    def write_to(self, out_dir: str) -> List[str]:
        """Write the schema definition, loader, and ``forge.rows.json`` under
        ``out_dir`` using the dialect's file names (``forge.sql`` /
        ``forge.load.sql`` for SQL dialects) and return the paths written."""
        import os

        from .dialects import get_dialect

        dialect = get_dialect(self.dialect)
        os.makedirs(out_dir, exist_ok=True)
        paths = []
        for filename, content in (
            (dialect.ddl_filename, self.ddl),
            (dialect.loader_filename, self.load_sql),
            ("forge.rows.json", json.dumps(self.rows, indent=2, sort_keys=True) + "\n"),
        ):
            path = os.path.join(out_dir, filename)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(content)
            paths.append(path)
        return paths


def table_name(entity: str) -> str:
    """CC-12 inverse for classes: singular PascalCase -> plural snake_case."""
    return pluralize(convert_identifier(entity, "snake"))


def column_name(prop: str) -> str:
    """CC-12 inverse for properties: lowerCamel -> snake_case."""
    return convert_identifier(prop, "snake")


def foreign_key_column(to_entity: str) -> str:
    """The generated FK column pointing at ``to_entity``: ``account_id``."""
    return f"{convert_identifier(to_entity, 'snake')}_id"


def edge_collection_name(from_entity: str, to_entity: str) -> str:
    """The edge collection r2g's forward Auto-Map derives for an FK
    (``config.ConfigManager.generate_default_config``): ``<from>_to_<to>``.
    The ``arango`` dialect emits exactly this name so ASA reads it back."""
    return f"{table_name(from_entity)}_to_{table_name(to_entity)}"


def expected_relationship_type(from_entity: str, to_entity: str) -> str:
    """The relationship name the forward pipeline re-derives for an FK:
    Auto-Map names the edge ``<from_table>_to_<to_table>`` and the CSI emitter
    lowerCamels it."""
    return owl_property_name(edge_collection_name(from_entity, to_entity))


# ── The dialect-independent schema plan ──────────────────────────────


@dataclass(frozen=True)
class ColumnPlan:
    """One planned column in canonical snake_case spelling.

    ``role`` is one of :data:`ROLE_PRIMARY_KEY`, :data:`ROLE_FOREIGN_KEY`,
    :data:`ROLE_PROPERTY`; ``references`` names the parent *table* for an FK;
    ``prop`` is the conceptual lowerCamel property for a property column.
    Key columns are NOT NULL, property columns nullable — the same across
    every dialect so the introspected nullability agrees.
    """

    name: str
    json_type: str
    role: str
    references: Optional[str] = None
    prop: Optional[str] = None

    @property
    def nullable(self) -> bool:
        return self.role == ROLE_PROPERTY


@dataclass(frozen=True)
class TablePlan:
    """One entity's table: surrogate PK first, then FK spine columns (sorted),
    then the conceptual properties in declared order."""

    entity: str
    table: str
    columns: Tuple[ColumnPlan, ...]

    @property
    def primary_key(self) -> ColumnPlan:
        return self.columns[0]

    @property
    def foreign_keys(self) -> Tuple[ColumnPlan, ...]:
        return tuple(c for c in self.columns if c.role == ROLE_FOREIGN_KEY)

    @property
    def properties(self) -> Tuple[ColumnPlan, ...]:
        return tuple(c for c in self.columns if c.role == ROLE_PROPERTY)


@dataclass(frozen=True)
class EdgePlan:
    """One relationship as the forward pipeline sees it: an FK column on the
    from-table pointing at the to-table's surrogate key, re-derived as the
    edge collection ``<from_table>_to_<to_table>``."""

    relationship: str
    from_entity: str
    to_entity: str
    from_table: str
    to_table: str
    fk_column: str
    edge_collection: str


@dataclass(frozen=True)
class SchemaPlan:
    """The whole generated schema, tables in parent-before-child order.

    A pure function of the ontology (never of the seed), so every dialect
    projects the same plan and the same rows."""

    tables: Tuple[TablePlan, ...]
    edges: Tuple[EdgePlan, ...]

    def table(self, name: str) -> TablePlan:
        for t in self.tables:
            if t.table == name:
                return t
        raise ForgeError(f"unknown planned table {name!r}")


def _topological_entity_order(ontology: ForgeOntology) -> List[str]:
    """Parents before children (FK targets first), deterministic tie-break by
    name. Cycles are refused — the forge generates trees/DAGs only."""
    names = sorted(e.name for e in ontology.entities)
    depends_on: Dict[str, set[str]] = {n: set() for n in names}
    for r in ontology.relationships:
        depends_on[r.from_entity].add(r.to_entity)

    ordered: List[str] = []
    placed: set[str] = set()
    while len(ordered) < len(names):
        progress = False
        for n in names:
            if n in placed or not depends_on[n] <= placed:
                continue
            ordered.append(n)
            placed.add(n)
            progress = True
        if not progress:
            cyclic = sorted(set(names) - placed)
            raise ForgeError(f"relationship cycle among {cyclic}; the forge generates DAGs only")
    return ordered


def plan_schema(ontology: ForgeOntology) -> SchemaPlan:
    """Lay out the canonical physical schema for ``ontology`` (F-2/F-4).

    Refuses cyclic relationship graphs (via :func:`_topological_entity_order`)
    and re-runs :meth:`ForgeOntology.validate_for_forge` so a hand-built
    ontology object gets the same guarantees as one from
    :meth:`ForgeOntology.from_conceptual`.
    """
    ontology.validate_for_forge()
    order = _topological_entity_order(ontology)

    fk_by_entity: Dict[str, List[ColumnPlan]] = {e.name: [] for e in ontology.entities}
    edges: List[EdgePlan] = []
    for r in ontology.relationships:
        fk_by_entity[r.from_entity].append(
            ColumnPlan(
                name=foreign_key_column(r.to_entity),
                json_type="integer",
                role=ROLE_FOREIGN_KEY,
                references=table_name(r.to_entity),
            )
        )
        edges.append(
            EdgePlan(
                relationship=r.type,
                from_entity=r.from_entity,
                to_entity=r.to_entity,
                from_table=table_name(r.from_entity),
                to_table=table_name(r.to_entity),
                fk_column=foreign_key_column(r.to_entity),
                edge_collection=edge_collection_name(r.from_entity, r.to_entity),
            )
        )

    tables: List[TablePlan] = []
    for name in order:
        entity = ontology.entity(name)
        columns: List[ColumnPlan] = [
            ColumnPlan(name=SURROGATE_KEY, json_type="integer", role=ROLE_PRIMARY_KEY)
        ]
        columns.extend(sorted(fk_by_entity[name], key=lambda c: c.name))
        columns.extend(
            ColumnPlan(name=column_name(p.name), json_type=p.type, role=ROLE_PROPERTY, prop=p.name)
            for p in entity.properties
        )
        tables.append(TablePlan(entity=name, table=table_name(name), columns=tuple(columns)))

    # Edges in the same deterministic order as the tables they hang off.
    table_rank = {t.table: i for i, t in enumerate(tables)}
    edges.sort(key=lambda e: (table_rank[e.from_table], e.fk_column))
    return SchemaPlan(tables=tuple(tables), edges=tuple(edges))


# ── Seeded synthesis: once, dialect-independent (F-5, ADR-0006 D-2/D-5) ──


def _synthesize_value(rng: random.Random, json_type: str, prop: str) -> Any:
    if json_type == "integer":
        return rng.randrange(0, 100_000)
    if json_type == "float":
        return round(rng.uniform(0, 100_000), 6)
    if json_type == "boolean":
        return rng.random() < 0.5
    return f"{prop}-{rng.randrange(0, 100_000):05d}"


def synthesize_rows(plan: SchemaPlan, seed: int, rows_per_entity: int) -> Rows:
    """Fill ``plan`` with ``random.Random(seed)`` end to end.

    Rows are produced per table in plan (topological) order, every FK value
    drawn from the already-synthesized parent ids — join-spine agreement by
    construction. Keys are the canonical snake_case names from the plan, so
    the result is byte-identical across dialects for the same
    ``(ontology, seed, rows_per_entity)``; dialects only *project* it.
    """
    if rows_per_entity < 1:
        raise ForgeError("rows_per_entity must be >= 1")
    rng = random.Random(seed)
    rows: Rows = {}
    for table in plan.tables:
        table_rows: List[Dict[str, Any]] = []
        for i in range(1, rows_per_entity + 1):
            row: Dict[str, Any] = {SURROGATE_KEY: i}
            for fk in table.foreign_keys:
                assert fk.references is not None  # planned FKs always reference
                parent_ids = [r[SURROGATE_KEY] for r in rows[fk.references]]
                row[fk.name] = rng.choice(parent_ids)
            for col in table.properties:
                assert col.prop is not None
                row[col.name] = _synthesize_value(rng, col.json_type, col.prop)
            table_rows.append(row)
        rows[table.table] = table_rows
    return rows


def load_ontology_file(path: str) -> ForgeOntology:
    """Read a conceptual ontology (bare or full CSI v1 document) from JSON.

    Unreadable or malformed input is a caller problem and raises
    :class:`ForgeError` (never a bare ``OSError``), so the CLI reports it as a
    refusal with the reason attached.
    """
    try:
        with open(path, "r", encoding="utf-8") as fh:
            try:
                document = json.load(fh)
            except json.JSONDecodeError as exc:
                raise ForgeError(f"{path} is not valid JSON: {exc}") from exc
    except OSError as exc:
        raise ForgeError(f"cannot read ontology file {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ForgeError(f"{path} must contain a JSON object")
    return ForgeOntology.from_conceptual(document)


__all__ = [
    "JSON_TYPES",
    "ROLE_FOREIGN_KEY",
    "ROLE_PRIMARY_KEY",
    "ROLE_PROPERTY",
    "SURROGATE_KEY",
    "ColumnPlan",
    "EdgePlan",
    "ForgeArtifacts",
    "ForgeEntity",
    "ForgeError",
    "ForgeOntology",
    "ForgeProperty",
    "ForgeRelationship",
    "Rows",
    "SchemaPlan",
    "TablePlan",
    "column_name",
    "edge_collection_name",
    "expected_relationship_type",
    "foreign_key_column",
    "load_ontology_file",
    "plan_schema",
    "synthesize_rows",
    "table_name",
]
