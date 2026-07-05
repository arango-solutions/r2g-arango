from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_serializer, model_validator
from relational_schema_analyzer.types import Column as _RsaColumn
from relational_schema_analyzer.types import ForeignKey as ForeignKey  # noqa: F401  (re-export)
from relational_schema_analyzer.types import PhysicalSchema as _RsaSchema
from relational_schema_analyzer.types import Table as _RsaTable

# ArangoDB system attributes. These are managed by ArangoDB / the R2G
# transformers (``_key`` is derived from the source PK; ``_from``/``_to`` are
# built from FK data) and must never be renamed or used as a mapping target.
RESERVED_ATTRIBUTES: frozenset[str] = frozenset({"_id", "_key", "_rev", "_from", "_to"})


# ``ForeignKey`` is re-exported from ``relational_schema_analyzer.types`` (imported
# above). RSA's ForeignKey is a superset of r2g's original — identical fields,
# accessors (``column``/``foreign_column``/``is_composite``), singular-form
# acceptance, and serializer key order — plus an ``is_unique`` cardinality hint
# that r2g never sets and RSA omits from output when false. It therefore
# serializes byte-identically to r2g's historical shape (asserted by the
# serialization compat corpus), so unifying it here is a no-op on disk.


class Classification(BaseModel):
    """Governance classification carried from an external catalog (PRD Phase 9).

    ``tags`` are catalog tag FQNs (e.g. ``"PII.Sensitive"``); ``tier`` is a
    confidentiality-tier FQN if present (e.g. ``"Tier.Tier1"``); ``glossary_terms``
    are business-glossary references; ``source`` records provenance. Everything is
    optional/empty by default, so sources not imported from a catalog carry no
    classification and behave exactly as before.
    """

    tags: List[str] = Field(default_factory=list)
    tier: Optional[str] = None
    glossary_terms: List[str] = Field(default_factory=list)
    source: str = "catalog"

    @property
    def is_empty(self) -> bool:
        return not self.tags and self.tier is None and not self.glossary_terms


class Column(_RsaColumn):
    """Physical column: the shared RSA column + r2g's Phase-9 governance.

    Subclasses ``relational_schema_analyzer.types.Column`` so RSA's own code paths
    (typemap, baseline, FK heuristics) accept r2g columns directly — the basis for
    the dependency reversal (see ``docs/internal/DESIGN-rsa-compat-layer.md``).
    r2g re-adds ``classification`` as a first-class field and serializes to its
    **historical 5-key shape** (dropping RSA's enrichment fields, the computed
    ``type_category``, and the ``extra`` passthrough) so existing snapshots and
    ``catalog.json`` stay byte-identical. On input, a ``classification`` carried in
    RSA's ``extra['classification']`` (an RSA-native producer) is lifted onto the
    field so both representations round-trip.
    """

    # Governance classification (PRD Phase 9). ``None`` for non-catalog sources
    # and untagged columns; populated at ``source snapshot`` for catalog-imported
    # sources by merging the resolved classification map onto the schema.
    classification: Optional[Classification] = None

    @model_validator(mode="before")
    @classmethod
    def _accept_classification_from_extra(cls, data: Any) -> Any:
        if isinstance(data, dict) and not data.get("classification"):
            extra = data.get("extra")
            if isinstance(extra, dict) and extra.get("classification"):
                data = {**data, "classification": extra["classification"]}
        return data

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        full = handler(self)
        return {
            "name": full["name"],
            "data_type": full["data_type"],
            "is_nullable": full["is_nullable"],
            "is_primary_key": full["is_primary_key"],
            "classification": full.get("classification"),
        }


class Table(_RsaTable):
    """Physical table: the shared RSA table narrowed to r2g's :class:`Column`.

    Serializes to r2g's historical key set (dropping RSA's ``schema_name`` /
    ``comment`` / ``is_view`` / constraint / index enrichment and the ``extra``
    passthrough) for byte-stable snapshots.
    """

    # Narrowing the inherited RSA field to r2g's Column subclass (safe: every
    # r2g Column is an RSA Column; the invariance warning does not apply at runtime).
    columns: List[Column]  # type: ignore[assignment]

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        full = handler(self)
        return {
            "name": full["name"],
            "columns": full["columns"],
            "primary_key": full["primary_key"],
            "foreign_keys": full["foreign_keys"],
            "is_partitioned": full["is_partitioned"],
            "partition_of": full["partition_of"],
        }


class Schema(_RsaSchema):
    """Physical schema: the shared RSA :class:`PhysicalSchema` narrowed to r2g's
    :class:`Table`. Serializes to ``{"tables": …}`` only (dropping RSA's optional
    ``source`` provenance) for byte-stable snapshots."""

    # Narrowing the inherited RSA field to r2g's Table subclass (safe, as above).
    tables: Dict[str, Table] = {}  # type: ignore[assignment]

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        full = handler(self)
        return {"tables": full["tables"]}

    def save_to_file(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load_from_file(cls, path: str) -> Schema:
        with open(path, "r") as f:
            return cls.model_validate_json(f.read())


class EdgeDefinition(BaseModel):
    """Defines how a foreign key becomes an edge collection.

    Supports single-column (``from_field``/``to_field``) and composite
    (``from_fields``/``to_fields``) FK relationships.
    """
    edge_collection: str
    from_collection: str
    to_collection: str
    from_fields: List[str]
    to_fields: List[str]

    @model_validator(mode="before")
    @classmethod
    def _accept_singular(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "from_field" in data and "from_fields" not in data:
                data["from_fields"] = cls._split_field_spec(data.pop("from_field"))
            if "to_field" in data and "to_fields" not in data:
                data["to_fields"] = cls._split_field_spec(data.pop("to_field"))
            # Also normalize pre-existing list forms in case a caller passed
            # ``from_fields=["a, b"]`` (a single comma-joined entry) rather
            # than a proper list.
            for plural in ("from_fields", "to_fields"):
                if plural in data and isinstance(data[plural], list):
                    flat: list[str] = []
                    for item in data[plural]:
                        flat.extend(cls._split_field_spec(item))
                    data[plural] = flat
        return data

    @staticmethod
    def _split_field_spec(value: Any) -> list[str]:
        """Accept a single column name or a comma-separated list and return
        a clean list of column names.

        The UI represents composite FKs as ``"order_id, product_id"`` in a
        single string field; normalize that here so validation and loading
        treat each column individually.
        """
        if value is None:
            return []
        if isinstance(value, list):
            out: list[str] = []
            for v in value:
                out.extend(EdgeDefinition._split_field_spec(v))
            return out
        s = str(value).strip()
        if not s:
            return []
        if "," in s:
            return [part.strip() for part in s.split(",") if part.strip()]
        return [s]

    @property
    def from_field(self) -> str:
        return self.from_fields[0]

    @property
    def to_field(self) -> str:
        return self.to_fields[0]

    @property
    def is_composite(self) -> bool:
        return len(self.from_fields) > 1

    @model_serializer
    def _serialize(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "edge_collection": self.edge_collection,
            "from_collection": self.from_collection,
            "to_collection": self.to_collection,
        }
        if len(self.from_fields) <= 1 and len(self.to_fields) <= 1:
            d["from_field"] = self.from_fields[0] if self.from_fields else ""
            d["to_field"] = self.to_fields[0] if self.to_fields else ""
        else:
            d["from_fields"] = self.from_fields
            d["to_fields"] = self.to_fields
        return d


class FieldExpression(BaseModel):
    """A mapping function that computes a target property from one or more source columns.

    The default (identity) mapping has ``expression=""`` and ``sources=[target]`` (or a single
    rename when ``sources`` is set to a different column). Non-identity mappings carry an
    expression string in the selected engine. Multiple entries in ``sources`` represent
    fan-in: several source columns feed a single target property.
    """

    target: str
    sources: List[str] = Field(default_factory=list)
    expression: str = ""
    engine: Literal["aql", "ksql", "python"] = "aql"
    description: str = ""

    @property
    def is_identity(self) -> bool:
        """True when this mapping is a pure pass-through (no expression, single source)."""
        return self.expression.strip() == "" and len(self.sources) <= 1


class CollectionMapping(BaseModel):
    """Maps a PostgreSQL table to an ArangoDB collection."""
    source_table: str
    target_collection: str
    collection_type: str = "document"  # "document" or "edge"
    is_join_table: bool = False
    field_mappings: Dict[str, str] = Field(default_factory=dict)
    exclude_fields: List[str] = Field(default_factory=list)
    include_fields: Optional[List[str]] = None
    field_expressions: List[FieldExpression] = Field(default_factory=list)


NameCase = Literal["preserve", "snake", "camel", "pascal"]


class NamingConvention(BaseModel):
    """A naming convention applied across a mapping.

    Each field selects the case style for a class of identifiers. ``preserve``
    leaves names untouched (the historical pass-through behaviour). Stored on
    :class:`MappingConfig` as a record of the last convention applied; the
    actual names are *materialized* into ``target_collection`` /
    ``field_mappings`` / ``edge_collection`` so they remain visible and editable.
    """

    collections: NameCase = "preserve"
    properties: NameCase = "preserve"
    edges: NameCase = "preserve"


class MappingConfig(BaseModel):
    """Top-level mapping configuration."""
    source_schema: str = "public"
    collections: Dict[str, CollectionMapping] = Field(default_factory=dict)
    edges: List[EdgeDefinition] = Field(default_factory=list)
    type_overrides: Dict[str, str] = Field(default_factory=dict)
    key_separator: str = "_"
    naming_convention: Optional[NamingConvention] = None

    def save_to_file(self, path: str):
        with open(path, 'w') as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load_from_file(cls, path: str) -> "MappingConfig":
        with open(path, 'r') as f:
            return cls.model_validate_json(f.read())
