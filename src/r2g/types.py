from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_serializer, model_validator

# ArangoDB system attributes. These are managed by ArangoDB / the R2G
# transformers (``_key`` is derived from the source PK; ``_from``/``_to`` are
# built from FK data) and must never be renamed or used as a mapping target.
RESERVED_ATTRIBUTES: frozenset[str] = frozenset({"_id", "_key", "_rev", "_from", "_to"})


class ForeignKey(BaseModel):
    """A foreign key constraint, supporting both single- and multi-column FKs.

    Accepts legacy ``column``/``foreign_column`` (str) or composite
    ``columns``/``foreign_columns`` (list[str]).
    """
    columns: List[str]
    foreign_table: str
    foreign_columns: List[str]
    constraint_name: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _accept_singular(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "column" in data and "columns" not in data:
                data["columns"] = [data.pop("column")]
            if "foreign_column" in data and "foreign_columns" not in data:
                data["foreign_columns"] = [data.pop("foreign_column")]
        return data

    @property
    def column(self) -> str:
        return self.columns[0]

    @property
    def foreign_column(self) -> str:
        return self.foreign_columns[0]

    @property
    def is_composite(self) -> bool:
        return len(self.columns) > 1

    @model_serializer
    def _serialize(self) -> dict[str, Any]:
        d: dict[str, Any] = {"foreign_table": self.foreign_table}
        if len(self.columns) == 1:
            d["column"] = self.columns[0]
            d["foreign_column"] = self.foreign_columns[0]
        else:
            d["columns"] = self.columns
            d["foreign_columns"] = self.foreign_columns
        if self.constraint_name is not None:
            d["constraint_name"] = self.constraint_name
        return d


class Column(BaseModel):
    name: str
    data_type: str
    is_nullable: bool = False
    is_primary_key: bool = False


class Table(BaseModel):
    name: str
    columns: List[Column]
    primary_key: List[str] = []
    foreign_keys: List[ForeignKey] = []


class Schema(BaseModel):
    tables: Dict[str, Table] = {}

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


class TypeMapping(BaseModel):
    """PostgreSQL to JSON type coercion rules."""
    pg_type: str
    json_type: str  # "string", "integer", "float", "boolean", "array", "object"


class TargetGraphSchema(BaseModel):
    """Schema of an ArangoDB target database, obtained via introspection."""

    document_collections: list[dict[str, Any]] = Field(default_factory=list)
    edge_collections: list[dict[str, Any]] = Field(default_factory=list)
    graphs: list[dict[str, Any]] = Field(default_factory=list)


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
