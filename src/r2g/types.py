from pydantic import BaseModel, Field
from typing import List, Optional, Dict


class ForeignKey(BaseModel):
    column: str
    foreign_table: str
    foreign_column: str
    constraint_name: Optional[str] = None


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

    def save_to_file(self, path: str):
        with open(path, 'w') as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load_from_file(cls, path: str) -> "Schema":
        with open(path, 'r') as f:
            return cls.model_validate_json(f.read())


class EdgeDefinition(BaseModel):
    """Defines how a foreign key becomes an edge collection."""
    edge_collection: str  # name of the ArangoDB edge collection
    from_collection: str  # source vertex collection
    to_collection: str    # target vertex collection
    from_field: str       # FK column in the source table
    to_field: str         # PK column in the target table


class CollectionMapping(BaseModel):
    """Maps a PostgreSQL table to an ArangoDB collection."""
    source_table: str
    target_collection: str
    collection_type: str = "document"  # "document" or "edge"
    is_join_table: bool = False
    field_mappings: Dict[str, str] = Field(default_factory=dict)
    exclude_fields: List[str] = Field(default_factory=list)
    include_fields: Optional[List[str]] = None


class TypeMapping(BaseModel):
    """PostgreSQL to JSON type coercion rules."""
    pg_type: str
    json_type: str  # "string", "integer", "float", "boolean", "array", "object"


class MappingConfig(BaseModel):
    """Top-level mapping configuration."""
    source_schema: str = "public"
    collections: Dict[str, CollectionMapping] = Field(default_factory=dict)
    edges: List[EdgeDefinition] = Field(default_factory=list)
    type_overrides: Dict[str, str] = Field(default_factory=dict)
    key_separator: str = "_"

    def save_to_file(self, path: str):
        with open(path, 'w') as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load_from_file(cls, path: str) -> "MappingConfig":
        with open(path, 'r') as f:
            return cls.model_validate_json(f.read())
