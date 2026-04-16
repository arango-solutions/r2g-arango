from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, Field

from r2g.types import Schema


class SourceConfig(BaseModel):
    name: str
    source_type: str = "postgresql"
    connection_string: str
    description: str = ""
    owner: str = ""
    created_at: datetime
    updated_at: datetime


class SchemaSnapshot(BaseModel):
    id: str
    source_name: str
    schema_data: Schema
    captured_at: datetime
    pg_schema: str = "public"


class Project(BaseModel):
    name: str
    source_name: str
    schema_snapshot_id: str
    mapping_config_path: str
    arango_endpoint: str = "http://localhost:8529"
    arango_database: str = "_system"
    created_at: datetime
    updated_at: datetime


class LoadRecord(BaseModel):
    id: str
    project_name: str
    started_at: datetime
    completed_at: datetime | None = None
    load_type: str
    collections_loaded: list[str] = Field(default_factory=list)
    rows_loaded: int = 0
    errors: int = 0
    mapping_hash: str = ""
    status: str = "running"


class Catalog(BaseModel):
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    snapshots: dict[str, SchemaSnapshot] = Field(default_factory=dict)
    projects: dict[str, Project] = Field(default_factory=dict)
    load_history: list[LoadRecord] = Field(default_factory=list)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class CatalogManager:
    def __init__(self, catalog_dir: str | Path | None = None):
        if catalog_dir is None:
            self._dir = Path.home() / ".r2g"
        else:
            self._dir = Path(catalog_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / "catalog.json"

    def _load(self) -> Catalog:
        if not self._path.exists():
            return Catalog()
        return Catalog.model_validate_json(self._path.read_text(encoding="utf-8"))

    def _save(self, catalog: Catalog) -> None:
        self._path.write_text(catalog.model_dump_json(indent=2), encoding="utf-8")

    # ── Source CRUD ───────────────────────────────────────────────────

    def add_source(
        self,
        name: str,
        source_type: str,
        connection_string: str,
        description: str = "",
        owner: str = "",
    ) -> SourceConfig:
        catalog = self._load()
        if name in catalog.sources:
            raise ValueError(f"Source '{name}' already exists")
        now = _now()
        source = SourceConfig(
            name=name,
            source_type=source_type,
            connection_string=connection_string,
            description=description,
            owner=owner,
            created_at=now,
            updated_at=now,
        )
        catalog.sources[name] = source
        self._save(catalog)
        return source

    def list_sources(self) -> list[SourceConfig]:
        return list(self._load().sources.values())

    def get_source(self, name: str) -> SourceConfig | None:
        return self._load().sources.get(name)

    def remove_source(self, name: str) -> bool:
        catalog = self._load()
        if name not in catalog.sources:
            return False
        del catalog.sources[name]
        self._save(catalog)
        return True

    # ── Snapshots ────────────────────────────────────────────────────

    def create_snapshot(self, source_name: str, schema: Schema, pg_schema: str = "public") -> SchemaSnapshot:
        catalog = self._load()
        snap = SchemaSnapshot(
            id=str(uuid4()),
            source_name=source_name,
            schema_data=schema,
            captured_at=_now(),
            pg_schema=pg_schema,
        )
        catalog.snapshots[snap.id] = snap
        self._save(catalog)
        return snap

    def get_latest_snapshot(self, source_name: str) -> SchemaSnapshot | None:
        catalog = self._load()
        matching = [s for s in catalog.snapshots.values() if s.source_name == source_name]
        if not matching:
            return None
        return max(matching, key=lambda s: s.captured_at)

    def list_snapshots(self, source_name: str) -> list[SchemaSnapshot]:
        catalog = self._load()
        return sorted(
            (s for s in catalog.snapshots.values() if s.source_name == source_name),
            key=lambda s: s.captured_at,
        )

    # ── Projects ─────────────────────────────────────────────────────

    def create_project(
        self,
        name: str,
        source_name: str,
        mapping_config_path: str,
        arango_endpoint: str = "http://localhost:8529",
        arango_database: str = "_system",
    ) -> Project:
        catalog = self._load()
        if source_name not in catalog.sources:
            raise ValueError(f"Source '{source_name}' not found")
        latest = self.get_latest_snapshot(source_name)
        snapshot_id = latest.id if latest else ""
        now = _now()
        project = Project(
            name=name,
            source_name=source_name,
            schema_snapshot_id=snapshot_id,
            mapping_config_path=mapping_config_path,
            arango_endpoint=arango_endpoint,
            arango_database=arango_database,
            created_at=now,
            updated_at=now,
        )
        catalog.projects[name] = project
        self._save(catalog)
        return project

    def list_projects(self) -> list[Project]:
        return list(self._load().projects.values())

    def get_project(self, name: str) -> Project | None:
        return self._load().projects.get(name)

    # ── Load history ─────────────────────────────────────────────────

    def start_load(self, project_name: str, load_type: str, mapping_hash: str = "") -> LoadRecord:
        catalog = self._load()
        record = LoadRecord(
            id=str(uuid4()),
            project_name=project_name,
            started_at=_now(),
            load_type=load_type,
            mapping_hash=mapping_hash,
        )
        catalog.load_history.append(record)
        self._save(catalog)
        return record

    def complete_load(
        self,
        load_id: str,
        rows_loaded: int,
        errors: int,
        collections_loaded: list[str],
        status: str = "completed",
    ) -> LoadRecord:
        catalog = self._load()
        for record in catalog.load_history:
            if record.id == load_id:
                record.completed_at = _now()
                record.rows_loaded = rows_loaded
                record.errors = errors
                record.collections_loaded = collections_loaded
                record.status = status
                self._save(catalog)
                return record
        raise ValueError(f"Load record '{load_id}' not found")

    def get_history(self, project_name: str | None = None, limit: int = 20) -> list[LoadRecord]:
        catalog = self._load()
        records = catalog.load_history
        if project_name:
            records = [r for r in records if r.project_name == project_name]
        return sorted(records, key=lambda r: r.started_at, reverse=True)[:limit]
