from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from r2g.log import get_logger
from r2g.security import CredentialCipher, load_secret_key
from r2g.types import Schema

logger = get_logger(__name__)


class DependencyError(Exception):
    """Raised when a source cannot be removed due to dependent resources."""

    def __init__(
        self,
        source_name: str,
        projects: list[str],
        snapshots: list[str],
        load_records: int,
    ) -> None:
        self.source_name = source_name
        self.projects = projects
        self.snapshots = snapshots
        self.load_records = load_records
        super().__init__(
            f"Cannot remove source '{source_name}': {len(projects)} projects, "
            f"{len(snapshots)} snapshots, {load_records} load records depend on it"
        )


class SourceConfig(BaseModel):
    name: str
    source_type: str = "postgresql"
    connection_string: str
    description: str = ""
    owner: str = ""
    source_params: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class TargetConfig(BaseModel):
    name: str
    endpoint: str = "http://localhost:8529"
    database: str = "_system"
    username: str = "root"
    password: str = ""
    description: str = ""
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
    target_name: str | None = None
    mapping_name: str = ""
    mapping_description: str = ""
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
    error_message: str = ""
    error_type: str = ""


class Catalog(BaseModel):
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    snapshots: dict[str, SchemaSnapshot] = Field(default_factory=dict)
    projects: dict[str, Project] = Field(default_factory=dict)
    load_history: list[LoadRecord] = Field(default_factory=list)
    targets: dict[str, TargetConfig] = Field(default_factory=dict)


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
        self._cipher = CredentialCipher(load_secret_key(self._dir))

    def _load(self) -> Catalog:
        if not self._path.exists():
            return Catalog()
        catalog = Catalog.model_validate_json(self._path.read_text(encoding="utf-8"))
        for src in catalog.sources.values():
            if self._cipher.is_encrypted(src.connection_string):
                src.connection_string = self._cipher.decrypt(src.connection_string)
        for tgt in catalog.targets.values():
            if self._cipher.is_encrypted(tgt.password):
                tgt.password = self._cipher.decrypt(tgt.password)
        return catalog

    def _save(self, catalog: Catalog) -> None:
        payload = catalog.model_dump(mode="json")
        for src in payload.get("sources", {}).values():
            cs = src.get("connection_string", "")
            if cs and not self._cipher.is_encrypted(cs):
                src["connection_string"] = self._cipher.encrypt(cs)
        for tgt in payload.get("targets", {}).values():
            pw = tgt.get("password", "")
            if pw and not self._cipher.is_encrypted(pw):
                tgt["password"] = self._cipher.encrypt(pw)
        self._path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    # ── Source CRUD ───────────────────────────────────────────────────

    def add_source(
        self,
        name: str,
        source_type: str,
        connection_string: str,
        description: str = "",
        owner: str = "",
        source_params: dict[str, Any] | None = None,
    ) -> SourceConfig:
        # Catalog accepts any *known* source type so future types
        # (csv, kafka) can be pre-registered; the connector factory
        # remains the strict gate on what we can actually introspect.
        KNOWN_TYPES = ("postgresql", "snowflake", "csv", "kafka")
        normalized = (source_type or "").strip().lower()
        if normalized in ("postgres", "pg"):
            normalized = "postgresql"
        if normalized not in KNOWN_TYPES:
            raise ValueError(
                f"Unsupported source_type '{source_type}'. "
                f"Expected one of: {', '.join(KNOWN_TYPES)}."
            )
        source_type = normalized
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
            source_params=source_params or {},
            created_at=now,
            updated_at=now,
        )
        catalog.sources[name] = source
        self._save(catalog)
        logger.info("source_added", name=name, source_type=source_type)
        return source

    def list_sources(self) -> list[SourceConfig]:
        return list(self._load().sources.values())

    def get_source(self, name: str) -> SourceConfig | None:
        return self._load().sources.get(name)

    def update_source(self, name: str, **kwargs: Any) -> SourceConfig:
        """Update fields on an existing source. Accepts any SourceConfig field."""
        catalog = self._load()
        if name not in catalog.sources:
            raise ValueError(f"Source '{name}' not found")
        source = catalog.sources[name]
        update_data = source.model_dump()
        update_data.update(kwargs)
        update_data["updated_at"] = _now()
        catalog.sources[name] = SourceConfig.model_validate(update_data)
        self._save(catalog)
        logger.info("source_updated", name=name, fields=list(kwargs.keys()))
        return catalog.sources[name]

    def remove_source(self, name: str, *, cascade: bool = False) -> bool:
        catalog = self._load()
        if name not in catalog.sources:
            return False

        dep_projects = [p.name for p in catalog.projects.values() if p.source_name == name]
        dep_snapshots = [s.id for s in catalog.snapshots.values() if s.source_name == name]
        dep_load_records = [r for r in catalog.load_history if r.project_name in dep_projects]

        has_deps = dep_projects or dep_snapshots or dep_load_records

        if has_deps and not cascade:
            raise DependencyError(
                source_name=name,
                projects=dep_projects,
                snapshots=dep_snapshots,
                load_records=len(dep_load_records),
            )

        if has_deps:
            for pid in dep_projects:
                del catalog.projects[pid]
            for sid in dep_snapshots:
                del catalog.snapshots[sid]
            dep_project_set = set(dep_projects)
            catalog.load_history = [r for r in catalog.load_history if r.project_name not in dep_project_set]
            logger.info(
                "cascade_delete",
                source=name,
                projects=len(dep_projects),
                snapshots=len(dep_snapshots),
                load_records=len(dep_load_records),
            )

        del catalog.sources[name]
        self._save(catalog)
        logger.info("source_removed", name=name, cascade=cascade)
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
        mapping_name: str = "",
        mapping_description: str = "",
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
            mapping_name=mapping_name,
            mapping_description=mapping_description,
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

    def update_project(self, name: str, /, **kwargs: Any) -> Project:
        """Update fields on an existing project and bump ``updated_at``.

        Accepts any :class:`Project` field (e.g. ``mapping_name``,
        ``mapping_description``, ``target_name``). ``name`` is immutable
        here and silently ignored if passed.
        """
        catalog = self._load()
        if name not in catalog.projects:
            raise ValueError(f"Project '{name}' not found")
        project = catalog.projects[name]
        update_data = project.model_dump()
        kwargs.pop("name", None)
        update_data.update(kwargs)
        update_data["updated_at"] = _now()
        catalog.projects[name] = Project.model_validate(update_data)
        self._save(catalog)
        logger.info("project_updated", name=name, fields=list(kwargs.keys()))
        return catalog.projects[name]

    def touch_project(self, name: str) -> None:
        """Bump a project's ``updated_at`` timestamp (e.g. after a mapping save)."""
        catalog = self._load()
        if name not in catalog.projects:
            return
        catalog.projects[name].updated_at = _now()
        self._save(catalog)

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
        error_message: str = "",
        error_type: str = "",
    ) -> LoadRecord:
        catalog = self._load()
        for record in catalog.load_history:
            if record.id == load_id:
                record.completed_at = _now()
                record.rows_loaded = rows_loaded
                record.errors = errors
                record.collections_loaded = collections_loaded
                record.status = status
                if error_message:
                    record.error_message = error_message
                if error_type:
                    record.error_type = error_type
                self._save(catalog)
                return record
        raise ValueError(f"Load record '{load_id}' not found")

    def get_history(self, project_name: str | None = None, limit: int = 20) -> list[LoadRecord]:
        catalog = self._load()
        records = catalog.load_history
        if project_name:
            records = [r for r in records if r.project_name == project_name]
        return sorted(records, key=lambda r: r.started_at, reverse=True)[:limit]

    # ── Target CRUD ──────────────────────────────────────────────────

    def add_target(
        self,
        name: str,
        endpoint: str = "http://localhost:8529",
        database: str = "_system",
        username: str = "root",
        password: str = "",
        description: str = "",
    ) -> TargetConfig:
        catalog = self._load()
        if name in catalog.targets:
            raise ValueError(f"Target '{name}' already exists")
        now = _now()
        target = TargetConfig(
            name=name,
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
            description=description,
            created_at=now,
            updated_at=now,
        )
        catalog.targets[name] = target
        self._save(catalog)
        logger.info("target_added", name=name, endpoint=endpoint, database=database)
        return target

    def list_targets(self) -> list[TargetConfig]:
        return list(self._load().targets.values())

    def get_target(self, name: str) -> TargetConfig | None:
        return self._load().targets.get(name)

    def update_target(self, name: str, **kwargs: Any) -> TargetConfig:
        """Update fields on an existing target. Accepts any TargetConfig field."""
        catalog = self._load()
        if name not in catalog.targets:
            raise ValueError(f"Target '{name}' not found")
        target = catalog.targets[name]
        update_data = target.model_dump()
        update_data.update(kwargs)
        update_data["updated_at"] = _now()
        catalog.targets[name] = TargetConfig.model_validate(update_data)
        self._save(catalog)
        logger.info("target_updated", name=name, fields=list(kwargs.keys()))
        return catalog.targets[name]

    def remove_target(self, name: str) -> bool:
        catalog = self._load()
        if name not in catalog.targets:
            return False
        del catalog.targets[name]
        self._save(catalog)
        logger.info("target_removed", name=name)
        return True
