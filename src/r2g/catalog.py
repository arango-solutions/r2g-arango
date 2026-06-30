from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from r2g.log import get_logger
from r2g.security import CredentialCipher, load_secret_key
from r2g.types import Classification, Schema

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
    # Governance carrier (PRD Phase 9a). ``classifications`` is the resolved
    # ``table → column → Classification`` map captured at ``catalog import-source``
    # so it survives without re-querying the catalog; ``data_owners`` / ``data_tier``
    # are the asset-level catalog owners and confidentiality tier. Empty for
    # sources not imported from a catalog (fully backward compatible).
    classifications: dict[str, dict[str, Classification]] = Field(default_factory=dict)
    data_owners: list[str] = Field(default_factory=list)
    data_tier: Optional[str] = None
    # Catalog provenance (Phase 9c) so `catalog resync-classifications` can
    # re-pull from the originating catalog/asset without re-specifying them.
    catalog_name: Optional[str] = None
    catalog_asset_fqn: Optional[str] = None
    # When classifications were last (re)synced from the bound catalog. Surfaced
    # in CLI/UI so staleness is visible.
    classifications_synced_at: Optional[datetime] = None
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


class CatalogProviderConfig(BaseModel):
    """A registered *external* data catalog (PRD Phase 8).

    Distinct from r2g's own internal catalog: this is a connection to an
    upstream enterprise catalog (e.g. OpenMetadata) used for source discovery.
    The ``token`` is encrypted at rest, like source/target secrets.
    """

    name: str
    provider_type: str = "openmetadata"
    endpoint: str
    token: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
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
    # Snapshot of the mapping config as of the last successful load, used to
    # compute change-management migrations against the live database. ``None``
    # until the project has been loaded at least once.
    loaded_mapping: dict[str, Any] | None = None
    loaded_at: datetime | None = None
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
    catalog_providers: dict[str, CatalogProviderConfig] = Field(default_factory=dict)


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

    @property
    def dir(self) -> Path:
        """The directory backing this catalog (the root for project files)."""
        return self._dir

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
        for cat in catalog.catalog_providers.values():
            if cat.token and self._cipher.is_encrypted(cat.token):
                cat.token = self._cipher.decrypt(cat.token)
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
        for cat in payload.get("catalog_providers", {}).values():
            tok = cat.get("token", "")
            if tok and not self._cipher.is_encrypted(tok):
                cat["token"] = self._cipher.encrypt(tok)
        self._path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        # Catalog holds encrypted secrets; keep it owner-only readable.
        try:
            self._path.chmod(0o600)
        except OSError:
            pass

    # ── Source CRUD ───────────────────────────────────────────────────

    def add_source(
        self,
        name: str,
        source_type: str,
        connection_string: str,
        description: str = "",
        owner: str = "",
        source_params: dict[str, Any] | None = None,
        classifications: dict[str, dict[str, Classification]] | None = None,
        data_owners: list[str] | None = None,
        data_tier: str | None = None,
        catalog_name: str | None = None,
        catalog_asset_fqn: str | None = None,
    ) -> SourceConfig:
        # Catalog accepts any *known* source type so future types
        # (csv, kafka) can be pre-registered; the connector factory
        # remains the strict gate on what we can actually introspect.
        # Single source of truth for the type list + aliasing lives in
        # connectors.base (folds postgres/pg → postgresql, mariadb → mysql).
        from r2g.connectors.base import SUPPORTED_SOURCE_TYPES, normalize_source_type

        normalized = normalize_source_type(source_type) if (source_type or "").strip() else ""
        if normalized not in SUPPORTED_SOURCE_TYPES:
            raise ValueError(
                f"Unsupported source_type '{source_type}'. "
                f"Expected one of: {', '.join(SUPPORTED_SOURCE_TYPES)}."
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
            classifications=classifications or {},
            data_owners=data_owners or [],
            data_tier=data_tier,
            catalog_name=catalog_name,
            catalog_asset_fqn=catalog_asset_fqn,
            classifications_synced_at=now if classifications else None,
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

    # ── External catalog providers (Phase 8) ─────────────────────────

    def add_catalog(
        self,
        name: str,
        provider_type: str,
        endpoint: str,
        *,
        token: str = "",
        params: dict[str, Any] | None = None,
        description: str = "",
    ) -> CatalogProviderConfig:
        from r2g.catalogs.base import SUPPORTED_CATALOG_TYPES, normalize_catalog_type

        normalized = normalize_catalog_type(provider_type)
        if normalized not in SUPPORTED_CATALOG_TYPES:
            raise ValueError(
                f"Unsupported catalog provider type '{provider_type}'. "
                f"Expected one of: {', '.join(SUPPORTED_CATALOG_TYPES)}."
            )
        catalog = self._load()
        if name in catalog.catalog_providers:
            raise ValueError(f"Catalog provider '{name}' already exists")
        now = _now()
        provider = CatalogProviderConfig(
            name=name,
            provider_type=normalized,
            endpoint=endpoint,
            token=token,
            params=params or {},
            description=description,
            created_at=now,
            updated_at=now,
        )
        catalog.catalog_providers[name] = provider
        self._save(catalog)
        logger.info("catalog_added", name=name, provider_type=normalized)
        return provider

    def list_catalogs(self) -> list[CatalogProviderConfig]:
        return list(self._load().catalog_providers.values())

    def get_catalog(self, name: str) -> CatalogProviderConfig | None:
        return self._load().catalog_providers.get(name)

    def remove_catalog(self, name: str) -> bool:
        catalog = self._load()
        if name not in catalog.catalog_providers:
            return False
        del catalog.catalog_providers[name]
        self._save(catalog)
        logger.info("catalog_removed", name=name)
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

    def update_snapshot_schema(self, snapshot_id: str, schema: Schema) -> SchemaSnapshot:
        """Replace a snapshot's schema in place (used by classification re-sync)."""
        catalog = self._load()
        snap = catalog.snapshots.get(snapshot_id)
        if snap is None:
            raise ValueError(f"Snapshot '{snapshot_id}' not found")
        snap.schema_data = schema
        catalog.snapshots[snapshot_id] = snap
        self._save(catalog)
        logger.info("snapshot_schema_updated", snapshot_id=snapshot_id)
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

    def delete_project(self, name: str) -> bool:
        """Delete a project and cascade-remove its load history.

        The on-disk mapping config (``mapping_config_path``) is intentionally
        left in place; only the catalog record and associated load records are
        removed. Returns ``False`` if the project does not exist.
        """
        catalog = self._load()
        if name not in catalog.projects:
            return False
        del catalog.projects[name]
        removed = [r for r in catalog.load_history if r.project_name == name]
        if removed:
            catalog.load_history = [r for r in catalog.load_history if r.project_name != name]
        self._save(catalog)
        logger.info("project_deleted", name=name, load_records_removed=len(removed))
        return True

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

    def set_loaded_mapping(self, project_name: str, mapping: dict[str, Any]) -> None:
        """Record the mapping config that is now live in the target database.

        Called after a successful (full or selective) load so future migrations
        can diff the live state against subsequent mapping edits.
        """
        catalog = self._load()
        project = catalog.projects.get(project_name)
        if project is None:
            return
        project.loaded_mapping = mapping
        project.loaded_at = _now()
        project.updated_at = _now()
        self._save(catalog)

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
