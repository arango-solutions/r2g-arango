"""OpenMetadata catalog provider (PRD Phase 8a).

Talks to the OpenMetadata REST API directly with ``httpx`` rather than the
heavyweight ``openmetadata-ingestion`` SDK: the backend is REST, the calls we
need are simple reads, and a thin client is trivial to mock in tests and pulls
no large transitive dependency tree.

Hierarchy (relational): ``databaseService → database → databaseSchema → table``.
Messaging: ``messagingService → topic`` (Kafka). The provider maps an
OpenMetadata ``serviceType`` to an r2g ``source_type`` and resolves a selected
database / schema / topic into a connectable :class:`~r2g.catalogs.base.ResolvedSource`.

Credentials are intentionally NOT read from the catalog (OpenMetadata encrypts
them on read). ``resolve_source`` emits ``$R2G_DB_USER`` / ``$R2G_DB_PASSWORD``
placeholders that r2g resolves from the environment / ``r2g secrets`` at connect
time.
"""

from __future__ import annotations

from typing import Any, Optional

from r2g.catalogs.base import (
    ASSET_DATABASE,
    ASSET_SCHEMA,
    ASSET_SERVICE,
    ASSET_TABLE,
    ASSET_TOPIC,
    CatalogAsset,
    ResolvedSource,
)
from r2g.log import get_logger

logger = get_logger(__name__)

# OpenMetadata serviceType (lower-cased) -> r2g source_type.
_SERVICETYPE_TO_R2G: dict[str, str] = {
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mariadb": "mysql",
    "mssql": "sqlserver",
    "sqlserver": "sqlserver",
    "snowflake": "snowflake",
    "kafka": "kafka",
    "redpanda": "kafka",
}

_DEFAULT_PORT: dict[str, int] = {
    "postgresql": 5432,
    "mysql": 3306,
    "sqlserver": 1433,
}


def _load_httpx() -> Any:
    try:
        import httpx
    except ImportError as err:
        raise ImportError(
            "OpenMetadata catalog support requires httpx. "
            "Install with: pip install 'r2g-arango[openmetadata]'"
        ) from err
    return httpx


def _api_base(endpoint: str) -> str:
    """Normalize a configured endpoint to the ``…/api/v1`` REST base."""
    root = endpoint.rstrip("/")
    for suffix in ("/api/v1", "/api"):
        if root.endswith(suffix):
            root = root[: -len(suffix)]
    return f"{root}/api/v1"


def _split_host_port(host_port: str, default_port: int | None) -> tuple[str, int | None]:
    if not host_port:
        return "", default_port
    if ":" in host_port:
        host, _, port = host_port.rpartition(":")
        try:
            return host, int(port)
        except ValueError:
            return host_port, default_port
    return host_port, default_port


class OpenMetadataProvider:
    """Read-only OpenMetadata discovery provider."""

    provider_type = "openmetadata"

    def __init__(
        self,
        endpoint: str,
        *,
        name: str = "openmetadata",
        token: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.name = name
        self.token = token
        self.params = params or {}
        self._api = _api_base(endpoint)
        self._client: Any = None

    # ── HTTP seam (mocked in unit tests) ────────────────────────────────

    def _http(self) -> Any:
        if self._client is None:
            httpx = _load_httpx()
            headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
            self._client = httpx.Client(base_url=self._api, headers=headers, timeout=30.0)
        return self._client

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET ``path`` (relative to the API base) and return parsed JSON."""
        resp = self._http().get(path, params=params or {})
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # noqa: BLE001
                pass
            self._client = None

    def __enter__(self) -> "OpenMetadataProvider":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ── Asset construction helpers ──────────────────────────────────────

    def _service_asset(self, entity: dict[str, Any], *, messaging: bool) -> CatalogAsset:
        stype = (entity.get("serviceType") or "").lower()
        r2g_type = _SERVICETYPE_TO_R2G.get(stype)
        config = (entity.get("connection") or {}).get("config") or {}
        hint: dict[str, Any] = {"serviceType": entity.get("serviceType")}
        if messaging:
            if config.get("bootstrapServers"):
                hint["bootstrapServers"] = config["bootstrapServers"]
            if config.get("schemaRegistryURL"):
                hint["schemaRegistryURL"] = config["schemaRegistryURL"]
        else:
            if config.get("hostPort"):
                hint["hostPort"] = config["hostPort"]
            if config.get("account"):
                hint["account"] = config["account"]
        return CatalogAsset(
            provider=self.name,
            provider_type=self.provider_type,
            fqn=entity.get("fullyQualifiedName") or entity.get("name", ""),
            kind=ASSET_SERVICE,
            name=entity.get("name", ""),
            source_type=r2g_type,
            connection_hint=hint,
        )

    def _child_asset(
        self,
        entity: dict[str, Any],
        kind: str,
        parent: CatalogAsset,
        *,
        hint_updates: dict[str, Any] | None = None,
    ) -> CatalogAsset:
        hint = dict(parent.connection_hint)
        if hint_updates:
            hint.update(hint_updates)
        return CatalogAsset(
            provider=self.name,
            provider_type=self.provider_type,
            fqn=entity.get("fullyQualifiedName") or entity.get("name", ""),
            kind=kind,
            name=entity.get("name", ""),
            source_type=parent.source_type,
            parent_fqn=parent.fqn,
            connection_hint=hint,
            tags=[t.get("tagFQN", "") for t in (entity.get("tags") or []) if t.get("tagFQN")],
        )

    # ── Discovery ───────────────────────────────────────────────────────

    def list_data_sources(self) -> list[CatalogAsset]:
        assets: list[CatalogAsset] = []
        db = self._get(
            "/services/databaseServices", {"fields": "connection", "limit": 1000}
        )
        for e in db.get("data", []):
            assets.append(self._service_asset(e, messaging=False))
        msg = self._get(
            "/services/messagingServices", {"fields": "connection", "limit": 1000}
        )
        for e in msg.get("data", []):
            assets.append(self._service_asset(e, messaging=True))
        return assets

    def list_children(self, asset: CatalogAsset) -> list[CatalogAsset]:
        out: list[CatalogAsset] = []
        if asset.kind == ASSET_SERVICE and asset.source_type == "kafka":
            data = self._get("/topics", {"service": asset.fqn, "limit": 1000})
            for e in data.get("data", []):
                out.append(self._child_asset(e, ASSET_TOPIC, asset))
        elif asset.kind == ASSET_SERVICE:
            data = self._get("/databases", {"service": asset.fqn, "limit": 1000})
            for e in data.get("data", []):
                out.append(
                    self._child_asset(
                        e, ASSET_DATABASE, asset, hint_updates={"database": e.get("name", "")}
                    )
                )
        elif asset.kind == ASSET_DATABASE:
            data = self._get("/databaseSchemas", {"database": asset.fqn, "limit": 1000})
            for e in data.get("data", []):
                out.append(
                    self._child_asset(
                        e, ASSET_SCHEMA, asset, hint_updates={"schema": e.get("name", "")}
                    )
                )
        elif asset.kind == ASSET_SCHEMA:
            data = self._get("/tables", {"databaseSchema": asset.fqn, "limit": 1000})
            for e in data.get("data", []):
                out.append(self._child_asset(e, ASSET_TABLE, asset))
        return out

    def search(self, query: str, *, limit: int = 50) -> list[CatalogAsset]:
        data = self._get(
            "/search/query",
            {"q": query, "index": "table_search_index", "size": limit},
        )
        hits = (data.get("hits") or {}).get("hits", [])
        out: list[CatalogAsset] = []
        for h in hits:
            src = h.get("_source") or {}
            stype = (
                src.get("serviceType") or (src.get("service") or {}).get("serviceType") or ""
            ).lower()
            out.append(
                CatalogAsset(
                    provider=self.name,
                    provider_type=self.provider_type,
                    fqn=src.get("fullyQualifiedName", ""),
                    kind=ASSET_TABLE,
                    name=src.get("name", ""),
                    source_type=_SERVICETYPE_TO_R2G.get(stype),
                    connection_hint={
                        "service": (src.get("service") or {}).get("name"),
                        "database": (src.get("database") or {}).get("name"),
                        "schema": (src.get("databaseSchema") or {}).get("name"),
                    },
                )
            )
        return out

    # ── Discover-then-connect bridge ────────────────────────────────────

    def _fetch_service_config(self, service_name: str, *, messaging: bool) -> dict[str, Any]:
        kind = "messagingServices" if messaging else "databaseServices"
        entity = self._get(f"/services/{kind}/name/{service_name}", {"fields": "connection"})
        return (entity.get("connection") or {}).get("config") or {}

    def _service_name(self, asset: CatalogAsset) -> str:
        svc = asset.connection_hint.get("service")
        if svc:
            return str(svc)
        # Fall back to the first FQN segment (common case: dot-free names).
        return asset.fqn.split(".")[0] if asset.fqn else ""

    def resolve_source(self, asset: CatalogAsset) -> ResolvedSource:
        if asset.source_type == "kafka":
            return self._resolve_kafka(asset)
        return self._resolve_relational(asset)

    def _resolve_relational(self, asset: CatalogAsset) -> ResolvedSource:
        r2g_type = asset.source_type
        if r2g_type not in ("postgresql", "mysql", "sqlserver", "snowflake"):
            raise ValueError(
                f"Cannot resolve asset '{asset.fqn}' to a relational source "
                f"(unsupported or unknown source_type {r2g_type!r})."
            )
        config = self._fetch_service_config(self._service_name(asset), messaging=False)

        database = asset.connection_hint.get("database")
        schema = asset.connection_hint.get("schema")
        if asset.kind == ASSET_DATABASE:
            database = asset.name
        notes = (
            "Credentials are not read from the catalog; set $R2G_DB_USER / "
            "$R2G_DB_PASSWORD (or edit the connection string / use r2g secrets)."
        )

        if r2g_type == "snowflake":
            account = config.get("account") or config.get("hostPort") or ""
            conn = f"snowflake://$R2G_DB_USER:$R2G_DB_PASSWORD@{account}/{database or ''}"
            return ResolvedSource(
                source_type=r2g_type, connection_string=conn, schema_name=schema, notes=notes
            )

        host, port = _split_host_port(
            config.get("hostPort", ""), _DEFAULT_PORT.get(r2g_type)
        )
        scheme = {"postgresql": "postgresql", "mysql": "mysql", "sqlserver": "mssql"}[r2g_type]
        hostspec = f"{host}:{port}" if port else host
        conn = f"{scheme}://$R2G_DB_USER:$R2G_DB_PASSWORD@{hostspec}/{database or ''}"
        return ResolvedSource(
            source_type=r2g_type, connection_string=conn, schema_name=schema, notes=notes
        )

    def _resolve_kafka(self, asset: CatalogAsset) -> ResolvedSource:
        config = self._fetch_service_config(self._service_name(asset), messaging=True)
        brokers = (
            asset.connection_hint.get("bootstrapServers")
            or config.get("bootstrapServers")
            or ""
        )
        params: dict[str, Any] = {}
        if asset.kind == ASSET_TOPIC:
            params["topic"] = asset.name
        registry = asset.connection_hint.get("schemaRegistryURL") or config.get(
            "schemaRegistryURL"
        )
        if registry:
            params["schema_registry_url"] = registry
        return ResolvedSource(
            source_type="kafka",
            connection_string=brokers,
            source_params=params,
            notes="Kafka brokers resolved from the catalog; set credentials/SASL via env if needed.",
        )

    def get_asset(self, fqn: str) -> Optional[CatalogAsset]:
        """Fetch a single asset by FQN, inferring its kind from segment count.

        Used by ``catalog import-source`` which takes an FQN string. Quoted
        FQN segments containing dots are not handled in V1 (dot-free names are
        the common case).
        """
        segments = [s for s in fqn.split(".") if s]
        n = len(segments)
        try:
            if n <= 1:
                entity = self._try_service(fqn)
                if entity is None:
                    return None
                messaging = (entity.get("serviceType") or "").lower() in ("kafka", "redpanda")
                return self._service_asset(entity, messaging=messaging)
            if n == 2:
                entity = self._get(f"/databases/name/{fqn}", {"fields": "tags"})
                return self._asset_from_entity(entity, ASSET_DATABASE)
            if n == 3:
                entity = self._get(f"/databaseSchemas/name/{fqn}", {"fields": "tags"})
                return self._asset_from_entity(entity, ASSET_SCHEMA)
            entity = self._get(f"/tables/name/{fqn}", {"fields": "tags"})
            return self._asset_from_entity(entity, ASSET_TABLE)
        except Exception as err:  # noqa: BLE001
            logger.warning("openmetadata_get_asset_failed", fqn=fqn, error=str(err))
            return None

    def _try_service(self, name: str) -> Optional[dict[str, Any]]:
        for kind in ("databaseServices", "messagingServices"):
            try:
                return self._get(f"/services/{kind}/name/{name}", {"fields": "connection"})
            except Exception:  # noqa: BLE001
                continue
        return None

    def _asset_from_entity(self, entity: dict[str, Any], kind: str) -> CatalogAsset:
        """Build an asset from a database/schema/table entity using its
        reference fields (service / database / schema names), avoiding fragile
        FQN string-splitting."""
        service_ref = entity.get("service") or {}
        db_ref = entity.get("database") or {}
        schema_ref = entity.get("databaseSchema") or {}
        # database/schema/table entities carry serviceType at the top level;
        # the service reference's `type` is just "databaseService".
        stype = (entity.get("serviceType") or "").lower()
        hint: dict[str, Any] = {"service": service_ref.get("name")}
        if kind == ASSET_DATABASE:
            hint["database"] = entity.get("name")
        if kind in (ASSET_SCHEMA, ASSET_TABLE):
            hint["database"] = db_ref.get("name")
        if kind == ASSET_SCHEMA:
            hint["schema"] = entity.get("name")
        if kind == ASSET_TABLE:
            hint["schema"] = schema_ref.get("name")
        return CatalogAsset(
            provider=self.name,
            provider_type=self.provider_type,
            fqn=entity.get("fullyQualifiedName") or entity.get("name", ""),
            kind=kind,
            name=entity.get("name", ""),
            source_type=_SERVICETYPE_TO_R2G.get(stype),
            connection_hint=hint,
            tags=[t.get("tagFQN", "") for t in (entity.get("tags") or []) if t.get("tagFQN")],
        )


__all__ = ["OpenMetadataProvider"]
