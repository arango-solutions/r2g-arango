"""Abstraction over external data-catalog providers.

Mirrors the design of ``r2g.connectors.base``:

- ``CatalogProvider`` is a structural ``Protocol`` describing the read-only
  discovery operations r2g performs against an external catalog: list the
  top-level data sources/services, descend the
  service → database → schema → table hierarchy, search, and — the crux —
  ``resolve_source`` an asset into a :class:`ResolvedSource` that the existing
  source machinery can turn into a normal ``SourceConfig``.
- ``CatalogAsset`` is the normalized, catalog-agnostic node returned by every
  provider. It carries a *connection hint* (host/port/database) but never
  secrets — catalogs mask credentials on read, so r2g reads where the data is
  and the user supplies credentials at connect time.
- ``create_catalog_provider`` is the thin factory the CLI / UI / MCP call. It
  lazy-imports the concrete provider so optional dependencies (e.g. ``httpx``)
  load only when a provider of that type is actually used.

Adding a provider is a single edit here plus a concrete implementation, exactly
like adding a source connector.
"""

from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field

# Catalog asset kinds, coarsely normalized across providers.
ASSET_SERVICE = "service"
ASSET_DATABASE = "database"
ASSET_SCHEMA = "schema"
ASSET_TABLE = "table"
ASSET_TOPIC = "topic"


class CatalogAsset(BaseModel):
    """A normalized node in an external catalog's metadata tree.

    ``source_type`` is the r2g source type this asset maps to (``postgresql`` /
    ``mysql`` / ``sqlserver`` / ``snowflake`` / ``kafka``) when known, else
    ``None`` (e.g. an intermediate schema node). ``connection_hint`` holds
    non-secret technical metadata (host, port, database) used to build a
    connection string; it must never contain credentials.
    """

    provider: str
    provider_type: str
    fqn: str
    kind: str
    name: str
    source_type: Optional[str] = None
    parent_fqn: Optional[str] = None
    connection_hint: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class ResolvedSource(BaseModel):
    """The result of resolving a :class:`CatalogAsset` to an r2g source.

    ``connection_string`` uses r2g's ``$ENV_VAR`` convention for credentials
    (resolved at connect time) since catalogs do not return usable secrets.
    """

    source_type: str
    connection_string: str
    source_params: dict[str, Any] = Field(default_factory=dict)
    schema_name: Optional[str] = None
    notes: str = ""


@runtime_checkable
class CatalogProvider(Protocol):
    """Structural interface every external-catalog provider must satisfy."""

    provider_type: str

    def list_data_sources(self) -> list[CatalogAsset]:
        """Return the catalog's top-level data sources / services."""
        ...

    def list_children(self, asset: CatalogAsset) -> list[CatalogAsset]:
        """Return the immediate children of *asset* (descend the tree)."""
        ...

    def search(self, query: str, *, limit: int = 50) -> list[CatalogAsset]:
        """Return assets matching *query* (typically tables)."""
        ...

    def resolve_source(self, asset: CatalogAsset) -> ResolvedSource:
        """Resolve *asset* into a connectable r2g source (discover-then-connect)."""
        ...

    def get_asset(self, fqn: str) -> Optional[CatalogAsset]:
        """Fetch a single asset by its fully-qualified name, or ``None``.

        Used by the import path (``catalog import-source`` / the UI / the MCP
        tool), which takes an FQN string and must resolve it to an asset before
        :meth:`resolve_source`.
        """
        ...


SUPPORTED_CATALOG_TYPES: tuple[str, ...] = ("openmetadata",)

_CATALOG_ALIASES: dict[str, str] = {
    "openmetadata": "openmetadata",
    "open-metadata": "openmetadata",
    "om": "openmetadata",
}


def normalize_catalog_type(provider_type: str | None) -> str:
    """Canonicalize a catalog provider-type string."""
    key = (provider_type or "").strip().lower()
    return _CATALOG_ALIASES.get(key, key)


def create_catalog_provider(
    provider_type: str,
    endpoint: str,
    *,
    name: str = "",
    token: str | None = None,
    params: dict[str, Any] | None = None,
) -> CatalogProvider:
    """Return a catalog provider matching ``provider_type``.

    Concrete classes are lazy-imported so optional dependencies load only when
    a provider of that type is used. Unknown types raise :class:`ValueError`;
    missing optional deps raise :class:`ImportError` with a pip-install hint.
    """
    key = normalize_catalog_type(provider_type)
    if key == "openmetadata":
        from r2g.catalogs.openmetadata import OpenMetadataProvider

        return OpenMetadataProvider(
            endpoint, name=name or "openmetadata", token=token, params=params or {}
        )
    raise ValueError(
        f"Unsupported catalog provider type '{provider_type}'. "
        f"Expected one of: {', '.join(SUPPORTED_CATALOG_TYPES)}."
    )
