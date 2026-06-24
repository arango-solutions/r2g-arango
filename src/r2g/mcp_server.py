"""R2G MCP Server — exposes relational-to-graph ETL capabilities via Model Context Protocol.

Tools let an AI agent introspect source schemas, generate and validate mappings,
trigger ingestion, compare schema snapshots, preview data, and manage the catalog.

Resources provide read-only access to schema snapshots, mapping configs, and load history.

Start with:  r2g mcp            (stdio, for Cursor / Claude Desktop)
         or: r2g mcp --sse      (SSE transport, for remote clients)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "r2g",
    instructions=(
        "R2G is a relational-to-graph ETL pipeline. Use these tools to introspect "
        "PostgreSQL schemas, generate ArangoDB graph mappings, validate configs, "
        "trigger data loads, compare schema snapshots, and manage the source/project catalog. "
        "You can also browse connected external data catalogs (e.g. OpenMetadata) via "
        "list_catalogs / catalog_browse and import a discovered asset as a source with "
        "catalog_import_source. Start by calling list_sources and list_projects to see what's configured."
    ),
)


def _get_catalog():
    from r2g.catalog import CatalogManager

    catalog_dir = os.environ.get("R2G_CATALOG_DIR")
    return CatalogManager(catalog_dir)


def _resolve_conn_string(raw: str) -> str:
    # Supports both whole-string ($PG_CONN) and inline ($USER:$PASS@host) refs,
    # unified with create_source_connector via expand_env_vars.
    from r2g.connectors.base import expand_env_vars

    return expand_env_vars(raw)


def _safe_error(exc: Exception) -> str:
    """Stringify an exception for a tool ``error`` field with DSN creds scrubbed.

    MCP tools touch source/target connection strings, so a raw ``str(exc)``
    can leak ``user:password@host`` into an agent transcript.
    """
    from r2g.security import scrub_dsn_credentials

    return scrub_dsn_credentials(str(exc))


def _jailed_save_path(catalog_root: Path, save_path: str) -> Path:
    """Resolve a client-supplied mapping save path inside the catalog jail.

    Writes are confined to ``<catalog>/projects`` so an agent cannot use
    ``generate_mapping(save_path=...)`` to clobber arbitrary files. Symlinks
    and ``..`` are resolved before the containment check.
    """
    root = (catalog_root / "projects").resolve()
    target = Path(save_path).expanduser()
    if not target.is_absolute():
        target = root / target
    target = target.resolve()
    if target != root and root not in target.parents:
        raise ValueError(
            f"save_path must be inside the catalog projects directory ({root})."
        )
    root.mkdir(parents=True, exist_ok=True)
    return target


from r2g.security import redact_source_dump as _redact_source  # noqa: E402
from r2g.security import redact_target_dump as _redact_target  # noqa: E402

# ── Catalog: Sources ─────────────────────────────────────────────────


@mcp.tool()
def list_sources() -> list[dict[str, Any]]:
    """List all registered data sources in the R2G catalog."""
    mgr = _get_catalog()
    return [_redact_source(s.model_dump(mode="json")) for s in mgr.list_sources()]


@mcp.tool()
def get_source(name: str) -> dict[str, Any]:
    """Get details of a specific data source by name."""
    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        return {"error": f"Source '{name}' not found"}
    return _redact_source(source.model_dump(mode="json"))


@mcp.tool()
def add_source(
    name: str,
    connection_string: str,
    source_type: str = "postgresql",
    description: str = "",
) -> dict[str, Any]:
    """Register a new data source in the catalog.

    Args:
        name: Unique name for the source (e.g. "prod_ecommerce")
        connection_string: Database connection URI or env var reference like "$PG_CONN"
        source_type: One of "postgresql", "mysql", "sqlserver", "snowflake", "csv", "kafka"
        description: Human-readable description
    """
    mgr = _get_catalog()
    try:
        source = mgr.add_source(name, source_type, connection_string, description=description)
        return {"status": "created", "source": _redact_source(source.model_dump(mode="json"))}
    except ValueError as e:
        return {"error": _safe_error(e)}


@mcp.tool()
def remove_source(name: str, cascade: bool = False) -> dict[str, Any]:
    """Remove a data source from the catalog.

    Args:
        name: Source name to remove
        cascade: If True, also removes dependent projects, snapshots, and load history
    """
    mgr = _get_catalog()
    try:
        if mgr.remove_source(name, cascade=cascade):
            return {"status": "removed", "name": name}
        return {"error": f"Source '{name}' not found"}
    except Exception as e:
        return {"error": _safe_error(e)}


# ── Catalog: Projects ────────────────────────────────────────────────


@mcp.tool()
def list_projects() -> list[dict[str, Any]]:
    """List all R2G projects in the catalog."""
    mgr = _get_catalog()
    return [p.model_dump(mode="json") for p in mgr.list_projects()]


@mcp.tool()
def get_project(name: str) -> dict[str, Any]:
    """Get details of a specific project by name."""
    mgr = _get_catalog()
    project = mgr.get_project(name)
    if project is None:
        return {"error": f"Project '{name}' not found"}
    return project.model_dump(mode="json")


@mcp.tool()
def create_project(
    name: str,
    source_name: str,
    mapping_config_path: str,
    arango_endpoint: str = "http://localhost:8529",
    arango_database: str = "_system",
) -> dict[str, Any]:
    """Create a new R2G project linking a source to an ArangoDB target.

    Args:
        name: Unique project name
        source_name: Name of a registered source
        mapping_config_path: Path to the YAML mapping config file
        arango_endpoint: ArangoDB server URL
        arango_database: Target ArangoDB database name
    """
    mgr = _get_catalog()
    try:
        project = mgr.create_project(
            name, source_name, mapping_config_path,
            arango_endpoint=arango_endpoint, arango_database=arango_database,
        )
        return {"status": "created", "project": project.model_dump(mode="json")}
    except ValueError as e:
        return {"error": _safe_error(e)}


# ── Catalog: Targets ─────────────────────────────────────────────────


@mcp.tool()
def list_targets() -> list[dict[str, Any]]:
    """List all registered ArangoDB target connections."""
    mgr = _get_catalog()
    return [_redact_target(t.model_dump(mode="json")) for t in mgr.list_targets()]


@mcp.tool()
def add_target(
    name: str,
    endpoint: str = "http://localhost:8529",
    database: str = "_system",
    username: str = "root",
    password: str = "",
    description: str = "",
) -> dict[str, Any]:
    """Register an ArangoDB target connection.

    Args:
        name: Unique name for the target
        endpoint: ArangoDB HTTP endpoint URL
        database: Database name
        username: ArangoDB username
        password: ArangoDB password
        description: Human-readable description
    """
    mgr = _get_catalog()
    try:
        target = mgr.add_target(name, endpoint, database, username, password, description)
        return {"status": "created", "target": _redact_target(target.model_dump(mode="json"))}
    except ValueError as e:
        return {"error": _safe_error(e)}


# ── External data catalogs (Phase 8) ─────────────────────────────────


def _redact_catalog(dump: dict[str, Any]) -> dict[str, Any]:
    """Redact the stored token before returning a catalog config to an agent."""
    from r2g.security import redact_for_display

    out = dict(dump)
    out["token"] = redact_for_display(out.get("token") or "")
    return out


def _build_catalog_provider(mgr: Any, name: str) -> Any:
    """Build a live catalog provider from a registered config.

    Raises ``ValueError`` if the catalog is unknown. A ``$ENV_VAR`` token
    reference is resolved from the environment at use time (mirroring the CLI
    and UI), so secrets stay out of the catalog file.
    """
    from r2g.catalogs.base import create_catalog_provider

    cfg = mgr.get_catalog(name)
    if cfg is None:
        raise ValueError(f"Catalog '{name}' not found. Register it with add_catalog.")
    token = cfg.token
    if token and token.startswith("$"):
        token = os.environ.get(token[1:], token)
    return create_catalog_provider(
        cfg.provider_type, cfg.endpoint, name=cfg.name, token=token or None, params=cfg.params
    )


@mcp.tool()
def list_catalogs() -> list[dict[str, Any]]:
    """List registered external data catalogs (e.g. OpenMetadata) used for source discovery."""
    mgr = _get_catalog()
    return [_redact_catalog(c.model_dump(mode="json")) for c in mgr.list_catalogs()]


@mcp.tool()
def add_catalog(
    name: str,
    endpoint: str,
    provider_type: str = "openmetadata",
    token: str = "",
    description: str = "",
) -> dict[str, Any]:
    """Register an external data catalog for source discovery.

    Args:
        name: Unique local name for this catalog connection
        endpoint: Catalog base URL, e.g. "http://localhost:8585"
        provider_type: Catalog type (currently "openmetadata")
        token: API token, or a "$ENV_VAR" reference; stored encrypted at rest
        description: Human-readable description
    """
    mgr = _get_catalog()
    try:
        cfg = mgr.add_catalog(name, provider_type, endpoint, token=token, description=description)
        return {"status": "created", "catalog": _redact_catalog(cfg.model_dump(mode="json"))}
    except ValueError as e:
        return {"error": _safe_error(e)}


@mcp.tool()
def remove_catalog(name: str) -> dict[str, Any]:
    """Remove a registered external data catalog.

    Args:
        name: Catalog name to remove
    """
    mgr = _get_catalog()
    if mgr.remove_catalog(name):
        return {"status": "removed", "name": name}
    return {"error": f"Catalog '{name}' not found"}


@mcp.tool()
def catalog_browse(
    name: str,
    path: str | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    """Browse an external data catalog.

    With no arguments, lists the catalog's top-level data sources/services. With
    ``path`` (an asset FQN), lists that asset's children (descending
    service → database → schema → table). With ``search``, returns matching tables.

    Args:
        name: Registered catalog name
        path: Optional asset FQN to descend into (e.g. "service.database")
        search: Optional text to search tables by
    """
    mgr = _get_catalog()
    try:
        provider = _build_catalog_provider(mgr, name)
    except ValueError as e:
        return {"error": _safe_error(e)}
    try:
        if search:
            assets = provider.search(search)
        elif path:
            asset = provider.get_asset(path)
            if asset is None:
                return {"error": f"Asset '{path}' not found in catalog '{name}'"}
            assets = provider.list_children(asset)
        else:
            assets = provider.list_data_sources()
    except Exception as e:
        return {"error": _safe_error(e)}
    return {
        "catalog": name,
        "count": len(assets),
        "assets": [a.model_dump(mode="json") for a in assets],
    }


@mcp.tool()
def catalog_import_source(
    name: str,
    asset_fqn: str,
    source_name: str,
    description: str = "",
) -> dict[str, Any]:
    """Resolve a catalog asset into an r2g source (discover-then-connect).

    Credentials are NOT taken from the catalog: the generated connection string
    uses "$R2G_DB_USER" / "$R2G_DB_PASSWORD" placeholders that r2g resolves from
    the environment at connect time.

    Args:
        name: Registered catalog name
        asset_fqn: Asset FQN to import (database / schema / topic)
        source_name: Name for the new r2g source
        description: Optional description for the new source
    """
    mgr = _get_catalog()
    try:
        provider = _build_catalog_provider(mgr, name)
    except ValueError as e:
        return {"error": _safe_error(e)}
    try:
        asset = provider.get_asset(asset_fqn)
        if asset is None:
            return {"error": f"Asset '{asset_fqn}' not found in catalog '{name}'"}
        resolved = provider.resolve_source(asset)
        source = mgr.add_source(
            source_name,
            resolved.source_type,
            resolved.connection_string,
            description=description or f"Imported from catalog '{name}' ({asset_fqn})",
            source_params=resolved.source_params,
        )
    except ValueError as e:
        return {"error": _safe_error(e)}
    except Exception as e:
        return {"error": _safe_error(e)}
    return {
        "status": "imported",
        "source": _redact_source(source.model_dump(mode="json")),
        "source_type": resolved.source_type,
        "schema_name": resolved.schema_name,
        "notes": resolved.notes,
    }


# ── Schema Introspection ─────────────────────────────────────────────


@mcp.tool()
def introspect_source_schema(
    source_name: str,
    pg_schema: str = "public",
    save_snapshot: bool = True,
) -> dict[str, Any]:
    """Connect to a PostgreSQL source and introspect its schema.

    Returns tables, columns, primary keys, and foreign keys.
    Optionally saves a snapshot to the catalog for future reference.

    Args:
        source_name: Name of a registered source
        pg_schema: PostgreSQL schema name (default: "public")
        save_snapshot: Whether to persist the snapshot in the catalog
    """
    mgr = _get_catalog()
    source = mgr.get_source(source_name)
    if source is None:
        return {"error": f"Source '{source_name}' not found"}

    try:
        from r2g.connectors.base import create_source_connector

        conn_str = _resolve_conn_string(source.connection_string)
        connector = create_source_connector(
            source.source_type or "postgresql",
            conn_str,
            schema_name=pg_schema,
            source_params=source.source_params,
        )
        schema = connector.get_schema()

        result: dict[str, Any] = {
            "tables": len(schema.tables),
            "schema": _schema_summary(schema),
        }

        if save_snapshot:
            snap = mgr.create_snapshot(source_name, schema, pg_schema=pg_schema)
            result["snapshot_id"] = snap.id
            result["captured_at"] = snap.captured_at.isoformat()

        return result
    except Exception as e:
        return {"error": _safe_error(e)}


@mcp.tool()
def introspect_target_graph(
    target_name: str,
) -> dict[str, Any]:
    """Introspect an ArangoDB target database to discover its existing schema.

    Returns document collections, edge collections, named graphs, and sampled property names.

    Args:
        target_name: Name of a registered target
    """
    mgr = _get_catalog()
    target = mgr.get_target(target_name)
    if target is None:
        return {"error": f"Target '{target_name}' not found"}

    try:
        from r2g.connectors.arango_reader import ArangoIntrospector

        intro = ArangoIntrospector(
            endpoint=target.endpoint,
            database=target.database,
            username=target.username,
            password=target.password,
        )
        return intro.introspect()
    except Exception as e:
        return {"error": _safe_error(e)}


# ── Mapping Generation & Validation ──────────────────────────────────


@mcp.tool()
def generate_mapping(
    source_name: str,
    save_path: str | None = None,
) -> dict[str, Any]:
    """Generate a default relational-to-graph mapping config from a source schema.

    Uses heuristics: tables → document collections, FKs → edge collections,
    join tables detected automatically, PKs → _key.

    Args:
        source_name: Name of a registered source (must have a schema snapshot)
        save_path: Optional file path to save the generated YAML mapping config
    """
    mgr = _get_catalog()
    snap = mgr.get_latest_snapshot(source_name)
    if snap is None:
        return {"error": f"No schema snapshot for source '{source_name}'. Run introspect_source_schema first."}

    from r2g.config import ConfigManager

    config = ConfigManager.generate_default_config(snap.schema_data, source_schema=snap.pg_schema)

    saved_to: str | None = None
    if save_path:
        try:
            jailed = _jailed_save_path(mgr.dir, save_path)
        except ValueError as e:
            return {"error": _safe_error(e)}
        ConfigManager.save_config(config, str(jailed))
        saved_to = str(jailed)

    return {
        "collections": len(config.collections),
        "edges": len(config.edges),
        "join_tables": [k for k, v in config.collections.items() if v.is_join_table],
        "mapping": config.model_dump(mode="json"),
        "saved_to": saved_to,
    }


@mcp.tool()
def validate_mapping(
    source_name: str,
    mapping_config_path: str,
) -> dict[str, Any]:
    """Validate a mapping config against a source schema.

    Checks that every collection references a known table, every edge references
    valid collections and columns, and field lists only name columns that exist.

    Args:
        source_name: Name of a registered source (must have a schema snapshot)
        mapping_config_path: Path to the YAML mapping config file
    """
    mgr = _get_catalog()
    snap = mgr.get_latest_snapshot(source_name)
    if snap is None:
        return {"error": f"No schema snapshot for source '{source_name}'"}

    from r2g.config import ConfigManager, validate_config

    try:
        config = ConfigManager.load_config(mapping_config_path)
        issues = validate_config(snap.schema_data, config)
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "collections": len(config.collections),
            "edges": len(config.edges),
        }
    except Exception as e:
        return {"error": _safe_error(e)}


# ── Schema Diff ──────────────────────────────────────────────────────


@mcp.tool()
def diff_schema_snapshots(
    source_name: str,
) -> dict[str, Any]:
    """Compare the two most recent schema snapshots for a source.

    Shows added/removed tables, column changes, FK changes. Useful for
    detecting schema drift before re-ingestion.

    Args:
        source_name: Name of a registered source (must have at least 2 snapshots)
    """
    mgr = _get_catalog()
    snaps = mgr.list_snapshots(source_name)
    if len(snaps) < 2:
        return {"error": f"Need at least 2 snapshots for source '{source_name}', found {len(snaps)}"}

    from r2g.schema_diff import diff_schemas

    old_snap = snaps[-2]
    new_snap = snaps[-1]
    diff = diff_schemas(old_snap.schema_data, new_snap.schema_data)

    return {
        "old_snapshot": old_snap.id,
        "new_snapshot": new_snap.id,
        "old_captured_at": old_snap.captured_at.isoformat(),
        "new_captured_at": new_snap.captured_at.isoformat(),
        **diff,
    }


# ── Data Preview ─────────────────────────────────────────────────────


@mcp.tool()
def preview_table(
    source_name: str,
    table_name: str,
    limit: int = 10,
) -> dict[str, Any]:
    """Preview sample rows from a source table.

    Connects to the source database and returns up to `limit` rows from the
    specified table. Useful for understanding the data before mapping.

    Args:
        source_name: Name of a registered source
        table_name: Name of the table to preview
        limit: Maximum number of rows to return (default 10, max 100)
    """
    mgr = _get_catalog()
    source = mgr.get_source(source_name)
    if source is None:
        return {"error": f"Source '{source_name}' not found"}

    stype = (source.source_type or "postgresql").lower()
    if stype not in ("postgresql", "postgres", "pg"):
        return {"error": f"Preview is only supported for PostgreSQL sources (got '{stype}')"}

    # Validate the table against the latest snapshot so the identifier is
    # never an unvetted string interpolated into SQL.
    snap = mgr.get_latest_snapshot(source_name)
    if snap is None or table_name not in snap.schema_data.tables:
        return {"error": f"Table '{table_name}' is not in the latest snapshot of '{source_name}'"}

    limit = max(1, min(limit, 100))

    try:
        from r2g.connectors.postgres import preview_table_rows

        schema_name = snap.pg_schema or "public"
        conn_str = _resolve_conn_string(source.connection_string)
        rows = preview_table_rows(conn_str, schema_name, table_name, limit)
        return {
            "table": table_name,
            "count": len(rows),
            "rows": rows,
        }
    except Exception as e:
        return {"error": _safe_error(e)}


# ── Ingestion ────────────────────────────────────────────────────────


@mcp.tool()
def start_load(
    project_name: str,
    workers: int = 1,
    batch_size: int = 10000,
    drop_collections: bool = False,
    dry_run: bool = False,
    graph_name: str | None = None,
) -> dict[str, Any]:
    """Trigger a streaming data load from PostgreSQL to ArangoDB.

    Runs the ingestion synchronously and returns results including
    rows loaded, collections created, errors, and elapsed time.

    For large datasets, use workers > 1 for parallel streaming.

    Args:
        project_name: Name of the project to load
        workers: Number of parallel streaming threads (default 1)
        batch_size: Rows per batch (default 10000)
        drop_collections: If True, drop and recreate collections before loading
        dry_run: If True, connect and transform but skip writes (pre-flight check)
        graph_name: Optional ArangoDB named graph to create
    """
    mgr = _get_catalog()
    project = mgr.get_project(project_name)
    if project is None:
        return {"error": f"Project '{project_name}' not found"}

    source = mgr.get_source(project.source_name)
    if source is None:
        return {"error": f"Source '{project.source_name}' not found"}

    snap = mgr.get_latest_snapshot(project.source_name)
    if snap is None:
        return {"error": f"No schema snapshot for source '{project.source_name}'"}

    from r2g.config import ConfigManager
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.streaming.pipeline import StreamingPipeline

    try:
        config = ConfigManager.load_config(project.mapping_config_path)
        conn_str = _resolve_conn_string(source.connection_string)

        writer = ArangoWriter(
            endpoint=project.arango_endpoint,
            database=project.arango_database,
        )

        pipeline = StreamingPipeline(
            pg_conn_string=conn_str,
            arango_writer=writer,
            schema=snap.schema_data,
            config=config,
            batch_size=batch_size,
            drop_collections=drop_collections,
            dry_run=dry_run,
            workers=workers,
            pg_schema=snap.pg_schema,
        )

        load_record = mgr.start_load(project_name, "streaming")

        try:
            result = pipeline.run(graph_name=graph_name)

            doc_results = result.get("documents", [])
            edge_results = result.get("edges", [])
            total_rows = sum(r[1] for r in doc_results) + sum(r[1] for r in edge_results)
            total_errors = sum(len(e) for e in pipeline.errors.values())

            all_collections = [r[0] for r in doc_results] + [r[0] for r in edge_results]
            mgr.complete_load(load_record.id, total_rows, total_errors, all_collections, "completed")

            return {
                "status": "completed",
                "load_id": load_record.id,
                "dry_run": dry_run,
                "documents": {name: count for name, count in doc_results},
                "edges": {name: count for name, count in edge_results},
                "total_rows": total_rows,
                "total_errors": total_errors,
                "elapsed_seconds": result.get("elapsed_seconds", 0),
                "skipped": result.get("skipped", []),
            }
        except Exception as e:
            mgr.complete_load(load_record.id, 0, 0, [], "failed")
            return {"status": "failed", "load_id": load_record.id, "error": _safe_error(e)}

    except Exception as e:
        return {"error": _safe_error(e)}


# ── Load History ─────────────────────────────────────────────────────


@mcp.tool()
def load_history(
    project_name: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Show recent load history, optionally filtered by project.

    Args:
        project_name: Optional project name to filter by
        limit: Maximum number of records to return (default 10)
    """
    mgr = _get_catalog()
    records = mgr.get_history(project_name=project_name, limit=limit)
    return [r.model_dump(mode="json") for r in records]


# ── Mapping Diff ─────────────────────────────────────────────────────


@mcp.tool()
def diff_mappings(
    old_mapping_path: str,
    new_mapping_path: str,
    source_name: str,
) -> dict[str, Any]:
    """Compare two mapping configs and produce a reload plan.

    Shows what changed (renamed collections, added/removed edges, field changes)
    and what ArangoDB operations would be needed to apply the changes.

    Args:
        old_mapping_path: Path to the current/old mapping YAML
        new_mapping_path: Path to the proposed/new mapping YAML
        source_name: Name of the source (for schema context)
    """
    mgr = _get_catalog()
    snap = mgr.get_latest_snapshot(source_name)
    if snap is None:
        return {"error": f"No schema snapshot for source '{source_name}'"}

    from r2g.config import ConfigManager
    from r2g.mapping_diff import diff_mappings as _diff_mappings

    try:
        old_config = ConfigManager.load_config(old_mapping_path)
        new_config = ConfigManager.load_config(new_mapping_path)
        plan = _diff_mappings(old_config, new_config, snap.schema_data)
        return plan.model_dump(mode="json")
    except Exception as e:
        return {"error": _safe_error(e)}


# ── Resources ────────────────────────────────────────────────────────


@mcp.resource("r2g://sources")
def resource_sources() -> str:
    """All registered data sources."""
    mgr = _get_catalog()
    sources = mgr.list_sources()
    return json.dumps(
        [_redact_source(s.model_dump(mode="json")) for s in sources], indent=2, default=str
    )


@mcp.resource("r2g://projects")
def resource_projects() -> str:
    """All registered projects."""
    mgr = _get_catalog()
    projects = mgr.list_projects()
    return json.dumps([p.model_dump(mode="json") for p in projects], indent=2, default=str)


@mcp.resource("r2g://targets")
def resource_targets() -> str:
    """All registered ArangoDB targets."""
    mgr = _get_catalog()
    targets = mgr.list_targets()
    return json.dumps(
        [_redact_target(t.model_dump(mode="json")) for t in targets], indent=2, default=str
    )


@mcp.resource("r2g://schema/{source_name}")
def resource_schema(source_name: str) -> str:
    """Latest schema snapshot for a source — tables, columns, PKs, FKs."""
    mgr = _get_catalog()
    snap = mgr.get_latest_snapshot(source_name)
    if snap is None:
        return json.dumps({"error": f"No snapshot for source '{source_name}'"})
    return json.dumps(_schema_summary(snap.schema_data), indent=2)


@mcp.resource("r2g://mapping/{project_name}")
def resource_mapping(project_name: str) -> str:
    """Current mapping config for a project as JSON."""
    mgr = _get_catalog()
    project = mgr.get_project(project_name)
    if project is None:
        return json.dumps({"error": f"Project '{project_name}' not found"})
    try:
        from r2g.config import ConfigManager

        config = ConfigManager.load_config(project.mapping_config_path)
        return json.dumps(config.model_dump(mode="json"), indent=2)
    except Exception as e:
        return json.dumps({"error": _safe_error(e)})


@mcp.resource("r2g://history/{project_name}")
def resource_history(project_name: str) -> str:
    """Load history for a project."""
    mgr = _get_catalog()
    records = mgr.get_history(project_name=project_name, limit=20)
    return json.dumps([r.model_dump(mode="json") for r in records], indent=2, default=str)


# ── Helpers ──────────────────────────────────────────────────────────


def _schema_summary(schema) -> dict[str, Any]:
    """Build a concise summary of a Schema for tool/resource output."""
    tables_summary = {}
    for tname, table in schema.tables.items():
        tables_summary[tname] = {
            "columns": [
                {
                    "name": c.name,
                    "type": c.data_type,
                    "nullable": c.is_nullable,
                    "pk": c.is_primary_key,
                }
                for c in table.columns
            ],
            "primary_key": table.primary_key,
            "foreign_keys": [
                {
                    "columns": fk.columns,
                    "references": f"{fk.foreign_table}({', '.join(fk.foreign_columns)})",
                }
                for fk in table.foreign_keys
            ],
        }
    return {"tables": tables_summary, "table_count": len(tables_summary)}

