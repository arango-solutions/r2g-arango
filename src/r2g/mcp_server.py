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
from typing import Any

from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "r2g",
    instructions=(
        "R2G is a relational-to-graph ETL pipeline. Use these tools to introspect "
        "PostgreSQL schemas, generate ArangoDB graph mappings, validate configs, "
        "trigger data loads, compare schema snapshots, and manage the source/project catalog. "
        "Start by calling list_sources and list_projects to see what's configured."
    ),
)


def _get_catalog():
    from r2g.catalog import CatalogManager

    catalog_dir = os.environ.get("R2G_CATALOG_DIR")
    return CatalogManager(catalog_dir)


def _resolve_conn_string(raw: str) -> str:
    if raw.startswith("$"):
        return os.environ.get(raw[1:], raw)
    return raw


# ── Catalog: Sources ─────────────────────────────────────────────────


@mcp.tool()
def list_sources() -> list[dict[str, Any]]:
    """List all registered data sources in the R2G catalog."""
    mgr = _get_catalog()
    return [s.model_dump(mode="json") for s in mgr.list_sources()]


@mcp.tool()
def get_source(name: str) -> dict[str, Any]:
    """Get details of a specific data source by name."""
    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        return {"error": f"Source '{name}' not found"}
    return source.model_dump(mode="json")


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
        source_type: One of "postgresql", "csv", "kafka"
        description: Human-readable description
    """
    mgr = _get_catalog()
    try:
        source = mgr.add_source(name, source_type, connection_string, description=description)
        return {"status": "created", "source": source.model_dump(mode="json")}
    except ValueError as e:
        return {"error": str(e)}


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
        return {"error": str(e)}


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
        return {"error": str(e)}


# ── Catalog: Targets ─────────────────────────────────────────────────


@mcp.tool()
def list_targets() -> list[dict[str, Any]]:
    """List all registered ArangoDB target connections."""
    mgr = _get_catalog()
    return [t.model_dump(mode="json") for t in mgr.list_targets()]


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
        return {"status": "created", "target": target.model_dump(mode="json")}
    except ValueError as e:
        return {"error": str(e)}


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
        from r2g.connectors.postgres import PostgresConnector

        conn_str = _resolve_conn_string(source.connection_string)
        connector = PostgresConnector(conn_str, schema_name=pg_schema)
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
        return {"error": str(e)}


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
        return {"error": str(e)}


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

    if save_path:
        ConfigManager.save_config(config, save_path)

    return {
        "collections": len(config.collections),
        "edges": len(config.edges),
        "join_tables": [k for k, v in config.collections.items() if v.is_join_table],
        "mapping": config.model_dump(mode="json"),
        "saved_to": save_path,
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
        return {"error": str(e)}


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

    limit = min(limit, 100)

    try:
        import psycopg
        from psycopg.rows import dict_row

        conn_str = _resolve_conn_string(source.connection_string)
        with psycopg.connect(conn_str, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))  # noqa: S608
                rows = cur.fetchall()

        return {
            "table": table_name,
            "count": len(rows),
            "rows": _serialize_rows(rows),
        }
    except Exception as e:
        return {"error": str(e)}


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
            return {"status": "failed", "load_id": load_record.id, "error": str(e)}

    except Exception as e:
        return {"error": str(e)}


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
        return {"error": str(e)}


# ── Resources ────────────────────────────────────────────────────────


@mcp.resource("r2g://sources")
def resource_sources() -> str:
    """All registered data sources."""
    mgr = _get_catalog()
    sources = mgr.list_sources()
    return json.dumps([s.model_dump(mode="json") for s in sources], indent=2, default=str)


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
    return json.dumps([t.model_dump(mode="json") for t in targets], indent=2, default=str)


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
        return json.dumps({"error": str(e)})


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


def _serialize_rows(rows: list[dict]) -> list[dict]:
    import datetime as dt
    from decimal import Decimal

    result = []
    for row in rows:
        converted = {}
        for k, v in row.items():
            if isinstance(v, (dt.datetime, dt.date)):
                converted[k] = v.isoformat()
            elif isinstance(v, Decimal):
                converted[k] = float(v)
            elif isinstance(v, bytes):
                converted[k] = v.hex()
            else:
                converted[k] = v
        result.append(converted)
    return result
