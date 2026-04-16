from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from r2g.catalog import CatalogManager
from r2g.config import ConfigManager, validate_config
from r2g.log import get_logger
from r2g.types import MappingConfig

logger = get_logger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"


def create_app(catalog_dir: str | None = None) -> FastAPI:
    """Factory function for the FastAPI application."""
    app = FastAPI(title="R2G Mapping Studio", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    catalog = CatalogManager(catalog_dir)

    if _STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.get("/")
    async def index():
        index_path = _STATIC_DIR / "index.html"
        if index_path.exists():
            return FileResponse(str(index_path), media_type="text/html")
        return HTMLResponse("<h1>R2G Mapping Studio</h1><p>Frontend not built yet.</p>")

    @app.get("/api/health")
    async def health():
        return {"status": "ok"}

    # ── Source endpoints ──────────────────────────────────────────────

    @app.get("/api/sources")
    async def list_sources():
        sources = catalog.list_sources()
        return [s.model_dump() for s in sources]

    @app.post("/api/sources", status_code=201)
    async def add_source(body: SourceCreateRequest):
        try:
            source = catalog.add_source(
                name=body.name,
                source_type=body.source_type,
                connection_string=body.connection_string,
                description=body.description,
                owner=body.owner,
            )
            return source.model_dump()
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.delete("/api/sources/{name}")
    async def remove_source(name: str):
        if not catalog.remove_source(name):
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        return {"removed": name}

    @app.post("/api/sources/{name}/snapshot")
    async def create_snapshot(name: str, pg_schema: str = "public"):
        source = catalog.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        try:
            from r2g.connectors.postgres import PostgresConnector

            connector = PostgresConnector(source.connection_string, schema_name=pg_schema)
            schema = connector.get_schema()
            snap = catalog.create_snapshot(name, schema, pg_schema=pg_schema)
            return {"id": snap.id, "tables": len(schema.tables), "captured_at": snap.captured_at.isoformat()}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # ── Schema endpoints ──────────────────────────────────────────────

    @app.get("/api/sources/{name}/schema")
    async def get_schema(name: str):
        snap = catalog.get_latest_snapshot(name)
        if snap is None:
            raise HTTPException(status_code=404, detail=f"No snapshots for source '{name}'")
        return snap.schema_data.model_dump()

    @app.get("/api/sources/{name}/preview/{table}")
    async def preview_table(name: str, table: str, limit: int = 20):
        source = catalog.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        try:
            import psycopg
            from psycopg.rows import dict_row

            with psycopg.connect(source.connection_string, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {table} LIMIT %s", (limit,))
                    rows = cur.fetchall()
            return {"table": table, "rows": _serialize_rows(rows), "count": len(rows)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # ── Project endpoints ─────────────────────────────────────────────

    @app.get("/api/projects")
    async def list_projects():
        return [p.model_dump() for p in catalog.list_projects()]

    @app.post("/api/projects", status_code=201)
    async def create_project(body: ProjectCreateRequest):
        try:
            project = catalog.create_project(
                name=body.name,
                source_name=body.source_name,
                mapping_config_path=body.mapping_config_path,
                arango_endpoint=body.arango_endpoint,
                arango_database=body.arango_database,
            )
            return project.model_dump()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/projects/{name}/mapping")
    async def get_mapping(name: str):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        try:
            config = ConfigManager.load_config(project.mapping_config_path)
            return config.model_dump()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.put("/api/projects/{name}/mapping")
    async def save_mapping(name: str, body: dict[str, Any]):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        try:
            config = MappingConfig.model_validate(body)
            ConfigManager.save_config(config, project.mapping_config_path)
            return {"saved": True, "collections": len(config.collections), "edges": len(config.edges)}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/projects/{name}/validate")
    async def validate_mapping(name: str):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            config = ConfigManager.load_config(project.mapping_config_path)
            issues = validate_config(snap.schema_data, config)
            return {"valid": len(issues) == 0, "issues": issues}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/projects/{name}/diff")
    async def diff_mapping(name: str, body: dict[str, Any]):
        """Diff the current mapping against a proposed mapping."""
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            from r2g.mapping_diff import diff_mappings

            old_config = ConfigManager.load_config(project.mapping_config_path)
            new_config = MappingConfig.model_validate(body)
            plan = diff_mappings(old_config, new_config, snap.schema_data)
            return plan.model_dump()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/projects/{name}/graph-data")
    async def get_graph_data(name: str):
        """Return D3-compatible graph data for the project mapping."""
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot")
        try:
            from r2g.generators.visualizer import MappingVisualizer

            config = ConfigManager.load_config(project.mapping_config_path)
            viz = MappingVisualizer(snap.schema_data, config)
            return {
                "graph": viz._build_graph_data(),
                "tables": viz._build_tables_data(),
                "edges": viz._build_edges_data(),
                "config": viz._build_config_data(),
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/projects/{name}/history")
    async def get_project_history(name: str, limit: int = 20):
        records = catalog.get_history(project_name=name, limit=limit)
        return [r.model_dump() for r in records]

    return app


# ── Request models ────────────────────────────────────────────────────


class SourceCreateRequest(BaseModel):
    name: str
    source_type: str = "postgresql"
    connection_string: str
    description: str = ""
    owner: str = ""


class ProjectCreateRequest(BaseModel):
    name: str
    source_name: str
    mapping_config_path: str
    arango_endpoint: str = "http://localhost:8529"
    arango_database: str = "_system"


def _serialize_rows(rows: list[dict]) -> list[dict]:
    """Convert non-JSON-serializable types to strings."""
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
