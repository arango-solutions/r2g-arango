from __future__ import annotations

import asyncio
import json
import os
import queue
import threading
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from r2g.catalog import CatalogManager
from r2g.config import ConfigManager, validate_config
from r2g.connectors.arango_writer import ArangoWriter
from r2g.log import get_logger
from r2g.streaming.pipeline import StreamingPipeline
from r2g.types import MappingConfig

logger = get_logger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"

_running_loads: dict[str, dict[str, Any]] = {}  # load_id -> {"thread", "queue", "pipeline"}


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
    async def remove_source(name: str, cascade: bool = False):
        try:
            import inspect

            sig = inspect.signature(catalog.remove_source)
            if "cascade" in sig.parameters:
                result = catalog.remove_source(name, cascade=cascade)
            else:
                result = catalog.remove_source(name)
            if not result:
                raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        except HTTPException:
            raise
        except Exception as e:
            if "DependencyError" in type(e).__name__ or "Cannot remove" in str(e):
                raise HTTPException(status_code=409, detail=str(e))
            raise
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

    @app.post("/api/projects/{name}/validate-draft")
    async def validate_mapping_draft(name: str, body: dict[str, Any]):
        """Validate an in-memory (draft) mapping without persisting it.

        Used by the UI for inline / debounced validation while editing.
        """
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            draft_config = MappingConfig.model_validate(body)
            issues = validate_config(snap.schema_data, draft_config)
            return {"valid": len(issues) == 0, "issues": issues}
        except Exception as e:
            # Surface the exception as a validation issue rather than a 500
            return {"valid": False, "issues": [f"Draft parse error: {e}"]}

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

    # ── Load endpoints ─────────────────────────────────────────────────

    @app.post("/api/projects/{name}/load", status_code=202)
    async def start_load(name: str, body: LoadRequest):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")

        source = catalog.get_source(project.source_name)
        if source is None:
            raise HTTPException(status_code=400, detail=f"Source '{project.source_name}' not found")

        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")

        try:
            config = ConfigManager.load_config(project.mapping_config_path)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to load mapping config: {e}")

        issues = validate_config(snap.schema_data, config)
        if issues:
            raise HTTPException(status_code=400, detail={"validation_errors": issues})

        load_record = catalog.start_load(project_name=name, load_type="streaming")
        load_id = load_record.id

        target = None
        target_name = getattr(project, "target_name", None)
        if target_name:
            target = catalog.get_target(target_name)
        if target is None:
            for _tgt in catalog.list_targets():
                if (
                    _tgt.endpoint == project.arango_endpoint
                    and _tgt.database == project.arango_database
                ):
                    target = _tgt
                    break

        if target is not None:
            arango_endpoint = target.endpoint
            arango_database = target.database
            arango_username = target.username
            arango_password = target.password
        else:
            arango_endpoint = project.arango_endpoint
            arango_database = project.arango_database
            arango_username = os.environ.get("ARANGO_USER", "root")
            arango_password = os.environ.get("ARANGO_PASSWORD", "")

        writer = ArangoWriter(
            endpoint=arango_endpoint,
            database=arango_database,
            username=arango_username,
            password=arango_password,
        )

        pipeline = StreamingPipeline(
            pg_conn_string=source.connection_string,
            arango_writer=writer,
            schema=snap.schema_data,
            config=config,
            batch_size=body.batch_size,
            on_duplicate=body.on_duplicate,
            pg_schema=snap.pg_schema,
            dry_run=body.dry_run,
            drop_collections=body.drop_collections,
            workers=body.workers,
            include_tables=set(body.include_tables) if body.include_tables else None,
            exclude_tables=set(body.exclude_tables) if body.exclude_tables else None,
        )

        progress_queue: queue.Queue[dict[str, Any]] = queue.Queue()

        def progress_callback(event_data: dict[str, Any]) -> None:
            progress_queue.put(event_data)

        def run_pipeline() -> None:
            try:
                result = pipeline.run(graph_name=body.graph_name, on_event=progress_callback)
                total_rows = sum(r[1] for r in result.get("documents", []))
                total_rows += sum(r[1] for r in result.get("edges", []))
                total_errors = sum(len(e) for e in pipeline.errors.values())
                collections = [r[0] for r in result.get("documents", [])] + [
                    r[0] for r in result.get("edges", [])
                ]
                catalog.complete_load(load_id, total_rows, total_errors, collections, "completed")
            except Exception as exc:
                import traceback as _tb

                err_type = type(exc).__name__
                err_msg = str(exc) or err_type
                tb_str = _tb.format_exc()
                logger.error(
                    "load_pipeline_failed",
                    load_id=load_id,
                    error_type=err_type,
                    error=err_msg,
                )
                progress_queue.put(
                    {
                        "event": "error",
                        "error": err_msg,
                        "error_type": err_type,
                        "traceback": tb_str,
                    }
                )
                catalog.complete_load(
                    load_id,
                    0,
                    0,
                    [],
                    "failed",
                    error_message=err_msg,
                    error_type=err_type,
                )

        thread = threading.Thread(target=run_pipeline, daemon=True)
        _running_loads[load_id] = {"thread": thread, "queue": progress_queue, "pipeline": pipeline}
        thread.start()

        return {"load_id": load_id, "status": "started"}

    @app.get("/api/projects/{name}/load/{load_id}/status")
    async def get_load_status(name: str, load_id: str):
        events: list[dict[str, Any]] = []
        if load_id in _running_loads:
            q = _running_loads[load_id]["queue"]
            while not q.empty():
                try:
                    events.append(q.get_nowait())
                except queue.Empty:
                    break
            thread_alive = _running_loads[load_id]["thread"].is_alive()
            status = "running" if thread_alive else "completed"
            if events and events[-1].get("event") == "error":
                status = "failed"
        else:
            status = "unknown"

        return {"load_id": load_id, "status": status, "events": events}

    @app.get("/api/projects/{name}/load/{load_id}/stream")
    async def stream_load_progress(name: str, load_id: str):
        if load_id not in _running_loads:
            raise HTTPException(status_code=404, detail="Load not found")

        async def event_generator():
            q = _running_loads[load_id]["queue"]
            while True:
                try:
                    event = q.get_nowait()
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("event") in ("complete", "error"):
                        break
                except queue.Empty:
                    if not _running_loads[load_id]["thread"].is_alive():
                        yield f"data: {json.dumps({'event': 'complete'})}\n\n"
                        break
                    await asyncio.sleep(0.5)
                    yield ": keepalive\n\n"

        return StreamingResponse(event_generator(), media_type="text/event-stream")

    @app.get("/api/projects/{name}/load/{load_id}/errors")
    async def get_load_errors(name: str, load_id: str, limit: int = 50, offset: int = 0):
        from r2g.dlq import DeadLetterQueue

        dlq = DeadLetterQueue(load_id)
        errors = dlq.read_errors(limit=limit, offset=offset)
        return {"load_id": load_id, "errors": errors, "count": len(errors)}

    # ── Auto-map endpoint ──────────────────────────────────────────────

    @app.post("/api/projects/{name}/auto-map")
    async def auto_map(name: str):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot")
        config = ConfigManager.generate_default_config(snap.schema_data, source_schema=snap.pg_schema)
        return config.model_dump()

    # ── Target endpoints ───────────────────────────────────────────────

    @app.get("/api/targets")
    async def list_targets():
        if not hasattr(catalog, "list_targets"):
            return []
        return [t.model_dump() for t in catalog.list_targets()]

    @app.post("/api/targets", status_code=201)
    async def add_target(body: TargetCreateRequest):
        if not hasattr(catalog, "add_target"):
            raise HTTPException(status_code=501, detail="Target management not yet available")
        try:
            target = catalog.add_target(
                name=body.name,
                endpoint=body.endpoint,
                database=body.database,
                username=body.username,
                password=body.password,
                description=body.description,
            )
            return target.model_dump()
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.delete("/api/targets/{name}")
    async def remove_target(name: str):
        if not hasattr(catalog, "remove_target"):
            raise HTTPException(status_code=501, detail="Target management not yet available")
        if not catalog.remove_target(name):
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        return {"removed": name}

    @app.post("/api/targets/{name}/introspect")
    async def introspect_target(name: str):
        if not hasattr(catalog, "get_target"):
            raise HTTPException(status_code=501, detail="Target management not yet available")
        target = catalog.get_target(name)
        if not target:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
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
            raise HTTPException(status_code=500, detail=str(e))

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


class LoadRequest(BaseModel):
    workers: int = 1
    batch_size: int = 10000
    on_duplicate: str = "replace"
    drop_collections: bool = False
    include_tables: list[str] | None = None
    exclude_tables: list[str] | None = None
    dry_run: bool = False
    graph_name: str | None = None


class TargetCreateRequest(BaseModel):
    name: str
    endpoint: str = "http://localhost:8529"
    database: str = "_system"
    username: str = "root"
    password: str = ""
    description: str = ""


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
