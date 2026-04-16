# Multi-Agent Implementation Plan: Mapping UI + Data Catalog + Selective Re-ingestion

## Context

R2G is a Python CLI tool (`src/r2g/`) that migrates relational databases (PostgreSQL) to ArangoDB graphs. It supports file-based ETL, direct streaming, CDC (logical replication), and Kafka (Debezium). The codebase uses Typer (CLI), Pydantic (models), python-arango, psycopg, structlog, Rich, and D3.js (visualizer). There are 502 unit tests.

Key existing modules:
- `src/r2g/types.py` -- Schema, Table, Column, ForeignKey, MappingConfig, CollectionMapping, EdgeDefinition
- `src/r2g/config.py` -- ConfigManager (generate/load/save YAML), validate_config, DEFAULT_TYPE_MAP
- `src/r2g/schema_diff.py` -- diff_schemas (added/removed/modified tables, columns, FKs)
- `src/r2g/config_migrate.py` -- migrate_config (evolve mapping YAML when schema changes)
- `src/r2g/generators/visualizer.py` -- generates static HTML with D3.js force-directed graph
- `src/r2g/connectors/arango_writer.py` -- ArangoWriter (bulk import, single-doc ops, named graphs)
- `src/r2g/streaming/pipeline.py` -- StreamingPipeline (PG -> ArangoDB with topo sort)
- `src/r2g/main.py` -- Typer CLI app with all commands

The existing visualizer (`test_run/mapping_viz.html`) is a self-contained static HTML file with three tabs: Graph Schema (D3 force-directed), Relational Schema (cards), Edge Mapping (table). It's read-only and generated once by `r2g visualize-mapping`.

## Implementation: Three Workstreams

### Workstream A: Data Catalog (do first -- others depend on it)

**Goal:** Persistent source registry, project state, and load history so the mapping UI and selective re-ingestion have something to operate on.

**File:** `src/r2g/catalog.py`

**Models (Pydantic, stored as `~/.r2g/catalog.json`):**

```python
class SourceConfig(BaseModel):
    name: str                          # user-assigned name, e.g. "prod_ecommerce"
    source_type: str = "postgresql"    # "postgresql" | "snowflake" (future)
    connection_string: str             # or env var reference like "$PG_CONN"
    description: str = ""
    owner: str = ""
    created_at: datetime
    updated_at: datetime

class SchemaSnapshot(BaseModel):
    source_name: str
    schema: Schema                     # reuse existing Schema model from types.py
    captured_at: datetime
    pg_schema: str = "public"

class Project(BaseModel):
    name: str
    source_name: str
    schema_snapshot_id: str            # reference to a SchemaSnapshot
    mapping_config_path: str           # path to the YAML mapping config
    arango_endpoint: str = "http://localhost:8529"
    arango_database: str = "_system"
    created_at: datetime
    updated_at: datetime

class LoadRecord(BaseModel):
    project_name: str
    started_at: datetime
    completed_at: datetime | None = None
    load_type: str                     # "full" | "streaming" | "cdc" | "kafka" | "selective"
    collections_loaded: list[str] = []
    rows_loaded: int = 0
    errors: int = 0
    mapping_hash: str                  # hash of the mapping config used
    status: str = "running"            # "running" | "completed" | "failed"

class Catalog(BaseModel):
    sources: dict[str, SourceConfig] = {}
    snapshots: dict[str, SchemaSnapshot] = {}
    projects: dict[str, Project] = {}
    load_history: list[LoadRecord] = []
```

**CLI commands (add to `src/r2g/main.py`):**

```
r2g source add --name prod_pg --type postgresql --conn "postgresql://..." --description "Production DB"
r2g source list
r2g source remove prod_pg
r2g source snapshot prod_pg                    # introspect schema, save snapshot
r2g source snapshot prod_pg --compare-last     # snapshot + diff against previous

r2g project create --name ecommerce_graph --source prod_pg --endpoint http://... --database mydb
r2g project list
r2g project status ecommerce_graph             # show last load, mapping hash, drift detection

r2g history --project ecommerce_graph          # show load history
```

**Storage:** `~/.r2g/catalog.json` for the catalog index, `~/.r2g/snapshots/{id}.json` for schema snapshots, `~/.r2g/projects/{name}/mapping.yaml` for mapping configs. Alternatively, use a single SQLite file at `~/.r2g/catalog.db` for better concurrent access.

**Tests:** `tests/test_catalog.py` -- CRUD for sources, projects, snapshots, load records. Fixture with `tmp_path` for catalog storage.

**Dependencies on existing code:**
- `Schema.load_from_file` / `Schema.save_to_file` from `types.py`
- `ConfigManager.load_config` / `ConfigManager.save_config` from `config.py`
- `diff_schemas` from `schema_diff.py`
- `connectors/postgres.py` for schema introspection during snapshot

---

### Workstream B: Selective Re-ingestion (do second -- uses catalog)

**Goal:** When a mapping config changes, compute the minimal set of ArangoDB operations needed and execute only those, instead of full reload.

**File:** `src/r2g/mapping_diff.py`

**Mapping diff engine:**

```python
class MappingChange(BaseModel):
    change_type: str   # "collection_renamed" | "collection_added" | "collection_removed"
                       # | "field_mapping_added" | "field_mapping_removed"
                       # | "edge_added" | "edge_removed" | "edge_modified"
                       # | "type_override_changed" | "key_separator_changed"
    collection: str | None = None
    edge: str | None = None
    details: dict[str, Any] = {}

class ReloadPlan(BaseModel):
    changes: list[MappingChange]
    actions: list[ReloadAction]
    estimated_rows: int = 0
    estimated_time_seconds: float = 0.0

class ReloadAction(BaseModel):
    action_type: str   # "rename_collection" | "drop_collection" | "reload_collection"
                       # | "drop_edge" | "reload_edge" | "aql_update" | "noop"
    collection: str
    reason: str
    sql_query: str | None = None      # for row count estimation
    aql_query: str | None = None      # for in-place field renames
```

**Core function:**

```python
def diff_mappings(old: MappingConfig, new: MappingConfig, schema: Schema) -> ReloadPlan:
    """Compare two mapping configs and produce a minimal reload plan."""
```

Logic:
1. **Collection renamed** (`target_collection` changed for same `source_table`): `db.rename_collection(old, new)` + update `_from`/`_to` in edge collections via AQL
2. **Collection added** (new source_table mapped): full reload of that table only
3. **Collection removed** (source_table unmapped): drop collection + associated edges
4. **Field mapping added/removed**: AQL `UPDATE` in place (rename field, remove field)
5. **Edge added**: reload that edge collection from source
6. **Edge removed**: drop edge collection
7. **Edge modified** (from_field/to_field changed): drop + reload edge collection
8. **Type override changed**: reload affected collection (types are baked into documents)
9. **key_separator changed**: full reload of all collections (keys change)

**File:** `src/r2g/selective_reload.py`

**Executor:**

```python
class SelectiveReloader:
    def __init__(self, writer: ArangoWriter, pipeline: StreamingPipeline, plan: ReloadPlan): ...

    def execute(self, dry_run: bool = False) -> ReloadReport:
        """Execute the reload plan. Returns a report of what was done."""

    def _rename_collection(self, action: ReloadAction) -> None: ...
    def _reload_collection(self, action: ReloadAction) -> None: ...
    def _reload_edge(self, action: ReloadAction) -> None: ...
    def _aql_update(self, action: ReloadAction) -> None: ...
    def _drop_collection(self, action: ReloadAction) -> None: ...
```

**CLI commands:**

```
r2g mapping-diff old_mapping.yaml new_mapping.yaml schema.json
    # Output: table of changes + proposed actions + estimated impact

r2g reload --changes-only old_mapping.yaml new_mapping.yaml schema.json \
    --pg-conn "postgresql://..." --endpoint http://... --database mydb
    # Execute selective reload

r2g reload --changes-only old_mapping.yaml new_mapping.yaml schema.json --dry-run
    # Show plan without executing
```

**Tests:** `tests/test_mapping_diff.py` -- diff detection for each change type. `tests/test_selective_reload.py` -- mock writer/pipeline to verify correct actions dispatched.

**Dependencies on existing code:**
- `ArangoWriter` from `connectors/arango_writer.py` (for rename, drop, AQL)
- `StreamingPipeline` from `streaming/pipeline.py` (for selective table reload)
- `MappingConfig` from `types.py`
- `ConfigManager` from `config.py`
- `Catalog` / `LoadRecord` from `catalog.py` (to record the selective reload)

---

### Workstream C: Interactive Mapping UI (do third -- uses catalog + re-ingestion)

**Goal:** A web-based mapping editor (like TigerGraph GraphStudio) that replaces the static HTML visualizer. Users can visually create and edit mappings, preview data, validate, and trigger loads.

**Architecture:** FastAPI backend + self-contained frontend (single HTML or small React/Svelte app served by FastAPI).

**File:** `src/r2g/ui/server.py`

**Backend API (FastAPI):**

```python
# Source management
GET    /api/sources                          # list sources from catalog
POST   /api/sources                          # add source
DELETE /api/sources/{name}                   # remove source
POST   /api/sources/{name}/snapshot          # introspect schema

# Schema
GET    /api/sources/{name}/schema            # current schema snapshot
GET    /api/sources/{name}/preview/{table}   # SELECT * FROM table LIMIT 20

# Project / mapping
GET    /api/projects                         # list projects
POST   /api/projects                         # create project
GET    /api/projects/{name}/mapping          # current mapping config
PUT    /api/projects/{name}/mapping          # save mapping config (full replace)
PATCH  /api/projects/{name}/mapping          # partial update (single collection or edge)
POST   /api/projects/{name}/validate         # validate mapping against schema

# Mapping diff + selective reload
POST   /api/projects/{name}/diff             # diff current vs proposed mapping
POST   /api/projects/{name}/reload           # execute selective reload
GET    /api/projects/{name}/reload/status     # poll reload progress

# Visualizer
GET    /api/projects/{name}/graph-data       # D3-compatible graph data (nodes + links)

# Load history
GET    /api/projects/{name}/history          # load records

# Health
GET    /api/health
```

**File:** `src/r2g/ui/app.py` (or inline in server.py)

**Frontend (self-contained HTML served at `/`):**

Reuse and extend the existing D3.js visualizer pattern but make it interactive:

1. **Source panel** (left sidebar): list sources, snapshot button, data preview
2. **Graph canvas** (center): D3 force-directed graph of the mapping
   - Drag tables from source panel onto canvas to create vertex collections
   - Right-click table to mark as join table (converts to edge)
   - Click FK line to edit edge definition (rename, change fields)
   - Click node to edit collection mapping (rename target, field mappings, exclude fields)
   - Visual indicators for validation warnings (red badges)
3. **Properties panel** (right sidebar): shows selected node/edge details
   - Collection name, source table, field mappings, type overrides
   - Data preview (sample rows from source table)
   - YAML preview of the current mapping section
4. **Toolbar** (top): Validate, Save, Diff, Reload, Export YAML, Load History
5. **Diff view** (modal): side-by-side old vs new mapping with highlighted changes and reload plan

**CLI command:**

```
r2g ui                                      # start web UI on localhost:8501
r2g ui --port 8501 --host 0.0.0.0           # custom bind
r2g ui --project ecommerce_graph            # open directly to a project
```

**File:** `src/r2g/ui/static/index.html` (or templates/)

**Dependencies:**
- `fastapi` + `uvicorn` added as optional dependency: `pip install r2g[ui]`
- Catalog from workstream A
- Mapping diff + selective reload from workstream B
- Existing: Schema, MappingConfig, ConfigManager, validate_config, visualizer graph data generation

**Tests:** `tests/test_ui_api.py` -- use `httpx.AsyncClient` with FastAPI TestClient to test each endpoint. Mock catalog and database connections.

---

## Execution Order and Dependencies

```
Workstream A (Catalog)          Workstream B (Re-ingestion)      Workstream C (UI)
    |                                |                               |
    |  catalog.py                    |                               |
    |  CLI commands                  |                               |
    |  tests                         |                               |
    v                                |                               |
    DONE -----> depends on A ------> |                               |
                                     |  mapping_diff.py              |
                                     |  selective_reload.py          |
                                     |  CLI commands                 |
                                     |  tests                        |
                                     v                               |
                                     DONE --> depends on A+B ------> |
                                                                     |  ui/server.py
                                                                     |  ui/static/
                                                                     |  CLI command
                                                                     |  tests
                                                                     v
                                                                     DONE
```

Workstreams A and B can be partially parallelized: the mapping_diff engine (workstream B) doesn't strictly require the catalog -- it operates on two MappingConfig objects + a Schema. The catalog integration (recording load history, project state) can be wired in after both A and B are done.

## Parallel Agent Assignment

**Agent 1: Data Catalog**
- Create `src/r2g/catalog.py` with all models and CRUD operations
- Add CLI commands to `src/r2g/main.py`: `source add/list/remove/snapshot`, `project create/list/status`, `history`
- Create `tests/test_catalog.py`
- Update `pyproject.toml` if any new dependencies needed
- Do NOT edit the plan file

**Agent 2: Mapping Diff Engine**
- Create `src/r2g/mapping_diff.py` with `diff_mappings()` returning `ReloadPlan`
- Handle all 9 change types listed above
- Create `tests/test_mapping_diff.py` with tests for each change type
- Do NOT create the executor yet (Agent 3 does that)
- Do NOT edit the plan file

**Agent 3: Selective Reload Executor** (start after Agent 2)
- Create `src/r2g/selective_reload.py` with `SelectiveReloader`
- Add CLI commands to `src/r2g/main.py`: `mapping-diff`, `reload --changes-only`
- Create `tests/test_selective_reload.py`
- Wire in `LoadRecord` from catalog if Agent 1 is done
- Do NOT edit the plan file

**Agent 4: Interactive UI Backend** (start after Agents 1+2+3)
- Add `fastapi` and `uvicorn` as optional deps in `pyproject.toml` under `[project.optional-dependencies] ui = [...]`
- Create `src/r2g/ui/__init__.py`, `src/r2g/ui/server.py` with FastAPI app and all API endpoints
- Create `tests/test_ui_api.py`
- Add `r2g ui` CLI command to `src/r2g/main.py`
- Do NOT edit the plan file

**Agent 5: Interactive UI Frontend** (start after Agent 4)
- Create `src/r2g/ui/static/index.html` -- single self-contained HTML with D3.js
- Extend the existing visualizer pattern (dark theme, force-directed graph) but add:
  - Source panel (sidebar), properties panel, toolbar
  - Drag-drop mapping creation, click-to-edit, right-click context menus
  - Data preview panel (fetches from `/api/sources/{name}/preview/{table}`)
  - Validation overlay (fetches from `/api/projects/{name}/validate`)
  - Diff modal (fetches from `/api/projects/{name}/diff`)
  - Save/export buttons
- Do NOT edit the plan file

## Post-Implementation

After all agents complete:
1. Run `python -m pytest tests/ -q --ignore=tests/integration` -- all tests must pass
2. Run `python -m ruff check src/ tests/` -- all lint must pass
3. Update `PRD.md` with new requirements for catalog, selective re-ingestion, and UI
4. Update `README.md` with new CLI commands and UI documentation
5. Commit and push

## Style and Convention Notes

- Follow existing patterns: Pydantic models in `types.py` style, Typer commands in `main.py` style
- Use `structlog` for logging (via `r2g.log.get_logger`)
- Use `Rich` for CLI output (tables, progress bars)
- Tests use `pytest` with fixtures, mocks via `unittest.mock`
- Lint with `ruff` (config in `pyproject.toml`: E, F, I, W rules, 120 char lines)
- Do NOT add comments that just narrate what code does
- Do NOT create documentation files unless explicitly asked
