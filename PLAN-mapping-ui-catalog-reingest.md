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

## Post-Implementation (Workstreams A-C)

After all agents complete:
1. Run `python -m pytest tests/ -q --ignore=tests/integration` -- all tests must pass
2. Run `python -m ruff check src/ tests/` -- all lint must pass
3. Update `PRD.md` with new requirements for catalog, selective re-ingestion, and UI
4. Update `README.md` with new CLI commands and UI documentation
5. Commit and push

---

## Implementation Plan: Phase 5b — Visual Graph Data Mapper & Ingestion Engine

### Current State Assessment

The following components from Workstreams A–C already exist and form the foundation for Phase 5b:

| Component | File(s) | Status | PRD Coverage |
|-----------|---------|--------|-------------|
| **Data Catalog** (models + CRUD) | `src/r2g/catalog.py` | Done | P5b.1.1 (partial — PostgreSQL only) |
| **Catalog CLI** | `src/r2g/main.py` (source/project/history subcommands) | Done | P5b.1.1 |
| **Schema introspection** (PostgreSQL) | `src/r2g/connectors/postgres.py` | Done | P5b.1.2 (PostgreSQL only) |
| **Mapping diff engine** | `src/r2g/mapping_diff.py` | Done | Supporting P5b.2.2 |
| **Selective reload** | `src/r2g/selective_reload.py` | Done | Supporting P5b.3.1 |
| **FastAPI backend** | `src/r2g/ui/server.py` | Done | P5b.2.1 (all API endpoints) |
| **Mapping Studio frontend** | `src/r2g/ui/static/index.html` | Done | P5b.2.1 (partial — no drag-drop mapping, no target graph, no Load button) |
| **Static visualizer** | `src/r2g/generators/visualizer.py` | Done | Read-only; superseded by Mapping Studio for interactive use |

### Gap Analysis

| PRD Requirement | Gap | Priority |
|----------------|-----|----------|
| **P5b.1.1 — Multi-source CRUD** | CSV and Kafka source types not registrable; no "Update" for sources | Medium |
| **P5b.1.2 — CSV/Kafka introspection** | CSV header parsing exists in `dump_reader.py` but not wired to catalog; Kafka schema registry introspection not implemented | Medium |
| **P5b.1.3 — Target graph definition** | No target graph introspection (fetch existing ArangoDB collections/graph schema) | High |
| **P5b.1.4 — Cascading deletes** | `catalog.remove_source()` deletes the source but does not cascade to projects, snapshots, or load history | High |
| **P5b.2.1 — Split-screen polish** | Left pane shows source tables but not as "entity cards" with columns inline; right pane is center graph, not a dedicated target panel | Medium |
| **P5b.2.2 — Map object metadata** | Mappings are saved as YAML files but lack name/description/last-modified metadata | Low |
| **P5b.2.3 — Parallel connection config** | `StreamingPipeline` supports `--workers` but UI has no field for configuring parallelism | Medium |
| **P5b.2.4 — Default mapping in UI** | `generate-config` CLI exists but no "Auto-Map" button in the UI | High |
| **P5b.2.5 — Drag-and-drop mapping** | Properties panel allows click-to-edit; no drag from source column to target property | Medium |
| **P5b.3.1 — Load button** | No "Load" / "Run" button; no `/api/projects/{name}/load` endpoint | **Critical** |
| **P5b.3.2 — Real-time job monitoring** | Load history exists; no SSE/WebSocket progress streaming | High |
| **P5b.3.3 — Dead-letter queue** | Streaming pipeline logs errors but no structured DLQ | Medium |
| **Security — Credential encryption** | Connection strings stored in plaintext in `catalog.json` | Medium |

---

### Workstream D: Data Catalog Enhancements

**Goal:** Close gaps P5b.1.1 through P5b.1.4.

#### D1: Multi-source type support
**File:** `src/r2g/catalog.py`

- Add `CsvSourceConfig` and `KafkaSourceConfig` as discriminated union variants of `SourceConfig` (or use a `source_params: dict` field for type-specific config like `directory_path`, `delimiter`, `bootstrap_servers`, `topic`, `schema_registry_url`).
- Extend `add_source` to accept type-specific parameters.
- Wire CSV introspection: reuse `DumpReader` from `input/dump_reader.py` to parse headers and infer types from a sample of rows. Create a `SchemaSnapshot` from the result.
- Wire Kafka introspection: use `confluent_kafka.schema_registry` (add to `[kafka]` optional deps) to fetch Avro/JSON schema from the registry. Parse to `Schema` model.
- Add `update_source()` method for editing connection strings and metadata.

**File:** `src/r2g/ui/server.py`
- Add `PUT /api/sources/{name}` endpoint.
- Extend `POST /api/sources/{name}/snapshot` to dispatch to the correct introspector based on `source_type`.

**Tests:** `tests/test_catalog.py` — add cases for CSV/Kafka source creation and snapshot.

#### D2: Target graph introspection
**File:** `src/r2g/connectors/arango_writer.py` (or new `src/r2g/connectors/arango_reader.py`)

- Add `introspect_graph()` method: use `python-arango` to list collections (document + edge), fetch collection properties, and enumerate named graphs. Return a structured model (`GraphSchema` with `VertexType`, `EdgeType`, `PropertyDefinition`).
- New Pydantic model `TargetGraphSchema` in `types.py`.

**File:** `src/r2g/catalog.py`
- Add `TargetConfig` model (endpoint, database, credentials, optional named graph).
- `create_target()`, `list_targets()`, `get_target()`, `remove_target()` methods.
- `snapshot_target()` — introspect and store `TargetGraphSchema`.

**File:** `src/r2g/ui/server.py`
- `GET/POST/DELETE /api/targets` endpoints.
- `POST /api/targets/{name}/snapshot` — trigger target introspection.
- `GET /api/targets/{name}/schema` — return target graph schema.

**Tests:** `tests/test_target_introspection.py` — mock `python-arango` client, verify schema extraction.

#### D3: Cascading deletes with referential integrity
**File:** `src/r2g/catalog.py`

- Modify `remove_source()` to:
  1. Find all projects referencing this source.
  2. Find all snapshots for this source.
  3. Find all load history for those projects.
  4. Return a `DependencyReport` listing what will be deleted.
  5. Accept a `force: bool = False` parameter; if `False` and dependencies exist, raise `DependencyError`.
  6. If `force=True`, delete all dependent objects.

**File:** `src/r2g/ui/server.py`
- `DELETE /api/sources/{name}` gains `?force=true` query param.
- Without `force`, returns 409 with dependency report so the UI can show a confirmation dialog.

**Tests:** `tests/test_catalog.py` — cascading delete scenarios.

#### D4: Credential security
**File:** `src/r2g/catalog.py`

- Add `encrypt_field()` / `decrypt_field()` using `cryptography.fernet` (add `cryptography` to optional `[ui]` deps).
- Key derived from a user-set master password or OS keychain via `keyring` library.
- Connection strings encrypted before writing to `catalog.json`, decrypted on read.
- Alternatively, support env var references (`$PG_CONN`) and resolve at runtime — this is already partially done.

**Priority:** Lower than D1–D3. Can ship V1 with env var references and add encryption later.

---

### Workstream E: Visual Mapping Interface Enhancements

**Goal:** Close gaps P5b.2.1 through P5b.2.5.

#### E1: Auto-Map button in UI
**File:** `src/r2g/ui/server.py`

- Add `POST /api/projects/{name}/auto-map` endpoint:
  1. Fetch the source schema snapshot.
  2. Call `ConfigManager.generate_config(schema)` (already exists).
  3. Return the generated `MappingConfig`.
  4. Optionally accept `target_name` to cross-reference target graph schema for smarter matching.

**File:** `src/r2g/ui/static/index.html`
- Add "Auto-Map" button to toolbar (between "Save" and "Diff").
- On click: POST to auto-map endpoint, merge result into `editState`, re-render graph.

#### E2: Parallel connection configuration
**File:** `src/r2g/ui/static/index.html`

- Add a "Settings" modal accessible from toolbar (gear icon).
- Fields: `workers` (number input, default 1), `batch_size` (number, default 10000), `on_duplicate` (select: replace/ignore/update).
- These values are stored in `editState` and passed to the Load endpoint (E3/F1).

#### E3: Source column → Target property drag-and-drop
**File:** `src/r2g/ui/static/index.html`

- Enhance the left sidebar to show source tables as expandable cards with draggable column items.
- D3 graph nodes on the center canvas accept drops.
- Dropping a column onto a vertex node adds a field mapping; dropping between two nodes creates an edge definition.
- Visual feedback: dashed connector line follows the cursor during drag; drop target highlights.

**Implementation approach:** Use HTML5 drag-and-drop API for sidebar items; D3 event handlers for canvas drop targets. This is a significant frontend effort.

**Priority:** Medium — click-to-edit in properties panel is functional for V1. Drag-and-drop is a polish item.

#### E4: Map object metadata
**File:** `src/r2g/catalog.py`

- Extend `Project` model with `mapping_name`, `mapping_description` fields.
- Add `updated_at` auto-update on mapping save.

**File:** `src/r2g/ui/server.py`
- `PUT /api/projects/{name}/mapping` updates `project.updated_at`.

**Priority:** Low — functional without this.

---

### Workstream F: Ingestion & Execution Engine (UI-triggered)

**Goal:** Close gaps P5b.3.1 through P5b.3.3. This is the **critical** workstream.

#### F1: Load endpoint and execution trigger
**File:** `src/r2g/ui/server.py`

- Add `POST /api/projects/{name}/load` endpoint:
  1. Validate mapping first (reuse `validate_config`).
  2. Record a `LoadRecord` via `catalog.start_load()`.
  3. Spawn the streaming pipeline in a background thread/process.
  4. Return the `load_id` immediately (202 Accepted).
  5. Pipeline uses `StreamingPipeline` with the project's PG connection, ArangoDB target, and mapping config.
  6. On completion, call `catalog.complete_load()` with results.

- Add `POST /api/projects/{name}/load` body:
  ```json
  {
    "workers": 4,
    "batch_size": 10000,
    "on_duplicate": "replace",
    "drop_collections": false,
    "include_tables": ["orders", "customers"],
    "dry_run": false
  }
  ```

- Add `GET /api/projects/{name}/load/{load_id}/status` endpoint:
  - Returns current status, rows processed, errors, elapsed time.

**File:** `src/r2g/ui/static/index.html`
- Add "Load" button (green, prominent) to toolbar.
- On click: open a confirmation modal showing the mapping summary, parallelism settings, and a "Run" button.
- After triggering: switch to a progress view showing real-time metrics.

#### F2: Real-time job monitoring via SSE
**File:** `src/r2g/ui/server.py`

- Add `GET /api/projects/{name}/load/{load_id}/stream` endpoint using FastAPI `StreamingResponse` with Server-Sent Events (SSE).
- The streaming pipeline emits progress events (rows processed, current table, errors) to a shared queue.
- SSE endpoint reads from the queue and sends events to the browser.

**File:** `src/r2g/streaming/pipeline.py`
- Add an optional `progress_callback: Callable[[dict], None]` parameter.
- Call the callback after each batch with `{"table": name, "rows": count, "total": total, "errors": errs}`.

**File:** `src/r2g/ui/static/index.html`
- When a load is running, open an `EventSource` to the SSE endpoint.
- Render a progress panel: animated progress bars per table, total rows counter, error count, elapsed time.
- On completion: show summary modal, refresh history.

#### F3: Dead-letter queue
**File:** `src/r2g/streaming/pipeline.py` (or new `src/r2g/dlq.py`)

- Failed records (type mismatch, missing keys, ArangoDB rejection) are written to a DLQ file: `~/.r2g/dlq/{load_id}.jsonl`.
- Each DLQ entry: `{"table": "...", "row": {...}, "error": "...", "timestamp": "..."}`.
- DLQ is queryable via API: `GET /api/projects/{name}/load/{load_id}/errors?limit=50`.

**File:** `src/r2g/ui/static/index.html`
- Error count in the progress view is clickable → opens a modal with DLQ entries.

---

### Execution Order

```
Phase 5b implementation order:

  D1 (multi-source) ──┐
  D2 (target graph) ──┼── can run in parallel
  D3 (cascading del) ─┘
           │
           v
  E1 (auto-map button) ──── quick win, do early
  E2 (parallel config) ──── quick win, do early
           │
           v
  F1 (Load endpoint) ─────── CRITICAL PATH
  F2 (SSE monitoring) ─────── depends on F1
  F3 (DLQ) ────────────────── depends on F1
           │
           v
  E3 (drag-and-drop) ─────── polish, can defer
  D4 (encryption) ─────────── polish, can defer
  E4 (map metadata) ────────── polish, can defer
```

### Estimated Effort

| Work Item | Effort | Files Touched |
|-----------|--------|---------------|
| D1: Multi-source types | 1 day | catalog.py, server.py, index.html |
| D2: Target graph introspection | 1 day | arango_writer.py or new file, types.py, catalog.py, server.py |
| D3: Cascading deletes | 0.5 day | catalog.py, server.py |
| E1: Auto-map button | 0.5 day | server.py, index.html |
| E2: Parallel config | 0.5 day | index.html |
| **F1: Load endpoint** | **2 days** | **server.py, pipeline.py, index.html** |
| F2: SSE monitoring | 1.5 days | server.py, pipeline.py, index.html |
| F3: DLQ | 1 day | pipeline.py or new dlq.py, server.py, index.html |
| E3: Drag-and-drop | 2 days | index.html |
| D4: Credential encryption | 1 day | catalog.py, pyproject.toml |
| E4: Map metadata | 0.5 day | catalog.py, server.py |
| **Total** | **~11.5 days** | |

### Parallel Agent Assignment (Phase 5b)

**Agent 6: Catalog Enhancements (D1 + D3)**
- Extend `src/r2g/catalog.py` with multi-source types, update_source, cascading deletes
- Add/update API endpoints in `src/r2g/ui/server.py`
- Add tests

**Agent 7: Target Graph Introspection (D2)**
- Create target introspection logic (arango_reader or extend arango_writer)
- Add `TargetConfig` / `TargetGraphSchema` models
- Add catalog CRUD for targets
- Add API endpoints and tests

**Agent 8: Load Engine & UI (F1 + F2)** — Critical path
- Add `POST /api/projects/{name}/load` with background execution
- Add SSE progress streaming
- Add progress_callback to StreamingPipeline
- Add Load button + progress UI to index.html
- Add tests

**Agent 9: UI Enhancements (E1 + E2 + E3)**
- Auto-map button + endpoint
- Parallel config settings modal
- Drag-and-drop mapping (if time permits)

**Agent 10: Error Handling (F3)**
- DLQ implementation
- Error API endpoint
- Error viewer in UI

---

---

## Phase 5c Workstream G: Expression Engine + Graph-of-Graphs UI

Phase 5c is additive on top of Phase 5b. The split-screen mapper built in Workstream E is kept; the center canvas now hosts mapping function nodes and the side panes grow mini graph visualizations.

### G1: FieldExpression data model
**Files:** `src/r2g/types.py`, `tests/test_types.py`

- Add `FieldExpression(BaseModel)` with fields:
  - `target: str`
  - `sources: list[str] = []` (empty = identity on the column sharing `target`'s name)
  - `expression: str = ""` (empty = identity pass-through)
  - `engine: Literal["aql", "ksql", "python"] = "aql"`
  - `description: str = ""`
- Extend `CollectionMapping` with `field_expressions: list[FieldExpression] = []`.
- Backward compatibility: loading a legacy config without `field_expressions` must succeed and leave the list empty. Saving a config with only legacy `field_mappings` renames must serialize identically to before.
- Precedence rule (documented and enforced in the transformer later): for any target property name `t`, if a `FieldExpression` with `target == t` exists, it is authoritative and `field_mappings` entries for the same `t` are ignored. Otherwise `field_mappings` and default pass-through apply as today.

### G2: UI server graph-data surface
**File:** `src/r2g/ui/server.py` + `src/r2g/generators/visualizer.py`

- Extend `_build_config_data` so each collection block includes `fieldExpressions: [...]` (camelCase for UI parity with `fieldMappings`).
- Accept `field_expressions` on `PUT /api/projects/{name}/mapping` round-tripping through `MappingConfig`.

### G3: Mapping function nodes (center canvas)
**File:** `src/r2g/ui/static/index.html`

- Between each source column dot and its target property dot, render a **function circle** on the midpoint of the Bezier connector.
- Identity pass-through: small hollow circle, muted stroke, no label.
- Non-identity expressions: filled circle with engine badge (`aql` / `ksql` / `py`), label = target property name.
- Click opens an expression editor modal with:
  - Engine selector (radio)
  - `sources` multi-select populated from the connected source table's columns
  - Expression textarea (monospace)
  - Save / Cancel
- Saving writes a `FieldExpression` into `editState.collections[<key>].fieldExpressions` and redraws.

### G4: Fan-in drag-and-drop
**File:** `src/r2g/ui/static/index.html`

- Existing drag-and-drop from source column dot already targets target property dots. Extend it so that dropping onto a **function circle** adds the dragged column to that function's `sources` list (and does NOT create a new mapping).
- Visual feedback during drag: any function circle within range highlights; property dots highlight as before.

### G5: In-pane source ER graph
**File:** `src/r2g/ui/static/index.html`

- Within the source pane, draw SVG edges between source-table card headers for each foreign-key relationship. Edges start at the destination of the FK (referenced table) and terminate at the source (referencing table) with an arrowhead indicating the FK direction.
- Edges are drawn in a dedicated SVG layer in the source pane, repositioned on scroll/resize.
- Clicking an inter-table edge highlights both endpoints and reveals the FK columns in the properties panel.

### G6: In-pane target graph model
**File:** `src/r2g/ui/static/index.html`

- Within the target pane, draw SVG edges between target **vertex-collection** card headers for each edge-collection definition, label each edge with the edge collection name.
- Edges reposition on scroll/resize.
- Clicking an edge collection arrow selects the edge card and populates the properties panel.

### G7: Expression evaluator (backend, deferred)
**Files:** new `src/r2g/expressions.py`, `src/r2g/transformers/node_transformer.py`

- Minimal AQL subset evaluator (CONCAT, UPPER, LOWER, SUBSTRING, LENGTH, LTRIM, RTRIM, TO_STRING, arithmetic, comparisons, null handling).
- Node transformer consults `field_expressions` first, falls back to `field_mappings`.
- For expressions outside the subset, delegate to ArangoDB via a per-batch `FOR doc IN @docs LET <assignments> RETURN doc` query.
- Arangoimport bulk path: pre-evaluate in Python so `arangoimport` receives final JSONL.
- This requirement is tracked in PRD (P5c.1.4, P5c.1.5, P5c.1.6) and implemented in a subsequent iteration.

### Execution order for Phase 5c

```
G1 (FieldExpression types) ─┐
G2 (server graph-data)     ─┼── backend first (cheap)
                            │
G5 (source ER edges) ───────┼── UI additions, parallelizable
G6 (target graph edges) ────┤
                            │
G3 (function circles) ─────┬┘
G4 (fan-in drop)     ──────┘
                            │
                            v
G7 (evaluator, deferred)
```

### Estimated effort (Phase 5c)

| Work Item | Effort |
|-----------|--------|
| G1: FieldExpression model | 0.5 day |
| G2: Server graph-data surface | 0.5 day |
| G3: Function circles + editor | 1.5 days |
| G4: Fan-in drop | 0.5 day |
| G5: Source ER edges | 1 day |
| G6: Target graph edges | 1 day |
| G7: Evaluator (deferred) | 3 days |
| **Total (excl. G7)** | **5 days** |

---

## Style and Convention Notes

- Follow existing patterns: Pydantic models in `types.py` style, Typer commands in `main.py` style
- Use `structlog` for logging (via `r2g.log.get_logger`)
- Use `Rich` for CLI output (tables, progress bars)
- Tests use `pytest` with fixtures, mocks via `unittest.mock`
- Lint with `ruff` (config in `pyproject.toml`: E, F, I, W rules, 120 char lines)
- Do NOT add comments that just narrate what code does
- Do NOT create documentation files unless explicitly asked
