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

| PRD Requirement | Gap | Priority | Status |
|----------------|-----|----------|--------|
| **P5b.1.1 — Multi-source CRUD** | CSV and Kafka source types not registrable; no "Update" for sources | Medium | **Done for PostgreSQL** (CLI + API + in-UI "+ New source" form; CSV / Kafka registration still deferred) |
| **P5b.1.2 — CSV/Kafka introspection** | CSV header parsing exists in `dump_reader.py` but not wired to catalog; Kafka schema registry introspection not implemented | Medium | Open (PostgreSQL introspection done and auto-triggered after in-UI source creation) |
| **P5b.1.3 — Target graph definition** | No target graph introspection (fetch existing ArangoDB collections/graph schema) | High | **Done** — `src/r2g/connectors/arango_reader.py`, `TargetConfig`, `/api/targets*` endpoints, tests in `tests/test_target_introspection.py`, and in-UI "+ New target" form + right-click actions |
| **P5b.1.4 — Cascading deletes** | `catalog.remove_source()` deletes the source but does not cascade to projects, snapshots, or load history | High | **Done** — `CatalogManager.remove_source(cascade=True)` + `DependencyError`; `DELETE /api/sources/{name}?cascade=true`; in-UI right-click "Remove source…" shows dependency report + cascade confirmation |
| **P5b.2.1 — Split-screen polish** | Left pane shows source tables but not as "entity cards" with columns inline; right pane is center graph, not a dedicated target panel | Medium | **Done (Phase 5e)** — persistent three-zone layout with sources sidebar, center canvas carrying source / mapping / target graphs, and floating / overlay property surfaces |
| **P5b.2.2 — Map object metadata** | Mappings are saved as YAML files but lack name/description/last-modified metadata | Low | Open (deferred to Phase 5d) |
| **P5b.2.3 — Parallel connection config** | `StreamingPipeline` supports `--workers` but UI has no field for configuring parallelism | Medium | **Done** — Settings modal fields: `workers`, `batch_size`, `on_duplicate`, `drop_collections`; wired into Load payload |
| **P5b.2.4 — Default mapping in UI** | `generate-config` CLI exists but no "Auto-Map" button in the UI | High | **Done** — `POST /api/projects/{name}/auto-map`, toolbar "Auto-Map" button, shortcut `a` |
| **P5b.2.5 — Drag-and-drop mapping** | Properties panel allows click-to-edit; no drag from source column to target property | Medium | **Done (Phase 5e)** — column-dot to property-dot drag, Shift / Alt drag between target cards to add edges, right-click menus for edit / delete / promote |
| **P5b.3.1 — Load button** | No "Load" / "Run" button; no `/api/projects/{name}/load` endpoint | **Critical** | **Done** — `POST /api/projects/{name}/load` runs `StreamingPipeline` in a background thread; toolbar "Load" action + confirmation modal; shortcut Ctrl/Cmd+Enter; tests in `tests/test_load_engine.py` |
| **P5b.3.2 — Real-time job monitoring** | Load history exists; no SSE/WebSocket progress streaming | High | **Done** — `GET /api/projects/{name}/load/{load_id}/status` + SSE `/stream`; `progress_callback` in `StreamingPipeline`; floating progress card in the UI; bottom timeline strip for completed runs |
| **P5b.3.3 — Dead-letter queue** | Streaming pipeline logs errors but no structured DLQ | Medium | **Done** — `src/r2g/dlq.py` `DeadLetterQueue` writes `~/.r2g/dlq/{load_id}.jsonl`; `GET /api/projects/{name}/load/{load_id}/errors` exposes entries; tests in `tests/test_dlq.py` |
| **Security — Credential encryption** | Connection strings stored in plaintext in `catalog.json` | Medium | Done (Fernet envelope with tagged `enc:v1:` ciphertexts; key from `R2G_SECRET_KEY` or `~/.r2g/secret.key` 0600; API responses redacted; `r2g secrets` CLI ships) |

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

#### D4: Credential security — Done

- `src/r2g/security.py` ships a `CredentialCipher` wrapping `cryptography.fernet` plus a `load_secret_key(catalog_dir)` that looks at `R2G_SECRET_KEY` first and falls back to `<catalog_dir>/secret.key` (0600, auto-generated on first use).
- `cryptography>=42` is now a core dependency — the catalog is unusable without it, so a core dep is the honest choice.
- `CatalogManager._load` / `_save` transparently encrypt-on-write and decrypt-on-read, tagging values with `enc:v1:` so legacy plaintext catalogs are still readable. Empty passwords stay empty.
- `src/r2g/ui/server.py` redacts secrets in API responses via `redact_connection_string` and `redact_for_display` so the browser only ever receives masked forms (`u:***@host:5432/db`, `***VAL`).
- `r2g secrets init|status|migrate` (in `src/r2g/main.py`) manages the key, reports its active source, and force-upgrades existing catalogs.
- Tests: `tests/test_security.py` (26 cases) covers round-trip, idempotent encrypt, mismatched-key failure, env-var-over-file precedence, file permission bits, legacy plaintext read, upgrade-on-save, and every redaction branch. `tests/test_ui_api.py` covers API-level redaction for both sources and targets.

**Deferred:** OS-keychain (`keyring`) integration and a key-rotation command that re-encrypts existing ciphertexts with a new active key.

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

- **Done (P5c.1.4 / P5c.1.6 — Python subset).** `src/r2g/expressions.py` ships a safe AQL-subset evaluator (literals, `@bind` references, arithmetic with null propagation, comparisons, `&&`/`||`/`NOT`, `??`, ternary `? :`, and the function set `CONCAT`, `CONCAT_SEPARATOR`, `UPPER`, `LOWER`, `SUBSTRING`, `LENGTH`, `LTRIM`, `RTRIM`, `TRIM`, `TO_STRING`, `TO_NUMBER`, `TO_BOOL`, `CONTAINS`, `COALESCE`).
- **Done.** `NodeTransformer` compiles each collection's `field_expressions` once in `__init__`, applies them per row in `transform_row` after the column-level `field_mappings` pass, and drops field-mapping outputs for any target that is also produced by an expression. Identity expressions, expressions on non-AQL engines, and uncompilable expressions fall back to a pass-through read with a structured-log warning.
- **Done.** `validate_config` parse-checks every AQL expression against the snapshot columns, reporting both source-reference and `@binding` mismatches so they surface in the UI validation lens and through `POST /api/projects/{name}/validate-draft`.
- **Done.** `/api/expressions/functions` advertises the supported function set and `/api/expressions/compile` is a parse-only check used by the in-modal live syntax indicator; save is blocked when the editor shows a compile error.
- **Deferred (P5c.1.5).** Per-batch AQL delegation for expressions outside the supported Python subset (and for the `ksql` / `python` engines) is still open; the evaluator currently falls back to identity pass-through with a warning so loads keep making progress.

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

## Phase 5e Workstream H: Mapping UI Architecture Upgrade — Implemented

Phase 5b delivered the functional split-screen mapper and Phase 5c added expression nodes and the graph-of-graphs layout. Phase 5e is the alignment pass that realigns the whole workspace with the object-centric UI contract in `.cursor/rules/ui-architecture.mdc`: one persistent stage, context-menu-primary, paint-only lenses, floating non-blocking work surfaces, first-class edges, legible legend, and inline validation. It is additive on top of 5b / 5c; no new routes.

### Delivered

| Work Item | Files | PRD | Status |
|-----------|-------|-----|--------|
| **H1: Shared primitives** — custom context-menu component with submenus, keyboard shortcut registry with scoped dispatch, `?` help overlay, draggable / minimizable / restorable floating-card stack with tray | `src/r2g/ui/static/index.html` | P5e.1.1 – P5e.1.4 | Done |
| **H2: Right-click everywhere** — entity-specific menus on source tables, columns, target collections, properties, function circles, connectors, target edges, source FK lines, canvas blank space, sources sidebar, history rows; "View as" submenu on canvas | `src/r2g/ui/static/index.html` | P5e.2.1 – P5e.2.2 | Done |
| **H3: First-class edges** — floating edge editor (name / direction / from-to / fields / delete), Shift / Alt drag-to-add between target cards, promote join table to explicit edge, composite FK round-trip (`from_fields` / `to_fields`) | `src/r2g/ui/static/index.html`, `src/r2g/types.py` (`EdgeDefinition._accept_singular`, `_split_field_spec`, `_serialize`) | P5e.3.1 – P5e.3.4 | Done |
| **H4: Non-blocking execution** — floating progress card replaces the canvas-blocking overlay, SSE stream updates live counters, minimize-to-tray; collapsible bottom timeline strip of recent loads with pill pattern + right-click actions, shortcut `h` | `src/r2g/ui/static/index.html`, `src/r2g/ui/server.py` (SSE endpoint + progress callback wiring) | P5e.4.1 – P5e.4.2 | Done |
| **H5: Legend + lenses** — complete legend covering every node / edge / connector / function encoding; `currentLens` + `applyLens()` paint-only; Topology / Coverage / Validation / Diff lenses; header `lens-chip`; shortcuts `1`–`4` | `src/r2g/ui/static/index.html` | P5e.5.1 – P5e.5.3 | Done |
| **H6: Inline validation** — `POST /api/projects/{name}/validate-draft` tolerates parse errors (reports them as issues), debounced silent revalidation, per-entity badges + tooltip issue lists, validation lens reuses the same data | `src/r2g/ui/server.py`, `src/r2g/ui/static/index.html` | P5e.6.1 – P5e.6.3 | Done |
| **H7: A11y + hygiene** — `role="menu"` / `menuitem` / `region` / `complementary`, `aria-label`s, Esc pops the topmost overlay, empty-state copy points at right-click + `?` | `src/r2g/ui/static/index.html` | P5e.7.1 – P5e.7.3 | Done |
| **H8: In-UI catalog management** — "+ New source", "+ New target", "+ New project" floating-card forms; new Targets panel in the left sidebar; right-click "Remove source…" with cascade confirmation + dependency report; empty-state CTA copy on all three surfaces; auto-introspect after target create, auto-snapshot after source create | `src/r2g/ui/static/index.html` | P5e.8.1 – P5e.8.4 | Done |

### Invariants enforced by this phase

- **No new routes.** Every feature integrates into the existing `r2g ui` workspace. New detail editors are floating cards over the canvas, never page navigations.
- **Context-menu-primary.** For every entity type, at least one action lives on the right-click menu; toolbar / side-panel buttons are secondary paths.
- **Lens changes never relayout.** `applyLens()` is paint-only. Topology changes (add / remove / filter entities) are the only trigger for relayout; this split is documented in the legend.
- **Floating cards stack.** Open, minimize, restore, close operations are composable; restoring preserves prior coordinates and content.
- **Composite keys are a single concept.** UI always sends plural `from_fields` / `to_fields` when either side has more than one column; the backend normalizes comma-separated strings through `EdgeDefinition._split_field_spec` and round-trips them through `_serialize`.

### Verification

- All lint clean on `src/r2g/ui/static/index.html`, `src/r2g/ui/server.py`, `src/r2g/types.py`.
- `python -m pytest -q` → `673 passed, 6 skipped`.
- Manual smoke: save / validate / diff / load with both simple and composite foreign keys; context menus on every entity; lens switching does not retrigger force layout; progress card survives minimize / restore during an active load.

### Followups (explicitly deferred)

- **Expression editor polish.** Syntax highlighting and server-side preview (P5c.2.5) remain partial; the editor ships as a plain monospace textarea with engine selector and sources picker.
- **Drag-and-drop for node / edge promotion across target cards.** Current drop is Shift/Alt + mouse drag; pure HTML5 DnD parity with explorer items is a polish item.
- **KSQL / Python expression engines.** The in-UI editor accepts them but only the `aql` engine is evaluated in-process today (P5c.1.4 / P5c.1.6 for the supported subset). Per-batch AQL delegation for the remainder of AQL and translation layers for KSQL / Python are still TODO under P5c.1.5 / P5c.1.7 — unsupported expressions currently fall back to identity pass-through with a structured-log warning.

---

## Phase 6 — Snowflake integration (in progress)

This plan doc was originally scoped to the mapping/UI/catalog/re-ingest work (Phase 5). Phase 6 (Snowflake) was tracked directly in the PRD; the first slice has now landed and is summarized here for cross-reference.

### Slice 1 — Source abstraction + introspect-only Snowflake — **Done**

Goal: let the catalog and Mapping Studio *see* a Snowflake schema without committing to dump / streaming / FK-inference yet.

Shipped:

- **`src/r2g/connectors/base.py`** (new): `SourceConnector` Protocol (`connection_string`, `schema_name`, `get_schema() -> Schema`), `SUPPORTED_SOURCE_TYPES = ("postgresql", "snowflake")`, and `create_source_connector(source_type, connection_string, schema_name)` factory with lazy imports. `PostgresConnector` satisfies the protocol without modification (P6.5).
- **`src/r2g/connectors/snowflake.py`** (new): `SnowflakeConnector` reading `INFORMATION_SCHEMA.TABLES` / `INFORMATION_SCHEMA.COLUMNS` plus `SHOW PRIMARY KEYS` / `SHOW IMPORTED KEYS`. Connection strings use the Snowflake SQLAlchemy URL shape: `snowflake://user:pass@account/DATABASE[/SCHEMA]?warehouse=WH&role=R`. Missing `snowflake-connector-python` raises `ImportError` with a `pip install 'r2g[snowflake]'` hint; transient driver errors are wrapped as `RuntimeError` (P6.1, introspection only).
- **`src/r2g/config.py`**: extended `DEFAULT_TYPE_MAP` with `NUMBER`, `FIXED`, `FLOAT`/`DOUBLE`, `VARIANT`, `OBJECT`, `ARRAY`, `TIMESTAMP_LTZ/NTZ/TZ`, `BINARY`/`VARBINARY`, `GEOGRAPHY`/`GEOMETRY`, `VECTOR`. `pg_type_to_json_type` is now a shared source-agnostic mapper (P6.2).
- **`pyproject.toml`**: new `snowflake` optional-deps group (`snowflake-connector-python>=3.0.0`).
- **Dispatch wired everywhere that introspects**: `POST /api/sources/{name}/snapshot` (UI), `introspect_source_schema` (MCP), and `r2g source snapshot` (CLI) now call `create_source_connector(source.source_type, ...)`. UI surface responds `501` when the Snowflake extra is missing and `400` for unknown types.
- **`src/r2g/catalog.py`**: `add_source` enforces a known-types allowlist (`postgresql`, `snowflake`, `csv`, `kafka`) while the factory keeps a stricter allowlist for what we can actually instantiate.
- **UI**: "+ New source" dropdown now offers `PostgreSQL` and `Snowflake (introspect-only)`; connection-string hint explains both formats.
- **Tests**: `tests/test_connectors_base.py` (factory + Protocol conformance), `tests/test_snowflake_connector.py` (URL parsing, constructor semantics, full introspection against a fake driver, composite-FK ordering, missing-driver + driver-exception paths, type-map round-trip for VARIANT/TIMESTAMP/NUMBER), additional cases in `tests/test_ui_api.py::TestSourceEndpoints` for Snowflake acceptance and unsupported-type rejection. 785 pass, 6 skipped.

### Slice 2 — FK inference (P6.6) — **Done**

Goal: rescue schemas (Snowflake or legacy PG) that shipped without declared FKs, without silently inventing graph topology.

Shipped:

- **`src/r2g/fk_inference.py`** (new): pure-Python heuristic engine (`infer_foreign_keys`, `InferenceOptions`, `InferredForeignKey`) plus `PostgresValueSampler` for bounded `LEFT JOIN` overlap queries. Patterns covered: `{prefix}_id`, `{prefix}id` (penalised), `{prefix}_{pkcol}`, non-generic PK-name direct match, and a composite-PK pass that finds local tables carrying every column of a multi-column PK. Type compatibility is checked via the shared `pg_type_to_json_type` mapper (integer↔float compat so Snowflake NUMBER ↔ PG integer works). Sampler results can boost or veto a candidate (confidence ±) and are fully optional — the engine is safe to run on metadata alone.
- **`POST /api/sources/{name}/infer-fks`**: returns ranked candidates for the latest snapshot. Accepts `{ sample, sample_limit, min_confidence, veto_on_zero_overlap }`. Returns `{ source, snapshot_id, sample_used, candidates: [...] }`. Sampling is silently skipped on non-PostgreSQL sources with a logged note.
- **`GET /api/projects/{name}`**: new convenience endpoint so the UI can resolve the source name without scanning the list.
- **Mapping Studio UI**: new **Suggest FKs** toolbar button (shortcut `i`, canvas right-click menu entry) opens a floating card with per-row `Accept as edge` / `Dismiss` plus an `Accept all` action. Accepted candidates become real `EdgeDefinition`s with collision-proof naming (`<from>_to_<to>`, `_2`, `_3`, …) and mark the project dirty for save. Confidence is visualised with a green/yellow/pink pill and evidence strings are shown below each row.
- **CLI**: `r2g source infer-fks <name> [--sample] [--sample-limit N] [--min-confidence 0.4] [--accept]`. Prints a Rich table of candidates; `--accept` writes every candidate above the threshold back into the catalog as a new snapshot with merged FKs so downstream auto-map / mapping generation picks them up.
- **Tests**: `tests/test_fk_inference.py` (28 cases — name heuristic, composite, type-compat, nullability, sampler boost/veto/neutral/exception, edge-definition round-trip, parametric coverage, PG sampler plumbing) and 5 API cases in `tests/test_ui_api.py::TestInferFksEndpoint`. **819 tests passing, 34 new.**

### Slice 3 — Snowflake dump + streaming (P6.3 + P6.4) — **Done**

Goal: make the streaming pipeline and dump path source-agnostic so the same UI and CLI work against PostgreSQL and Snowflake without branching on type at every call site.

Shipped:

- **`src/r2g/connectors/session.py`** (new): `SourceSession` Protocol with `count_rows`, `stream_rows`, `dump_table_to_csv`, `close`. Structural so existing test doubles satisfy it without inheritance.
- **`src/r2g/connectors/base.py`**: `SourceConnector` Protocol gains `open_session() -> SourceSession`. Both connectors implement it.
- **`src/r2g/connectors/postgres.py`**: new `PostgresSession` owns one `REPEATABLE READ` autocommit=False connection, uses a named server-side cursor for `stream_rows`, and routes `dump_table_to_csv` through `COPY TO STDOUT WITH CSV HEADER` (the same fast path the legacy `r2g dump-tables` used, now source-agnostic).
- **`src/r2g/connectors/snowflake.py`**: new `SnowflakeSession` opens a `BEGIN`/`COMMIT` transaction for implicit snapshot isolation, streams rows via `cursor.fetchmany(batch_size)`, and writes CSV through Python's `csv` module (header row, empty-string NULLs, `"` quoting). Lazy driver import surfaces the same `pip install 'r2g[snowflake]'` hint as introspection.
- **`src/r2g/streaming/pipeline.py`**: fully rewritten to consume a `SourceConnector`. Single-worker path opens one session; parallel workers each open their own session for per-snapshot isolation. `pg_conn_string=…` constructor keyword is preserved as a backward-compat shim that builds a `PostgresConnector` automatically (every existing caller, including `ui/server.py`, `mcp_server.py`, and `selective_reload.py`, still works unchanged while new call sites pass `source_connector=…`).
- **`src/r2g/ui/server.py`**: `POST /api/projects/{name}/load` resolves the source through `create_source_connector(source_type, …)`; 501 on missing optional extras, 400 on unknown types.
- **`src/r2g/main.py`**: `r2g stream --source <name>` resolves a catalog source and drives the pipeline through the abstraction; `--pg-conn` still works as the legacy path. New `r2g source dump <name> [--tables a,b,c] [--output-dir ./dumps]` writes one CSV per table via `SourceSession.dump_table_to_csv` and works identically on PostgreSQL and Snowflake.
- **Tests**: `tests/test_source_sessions.py` (14 cases — Protocol conformance, Snowflake `count_rows`/`stream_rows`/`dump_table_to_csv`/`since`/`BEGIN`+`COMMIT` semantics/missing-driver/context-manager, PostgresSession smoke for `REPEATABLE READ` + close-drops-connection + connector wiring); `tests/test_streaming_pipeline.py` rewritten around a `FakeSourceConnector`/`FakeSession` pair (25 cases, including explicit PG-session `REPEATABLE READ` observation, `since` propagation, backward-compat shim, and error rejection); `tests/test_cli_source_dump.py` (5 cases — dumps every snapshot table, `--tables` filter, unknown source, missing Snowflake extra, `r2g stream --source` dispatch). **845 tests passing, 26 new.**

### Phase 6 close-out

Every Phase 6 task (P6.1–P6.6) is shipped. End-to-end verification against a live Snowflake warehouse remains a field-validation exercise; enabling `--sample` for FK inference on Snowflake is a clean follow-up (just add a `SnowflakeValueSampler` that opens a `SourceSession` instead of a raw `psycopg` connection).

---

## Style and Convention Notes

- Follow existing patterns: Pydantic models in `types.py` style, Typer commands in `main.py` style
- Use `structlog` for logging (via `r2g.log.get_logger`)
- Use `Rich` for CLI output (tables, progress bars)
- Tests use `pytest` with fixtures, mocks via `unittest.mock`
- Lint with `ruff` (config in `pyproject.toml`: E, F, I, W rules, 120 char lines)
- Do NOT add comments that just narrate what code does
- Do NOT create documentation files unless explicitly asked
