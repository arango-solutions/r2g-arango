# R2G-ETL Pipeline

*Product Requirements Document (PRD) -- Experimental Reference Implementation*

| Field | Value |
| :--- | :--- |
| **Product name** | R2G-ETL Pipeline (Relational to Graph -- Extract, Transform, Load) |
| **Version** | 0.1.0 (experimental) |
| **Date** | Originally drafted December 2025, consolidated April 2026 |
| **Status** | Phases 1--4 implemented and hardened; Phase 5b (Visual Mapper) and Phase 5c (Expression / Graph-of-Graphs UI) largely implemented and augmented by Phase 5e (UI Architecture Upgrade); Phases 5 (Temporal), 5d (ArangoDB-backed catalog), and 6--7 are planned or exploratory |
| **Target users** | Database architects, data engineers, and developers evaluating relational-to-graph migration with ArangoDB |

---

## 1. Goals and objectives

The primary goal of the R2G-ETL Pipeline is an experimental, configurable tool for transforming and loading data from relational schemas into ArangoDB graph schemas. It serves as a reference implementation demonstrating the mechanical mapping patterns. While PostgreSQL is the primary supported source, the architecture is designed to accommodate additional relational sources (see Phase 6: Snowflake integration).

### Key objectives

| Objective | Detail |
| :--- | :--- |
| **Automation** | Eliminate manual spreadsheet-based mapping and script generation for initial data migration. |
| **Flexibility** | Support multiple ingestion paths: flat files, direct connection, CDC (PostgreSQL logical replication), and Kafka (Debezium / custom producers). All implemented. |
| **Schema management** | Ingest PostgreSQL schema and maintain metadata for mapping to target ArangoDB graph topologies (property graph, labeled property graph). |
| **Scalability** | Use `arangoimport` for efficient, high-volume bulk loading. |
| **Synchronization** | Synchronize the relational system through live stream processing of delta changes via CDC (Phase 3) and Kafka (Phase 4), with configurable conflict resolution. |

---

## 2. Solution overview

The product is a multi-phased pipeline that reads relational schema, applies a configurable mapping, and loads data into ArangoDB via multiple paths: `arangoimport` scripts (file-based), HTTP API bulk streaming (direct connection), CDC logical replication (near real-time), and Kafka consumption (Debezium / custom producers).

### Core components

| Component | Function |
| :--- | :--- |
| **Schema reader** | Connects to PostgreSQL to read and parse schema metadata: tables, columns, primary keys, and foreign keys (including composite FKs). Supports any named schema via `--pg-schema`. |
| **Metadata store** | Persists the ingested PostgreSQL schema and the user-defined target ArangoDB ontology/schema as JSON and YAML files. |
| **Mapping engine** | Applies transformation logic: tables to document collections; foreign keys to edge collections (PK/FK values to `_from` / `_to` with collection prefixes). |
| **Data egress / import generator** | Generates executable bash import scripts. Supports two modes: JSONL-based (transforms CSV to intermediate JSONL) and CSV-direct (uses `arangoimport --type csv` with `--translate` and `--datatype` flags to import PG dumps without intermediate files). |
| **Mapping visualizer** | Generates self-contained HTML reports with an interactive D3.js force-directed graph showing the PG-to-ArangoDB mapping, relational schema cards, edge mapping details, and a mapping editor with YAML export. |
| **Streaming engine** | Reads from PostgreSQL using server-side cursors with REPEATABLE READ isolation and writes directly to ArangoDB via python-arango HTTP bulk import API, with configurable batch sizes. Supports `--dry-run` for pre-flight validation, `--drop-collections` for idempotent re-import, `--workers` for parallel streaming with per-worker connections, and retry with exponential backoff. Rich progress bars and throughput reporting. Topological import ordering via FK dependency analysis. `--since` incremental filtering. No intermediate files. |
| **Table dumper** | Connects to PostgreSQL and exports each table as a CSV file via `COPY ... TO STDOUT WITH CSV HEADER`, automating the manual dump step. |
| **CDC engine** | Near real-time PostgreSQL-to-ArangoDB sync via logical replication. `PGReplicationListener` manages replication slots and polls `pg_logical_slot_get_changes`. Parsers for `test_decoding` (built-in) and `wal2json` output plugins. `DeltaTransformer` converts row-level changes to graph mutations. `CDCHandler` orchestrates event processing with transaction grouping, stats tracking, and configurable conflict resolution (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`). |
| **Kafka consumer** | Consumes CDC events from Kafka topics via `confluent-kafka`. `DebeziumParser` handles Debezium JSON envelopes (including Kafka Connect wrappers and snapshot reads). `FlatJsonParser` for custom producers. At-least-once delivery with post-write offset commits. Reuses the CDC engine's handler and conflict resolution. Optional dependency (`pip install r2g[kafka]`). |
| **Schema diff / config migration** | `diff-schema` compares two schema snapshots (added/removed tables, column changes, FK changes). `migrate-config` auto-updates mapping YAML when the source schema evolves, preserving user customizations. |
| **Data validator** | `validate-data` checks FK referential integrity of dump files before import, detecting orphaned references that would create dangling edges. |

### Relational-to-graph mapping logic

The transformation is largely mechanical and can be described in three layers (these are **mapping** concerns, not the same as project Phases 1--4 in Section 3):

1. **Transliteration (structural mapping)**
   - Each relational **table** maps to an ArangoDB **document collection**.
   - The table **primary key** feeds document **`_key`** (or another agreed unique identifier).
   - Each **foreign key** relationship maps to an **edge collection**: traverse the dependent table and map PK/FK values to ArangoDB **`_from`** and **`_to`**, with correct collection prefixes.
   - Table columns become document properties, with appropriate JSON type conversion.

2. **Join tables**
   Join tables that implement many-to-many relationships in the relational model are modeled as **edges** in the graph. The two FK columns become the `_from` and `_to` endpoints.

3. **Normalization**
   Categorical attributes (e.g., country codes) may be normalized into dedicated **vertex collections** with connecting edges when richer category data or reuse across entities is required.

### Edge cases in mapping

The mechanical mapping handles several non-trivial patterns:

- **Self-referential FKs** (e.g., `employees.manager_id -> employees.id`): produces edges within the same vertex collection. This is correct graph modeling but may surprise users expecting separate collections.
- **Multiple FKs to the same table** (e.g., `orders.customer_id` and `orders.referrer_id` both referencing `customers`): each FK produces a separate edge collection, named `{source}_to_{target}` with a `_{fk_column}` suffix to disambiguate.
- **Nullable FKs**: rows where the FK value is NULL are silently skipped (no edge is created). This is intentional -- a NULL FK means "no relationship."
- **Tables with no primary key**: these cannot produce meaningful `_key` values. The tool warns during config validation and streaming; documents receive auto-generated `_key` values and edges referencing such tables are flagged.

The following patterns are **handled with caveats**:

- **Circular FK dependencies** (table A references B, B references A): detected by topological sort (Kahn's algorithm). The tool warns about cycles and proceeds with a best-effort ordering.
- **Inheritance patterns** (single-table inheritance, table-per-type): no special handling; each table is mapped independently.
- **Polymorphic associations**: not supported.

---

## 3. Project phases and requirements

The roadmap is organized into seven phases: four implemented (MVP through Kafka), two planned (temporal graph mode, Snowflake), and one exploratory (future sources and advanced features).

### Phase 1: Table dump file processing (MVP) -- Implemented

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P1.1** | **Schema ingestion** | Connect to PostgreSQL (credentials/URL) and read table, column, PK, and FK definitions. | Done |
| **P1.2** | **Metadata storage** | Store ingested PostgreSQL schema and user-defined target ArangoDB schema (ontology) as metadata. | Done |
| **P1.3** | **Dump file input** | Accept flat-file dumps (e.g., CSV, TSV, GZ) of individual PostgreSQL tables. | Done |
| **P1.4** | **Node transformation** | Transform dump rows into ArangoDB document form with type coercion for `arangoimport`. | Done |
| **P1.5** | **Edge transformation** | Build edge collections by cross-referencing PKs and FKs; map to `_from` and `_to` including collection prefixes. | Done |
| **P1.6** | **`arangoimport` script generation** | Emit executable shell scripts to run `arangoimport` for all generated document and edge files. | Done |
| **P1.7** | **CSV-direct import** | Generate `arangoimport --type csv` scripts that import PG CSV dumps directly using `--translate` for key remapping, `--datatype` for type coercion, and `--from-collection-prefix` / `--to-collection-prefix` for edge `_from`/`_to`. No intermediate JSONL step required. | Done |
| **P1.8** | **Mapping visualizer** | Interactive HTML visualization of the relational-to-graph mapping using D3.js force-directed graph layout, with relational schema cards and edge mapping detail views. | Done |

### Phase 2: Direct PostgreSQL connection and streaming -- Implemented

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P2.1** | **Direct read interface** | Establish direct, persistent connections to the live PostgreSQL database via psycopg server-side cursors. | P1.1 | Done |
| **P2.2** | **Batched data extraction** | Read data in controlled batches (configurable `--batch-size`, default 10,000) using named server-side cursors to bound memory use. | P2.1 | Done |
| **P2.3** | **Streaming import** | Stream transformed data to ArangoDB via the python-arango HTTP bulk import API (`import_bulk`) without intermediate files. | P2.2, P1.4, P1.5 | Done |
| **P2.4** | **Snapshotting logic** | Full initial load with REPEATABLE READ transaction isolation for consistent snapshot semantics. | P2.3 | Done |

### Phase 3: Change Data Capture (CDC) integration -- Complete

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P3.0** | **CDC event model** | Pydantic models for `ChangeEvent` (INSERT/UPDATE/DELETE with old/new row, LSN, timestamp, transaction ID), `ArangoDelta` (target mutation), and `TransactionBatch` (grouped deltas). | P1.4 | Done |
| **P3.0b** | **Delta transformer** | `DeltaTransformer` converts `ChangeEvent`s into `ArangoDelta`s using existing `NodeTransformer` and `EdgeTransformer`. Handles INSERT→insert, UPDATE→replace (document + edges), DELETE→delete (document + edge cleanup). | P3.0, P1.4, P1.5 | Done |
| **P3.0c** | **CDC handler** | `CDCHandler` orchestrates event consumption, transformation, and application. Supports single events, event streams, and transaction-grouped batches. Tracks stats (events, deltas, failures, LSN). | P3.0b | Done |
| **P3.0d** | **Single-document writer ops** | `ArangoWriter.insert_document`, `replace_document`, `delete_document`, and `apply_delta` methods with retry logic for CDC use. | P2.3 | Done |
| **P3.1** | **CDC listener** | `PGReplicationListener` manages logical replication slots via `pg_create_logical_replication_slot` / `pg_drop_replication_slot`. Polls changes via `pg_logical_slot_get_changes`. Supports `test_decoding` (built-in) and `wal2json` output plugins with dedicated parsers. Feeds parsed `ChangeEvent`s into `CDCHandler` grouped by transaction. CLI commands: `cdc-setup`, `cdc-teardown`, `cdc-status`, `cdc-start`. | P2.1, P3.0c | Done |
| **P3.2** | **Delta transformation** | Map captured changes to ArangoDB replace/insert/delete operations. | P1.4, P1.5 | Done (P3.0b) |
| **P3.3** | **Live stream processing** | `cdc-start` command runs a continuous polling loop with configurable `--poll-interval` and `--batch-size`. Graceful shutdown via SIGINT/SIGTERM. Session statistics displayed on exit. | P3.1, P2.3 | Done |
| **P3.4** | **Conflict resolution** | Configurable conflict policies for CDC delta application: `source_wins` (default, PG is truth — upsert on duplicate, insert on missing), `last_write_wins` (LSN comparison, reject stale writes via `_r2g_lsn` field), `log_and_skip` (log conflicts, skip writes), `fail` (raise on any conflict). `ConflictResolver` wraps write operations, detects conflict types (INSERT_DUPLICATE, REPLACE_MISSING, DELETE_MISSING, STALE_OVERWRITE, ORPHAN_EDGE), and resolves per policy. `ConflictLog` accumulates conflict events with per-type counts and session summary. Integrated into `CDCHandler._apply_delta` and `cdc-start --conflict-policy` CLI option. | P3.3 | Done |

### Phase 4: Kafka integration -- Complete

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P4.1** | **Kafka producer/connector** | Connect to an external CDC pipeline (e.g., Debezium) that streams PostgreSQL changes to Kafka topics. R2G consumes from the Kafka side; Debezium connector setup is external. | P3.1 | Done (external) |
| **P4.2** | **Kafka consumer** | `KafkaConsumer` wraps `confluent-kafka`, subscribes to topics, polls in batches, commits offsets after successful processing (at-least-once semantics). Graceful shutdown via SIGINT/SIGTERM. Optional dependency via `pip install r2g[kafka]`. | P4.1 | Done |
| **P4.3** | **Kafka message transformation** | `DebeziumParser` parses Debezium JSON envelope (`before`/`after`/`op`/`source`) including Kafka Connect `payload` wrapper, snapshot reads (`op: r`). `FlatJsonParser` for custom producers. Both produce `ChangeEvent` objects fed into existing `CDCHandler`. | P4.2, P3.2 | Done |
| **P4.4** | **Transactional ordering** | Messages consumed in Kafka partition order. Events grouped by `transaction_id` (from Debezium `source.txId`) and applied through `CDCHandler.handle_transaction` for ordered delta application. Conflict resolution policies apply. | P4.3 | Done |

### Phase 5: Temporal graph mode -- Planned

CDC and Kafka pipelines currently apply changes as direct replaces/deletes. Temporal graph mode adds an alternative write strategy using the **immutable-proxy time travel pattern** (ProxyIn / Entity / ProxyOut), enabling full version history, point-in-time queries, and soft deletes with automatic TTL-based garbage collection.

#### Architecture

Stable identity (proxies) is separated from mutable state (versioned entities). Topology edges attach to proxies and are never rewritten when entities change.

```
Topology edges --> EntityProxyIn --hasVersion--> Entity v0 (current: expired=NEVER)
                   (stable _key)  --hasVersion--> Entity v1 (historical: expired=T)
                   EntityProxyOut <-- Entity     (outbound version link)
```

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P5.1** | **Temporal write strategy** | `--temporal` flag on `cdc-start` and `kafka-start` that switches the delta application from direct replace/delete to versioned writes. CDC INSERT creates ProxyIn + ProxyOut + Entity v0 (with `created=now`, `expired=NEVER_EXPIRES`). CDC UPDATE expires the current Entity (`expired=now`) and inserts a new version (`created=now`, `expired=NEVER_EXPIRES`). CDC DELETE soft-deletes by setting `expired=now` on the current Entity; proxies and topology edges are preserved. | P3.4, P4.4 |
| **P5.2** | **Proxy collection management** | Auto-create `{Collection}ProxyIn` and `{Collection}ProxyOut` document collections alongside each mapped entity collection. Proxies carry only the shard key attribute (for SmartGraph compatibility) and a stable `_key`. | P5.1 |
| **P5.3** | **hasVersion edge collection** | Auto-create `hasVersion` edge collection with bidirectional edges: `ProxyIn -> Entity` (inbound) and `Entity -> ProxyOut` (outbound). Edges carry `created` and `expired` timestamps matching their entity version. | P5.2 |
| **P5.4** | **Interval semantics** | Every versioned entity and version edge carries `created` (float, unix timestamp) and `expired` (float, unix timestamp or sentinel `sys.maxsize = 9223372036854775807` for current). Current entities: `expired == NEVER_EXPIRES`. Historical entities: `expired` is a finite timestamp. | P5.1 |
| **P5.5** | **TTL aging** | Automatic garbage collection of historical versions via TTL indexes. Only documents with `expired != NEVER_EXPIRES` receive a `ttlExpireAt` field (`expired + ttl_retain_seconds`). TTL index is `sparse: true` to skip current documents. Configurable retention period via `--ttl-seconds` (default: 30 days). Static reference data and proxy collections are excluded from TTL. | P5.4 |
| **P5.6** | **MDI-prefixed temporal indexes** | Create `mdi-prefixed` indexes on `[created, expired]` for all versioned entity and hasVersion edge collections. Accelerates point-in-time snapshot queries and interval intersection queries. Verify usage via `zkd` index type in query execution plans. | P5.4 |
| **P5.7** | **Point-in-time query templates** | Emit AQL query templates for common temporal operations: snapshot at time T (`created <= @t AND expired > @t`), version history traversal (ProxyIn -> hasVersion -> Entity, sorted by `created DESC`), temporal overlap/interval intersection (`created <= @end AND expired >= @start`), and "what changed between T1 and T2". | P5.6 |
| **P5.8** | **SmartGraph compatibility** | Key structure supports SmartGraph shard key prefixes (`{shardKey}:{entityType}{index}` for proxies, `{shardKey}:{entityType}{index}-{version}` for entities). Optional `--smart-field` parameter for multi-tenant isolation. Satellite collections for shared taxonomy/classification data. | P5.2 |

#### Temporal-specific considerations

- **Write amplification.** Each source UPDATE produces 3-5 ArangoDB writes (expire old entity + insert new entity + 2 hasVersion edges + optional classification edges). Size RocksDB write buffers and monitor compaction accordingly.
- **Storage growth.** Without TTL, historical versions accumulate indefinitely. Monitor collection document counts; unbounded growth indicates TTL misconfiguration.
- **Conflict resolution interaction.** `last_write_wins` is recommended for temporal mode to prevent out-of-order events from creating phantom versions. A replayed INSERT should not create a duplicate entity version.
- **Topology edge stability.** Topology edges (connections, associations, locations) attach to proxies, NOT to versioned entities. This is the key invariant -- relationships survive entity versioning without being rewritten.
- **DELETE semantics.** CDC DELETEs become soft deletes (set `expired=now`). The entity remains queryable at any historical point in time. Physical removal is handled exclusively by TTL.

---

## 4. Technical requirements

| Category | Requirement | Details |
| :--- | :--- | :--- |
| **Architecture** | Modularity | Design so data sources can be swapped (e.g., PostgreSQL replaced by Snowflake or MySQL) without rewriting the whole tool. Currently PostgreSQL-only; Snowflake planned (Phase 6). |
| **Target DB** | ArangoDB | Load via `arangoimport` (file-based, Phase 1) and the ArangoDB HTTP API (streaming/CDC/Kafka, Phases 2--4). |
| **Transformation** | Schema mapping | Configurable prefix mapping for `_from` and `_to` (e.g., `user_1` to `Users/1`). |
| **Data integrity** | Key generation | Correct document `_key` values derived from source primary keys, including composite keys joined by a configurable separator. |
| **Technology stack** | Python | Chosen for ecosystem support (psycopg, python-arango, Polars, Pydantic, structlog, confluent-kafka, python-dotenv). |

### Known constraints

- **Referential integrity is opt-in**: the `validate-data` command checks FK values against PK sets from dump files, but this check is not enforced automatically during import. Orphaned references will still produce edges pointing to non-existent vertices if validation is skipped.
- **Bulk load idempotency**: re-running the streaming pipeline with `--drop-collections` replaces all data. For incremental updates, CDC and Kafka pipelines provide configurable conflict resolution (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`) with at-least-once delivery semantics.
- **Credential handling**: connection parameters can be loaded from `.env` files or environment variables (`PG_CONN`, `ARANGO_ENDPOINT`, etc.), but generated import scripts still contain connection defaults. No integrated secrets management (e.g., HashiCorp Vault).

### Phase 6: Snowflake integration -- Planned

Snowflake is a common data warehouse among R2G users. This phase adds Snowflake as a source alongside PostgreSQL, reusing the existing mapping, transformation, and loading infrastructure.

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P6.1** | **Snowflake schema reader** | Connect to Snowflake via the Snowflake Connector for Python (`snowflake-connector-python`) and introspect `INFORMATION_SCHEMA` to extract tables, columns, primary keys, and foreign key constraints (imported/inferred). Output the same `Schema` model used by PostgreSQL. | P1.1 |
| **P6.2** | **Snowflake type mapping** | Map Snowflake data types (`NUMBER`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP_*`, `VARIANT`, `ARRAY`, `OBJECT`, `GEOGRAPHY`, `GEOMETRY`, etc.) to JSON types. `VARIANT`/`OBJECT` map to JSON objects; `ARRAY` maps to JSON arrays. Extend `DEFAULT_TYPE_MAP` with Snowflake-specific entries. | P1.4 |
| **P6.3** | **Snowflake dump export** | `dump-tables` command variant that uses `COPY INTO @stage` or cursor-based extraction to export Snowflake tables as CSV files. Handle Snowflake-specific CSV quoting and NULL representation. | P6.1 |
| **P6.4** | **Snowflake streaming** | `stream` command variant that reads from Snowflake using the Python connector's cursor (Snowflake does not support server-side cursors like PostgreSQL, but supports `fetch_pandas_all()` / `fetch_arrow_all()` for batched reads). Reuse the ArangoDB writer path. Snowflake's `RESULT_SCAN` or warehouse-level snapshot isolation provides read consistency. | P6.1, P2.3 |
| **P6.5** | **Source abstraction layer** | Refactor the schema reader and streaming pipeline behind a `SourceConnector` protocol/ABC so PostgreSQL and Snowflake (and future sources) share a common interface. CLI commands accept `--source-type pg|snowflake` or auto-detect from connection string format. | P6.1, P6.4 |
| **P6.6** | **Snowflake FK inference** | Snowflake does not enforce foreign key constraints (they are informational only and often absent). Provide a `--infer-fks` option that analyzes column naming conventions (e.g., `user_id` matching `users.id`) and value overlap to suggest FK relationships. Require user confirmation via the mapping config. | P6.1 |

#### Snowflake-specific considerations

- **FK constraints are not enforced in Snowflake.** They can be declared but are informational only. Many Snowflake schemas have no FK metadata at all. The FK inference feature (P6.6) addresses this gap.
- **Semi-structured data.** Snowflake `VARIANT`, `OBJECT`, and `ARRAY` columns can contain nested JSON. These should be preserved as nested structures in ArangoDB documents rather than flattened.
- **Large tables.** Snowflake tables can be very large. The streaming path should support `LIMIT`/`OFFSET` pagination or warehouse-level result caching to manage memory. Arrow-based fetching (`fetch_arrow_all()`) provides the best throughput for large result sets.
- **Authentication.** Snowflake supports multiple auth methods (user/password, key-pair, SSO/OAuth, external browser). The connector should accept standard Snowflake connection parameters: `account`, `user`, `password`, `warehouse`, `database`, `schema`, `role`. These should be loadable from env vars (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.) and `.env` files.
- **Cost implications.** Every query against Snowflake consumes warehouse credits. The schema reader and streaming pipeline should minimize the number of queries. `--dry-run` should clearly report query cost implications.

### Phase 5b: Visual Graph Data Mapper & Ingestion Engine -- Planned

The existing Mapping Studio UI (`r2g ui`) provides basic project selection, graph visualization, and YAML export. Phase 5b evolves it into a full-featured visual mapping and ingestion tool, inspired by TigerGraph GraphStudio. This phase is defined by three epics: Data Catalog, Visual Mapping Interface, and Ingestion & Execution Engine.

#### Target personas

- **Data Engineer:** Needs to set up reliable, high-throughput pipelines from relational/flat sources to the graph database.
- **Graph Architect / Data Modeler:** Needs to ensure that the source data correctly maps to the target ontology (vertices, edges, properties) without writing complex scripts.

#### Product principles

- **Object-centricity:** Users interact with visual representations of data structures (tables, streams, vertices, edges) rather than text lists or code.
- **Intelligent automation:** The system does the heavy lifting where possible -- introspecting schemas upon connection and generating intelligent default mappings.
- **Referential integrity:** The system maintains strict internal consistency. Deleting a foundational object (like a source connection) cleanly cascades to dependent objects (mappings, projects, load history).

#### Epic 1: Data Catalog Interface

The Data Catalog is the central repository for all incoming data sources and target database connections.

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5b.1.1** | **Data source CRUD** | Users can Create, Read, Update, and Delete data sources. Supported source types: CSV directory, RDBMS (PostgreSQL; MySQL, Oracle planned), Kafka topics. | -- | Done for PostgreSQL (CLI, API, and in-UI "+ New source" form + right-click remove, see P5e.8.1); CSV / Kafka source registration deferred |
| **P5b.1.2** | **Automated schema introspection** | Upon saving a new data source, the system automatically introspects the source. CSV: parse headers and infer types. RDBMS: extract tables, columns, PKs, FKs. Kafka: extract schema from Schema Registry or parse sample payload. Display as hierarchical tree or entity cards. | P5b.1.1 | Done for PostgreSQL (`POST /api/sources/{name}/snapshot`, auto-triggered after in-UI source creation and exposed as right-click "Re-introspect"); CSV / Kafka introspection deferred |
| **P5b.1.3** | **Target graph definition** | Users define connections to target graph databases. System introspects the target graph to fetch existing vertex types, edge types, and their properties. | -- | Done (`TargetConfig` in catalog, `src/r2g/connectors/arango_reader.py`, `GET/POST/DELETE /api/targets`, `POST /api/targets/{name}/introspect`, and in-UI "+ New target" form + right-click actions, see P5e.8.2) |
| **P5b.1.4** | **Referential integrity & cascading deletes** | Deleting a data source warns the user of dependent mappings/projects. Upon confirmation, all associated mappings and load history are deleted. | P5b.1.1 | Done (`CatalogManager.remove_source(cascade=True)` + `DependencyError`; `DELETE /api/sources/{name}?cascade=true`; in-UI right-click "Remove source…" shows a confirmation dialog before cascading) |

#### Epic 2: Visual Mapping Interface

The core workspace where users define how source data populates the graph.

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5b.2.1** | **Split-screen object-centric UI** | Left pane: introspected source schema (table cards with columns). Right pane: visual graph schema of the target database (vertices and edges). | P5b.1.2 | Done (persistent three-zone layout: sources sidebar, center canvas with source / mapping / target graphs, floating and overlay property surfaces; see Phase 5e) |
| **P5b.2.2** | **Mapping management (CRUD)** | Users can create, read, update, and delete "Map Objects" that save the state of source-to-target connections. Maps have metadata: name, description, source ID, target ID, last modified. | P5b.2.1 | Partial (save / load via `PUT /api/projects/{name}/mapping`; per-map metadata and multi-map management pending, see Phase 5d) |
| **P5b.2.3** | **Parallel connection configuration** | Within the mapping interface, users can specify ingestion parallelism: number of parallel connections/threads for reading from the source (`fetch_size`, partition strategies for RDBMS, consumer group concurrency for Kafka). | P5b.2.1 | Done (Settings modal exposes `workers`, `batch_size`, `on_duplicate`, `drop_collections`; wired into the Load payload) |
| **P5b.2.4** | **Automated default mapping** | When source and target are selected, generate a default mapping via heuristics (column name matching to vertex/edge property names, PK → Vertex ID). Render as visual connectors. | P5b.1.2 | Done (`POST /api/projects/{name}/auto-map` + toolbar "Auto-Map" action, shortcut `a`) |
| **P5b.2.5** | **Mapping customization** | Users can drag-and-drop to draw new connections between source columns and target properties. Select existing mapping lines to delete or edit. Map a single source table to multiple vertex types or edge types. | P5b.2.4 | Done (drag from source column dot to target property dot; Shift / Alt drag between target cards to add edges; right-click context menus on every connector, column, property, FK line, and edge; promote join table to explicit edge) |

#### Epic 3: Ingestion & Execution Engine

The mechanics of moving data based on the accepted map.

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5b.3.1** | **Execution trigger ("Load" button)** | Once a mapping is saved, a "Load" / "Run" button provisions the ingestion job based on parallel connection settings and the active map. | P5b.2.2 | Done (`POST /api/projects/{name}/load` runs the streaming pipeline in a background thread with load id tracking; toolbar "Load" action + confirmation modal; shortcut Ctrl/Cmd+Enter) |
| **P5b.3.2** | **Job monitoring** | Status indicator (Pending, Running, Success, Failed). Metrics: rows processed, vertices created, edges created, error count. | P5b.3.1 | Done (`GET /api/projects/{name}/load/{load_id}/status` + Server-Sent Events stream at `/stream`; `progress_callback` in `StreamingPipeline`; floating progress card in the UI with live per-table counters, error count, elapsed time, minimize-to-tray; bottom timeline strip records completed runs) |
| **P5b.3.3** | **Error handling** | Records that fail mapping constraints (missing Vertex IDs, type mismatch) are routed to a dead-letter queue or error log without stopping the ingestion job. | P5b.3.1 | Done (`src/r2g/dlq.py` `DeadLetterQueue` writes `~/.r2g/dlq/{load_id}.jsonl`; `GET /api/projects/{name}/load/{load_id}/errors` exposes entries; streaming pipeline continues past per-record failures and records them to the DLQ) |

#### Non-functional requirements (Phase 5b)

| Category | Requirement |
| :--- | :--- |
| **Performance** | UI must render schemas with up to 500 tables/vertices without noticeable lag. Ingestion must support parallel data streams for TB-scale data. |
| **Scalability** | Backend ingestion engine should be decoupled from the UI, ideally supporting distributed workers (e.g., Kubernetes). |
| **Security** | Passwords and tokens for data sources and target graphs must be encrypted at rest (AES-256 or OS keychain). |

#### Out of scope (V1 of Phase 5b)

- **Bi-directional sync:** Strictly source-to-graph ingestion, not graph-to-relational export.
- **Scheduling:** Cron jobs for recurring loads are out of scope; all ingestion is manually triggered via the "Load" button.

### Phase 5c: Expression Mapping & Graph-of-Graphs UI -- Planned

Phase 5b delivers a card-based split-screen mapper with 1:1 pass-through mappings. Phase 5c evolves the mapper into a **three-graph workspace** (source graph, mapping graph, target graph) and introduces a first-class **expression engine** so users can transform values during ingestion, not just rename them.

#### Conceptual model

Every target property is produced by a **mapping function** that takes one or more source properties as inputs and emits a single value. The function body is an expression string in a supported engine (AQL inline expressions for bulk load; KSQL or equivalent streaming SQL for streaming loads). The default function is identity (pass-through of a single source property). Fan-in is supported natively (multiple source columns flowing into one function).

```
source.first_name ─┐
                   ├─► function: CONCAT(@first_name, " ", @last_name) ─► Person.fullName
source.last_name  ─┘
```

#### Visual workspace

Three vertically-aligned graphs, left to right:

- **Source graph (left):** Force-laid-out entity-relationship diagram of the source tables. Tables are rectangular nodes; foreign-key relationships are directed edges between tables. Each table expands on click to reveal its columns with connector ports.
- **Mapping graph (center):** Mapping function nodes rendered as circles on the connector lines. Each function node shows its target-property name; clicking it opens an expression editor.
- **Target graph (right):** Force-laid-out graph model of the ArangoDB target. Vertex collections are nodes; edge collections are labeled, directed edges between vertex nodes. Each vertex collection expands to show its property list with connector ports.

#### Expression engines

| Load path | Engine | Execution | Notes |
| :--- | :--- | :--- | :--- |
| `arangoimport` bulk | **AQL inline expressions** | Per-row `LET` expressions evaluated via `arangoimport --auto-upgrade` + `--overwrite` with a transform query; or pre-transformed in-memory before writing JSONL. | Expression references source columns as `@col_name`. Supports most AQL string/number/date/array functions. |
| Streaming pipeline | **AQL via transform step** | Python-side evaluation using a minimal AQL subset (CONCAT, SUBSTRING, UPPER/LOWER, arithmetic) or delegation to ArangoDB via a per-batch `RETURN` query. | Same expression language surface as bulk for portability. |
| Kafka / CDC streaming | **KSQL (or ksqlDB-compatible SQL)** | Applied in the Kafka pipeline before write, supporting time-windowed joins and stateful transforms. | Deferred to later revision of P5c. Placeholder grammar-compatible with AQL where possible. |

#### Epic 1: Expression-aware data model

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5c.1.1** | **FieldExpression model** | New Pydantic `FieldExpression` type with fields: `target` (str), `sources` (list[str], fan-in), `expression` (str; empty = identity on `sources[0]`), `engine` ("aql" \| "ksql" \| "python"), `description` (str). | -- | Done |
| **P5c.1.2** | **CollectionMapping.field_expressions** | `CollectionMapping` gains an optional `field_expressions: list[FieldExpression]`. When non-empty it takes precedence over the legacy `field_mappings` dict for any target property it owns; otherwise `field_mappings` fallback applies. | P5c.1.1 | Done |
| **P5c.1.3** | **Serialization** | `FieldExpression` round-trips through YAML and JSON using standard Pydantic v2 serializers. Legacy mapping configs (no `field_expressions`) load without change. | P5c.1.2 | Done |
| **P5c.1.4** | **Expression evaluator (AQL subset)** | A Python-side evaluator that executes a safe subset of AQL string/number/date/array functions (`CONCAT`, `UPPER`, `LOWER`, `SUBSTRING`, `LENGTH`, `LTRIM`, `RTRIM`, `TO_STRING`, arithmetic, comparison, `NULL` handling) used by the streaming pipeline. Unsupported expressions fall back to delegation. | P5c.1.1 | Not started |
| **P5c.1.5** | **AQL delegation for complex expressions** | For expressions outside the Python evaluator's subset, the streaming pipeline submits a per-batch AQL `FOR doc IN @@batch LET ... RETURN doc` query to ArangoDB and uses the rewritten result as the ingestion payload. | P5c.1.4 | Not started |
| **P5c.1.6** | **arangoimport pre-transformation** | For bulk load, the JSONL generator applies expressions in-memory (reusing the Python evaluator + AQL delegation path) so `arangoimport` only sees the final document shape. No changes to `arangoimport` invocation required. | P5c.1.4 | Not started |
| **P5c.1.7** | **KSQL translation layer** | For Kafka/streaming loads, a translator that rewrites the canonical AQL-flavoured expressions into KSQL (ksqlDB-compatible) `SELECT` projections. Initial scope: arithmetic, string concat, CASE. | P5c.1.3 | Not started |

#### Epic 2: Graph-of-Graphs UI

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5c.2.1** | **Source ER-graph visualization** | Left pane renders source tables as graph nodes with FK relationships drawn as directed edges between tables. Click to expand a table inline, exposing its columns with connector ports on the right edge. | P5b.2.1 | Partial (split-screen exists; inter-table FK edges in-pane added) |
| **P5c.2.2** | **Target graph-model visualization** | Right pane renders target vertex collections as graph nodes and edge collections as labeled directed edges between them. Click to expand a vertex to show its properties with connector ports on the left edge. | P5b.2.1 | Partial (split-screen exists; inter-collection edges in-pane added) |
| **P5c.2.3** | **Mapping function nodes** | Each connector line carries a circular function node in the center canvas. The node labels its target property; hovering shows the expression preview; clicking opens an expression editor modal. Default (identity) functions render as small hollow circles; non-identity as filled circles in a distinct colour. | P5c.1.1 | Done |
| **P5c.2.4** | **Fan-in via drag-and-drop** | Users drag a source column connector dot onto an existing function circle to add that column as another input (multi-input fan-in). The function's `sources` list is updated in the mapping config. | P5c.2.3 | Done |
| **P5c.2.5** | **Expression editor** | Modal editor with engine selector (AQL / KSQL), syntax-highlighted textarea, available-sources picker (columns already bound to this function), optional description field, and a live preview button that evaluates the expression against one source row. | P5c.2.3 | Partial (editor modal + engine selector + sources picker + plain textarea + reset-to-identity; syntax highlighting and server-side preview deferred) |
| **P5c.2.6** | **Graph-of-graphs consistency** | All three graphs (source, mapping, target) stay aligned during scroll, window resize, and layout simulation ticks. Connectors reroute when either source or target nodes move. | P5c.2.1, P5c.2.2, P5c.2.3 | Done |
| **P5c.2.7** | **Expression validation feedback** | When a user closes the editor, the mapping is validated: source references must exist, the expression must parse under the selected engine, and the target property must not conflict with another mapping. Errors shown inline in the properties panel. | P5c.2.5 | Done (inline validation via `POST /api/projects/{name}/validate-draft` on a debounce; per-entity validation badges and a validation lens shared with Phase 5e) |

#### Out of scope (V1 of Phase 5c)

- **User-defined functions (UDFs):** Custom AQL functions registered on the server are not exposed to the expression editor in V1.
- **Expression autocomplete:** The editor is a plain textarea in V1; autocomplete with function signatures is deferred.
- **Full KSQL feature parity:** V1 supports a minimal KSQL subset (string, numeric, date, CASE); windowed joins and stateful aggregations are deferred.

### Phase 5d: ArangoDB-backed data catalog -- Planned

The catalog currently persists to `~/.r2g/catalog.json` and mapping configs to YAML files. Phase 5d migrates all catalog state (sources, snapshots, targets, projects, mapping configs, mapping expressions, load history) into an ArangoDB database so the catalog itself is a graph and can be queried, versioned, and shared across users.

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P5d.1** | **Catalog schema** | ArangoDB collections: `r2g_sources`, `r2g_targets`, `r2g_snapshots`, `r2g_projects`, `r2g_mappings`, `r2g_loads`. Edge collections: `r2g_snapshot_of` (snapshot -> source), `r2g_project_uses` (project -> source/target/snapshot/mapping), `r2g_load_of` (load record -> project). A named graph `r2g_catalog` ties it all together. | P5b.1.1 |
| **P5d.2** | **CatalogManager backend swap** | `CatalogManager` gains a pluggable persistence layer: `FileCatalogBackend` (current behaviour) and `ArangoCatalogBackend` (new). Selectable via `R2G_CATALOG_BACKEND=arango` env var or `r2g catalog use arango --endpoint ... --database r2g_meta`. Identical Python API so the UI and CLI are unchanged. | P5d.1 |
| **P5d.3** | **Catalog initialization** | `r2g catalog init` creates the catalog database, collections, indexes, and graph. Idempotent. Supports a migration path from `~/.r2g/catalog.json`: `r2g catalog migrate --from-file ~/.r2g/catalog.json`. | P5d.2 |
| **P5d.4** | **Catalog introspection UI** | Mapping Studio gains a "Catalog" view (read-only) rendering the catalog graph itself (sources, projects, mappings) using the same graph visualization primitives as the source and target panes. | P5d.2 |
| **P5d.5** | **Multi-user concurrency** | Optimistic concurrency via `_rev`: mapping save compares the cached revision and rejects on mismatch with a merge prompt in the UI. | P5d.2 |

#### Phase 5d non-functional notes

- The catalog database is a separate ArangoDB instance (or database within an instance) from the data target; they must not be conflated.
- Connection strings and credentials stored in the catalog are encrypted at rest using either the OS keychain (via `keyring`) or an AES-256-GCM key material bound to the user account.
- A lightweight "zero-config" mode continues to use the filesystem backend for single-user local development.

### Phase 5e: Mapping UI architecture upgrade -- Implemented

Phases 5b and 5c delivered the functional split-screen visual mapper and expression nodes. Phase 5e realigns the workspace with the object-centric UI contract (`/.cursor/rules/ui-architecture.mdc`): one persistent stage, context-menu-primary interactions, lenses that only repaint, floating non-blocking work surfaces, first-class edges, legible legend, and inline validation. It is additive on top of 5b / 5c.

#### Product principles (reaffirmed)

- **Single stage, no new routes.** The Mapping Studio (`r2g ui`) is the only workspace; every feature integrates into it.
- **Context-over-navigation.** Every entity (source table, column, target collection, property, function circle, connector, edge, FK line, canvas blank space, sources sidebar item, history row) exposes its actions through right-click context menus; side-panel buttons are secondary paths.
- **Overlay, not replacement.** Detail editors (expression editor, edge editor, progress view, validation and help modals) float over the canvas with drag / minimize / dismiss.
- **Lenses repaint, never relayout.** Topology, Coverage, Validation, and Diff lenses change only colors / badges / opacities on the stable graph.
- **Legible encoding.** A persistent legend documents every node, edge, connector, and function visual encoding per lens.

#### Epic 1: Shared primitives

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.1.1** | **Context-menu component** | Vanilla-JS `openContextMenu` / `closeContextMenu` primitive with submenu support, click-outside / Esc dismissal, `role="menu"` / `role="menuitem"` ARIA semantics, and per-entity menu builders. | Done |
| **P5e.1.2** | **Keyboard shortcut registry** | `registerShortcut(combo, description, scope, handler)` with modifier normalization, scoped dispatch (global / canvas / modal), and an introspectable registry that feeds the help overlay. | Done |
| **P5e.1.3** | **Help overlay** | `?` opens a modal listing every registered shortcut and the right-click contract. Toolbar `?` button and shortcut `?` / `Shift+?`. | Done |
| **P5e.1.4** | **Floating card primitive** | `openFloatingCard` / `minimizeFloatingCard` / `restoreFloatingCard` / `closeFloatingCard` with draggable headers, minimized tray, and restore-on-reopen. Used by progress, edge editor, expression editor, diff, validation detail. | Done |

#### Epic 2: Right-click everywhere

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.2.1** | **Entity-specific context menus** | Menus bound to source tables, source columns, target collections, target properties, mapping function circles, connectors, target edges, source FK lines, canvas blank space, sources sidebar items, and history rows. Each menu surfaces only the legal operations for that entity (edit, delete, reverse, promote, include / exclude, copy id, etc.). | Done |
| **P5e.2.2** | **View-as submenu on canvas** | Canvas right-click exposes a "View as" submenu that switches lens (Topology, Coverage, Validation, Diff); same action is available via keyboard accelerators `1`–`4`. | Done |

#### Epic 3: First-class edges

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.3.1** | **Floating edge editor** | Dedicated floating card to edit an edge's name, direction, from / to collections, from / to fields (including composite keys), and delete. Same surface as node editors. | Done |
| **P5e.3.2** | **Drag-to-add edges** | Shift / Alt drag between two target-vertex cards creates a new edge. Visual feedback during drag; drop target highlights. | Done |
| **P5e.3.3** | **Promote join table** | Right-click a join table offers "Promote to edge" which converts the join-table mapping into an explicit edge definition and removes the vertex mapping. | Done |
| **P5e.3.4** | **Composite keys** | `EdgeDefinition` round-trips both singular (`from_field` / `to_field`) and plural (`from_fields` / `to_fields`) forms; the UI emits plural when any side has more than one column, and the backend normalizes comma-separated strings into lists. | Done |

#### Epic 4: Non-blocking execution

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.4.1** | **Floating progress card** | Replaces the canvas-blocking overlay. Live SSE counters stream into a minimizable floating card so the user can keep mapping while loads run. Click a minimized card in the tray to restore. | Done |
| **P5e.4.2** | **Bottom timeline strip** | Collapsible bottom strip listing recent load runs as pills with summary stats; right-click a pill for actions (view report, copy run id, open visualization, re-load). Toggle with shortcut `h`. | Done |

#### Epic 5: Legend and lenses

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.5.1** | **Complete legend** | Collapsible legend documenting every node, edge, connector, and mapping-function visual encoding (pass-through vs expression, engine badges, edge-collection arrows, FK lines, dirty / validation badges, active-lens cues). Anchored near the canvas; visible at all times unless explicitly collapsed. | Done |
| **P5e.5.2** | **Lens infrastructure** | `currentLens` state + `applyLens()` paint-only step. Lens-change never reruns force layout. Header status chip (`#lens-chip`) shows the active lens. | Done |
| **P5e.5.3** | **Lens implementations** | Topology (default), Coverage (colors nodes / connectors by mapping completeness), Validation (colors by issue severity and pins tooltip lists), Diff (highlights changed entities vs the saved mapping). | Done |

#### Epic 6: Inline validation

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.6.1** | **Silent draft validation endpoint** | `POST /api/projects/{name}/validate-draft` accepts a draft `MappingConfig` (unsaved), returns bucketed issues, and tolerates parse errors (reported as validation issues rather than 500s). | Done |
| **P5e.6.2** | **Debounced revalidation** | Mapping edits mark the state dirty; a debounced request revalidates silently; responses update per-entity badges and tooltip issue lists. No user action required. | Done |
| **P5e.6.3** | **Validation badges** | Per-entity badges rendered on source tables, target collections, edges, and connectors; colored by severity; clicking a badge opens a floating validation detail card. | Done |

#### Epic 7: A11y + hygiene

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.7.1** | **ARIA roles** | Context menu uses `role="menu"` and `role="menuitem"`; bottom strip uses `role="region"`; floating-card tray uses `role="complementary"`; key buttons carry `aria-label`s. | Done |
| **P5e.7.2** | **Esc closes topmost overlay** | Global Esc handler pops the top of the overlay stack (context menu, floating card, modal) in order. | Done |
| **P5e.7.3** | **Discoverability copy** | Empty-state copy on the canvas and properties panel points the user at right-click and the `?` help overlay; status strings on the strip describe the right-click contract. | Done |

#### Epic 8: In-UI catalog management

New-user bootstrapping. Before Phase 5e.8, sources / targets / projects could only be created via CLI (`r2g source add`, `r2g project create`) or direct HTTP API calls; the Mapping Studio only listed and selected existing ones. Phase 5e.8 adds in-UI creation surfaces so a fresh user can reach a working mapping without leaving the workspace.

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.8.1** | **Source CRUD in UI** | Sources panel header exposes a "+ New source" button opening a floating-card form (name, type, connection string or env-var ref, description). On save: `POST /api/sources` → auto-trigger snapshot. Right-click a source → "Re-introspect", "Copy name", "Remove source…" with a cascade-delete confirmation that lists dependent projects / snapshots / loads. Toasts surface success / failure. | Done |
| **P5e.8.2** | **Target CRUD in UI** | New Targets panel in the left sidebar mirrors the Sources panel. "+ New target" floating-card form (name, endpoint, database, username, password, description). On save: `POST /api/targets` → optionally `POST /api/targets/{name}/introspect`. Right-click a target → "Introspect", "Copy endpoint", "Remove target". | Done |
| **P5e.8.3** | **Project CRUD in UI** | Toolbar gains a "+ New project" button next to the project selector, opening a floating-card form (name, source selector populated from the catalog, Arango endpoint, database, mapping config path with a sensible default). On save: `POST /api/projects` → auto-select. Right-click the project chip → "Rename project" (deferred), "Copy project name". | Done |
| **P5e.8.4** | **Discoverability copy** | Empty states on the Sources panel, Targets panel, and project selector include a direct CTA to the "+ New …" action instead of a dead-end "None configured" label. | Done |

#### Non-functional notes (Phase 5e)

- No new routes were introduced; all interactions are overlays, lenses, or context menus on the existing workspace.
- Lens changes are paint-only; topology changes (new / removed / filtered entities) are the only triggers for relayout.
- The floating-card stack survives page interaction: restore preserves prior coordinates and contents.
- The UI consumes `progress_callback` events from `StreamingPipeline` via SSE; the backend keeps at-least-once semantics unchanged.

---

## 6. Future considerations (Phase 7+) -- Exploratory

These ideas are exploratory and represent potential directions, not committed work. Each would require significant design effort.

- **Additional source databases:** MySQL, SQL Server, Oracle, and other relational databases could be added following the same `SourceConnector` pattern established in Phase 6. Each requires a source-specific schema reader, type map, and streaming adapter.
- **Ontology derivation (LLM integration):** Use a large language model to analyze the source schema and propose an optimized target ArangoDB graph schema for a given domain. This could suggest which tables should be vertices vs. edges, identify implicit relationships, and recommend denormalization strategies. Feasibility has improved significantly with current model capabilities.
- **ArangoRDF integration:** Emit data compatible with ArangoRDF so RDF, property graph, and labeled property graph representations can be selected as needed. Requires understanding the target use case (SPARQL queries, knowledge graphs, etc.) to choose the right representation.
- **Bi-directional synchronization:** Propagate changes from ArangoDB back to the source database. This is an extremely complex problem involving conflict resolution, schema evolution, and transactional consistency across two fundamentally different data models. Should be considered only if a concrete use case demands it.

---

## Document history

| Version | Date | Notes |
| :--- | :--- | :--- |
| Draft (Gemini-structured source) | December 2025 | Initial PRD with phased requirements P1.1--P4.4, technical requirements, and Phase 5+ items. |
| Narrative supplement (NotebookLM source) | December 2025 | Overlapping content with expanded relational-to-graph mapping (transliteration, join tables, normalization) and synchronization framing. |
| **Consolidated PRD** | **April 2026** | Single authoritative document. Gemini structure and requirement IDs preserved; NotebookLM mapping logic merged; conversational phrasing removed. Scope clarified as experimental reference implementation. Status columns added to phase tables. Edge cases, known constraints, and security notes added. "Antigravity" branding removed. |
| **Phase 1 extensions** | **April 2026** | CSV-direct import path (P1.7) and interactive mapping visualizer (P1.8) added. README updated to reflect the CSV-direct path as the preferred pipeline. |
| **Phase 2 implemented** | **April 2026** | Direct PG streaming to ArangoDB (P2.1--P2.4) implemented via psycopg server-side cursors and python-arango HTTP bulk import. `dump-tables` command, join table auto-detection, and interactive mapping editor with YAML export added. |
| **Hardening** | **April 2026** | Composite FK support (introspection, transformation, CSV-direct `--merge-attributes`). Multi-schema support (`--pg-schema`). Dry-run mode (`stream --dry-run`). GitHub Actions CI (pytest + ruff). 230 tests. |
| **Robustness & Performance** | **April 2026** | Rich progress bars for streaming. `validate-config` CLI command for static mapping/schema consistency checks. `--drop-collections` flag for idempotent re-import. Retry logic with exponential backoff in ArangoDB bulk writes. `--workers` flag for parallel table streaming (concurrent PG connections + ArangoDB writers). Elapsed time and throughput (rows/s) in stream output. Docker-based integration test suite. 251 tests. |
| **Data Integrity & UX** | **April 2026** | Import error surfacing (document-level errors captured and reported instead of silent failures). `--include-tables` / `--exclude-tables` for selective streaming. `source_schema` now populated from actual PG schema. Extended PG type map (50+ types). Tests for self-referential FKs and duplicate edge naming. 276 tests. |
| **Developer Experience** | **April 2026** | `diff-schema` command for comparing schema snapshots (added/removed tables, column changes, FK changes, with `--json` output). `--skip-existing` flag for resuming partial streaming runs. CLI integration test suite (31 tests via typer CliRunner). Fixed `transform-edges` unhashable EdgeDefinition bug. 314 tests. |
| **Config Migration** | **April 2026** | `migrate-config` command auto-updates mapping YAML when PG schema evolves: adds new tables/edges, removes stale edges, flags orphaned collections, cleans dropped-column references (field_mappings, include/exclude_fields, type_overrides). Preserves all user customizations. `--json-report` for CI pipelines. Typer upgraded from 0.12 to 0.24 for Click 8.3 compatibility. 341 tests. |
| **Usability & Safety** | **April 2026** | `.env` file and environment variable support (`PG_CONN`, `ARANGO_ENDPOINT`, `ARANGO_DB`, `ARANGO_USER`, `ARANGO_PASSWORD`) via python-dotenv -- credentials no longer required in CLI args. `validate-data` command checks FK referential integrity of dump files before import. Topological import ordering ensures FK targets are loaded before sources; circular FK deps detected and warned. `--since` timestamp filtering for basic incremental streaming. PK-less table warnings during validation and streaming. `.env.example` template. Snowflake integration planned as Phase 5. 373 tests. |
| **CDC Foundation** | **April 2026** | Phase 3 foundation: `ChangeEvent` model, `ArangoDelta`, `TransactionBatch`, `DeltaTransformer`, `CDCHandler`, `ArangoWriter` single-document ops with retry logic. 409 tests. |
| **CDC Listener** | **April 2026** | Phase 3 P3.1 + P3.3: `PGReplicationListener` manages logical replication slots, polls via `pg_logical_slot_get_changes`. Output plugin parsers for `test_decoding` and `wal2json`. Continuous polling loop with graceful shutdown. CLI commands: `cdc-setup`, `cdc-teardown`, `cdc-status`, `cdc-start`. 453 tests. |
| **Conflict Resolution** | **April 2026** | Phase 3 complete (P3.4): Configurable conflict policies (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`). `ConflictResolver` wraps writes with error classification and policy-based resolution. `ConflictLog` for session conflict tracking. `--conflict-policy` CLI option on `cdc-start`. LWW uses `_r2g_lsn` field for per-document LSN tracking. 468 tests. |
| **Kafka Integration** | **April 2026** | Phase 4 complete: DebeziumParser for Debezium JSON envelope, FlatJsonParser for custom producers, KafkaConsumer wraps confluent-kafka with batch polling and at-least-once offset commits, graceful shutdown. kafka-start CLI command. Optional dependency. 502 tests. |

| **Visual Mapper PRD** | **April 2026** | Phase 5b added: Visual Graph Data Mapper & Ingestion Engine. Three epics (Data Catalog Interface, Visual Mapping Interface, Ingestion & Execution Engine) incorporating TigerGraph-inspired split-screen mapping UI, automated introspection, parallel ingestion, job monitoring, and cascading referential integrity. Mapped against existing implementation (catalog, mapping diff, selective reload, Mapping Studio UI). |
| **Expression + Graph-of-Graphs** | **April 2026** | Phase 5c added: Expression Mapping & Graph-of-Graphs UI. Introduces `FieldExpression` first-class data model (fan-in of 1..N source properties into a single target property via AQL / KSQL / Python expressions, identity default), mapping function nodes rendered as circles on connector lines with an inline expression editor, fan-in via drag-and-drop, and mini ER/graph-model visualizations within the source and target panes (FK edges in-pane on the left, edge-collection arrows in-pane on the right). Phase 5d added: ArangoDB-backed data catalog (sources, targets, snapshots, projects, mappings, loads as a named graph). |
| **Phase 5b Execution Engine** | **April 2026** | P5b.3.1 Load trigger, P5b.3.2 SSE progress monitoring, and P5b.3.3 dead-letter queue (`src/r2g/dlq.py`) implemented. Target graph introspection (P5b.1.3) landed via `src/r2g/connectors/arango_reader.py`, `TargetConfig` in the catalog, and `/api/targets*` endpoints. Cascading deletes (P5b.1.4) wired through `remove_source(cascade=True)` and `DELETE /api/sources/{name}?cascade=true`. Settings modal + Auto-Map button closed P5b.2.3 / P5b.2.4. MCP server (`src/r2g/mcp_server.py`) exposes catalog and load operations for agent tooling. Composite foreign keys normalized on `EdgeDefinition` via plural `from_fields` / `to_fields` with round-trip serialization. |
| **UI Architecture Upgrade** | **April 2026** | Phase 5e added and implemented: shared primitives (context menu, keyboard registry, `?` help overlay, floating-card stack); right-click menus on every entity; first-class edges with floating edge editor, drag-to-add, and promote-join-table; floating non-blocking progress card replaces the canvas-blocking overlay; bottom timeline strip for recent loads; complete legend; paint-only lens infrastructure (Topology / Coverage / Validation / Diff) with `1`–`4` shortcuts and header status chip; inline silent validation via `POST /api/projects/{name}/validate-draft` with per-entity badges and tooltip issue lists; a11y and hygiene polish (menu / region / complementary roles, `aria-label`s, Esc pops the top overlay, discoverability copy). 673 tests passing. |
| **In-UI catalog management** | **April 2026** | Phase 5e Epic 8 added: in-UI "+ New source", "+ New target", and "+ New project" floating-card forms wired to the existing `POST /api/sources`, `POST /api/targets`, `POST /api/projects` endpoints; right-click "Remove source…" with cascade-delete confirmation listing dependent projects / snapshots / loads; new Targets panel in the left sidebar with introspect + remove actions; empty-state copy now includes direct CTAs. Closes the "how do I even start" gap so first-time users can go from zero to a working mapping without touching the CLI. |

The source files `PRD-gemini.md` and `PRD-notebooklm.md` remain in the repository for reference and are superseded by this file.
