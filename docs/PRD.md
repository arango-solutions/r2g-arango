# R2G-ETL Pipeline

*Product Requirements Document (PRD) -- Experimental Reference Implementation*

| Field | Value |
| :--- | :--- |
| **Product name** | R2G-ETL Pipeline (Relational to Graph -- Extract, Transform, Load) |
| **Version** | 0.1.0 (experimental) |
| **Date** | Originally drafted December 2025, consolidated April 2026 |
| **Status** | Phases 1--4 implemented and hardened; Phase 5 (Temporal graph mode) implemented (end-to-end field validation pending); Phase 5b (Visual Mapper), Phase 5c (Expression / Graph-of-Graphs UI), Phase 5e (UI Architecture Upgrade), Phase 5f (Naming conventions & rename change-management), and Phase 5g (Post-demo UX refinements) implemented; Phase 6 (Snowflake) done; MySQL/MariaDB and SQL Server sources added; Phase 5d (ArangoDB-backed catalog), Phase 8 (external data catalog integration; 8a–8b implemented), Phase 9 (classification propagation & entitlement-aware loading), Phase 10 (LLM-assisted ontology derivation), Phase 11 (denormalization & normal-form analysis), and Phase 7 are planned or exploratory |
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
| **Kafka consumer** | Consumes CDC events from Kafka topics via `confluent-kafka`. `DebeziumParser` handles Debezium JSON envelopes (including Kafka Connect wrappers and snapshot reads). `FlatJsonParser` for custom producers. At-least-once delivery with post-write offset commits. Reuses the CDC engine's handler and conflict resolution. Optional dependency (`pip install r2g-arango[kafka]`). |
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

The roadmap is organized into phases: Phases 1–4 (MVP through Kafka), Phase 5 (temporal graph mode), Phase 5f (naming conventions & rename change-management), Phase 5g (post-demo UX refinements), and Phase 6 (Snowflake + multi-source) are implemented; Phase 5d (ArangoDB catalog backend) is planned; Phase 7+ (additional sources and advanced features) is exploratory.

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
| **P4.2** | **Kafka consumer** | `KafkaConsumer` wraps `confluent-kafka`, subscribes to topics, polls in batches, commits offsets after successful processing (at-least-once semantics). Graceful shutdown via SIGINT/SIGTERM. Optional dependency via `pip install r2g-arango[kafka]`. | P4.1 | Done |
| **P4.3** | **Kafka message transformation** | `DebeziumParser` parses Debezium JSON envelope (`before`/`after`/`op`/`source`) including Kafka Connect `payload` wrapper, snapshot reads (`op: r`). `FlatJsonParser` for custom producers. Both produce `ChangeEvent` objects fed into existing `CDCHandler`. | P4.2, P3.2 | Done |
| **P4.4** | **Transactional ordering** | Messages consumed in Kafka partition order. Events grouped by `transaction_id` (from Debezium `source.txId`) and applied through `CDCHandler.handle_transaction` for ordered delta application. Conflict resolution policies apply. | P4.3 | Done |

### Phase 5: Temporal graph mode -- Implemented

CDC and Kafka pipelines apply changes as direct replaces/deletes by default. Temporal graph mode adds an alternative write strategy using the **immutable-proxy time travel pattern** (ProxyIn / Entity / ProxyOut), enabling full version history, point-in-time queries, and soft deletes with automatic TTL-based garbage collection.

All of P5.1--P5.8 are implemented (`src/r2g/temporal/` — `TemporalConfig` / `TemporalNaming` models, `TemporalApplier`, and AQL query templates in `queries.py`; CLI `--temporal`, `--ttl-seconds`, and `--smart-field` flags on `cdc-start` and `kafka-start`; covered by `tests/test_temporal_applier.py`, `test_temporal_models.py`, `test_temporal_queries.py`). End-to-end verification against a live temporal workload remains a field-validation exercise.

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
| **Architecture** | Modularity | Data sources are swappable via the `SourceConnector` / `SourceSession` abstraction (`connectors/base.py`). Implemented: PostgreSQL, Snowflake, CSV directories, and Kafka (streaming). MySQL / SQL Server remain exploratory (Phase 7+). |
| **Target DB** | ArangoDB | Load via `arangoimport` (file-based, Phase 1) and the ArangoDB HTTP API (streaming/CDC/Kafka, Phases 2--4). |
| **Transformation** | Schema mapping | Configurable prefix mapping for `_from` and `_to` (e.g., `user_1` to `Users/1`). |
| **Data integrity** | Key generation | Correct document `_key` values derived from source primary keys, including composite keys joined by a configurable separator. |
| **Technology stack** | Python | Chosen for ecosystem support (psycopg, python-arango, Polars, Pydantic, structlog, confluent-kafka, python-dotenv). |

### Known constraints

- **Referential integrity is opt-in**: the `validate-data` command checks FK values against PK sets from dump files, but this check is not enforced automatically during import. Orphaned references will still produce edges pointing to non-existent vertices if validation is skipped.
- **Bulk load idempotency**: re-running the streaming pipeline with `--drop-collections` replaces all data. For incremental updates, CDC and Kafka pipelines provide configurable conflict resolution (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`) with at-least-once delivery semantics.
- **System database protection**: loads into the ArangoDB `_system` database are refused by the UI; new targets default to a non-`_system` database to prevent accidental writes into the management database.
- **Reserved ArangoDB attributes**: `_id`, `_key`, `_rev` (documents) and `_from`, `_to` (edges) are reserved system attribute names. They are never produced or renamed by naming conventions or rename migrations, and `validate_config` flags any `field_mapping` / `field_expression` that targets one (see Phase 5f).
- **Credential handling**: connection parameters can be loaded from `.env` files or environment variables (`PG_CONN`, `ARANGO_ENDPOINT`, etc.). Credentials stored in the filesystem catalog (`~/.r2g/catalog.json`) are encrypted at rest via Fernet using a key loaded from `R2G_SECRET_KEY` or `~/.r2g/secret.key` (0600, auto-generated). Generated import scripts still contain connection defaults, and no integrated remote secrets manager (e.g., HashiCorp Vault) is provided.

### Phase 6: Snowflake integration -- **Done**

Snowflake is a common data warehouse among R2G users. This phase adds Snowflake as a source alongside PostgreSQL, reusing the existing mapping, transformation, and loading infrastructure.

Delivery was sliced: slice 1 (P6.1 introspect-only + P6.2 type mapping + P6.5 source abstraction), slice 2 (P6.6 FK inference), and slice 3 (P6.3 dump export + P6.4 streaming) are all **Done**. The streaming pipeline, `r2g stream` CLI, and UI `POST /api/projects/{name}/load` endpoint are now source-agnostic and dispatch through `create_source_connector` based on the cataloged `source_type`; every supported source implements `open_session()` returning a consistent-snapshot `SourceSession` that the pipeline and the new `r2g source dump` CLI consume. End-to-end verification against a live Snowflake account remains a field-validation exercise.

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P6.1** | **Snowflake schema reader** | Connect to Snowflake via the Snowflake Connector for Python (`snowflake-connector-python`, installed as the optional `r2g-arango[snowflake]` extra) and introspect `INFORMATION_SCHEMA.TABLES` / `INFORMATION_SCHEMA.COLUMNS` plus `SHOW PRIMARY KEYS` / `SHOW IMPORTED KEYS` to populate the same `Schema` model used by PostgreSQL. Delivered as `r2g.connectors.snowflake.SnowflakeConnector`; imports are lazy so the UI degrades gracefully with a 501 + pip-install hint when the extra is not installed. Connection strings use the Snowflake SQLAlchemy URL shape: `snowflake://user:pass@account/DATABASE[/SCHEMA]?warehouse=WH&role=R`. | P1.1 | **Done** (introspection, dump, and streaming all wired via `SnowflakeSession`; see P6.3–P6.5) |
| **P6.2** | **Snowflake type mapping** | Map Snowflake data types (`NUMBER`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP_*`, `VARIANT`, `ARRAY`, `OBJECT`, `GEOGRAPHY`, `GEOMETRY`, `BINARY`, `VECTOR`, etc.) to JSON types. `VARIANT`/`OBJECT` map to JSON objects; `ARRAY` maps to JSON arrays. `DEFAULT_TYPE_MAP` in `r2g.config` has been extended with Snowflake-specific entries and is shared with PostgreSQL via `pg_type_to_json_type`. | P1.4 | **Done** |
| **P6.3** | **Snowflake dump export** | `r2g source dump <name>` is the source-agnostic replacement for the legacy `r2g dump-tables --conn`. It reads the catalog entry, dispatches through `create_source_connector`, opens a `SourceSession`, and calls `SourceSession.dump_table_to_csv()` for every table in the latest snapshot (or a `--tables` subset). PostgreSQL goes through the server-side `COPY … TO STDOUT WITH CSV HEADER` fast path; Snowflake streams the cursor and writes CSV via Python's `csv` module with empty-string NULLs and `"` quoting. Output is one `<table>.csv` per table under `--output-dir`. The UI will gain a "Dump tables" button in a later polish pass. | P6.1 | **Done** |
| **P6.4** | **Snowflake streaming** | `StreamingPipeline` now consumes any `SourceConnector`; PG and Snowflake each implement `open_session()` returning a `SourceSession` with `count_rows`, `stream_rows`, `dump_table_to_csv`, and `close`. PG sessions set `REPEATABLE READ`; Snowflake sessions open a `BEGIN`/`COMMIT` transaction for implicit snapshot isolation and stream via `cursor.fetchmany(batch_size)`. `r2g stream --source <name>` resolves the catalog source and drives the pipeline through the abstraction; the legacy `--pg-conn` flag still works by wrapping the string in a `PostgresConnector`. `POST /api/projects/{name}/load` dispatches the same way. Value-overlap FK sampling (P6.6) against Snowflake becomes available as a follow-up once a `SnowflakeValueSampler` is wired on top of the session. | P6.1, P2.3 | **Done** |
| **P6.5** | **Source abstraction layer** | `r2g.connectors.base` exposes a structural `SourceConnector` Protocol (attributes `connection_string`, `schema_name`; methods `get_schema() -> Schema` and `open_session() -> SourceSession`), the `SUPPORTED_SOURCE_TYPES` registry, and a `create_source_connector(source_type, connection_string, schema_name)` factory. `r2g.connectors.session.SourceSession` is the matching bulk-read Protocol (`count_rows`, `stream_rows`, `dump_table_to_csv`, `close`). `PostgresConnector` / `PostgresSession` and `SnowflakeConnector` / `SnowflakeSession` both satisfy the Protocols. The UI, MCP server, streaming pipeline, and every `source` CLI subcommand now dispatch through the factory based on the stored `source.source_type`. | P6.1, P6.4 | **Done** |
| **P6.6** | **FK inference** | Many schemas (Snowflake especially, but also legacy PG dumps) ship without declared FKs. Delivered as `r2g.fk_inference`: a pure-Python name-heuristic engine with pluggable value-overlap sampling. Scans for `{prefix}_id`, `{prefix}id`, `{prefix}_{pkcol}`, and non-generic PK-name matches, filters by JSON-level type compatibility, and emits a composite-FK pass that grabs local tables covering every column of a multi-column PK. `POST /api/sources/{name}/infer-fks` returns scored candidates; the Mapping Studio exposes them via a "Suggest FKs" toolbar action (shortcut `i`) and right-click menu with per-row Accept / Accept all / Dismiss buttons that write real `EdgeDefinition`s to the project mapping. `r2g source infer-fks <name> [--sample] [--accept]` mirrors the surface on the CLI; `--sample` activates a value-overlap sampler (veto-on-zero by default, confidence boost on ≥50% / ≥90% overlap). `PostgresValueSampler` runs bounded `LEFT JOIN` overlap queries; `CsvValueSampler` reads the two CSV files with Polars and compares distinct values as raw text. Sampling is supported for PostgreSQL and CSV; Snowflake sampling falls back to name-only. Name-heuristic inference works against any source type. | P6.1 | **Done** |

#### Snowflake-specific considerations

- **FK constraints are not enforced in Snowflake.** They can be declared but are informational only. Many Snowflake schemas have no FK metadata at all. The FK inference feature (P6.6) addresses this gap.
- **Semi-structured data.** Snowflake `VARIANT`, `OBJECT`, and `ARRAY` columns can contain nested JSON. These should be preserved as nested structures in ArangoDB documents rather than flattened.
- **Large tables.** Snowflake tables can be very large. The streaming path should support `LIMIT`/`OFFSET` pagination or warehouse-level result caching to manage memory. Arrow-based fetching (`fetch_arrow_all()`) provides the best throughput for large result sets.
- **Authentication.** Snowflake supports multiple auth methods (user/password, key-pair, SSO/OAuth, external browser). The connector should accept standard Snowflake connection parameters: `account`, `user`, `password`, `warehouse`, `database`, `schema`, `role`. These should be loadable from env vars (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.) and `.env` files.
- **Cost implications.** Every query against Snowflake consumes warehouse credits. The schema reader and streaming pipeline should minimize the number of queries. `--dry-run` should clearly report query cost implications.

### Phase 5b: Visual Graph Data Mapper & Ingestion Engine -- Implemented

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
| **P5b.1.1** | **Data source CRUD** | Users can Create, Read, Update, and Delete data sources. Supported source types: CSV directory, RDBMS (PostgreSQL, Snowflake; MySQL, Oracle planned), Kafka topics. | -- | Done for PostgreSQL, Snowflake, CSV, and Kafka registration (`SUPPORTED_SOURCE_TYPES` in `connectors/base.py`; CLI, API, and in-UI "+ New source" form + right-click remove, see P5e.8.1) |
| **P5b.1.2** | **Automated schema introspection** | Upon saving a new data source, the system automatically introspects the source. CSV: parse headers and infer types. RDBMS: extract tables, columns, PKs, FKs. Kafka: extract schema from Schema Registry or parse sample payload. Display as hierarchical tree or entity cards. | P5b.1.1 | Done (`POST /api/sources/{name}/snapshot`, auto-triggered after in-UI source creation and exposed as right-click "Re-introspect") for PostgreSQL, Snowflake, and CSV (header + type inference via `CsvConnector`); Kafka exposes introspection-only in the studio (batch sync via `kafka-start`) |
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
| **P5b.3.2** | **Job monitoring** | Status indicator (Pending, Running, Success, Failed). Metrics: rows processed, vertices created, edges created, error count. | P5b.3.1 | Done (`GET /api/projects/{name}/load/{load_id}/status` + Server-Sent Events stream at `GET /api/projects/{name}/load/{load_id}/stream`; `progress_callback` in `StreamingPipeline`; floating progress card in the UI with live per-table counters, error count, elapsed time, minimize-to-tray; bottom timeline strip records completed runs) |
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

### Phase 5c: Expression Mapping & Graph-of-Graphs UI -- Implemented

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
| **P5c.1.4** | **Expression evaluator (AQL subset)** | A Python-side evaluator that executes a safe subset of AQL string/number/date/array functions (`CONCAT`, `CONCAT_SEPARATOR`, `UPPER`, `LOWER`, `SUBSTRING`, `LENGTH`, `LTRIM`, `RTRIM`, `TRIM`, `TO_STRING`, `TO_NUMBER`, `TO_BOOL`, `CONTAINS`, `COALESCE`) with arithmetic, comparison, boolean logic, ternary `? :`, null-coalescing `??`, and AQL-style `NULL` propagation. `NodeTransformer` applies `field_expressions` per row; un-compilable / non-AQL expressions fall back to identity pass-through with a structured-log warning. `validate_config` compile-checks every AQL expression and flags unresolved `@bindings`. The UI exposes `/api/expressions/functions` (list supported functions) and `/api/expressions/compile` (parse-check) so the modal editor shows live syntax status and the referenced columns. | P5c.1.1 | Done — see `src/r2g/expressions.py`, `tests/test_expressions.py` |
| **P5c.1.5** | **AQL delegation for complex expressions** | For expressions outside the Python evaluator's subset, the streaming pipeline submits a per-batch AQL `FOR doc IN @@batch LET ... RETURN doc` query to ArangoDB and uses the rewritten result as the ingestion payload. | P5c.1.4 | Done for AQL (streaming) — `NodeTransformer` flags delegated targets and `StreamingPipeline._apply_delegation` runs a per-batch AQL rewrite via `build_delegation_query` (see `tests/test_streaming_pipeline.py`). KSQL / Python engines remain TODO (P5c.1.7). |
| **P5c.1.6** | **arangoimport pre-transformation** | For bulk load, the JSONL generator applies expressions in-memory (reusing the Python evaluator + AQL delegation path) so `arangoimport` only sees the final document shape. No changes to `arangoimport` invocation required. | P5c.1.4 | Done for the Python subset — `NodeTransformer.transform_row` applies expressions before JSONL emission; the dedicated delegation path remains open (covered by P5c.1.5). |
| **P5c.1.7** | **KSQL translation layer** | For Kafka/streaming loads, a translator that rewrites the canonical AQL-flavoured expressions into KSQL (ksqlDB-compatible) `SELECT` projections. Initial scope: arithmetic, string concat, CASE. | P5c.1.3 | Not started |

#### Epic 2: Graph-of-Graphs UI

| ID | Requirement | Description | Pre-requisite | Status |
| :--- | :--- | :--- | :--- | :--- |
| **P5c.2.1** | **Source ER-graph visualization** | Left pane renders source tables as graph nodes with FK relationships drawn as directed edges between tables. Click to expand a table inline, exposing its columns with connector ports on the right edge. | P5b.2.1 | Partial (split-screen exists; inter-table FK edges in-pane added) |
| **P5c.2.2** | **Target graph-model visualization** | Right pane renders target vertex collections as graph nodes and edge collections as labeled directed edges between them. Click to expand a vertex to show its properties with connector ports on the left edge. | P5b.2.1 | Partial (split-screen exists; inter-collection edges in-pane added) |
| **P5c.2.3** | **Mapping function nodes** | Each connector line carries a circular function node in the center canvas. The node labels its target property; hovering shows the expression preview; clicking opens an expression editor modal. Default (identity) functions render as small hollow circles; non-identity as filled circles in a distinct colour. | P5c.1.1 | Done |
| **P5c.2.4** | **Fan-in via drag-and-drop** | Users drag a source column connector dot onto an existing function circle to add that column as another input (multi-input fan-in). The function's `sources` list is updated in the mapping config. | P5c.2.3 | Done |
| **P5c.2.5** | **Expression editor** | Modal editor with engine selector (AQL / KSQL), syntax-highlighted textarea, available-sources picker (columns already bound to this function), optional description field, and a live preview button that evaluates the expression against one source row. | P5c.2.3 | Done (editor modal + engine selector + sources picker + textarea + reset-to-identity; syntax highlighting and live server-side preview implemented via `highlightExpr()` and `POST /api/expressions/preview`; autocomplete / UDFs deferred) |
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
- **Done (April 2026) — credential encryption at rest.** The filesystem catalog (`~/.r2g/catalog.json`) encrypts every `SourceConfig.connection_string` and `TargetConfig.password` with a Fernet (AES-128-CBC + HMAC-SHA256) symmetric key that is read in order: (1) `R2G_SECRET_KEY` env var, (2) `~/.r2g/secret.key` (auto-generated on first use, 0600). Ciphertexts are tagged with `enc:v1:` so legacy plaintext catalogs continue to open read-only; any write-through re-emits them encrypted. API responses and logs are redacted via `redact_connection_string` / `redact_for_display`. The CLI ships `r2g secrets init|status|migrate` to bootstrap, inspect, and force-upgrade existing catalogs. OS-keychain integration (via `keyring`) remains a future enhancement.
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
| **P5e.7.4** | **Configurable hints (menus + buttons)** | A single styled tooltip surface (`#ctx-hint`, `role="tooltip"`, `aria-hidden` toggling, viewport-clamped). Context-menu items carry optional `hint` text rendered by `openContextMenu` (items with a hint show an ⓘ marker; tooltip appears to the right on hover / keyboard focus). Toolbar and panel buttons (New project, project menu, Validate, Save, Auto-Map, Suggest FKs, Diff, Export YAML, History, Settings, Help, Load, New source, New target) carry `data-hint` text shown via a delegated `mouseover` / `focusin` handler (tooltip placed below the button); these `data-hint`s replace the old native `title` tooltips while `aria-label`s are retained for screen readers. A single "Show menu hints" preference (default on, persisted in `localStorage` as `r2g.menuHints`) toggles both menus and buttons from the `?` help overlay — no new route. Menu hints are seeded across every entity menu (canvas, project, source table / column, target collection / property, function circle, connector, edge, FK, sidebar items / panels, target cluster, history rows). | Done |

#### Epic 8: In-UI catalog management

New-user bootstrapping. Before Phase 5e.8, sources / targets / projects could only be created via CLI (`r2g source add`, `r2g project create`) or direct HTTP API calls; the Mapping Studio only listed and selected existing ones. Phase 5e.8 adds in-UI creation surfaces so a fresh user can reach a working mapping without leaving the workspace.

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5e.8.1** | **Source CRUD in UI** | Sources panel header exposes a "+ New source" button opening a floating-card form (name, type, connection string or env-var ref, description). On save: `POST /api/sources` → auto-trigger snapshot. Right-click a source → "Re-introspect", "Copy name", "Remove source…" with a cascade-delete confirmation that lists dependent projects / snapshots / loads. Toasts surface success / failure. | Done |
| **P5e.8.2** | **Target CRUD in UI** | New Targets panel in the left sidebar mirrors the Sources panel. "+ New target" floating-card form (name, endpoint, database, username, password, description). On save: `POST /api/targets` → optionally `POST /api/targets/{name}/introspect`. Right-click a target → "Introspect", "Copy endpoint", "Remove target". | Done |
| **P5e.8.3** | **Project CRUD in UI** | Toolbar gains a "+ New project" button next to the project selector, opening a floating-card form (name, source selector populated from the catalog, Arango endpoint, database, mapping config path with a sensible default). On save: `POST /api/projects` → auto-select. Right-click the project chip → "Edit project…" (rename / change description / target endpoint / database via `PATCH /api/projects/{name}`), "Delete project…" (confirmation dialog, `DELETE /api/projects/{name}`), "Copy project name". | Done (full create / edit / delete in-UI; `PATCH`/`DELETE /api/projects/{name}`) |
| **P5e.8.4** | **Discoverability copy** | Empty states on the Sources panel, Targets panel, and project selector include a direct CTA to the "+ New …" action instead of a dead-end "None configured" label. | Done |

#### Non-functional notes (Phase 5e)

- No new routes were introduced; all interactions are overlays, lenses, or context menus on the existing workspace.
- Lens changes are paint-only; topology changes (new / removed / filtered entities) are the only triggers for relayout.
- The floating-card stack survives page interaction: restore preserves prior coordinates and contents.
- The UI consumes `progress_callback` events from `StreamingPipeline` via SSE; the backend keeps at-least-once semantics unchanged.

### Phase 5f: Naming conventions & rename change-management -- Implemented

Phases 5b/5c/5e let users rename individual collections, properties, and edges by hand. Phase 5f adds two related capabilities: (a) **bulk naming conventions** that re-case every generated name at once, and (b) safe **change-management for renames when the target database has already been loaded**, so renaming a collection, edge, or property reconciles the live graph in place instead of forcing a full drop-and-reload. Reserved ArangoDB system attributes are protected throughout.

#### Conceptual model

A `MappingConfig` carries an optional `naming_convention` describing the desired casing for collections (default PascalCase), properties, and edges (default camelCase). Applying a convention rewrites the *target* names while preserving any names the user set by hand and never touching reserved attributes. Separately, each successful load snapshots the live mapping (`Project.loaded_mapping`); when the user later edits names and saves, R2G diffs the live snapshot against the new mapping and offers an in-place migration.

Renames are reconciled with three primitives:

- **Document-collection rename** → rename the collection in place; affected edges are reloaded from the source so `_from`/`_to` point at the new collection name.
- **Edge-collection rename** → rename the edge collection in place (matched by relationship identity, not by name).
- **Property rename** → an AQL `UNSET`/`MERGE` rewrite on the owning collection.

After any rename the named graph is rebuilt so its edge definitions reference current collection names.

#### Epic 1: Naming conventions

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5f.1.1** | **NamingConvention model** | `NamingConvention` (each of `collections` / `properties` / `edges` is one of `preserve` \| `snake` \| `camel` \| `pascal`) added to `MappingConfig.naming_convention`; round-trips through YAML / JSON. | Done |
| **P5f.1.2** | **Identifier conversion engine** | `r2g.naming.split_identifier` / `convert_identifier` split mixed snake / camel / Pascal / kebab identifiers (including digit and acronym handling) and recase them to the requested style. | Done |
| **P5f.1.3** | **Apply convention** | `apply_naming_convention(config, convention, schema)` materializes the convention into `target_collection`, `field_mappings` targets, `field_expressions` targets, and `edge_collection` names, preserving user-set manual renames and skipping reserved attributes. Exposed as `POST /api/projects/{name}/apply-naming`, which returns the transformed (unsaved) config as an editable draft. | Done |
| **P5f.1.4** | **In-UI action** | Canvas right-click → "Apply naming convention…" opens a floating form (style pickers defaulting to PascalCase collections / camelCase properties and edges); the result loads into the editor as a draft for review before save. | Done |

#### Epic 2: Rename change-management (loaded target)

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5f.2.1** | **Loaded-state tracking** | `Project.loaded_mapping` / `loaded_at` snapshot the mapping that is live in the target; `CatalogManager.set_loaded_mapping` records it after every successful full or selective load (UI and CLI), giving migrations an accurate baseline to diff against. | Done |
| **P5f.2.2** | **Migration plan (smarter diff)** | `diff_mappings` detects document-collection renames by **source-table identity**, edge-collection renames by **relationship identity** (from/to collections + key columns), and property renames, emitting typed `ReloadAction`s with structured `params`. `GET /api/projects/{name}/migration-plan` returns the plan. | Done |
| **P5f.2.3** | **In-place executor** | `SelectiveReloader` renames collections in place, reloads affected edges from the source (rebuilding `_from`/`_to`), renames properties via parameter-bound AQL, and rebuilds the named graph. Idempotent (`has_collection` guards) and refuses to write into `_system`. `POST /api/projects/{name}/migrate` runs the plan (with `dry_run` support) and updates `loaded_mapping` on success. | Done |
| **P5f.2.4** | **Save-time migration prompt** | After a save where names changed and the project has load history, the UI surfaces an overlay summarizing the detected renames and offers "Migrate in place", "Full reload", or "Save mapping only". | Done |

#### Epic 3: System-attribute safety

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5f.3.1** | **Reserved-attribute guards** | Shared `RESERVED_ATTRIBUTES = {_id, _key, _rev, _from, _to}` in `r2g.types`. Naming conventions never remap a source column named like a system attribute and never emit a reserved target name; the diff engine skips any rename touching a reserved name; the executor refuses reserved renames (reported as skipped); and `validate_config` flags any `field_mapping` / `field_expression` target that is reserved. | Done |

#### Latent bug fixes folded into Phase 5f

- **Edge endpoint resolution.** `EdgeTransformer` and the named-graph builder previously built `_from`/`_to` and edge definitions from *source-table* names, which broke whenever a `target_collection` differed (e.g. after a rename). `EdgeTransformer` now accepts resolved `from_name` / `to_name`, and `ConfigManager.graph_edge_definitions` centralizes source→target resolution used by the streaming pipeline, CLI `transform-*` commands, the CDC delta transformer, and the graph rebuild step.

#### Out of scope (V1 of Phase 5f)

- **History rewriting of historical data values** when a property rename collides with existing data of a different shape — the AQL rewrite assumes a straight key rename.
- **Cross-collection moves** (re-pointing an edge to a different vertex collection) beyond the reload-from-source path.

---

### Phase 5g: Post-demo UX refinements -- Implemented

Captured from the first stakeholder demo (June 2026). The studio works but the
layout is heavy: too much permanent chrome, too many always-visible toolbar
buttons, and the relationship between a source element and its mapped target is
not visually obvious. This phase makes the canvas the focus, moves supporting
surfaces on-demand, and adds progressive disclosure so the mapping reads at the
level of detail the user wants. All work stays within the single `/workspace`
shell (no new routes), uses overlay / slide-out panels and context menus, and
preserves graph layout on lens-only changes per the UI architecture rules.

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P5g.1** | **Light mode default + theme toggle** | Ship a light theme as the default and a one-click toggle to dark mode. Drive both from the existing CSS custom properties via a `[data-theme]` attribute on `<html>`; persist the choice in `localStorage` (`r2g.theme`) and honor `prefers-color-scheme` on first run. A toggle control lives in the top bar (or the consolidated menu, P5g.2) with an accessible label and keyboard operability. | Done |
| **P5g.2** | **Simplified top bar (action overflow menu)** | The toolbar action row (Validate, Save, Auto-Map, Suggest FKs, Diff, Export YAML, History, Settings, Help, Load) is too dense. Keep only the highest-frequency primary actions inline (e.g. Save, Load, and the active-lens chip) and collapse the rest into an overflow / "Actions" menu (kebab) that reuses the existing context-menu primitive and hint system. Keyboard shortcuts and `aria-label`s are retained. | Done |
| **P5g.3** | **On-demand Sources & Targets** | The left Sources/Targets sidebar is always consuming width. Replace the permanent panel with an on-demand surface: a slide-out drawer (or popover) opened from a toolbar/explorer affordance, dismissible with Esc and click-outside. Source/Target CRUD and right-click actions (P5e.8.1/8.2) move into the drawer unchanged. (Note: the drawer currently defaults **open** on first run rather than collapsed.) | Done (drawer defaults open) |
| **P5g.4** | **On-demand detail (Properties) panel** | The right-hand Properties / collection-detail panel likewise becomes on-demand: it appears as a slide-in or floating overlay when an entity is selected (reusing the floating-card primitive, P5e.1.4) and is otherwise hidden, returning that real estate to the canvas. Minimize / dismiss supported. | Done |
| **P5g.5** | **Bidirectional mapping highlight** | Selecting a source table or column highlights the full mapping path to its target collection / property (connector + opposite-side card/row), and selecting a target collection or property highlights the path back to the source table / attribute. Hover gives a transient highlight; click pins it. Built on the existing `data-table` / `data-col` / `data-collection` / `data-prop` attributes and the `.connector.selected` styling — paint-only, no relayout. | Done |
| **P5g.6** | **Project explorer (IDE-style)** | Replace the awkward project dropdown + kebab menu with an optional left explorer panel listing available projects (grouped by source/target as useful), with the active project highlighted. Selecting opens it on the canvas; right-click exposes the existing project actions (edit / delete / copy name, P5e.8.3). Collapsible so it does not compete with the canvas. May share the slide-out shell with Sources/Targets (P5g.3) as an "explorer" with sections. | Done |
| **P5g.7** | **Progressive disclosure / mapping detail filter** | Control the level of mapping detail shown. Default: source tables and target collections render **collapsed**, showing only table→collection mappings. Expanding a source table reveals its attributes and, when the matching collection is also expanded, the attribute→property connectors; collapsing hides them again. Each source table and each target collection has an independent expand/collapse affordance. Connectors are drawn per visible level (table-conn always; col-conn only when both ends are expanded). Optionally complemented by explicit filters (e.g. show only unmapped, only edges). State persisted per project. Refinements: edge collections are themselves collapsible; a coarse table→edge "spine" keeps relationships visible while an edge is collapsed and is replaced by the detailed attribute→`_from`/`_to` connectors once it is expanded (no duplicate). Expanding a source table opens its whole relationship neighbourhood — its 1:1 document collection, every edge that references it, and the far-endpoint tables (+ their collections) — so the relationship renders entirely at the attribute→property level. Collapsing tidies edges with no remaining open endpoint back to the spine. The coarse spine connects **both** endpoint tables of an edge (deduped for self-edges). (Note: the optional "show only unmapped / only edges" detail filters are not yet built.) | Done (explicit filters pending) |
| **P5g.8** | **Open target database** | A per-project "Open database" action (project context menu + Actions menu) opens that project's ArangoDB target (`<endpoint>/_db/<database>/_admin/aardvark/index.html`) in a new browser tab. Resolves the endpoint/database from the linked target, falling back to the project's inline `arango_endpoint` / `arango_database`. | Done |
| **P5g.9** | **Function-node discoverability** | Make the mapping circles legibly read as editable transformation functions (Adria feedback). Identity (pass-through) circles carry a small italic `\u0192` glyph and a stronger hover/grow + pointer affordance; tooltips state "click to add/edit a transformation"; the legend's "Function nodes" section is titled "click a circle to edit" and its identity swatch shows the `\u0192` mark. | Done |
| **P5g.10** | **Composite / multi-input function nodes** | When several source attributes map to one target property, render a **single** function node with one input connector per source attribute fanning into the circle and one output to the target property, instead of several separate circles. The composite primary key (e.g. `order_id` + `product_id` -> `_key`) is the canonical case. `_key` is **read-only / informational**: the loader always derives it from the primary key joined by `key_separator` (`NodeTransformer._generate_key`), and edge `_from`/`_to` are built from the same join, so a user-defined `_key` expression would be ignored at load and would desync edges. The `_key` node is therefore muted/dashed (default cursor, legible `--text-muted` in both themes), and clicking it (from the node, detail panel, or context menu) shows an explanation rather than the expression editor. Non-`_key` properties remain fully editable, and their editor defaults inputs to all contributing source columns. | Done |

#### Sequencing & dependencies

- P5g.1 (theme) and P5g.2 (top-bar overflow) are independent, low-risk, and unblock a cleaner shell first.
- P5g.7 (progressive disclosure) changes the connector-drawing logic (`renderSourcePane` / `renderTargetPane` / `drawConnectors`) and is a prerequisite for P5g.5 (highlight) reading cleanly, since highlight targets must be visible.
- P5g.3 / P5g.4 / P5g.6 share a slide-out/overlay shell; build the shell once and host Sources, Targets, and the project explorer as sections.

#### Out of scope (V1 of Phase 5g)

- A full dockable / rearrangeable panel system (drag panels between zones).
- Multi-project tabs or split canvases.
- Server-side persistence of layout preferences (V1 uses `localStorage`).

---

### Phase 8: External data catalog integration -- 8a–8b implemented; 8c–8d planned

> **Distinct from Phase 5d.** Phase 5d is r2g's *own internal* catalog (its
> sources / projects / mappings, optionally ArangoDB-backed). Phase 8 connects
> r2g to *external enterprise data catalogs* (e.g. OpenMetadata, AWS Glue Data
> Catalog, Atlan) and uses them as an upstream **discovery** layer: browse the
> catalog, pick a database/schema/table (or file collection / Kafka topic) the
> user has access to, and register it as an r2g migration source. r2g still
> connects to the underlying data store itself to perform the migration.

**Motivation.** Enterprises increasingly treat a data catalog as the system of
record for "what data exists and where." Customers asked whether r2g can read
those catalogs so a user can select a database from the catalog instead of
hand-typing connection details. This lowers onboarding friction and slots
r2g into governed data estates.

#### Research summary (2026-06; see `docs/internal/PLAN-external-data-catalogs.md` for the cited landscape)

A cited research pass evaluated the catalog landscape against r2g's
"discover-then-connect" need. Key findings (with confidence):

- **OpenMetadata (OSS)** — *high confidence, recommended first integration.*
  Official type-safe Python SDK over a REST API; a canonical
  `DatabaseService → Database → Schema → Table → Column` hierarchy covering
  exactly r2g's sources (PostgreSQL, MySQL, SQL Server, Snowflake) plus Kafka
  via messaging services; service entities carry real connection metadata
  (host/port, db name, SSL). Self-hostable via Docker → end-to-end testable.
- **AWS Glue Data Catalog** — *high confidence.* `Connection` objects store
  genuine connection metadata (credentials, URI, VPC), reusable across
  sources/targets. AWS-centric; needs an AWS account (no local e2e).
- **Atlan** — *high confidence on API.* Mature official `pyatlan` SDK,
  API-token auth, 400+ asset types (SQL + object-store categories). Commercial;
  needs a tenant/sandbox.
- **DataHub (OSS)** — *partial.* Pull-based ingestion + entity-aspect model
  confirmed, but several read/discovery API claims were **refuted in
  verification** and must be re-validated before relying on its read API.
- **Collibra** — *medium / likely governance-only.* GraphQL "Knowledge Graph"
  API (BETA); no confirmed exposure of underlying-source *connection* metadata,
  which would limit discover-then-connect.
- **Common metadata model** — every catalog converges on
  `Source/Service → Database/Asset → Schema → Table → Column` plus
  tags/classification and lineage, so a single catalog-agnostic abstraction is
  feasible.

**Important caveats (do not treat as settled):** (1) Market-share / adoption
evidence (Gartner/Forrester position, revenue, customers, GitHub stars) was
**not** established by the research and remains an open question — the ordering
below rests on *API suitability and testability*, not verified penetration.
(2) A recurring constraint is that catalogs **encrypt/mask credentials on API
read**: r2g can read host / db-type / db-name for discovery, but the user must
still supply credentials at connect time. The design therefore reuses r2g's
existing `$ENV_VAR` connection-string convention and `r2g secrets` store rather
than expecting catalogs to hand over secrets.

#### Requirements

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P8.1** | **Catalog provider abstraction** | A `CatalogProvider` Protocol (mirroring `SourceConnector`): `list_data_sources()`, `list_databases()/list_schemas()/list_tables()`, `search(query)`, and `resolve_source(asset_ref) → ResolvedSource`. A normalized `CatalogAsset` model (provider, fqn/id, kind, name, `source_type`, connection hint without secrets, tags, parent refs). A `create_catalog_provider(provider_type, …)` factory with lazy imports, matching `create_source_connector`. | P6.5 |
| **P8.2** | **Catalog registry in the catalog** | New `CatalogProviderConfig` (provider type, endpoint, auth token) persisted alongside `SourceConfig`/`TargetConfig`, with the auth token encrypted at rest via the existing Fernet layer and redacted in API/log output. CLI: `r2g catalog add/list/remove`. | P8.1, P5d-D4 |
| **P8.3** | **OpenMetadata provider** | First concrete provider (`r2g-arango[openmetadata]`, official SDK): browse the `DatabaseService → Database → Schema → Table` hierarchy + messaging services (Kafka), map a selected asset to a `source_type` + connection hint. | P8.1 |
| **P8.4** | **Discover-then-connect bridge** | `r2g catalog browse <name> [--search …]` lists/searches assets; `r2g catalog import-source <catalog> <asset-fqn> --as <source-name>` resolves the asset to a `SourceConfig` and registers it as a normal r2g source (credentials supplied via `$ENV_VAR` / `r2g secrets`, never assumed from the catalog). Everything downstream (`source snapshot`, mapping, `stream`) is unchanged. | P8.3 |
| **P8.5** | **UI catalog browser** | "Import from catalog" path in the **+ New source** flow: pick a registered catalog → searchable tree (service → database → schema → table) → selection pre-fills the new-source form (host/type/db), leaving credentials to the user. | P8.4 |
| **P8.6** | **MCP catalog tools** | `list_catalogs`, `catalog_browse`, `catalog_import_source` MCP tools so agents can discover and register sources from a catalog. | P8.4 |
| **P8.7** | **AWS Glue provider** | Second provider via `boto3`: list Glue databases/tables and resolve `Connection` objects (which carry real connection metadata). | P8.1 |
| **P8.8** | **Atlan / DataHub provider** | Third provider — Atlan via `pyatlan` (commercial breadth) and/or DataHub (OSS), the latter pending re-validation of its read API. | P8.1 |

#### Phasing

- **8a (foundation + OpenMetadata):** P8.1–P8.4. OSS + dockerizable → unit + end-to-end integration tests in CI.
- **8b (UI + MCP):** P8.5, P8.6.
- **8c (Glue):** P8.7. Unit-tested with mocked `boto3`; live verification is a field exercise (needs AWS).
- **8d (Atlan / DataHub):** P8.8.
- **Later / out of scope for V1:** *publishing back* to the catalog — registering the resulting ArangoDB graph + column-level lineage as a downstream governed asset. Valuable for governance but a separate write-path effort.

#### Non-functional notes

- **Credentials.** Providers expose host/type/db for discovery; credentials are supplied by the user (env var or `r2g secrets`), consistent with the masked-on-read reality of catalog APIs and r2g's existing security model.
- **Testability gradient.** OpenMetadata (Docker) is end-to-end testable in CI; Glue/Atlan need cloud accounts and are unit-tested with mocked SDKs plus manual field validation — mirroring how Snowflake is handled today.
- **No catalog writes in V1.** Phase 8 is read-only against external catalogs.

---

### Phase 9: Classification propagation & entitlement-aware loading -- Planned

> **Built on the Phase 8 catalog backbone.** The moment r2g copies data out of a
> relational source into ArangoDB it creates a new system of record with *none*
> of the source's access controls. The source enforced who-sees-what via
> GRANTs, row-level security, column masking, and catalog policy; the graph copy
> has none of that by default. Worse, graph denormalization creates *new*
> sensitivity — joining `customers` + `orders` + `payments` into one
> neighborhood can expose a combined picture no single source table revealed
> (the **mosaic effect**). Phase 9 is therefore a governance problem the
> migration itself creates, not a feature bolted on.

**Lane discipline (non-negotiable).** r2g is a *migration tool, not a runtime
authorization engine*. It must not pretend to enforce access at query time.
The defensible posture is three tiers, only the first two of which r2g owns:

1. **Capture & propagate** (squarely r2g's job, cheap given Phase 8). The
   portable unit of entitlement is **not** engine-specific GRANTs (which do not
   map across systems or to graph consumers) — it is the
   **classifications / tags / owners / tiers** that live in the catalog
   (OpenMetadata already holds PII/PHI tags, confidentiality tiers, ownership,
   glossary terms). Pull those during discovery and stamp them onto the target
   graph as collection- and field-level annotations (`_classification`,
   `_sensitivity`, `_source_owner`) plus column-level lineage
   (`source table.column → graph property`). This makes the graph *governable*
   even though r2g does not enforce.
2. **Advise & gate** (also r2g's job). A pre-load **entitlement report** —
   "these 7 mapped fields are classified Restricted/PII; confirm before
   exfiltrating into ArangoDB" — with *exclude-above-a-threshold by default*,
   plus **transform-at-load** for sensitive fields (the existing field
   expression engine makes `HASH(@ssn)` / tokenize / redact a natural fit). The
   right answer to "how do we protect it?" is often *don't copy it in the clear*.
3. **Enable enforcement** (**not** r2g — the graph platform / app). ArangoDB has
   no native row/column security (collection-level RBAC in Enterprise,
   app-level otherwise), so r2g emits the *inputs* for whatever enforces:
   per-collection classification + suggested ArangoDB RBAC, an OPA/Rego policy
   artifact, and/or a **tier-based physical layout** (separate
   collections/databases/graphs per sensitivity tier) so coarse collection-RBAC
   can actually bite. r2g produces metadata + recommendations; the serving layer
   enforces.

**Motivation.** Customers operating in governed data estates need the
relational→graph boundary to preserve — and visibly account for — data
sensitivity. The high-leverage, uniquely-r2g value is carrying governance
metadata across that boundary and *refusing to silently launder sensitive
data*, without overreaching into being an authz engine it cannot be.

#### Data-model reality (sizes the work honestly)

Today `CatalogAsset` carries `tags: list[str]` **only at asset (table/database)
level**, and `ResolvedSource` drops tags entirely — so nothing flows past
discovery. Phase 9's real first task is not "stamp tags"; it is a **data-flow
thread**: column-level classification capture → a carrier through
`resolve_source` → `SourceConfig` → snapshot (`Column`) → mapping
(`CollectionMapping` / `FieldExpression`) → target collection/field annotations
+ lineage. Tiers 2 and 3 hang off that backbone.

#### Requirements

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P9.1** | **Column-level classification capture** | Extend the catalog provider + `CatalogAsset` to read column-level classifications (tags, glossary terms), owners, and confidentiality tier — not just asset-level tags. OpenMetadata exposes these via `tables?fields=columns,tags,owners` and the classification/glossary APIs. A normalized `Classification` model (tag FQN, source, confidence) keyed by column. | P8.3 |
| **P9.2** | **Classification carrier through the pipeline** | Thread classifications from `resolve_source` → `ResolvedSource` → `SourceConfig` (persisted) → snapshot, annotating `Column` with an optional classification, with a **sensitivity lattice** (e.g. `public < internal < confidential < restricted`) that orders tiers for comparison and rollup. | P9.1 |
| **P9.3** | **Target annotation & column-level lineage** | Stamp classification onto the target: collection- and field-level annotations (`_classification`, `_sensitivity`, `_source_owner`) emitted by the transformers, plus a **lineage manifest** mapping each `source table.column` to its graph property/edge. Stored as governance metadata (sidecar manifest + optional per-collection metadata document), never as silent data loss. | P9.2 |
| **P9.4** | **Mosaic recomputation** | A vertex/edge assembled from multiple source columns/tables takes the **maximum** sensitivity of its contributors (never blindly inherits one column's tier). Recomputed at mapping-build time over fan-in (`FieldExpression.sources`), edge endpoints, and denormalized neighborhoods; surfaced in the report and annotations. | P9.2 |
| **P9.5** | **Pre-load entitlement report + threshold gate** | `r2g entitlements report <project>` (and a UI panel) lists every mapped field at/above a configurable sensitivity threshold with its source lineage; `r2g load` **excludes above-threshold fields by default** and requires explicit confirmation / override to copy them. | P9.3, P9.4 |
| **P9.6** | **Transform-at-load masking** | First-class masking for classified fields via the existing expression engine: `HASH` / `TOKENIZE` / `REDACT` / `NULLIFY` helper expressions and a one-click "mask this field" in the mapper. Masking choice is recorded in the lineage manifest. | P9.3 |
| **P9.7** | **Enforcement artifact (for the serving layer)** | Emit, on load, a **classification manifest** (machine-readable) plus optional artifacts the serving layer can enforce: suggested ArangoDB collection-RBAC grants per tier, an OPA/Rego policy stub, and/or a **tier-based physical layout** recommendation (per-tier collections/databases/graphs). r2g never enforces these itself. | P9.3, P9.4 |
| **P9.8** | **Classification re-sync (staleness)** | A one-time copy snapshots policy at migration time; source policy drift will not propagate. Provide `r2g catalog resync-classifications <project>` to refresh annotations from the catalog, and carry classification *changes* through CDC/temporal mode (policy itself is not carried). Staleness (last-synced timestamp) is surfaced. | P9.3 |
| **P9.9** | **Sensitivity lens + entitlement panel (UI)** | A workspace **classification lens** (paint-only, per the UI architecture contract) coloring collections/fields by sensitivity tier with a legend, and a floating entitlement-report panel feeding the pre-load gate — no new routes, context-menu-primary. | P9.5 |

#### Phasing

- **9a (capture & propagate):** P9.1–P9.4. The data-model backbone, lineage, and
  mosaic rule. Fully unit-testable with the mocked OpenMetadata HTTP seam + an
  OpenMetadata e2e that seeds column tags; no enforcement surface yet.
- **9b (advise & gate):** P9.5, P9.6, P9.9. The entitlement report, threshold
  gate, transform-at-load masking, and the sensitivity lens.
- **9c (enable enforcement):** P9.7, P9.8. The classification manifest + RBAC/OPA
  /tier-layout artifacts and classification re-sync.

#### Non-functional notes

- **Lane discipline.** r2g emits governance *metadata and recommendations*; it is
  never in the query-time authorization path. Documentation and artifact names
  must make this boundary explicit.
- **Mosaic = max.** Combined-entity sensitivity is recomputed (max of
  contributors), not inherited from a single column — a first-class rule, not a
  footnote.
- **Identity is catalog-anchored, not GRANT-anchored.** Source DB roles ≠
  ArangoDB users ≠ end-user SSO identities; entitlements are expressed against
  the catalog's identity-agnostic tag/tier/owner layer (ultimately org IdP
  groups), which is exactly why GRANTs are the wrong carrier.
- **Safe by default.** Above-threshold fields are excluded unless explicitly
  confirmed; the migration refuses to silently launder sensitive data.
- **Reuse, don't reinvent.** Token/owner metadata rides the existing Fernet +
  redaction layer; masking rides the existing `FieldExpression` engine; the lens
  rides the existing paint-only lens infrastructure (Phase 5e).
- **Out of scope (V1).** Runtime enforcement; pulling engine GRANTs/RLS; mapping
  catalog policies to ArangoDB users automatically; writing classifications
  *back* to the catalog (a Phase 8 write-path concern).

---

### Phase 10: LLM-assisted ontology derivation -- Planned

> **The LLM proposes; the deterministic pipeline disposes.** Today a target
> graph is derived mechanically by `ConfigManager.generate_default_config`
> (tables → document collections, join tables → edges, FKs → edges) and refined
> by hand in the mapper. Phase 10 adds an *optional* path where a large language
> model analyzes the introspected schema and **proposes** a richer target
> ontology — which tables are really vertices vs. edges, implicit/undeclared
> relationships, denormalization/embedding opportunities, and clearer
> collection/property names. Crucially, the LLM never writes to the graph: its
> output is a candidate `MappingConfig` that flows through the **same**
> `validate_config` → mapper review → loader path as every other mapping. The
> deterministic Auto-Map remains the default and the safety net.

**Motivation.** `generate_default_config` is correct but naive: it mirrors the
relational structure 1:1 and cannot recognise that, say, an `order_items` table
is better modelled as an edge with properties, that two tables form an
inheritance hierarchy, or that a lookup table should be embedded rather than
linked. A domain-aware model can propose a graph that a human would otherwise
hand-craft, collapsing the modeling effort from hours to a review. Customers
have asked for exactly this "describe my domain, suggest the graph" capability.

**Design principles (non-negotiable).**
- **Human-in-the-loop, never auto-applied.** A proposal is rendered as a *diff*
  against the current (or Auto-Map) mapping; the user accepts / edits / rejects
  per item. Nothing is saved or loaded without explicit confirmation.
- **Schema-grounded, not data-dumping.** The model is fed the *introspected
  metadata* (tables, columns, types, PKs, FK-inference results) — never bulk row
  data. Optional value sampling is opt-in and **classification-aware**: columns
  tagged Restricted/PII by Phase 9 are never sent to an external model.
- **Validated, hallucination-resistant.** Every proposed collection/edge/field
  is checked against the real schema (`validate_config`); references to
  non-existent tables/columns are dropped or flagged, never silently loaded.
- **Reproducible & auditable.** The model, prompt, parameters (temperature 0 by
  default), and raw response are stored as provenance on the project.
- **Provider-agnostic & optional.** An `LLMProvider` abstraction (mirroring
  `CatalogProvider` / `SourceConnector`) with lazy imports; the whole feature is
  an optional extra and absent-by-default — r2g never requires an LLM.

#### Requirements

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P10.1** | **LLM provider abstraction** | An `LLMProvider` Protocol + `create_llm_provider(provider_type, …)` factory with lazy imports (OpenAI first; Anthropic / local/OpenAI-compatible endpoints to follow), mirroring `create_source_connector` / `create_catalog_provider`. API keys supplied via env / `r2g secrets` (`$ENV_VAR` convention), never persisted in plaintext. | P6.5 |
| **P10.2** | **Schema-grounded prompt builder** | Serialize the introspected `Schema` (tables, columns, types, nullability, PKs) + accepted/inferred FKs into a compact, model-friendly description, with an optional user **domain hint**. Redaction-aware: excludes (or masks) columns classified Restricted/PII per Phase 9; optional, opt-in value samples are bounded and classification-filtered. | P10.1, P9.2 (soft) |
| **P10.3** | **Ontology proposal → `MappingConfig`** | The model returns a **structured** ontology proposal (JSON-schema-constrained): vertex vs. edge designation, new/implicit relationships, embed-vs-link recommendations, and name suggestions. r2g maps this to a candidate `MappingConfig`, then runs `validate_config`; invalid items are repaired or dropped with reasons. | P10.2 |
| **P10.4** | **Human-in-the-loop review (diff)** | The proposal is presented as a `diff_mappings`-style diff against the current/Auto-Map mapping; users accept / modify / reject per collection / edge / property. No write occurs without confirmation. | P10.3 |
| **P10.5** | **CLI + API entry points** | `r2g ontology suggest <project> [--domain "…"] [--sample] [--apply]` and `POST /api/projects/{name}/suggest-ontology`; returns the proposal (+ validation notes), applying only on explicit `--apply` / UI accept. | P10.3 |
| **P10.6** | **Provenance & reproducibility** | Persist the provider, model, prompt, parameters, and raw response on the project (and surface cost/latency). Default `temperature=0` for repeatable proposals. | P10.3 |
| **P10.7** | **UI: "Suggest model (AI)" action + review panel** | A canvas/Actions entry that runs a suggestion and opens a floating review panel showing the proposed diff with per-item accept/reject — context-menu-primary, no new route, consistent with the workspace contract. | P10.4, P10.5 |
| **P10.8** | **Guardrails** | Privacy (no egress of classified columns; explicit opt-in for any value sampling), determinism, the `validate_config` gate, prompt-injection hardening of schema-derived text, and cost/rate limits with a hard token budget. | P10.2, P10.3 |

#### Phasing

- **10a (grounded proposal, structure-only):** P10.1–P10.3, P10.5 (CLI) + P10.6,
  P10.8. Metadata-only prompts (no row data), structured output → validated
  `MappingConfig`. Fully unit-testable with a **fake `LLMProvider`** seam (canned
  responses), exactly like the mocked-HTTP catalog/connector tests.
- **10b (review & apply in the Studio):** P10.4, P10.7. The diff review panel and
  apply path on top of the existing `diff_mappings` + mapper.
- **10c (enrichment):** opt-in, classification-aware value sampling; richer
  denormalization/embedding suggestions; additional providers
  (Anthropic / local) and domain-hint refinement.

#### Non-functional notes

- **Determinism & honesty.** Proposals are suggestions, scored and explained;
  the deterministic Auto-Map is always available and is the default. r2g states
  clearly when output is model-generated.
- **Privacy / governance tie-in.** Phase 10 explicitly respects Phase 9
  classifications: sensitive columns are withheld from (external) models — the
  migration must not leak via the *modeling* step either.
- **Optional & provider-agnostic.** Shipped as an extra (e.g.
  `r2g-arango[llm]`); no LLM dependency or network call unless the user invokes a
  suggestion.
- **Out of scope (V1).** Autonomous mapping without review; fine-tuning;
  LLM-driven data *transformation* at load time (expressions remain
  user-authored); natural-language querying of the resulting graph.

---

### Phase 11: Denormalization & normal-form analysis -- Planned

> **Deterministic, no LLM.** Phase 10 asks a model to *propose* a better
> ontology; Phase 11 is the complementary **rules + sampling** engine that
> *detects* denormalization in the source and advises on it — reusing exactly
> the machinery already proven in `fk_inference.py` (name heuristics + bounded
> value-overlap sampling via a pluggable sampler). It recognises patterns the
> 1:1 `generate_default_config` is blind to: embedded lookups, repeating column
> groups, multi-valued columns, and over-split 1:1 tables — and surfaces them as
> scored, evidence-backed *findings* the user can act on. It advises; it never
> silently rewrites the schema or the data.

**Motivation.** Real relational schemas are rarely clean 3NF. A table often
carries an *embedded lookup* (`zip → city, state`; `product_id → product_name,
category`), **repeating groups** (`phone1/phone2/phone3`), **multi-valued
columns** (`tags = "a,b,c"`), or redundant reference data duplicated across many
rows. Mapped 1:1, these become awkward graph models (redundant properties, no
shared vertices, list-in-a-string). Detecting them lets r2g recommend a better
target — extract a vertex, embed an array, split a multi-valued column — and also
produces high-quality, deterministic *grounding* for the Phase 10 LLM proposal.

**Design principles.**
- **Statistical signal, not proof.** Findings carry a confidence + concrete
  evidence (sampled counts/examples), exactly like FK inference; the user
  decides. Bounded, resilient sampling (sampler errors degrade to name/structure
  signals, never crash).
- **Advise, don't auto-rewrite.** V1 emits findings + a recommended action;
  applying a remediation (scaffolding a collection/edge or an embed expression)
  is explicit and optional.
- **Classification-aware (Phase 9 tie-in).** Value sampling reads data, so
  Restricted/PII columns are excluded from sampling — consistent with the
  governance lane.
- **Reuse, don't reinvent.** Same `create_value_sampler` seam, bounded-`LIMIT`
  queries, and Suggest-FKs-style review card as P6.6.

#### Requirements

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P11.1** | **Denormalization analyzer engine** | A deterministic `analyze_denormalization(schema, options, sampler=None) -> list[DenormFinding]` (new `src/r2g/denorm.py`), mirroring `infer_foreign_keys`: a pure-Python structural/name core plus an optional, bounded value sampler. `DenormFinding` carries kind, table, columns, recommended action, confidence, and evidence. | P6.6 |
| **P11.2** | **Repeating-group detection** | Recognise numbered/suffixed column families (`phone1/phone2`, `addr_line_1..3`) and repeated typed sets → recommend a child collection or an embedded array. Name + type structural heuristic; no sampling required. | P11.1 |
| **P11.3** | **Functional-/transitive-dependency detection (2NF/3NF)** | The flagship: detect a non-key column that *determines* other non-key columns (embedded lookup) by sampling — group by the candidate determinant and test whether dependents are single-valued per group. Recommend extracting the determinant+dependents into a shared vertex linked by an edge. | P11.1 |
| **P11.4** | **Redundant reference-data detection** | Flag sets of co-varying columns whose distinct combinations are small relative to row count (duplicated reference data) → candidate lookup/vertex extraction. Sampling-based, with distinct-ratio evidence. | P11.1 |
| **P11.5** | **Multi-valued attribute detection** | Detect delimited lists in a text column (`"a,b,c"`) via sampling for consistent delimiters → recommend splitting into an array or child collection. | P11.1 |
| **P11.6** | **1:1 over-normalization detection** | Detect two tables in strict 1:1 on a shared/identical key → recommend merging/embedding rather than two collections + an edge (the inverse modeling smell). | P11.1 |
| **P11.7** | **CLI + API entry points** | `r2g source analyze-denorm <name> [--sample] [--sample-limit N] [--min-confidence 0.4]` and `POST /api/sources/{name}/analyze-denorm` → scored findings with evidence and recommended actions (Rich table / JSON). Read-only by default. | P11.1 |
| **P11.8** | **UI findings panel** | A Suggest-FKs-style floating card listing findings with confidence pill + evidence + recommended action and per-finding Accept / Dismiss, opened from the Actions / canvas context menu — no new route, context-menu-primary. | P11.7 |
| **P11.9** | **Remediation scaffolding (opt-in)** | Accepting a finding can scaffold the mapping: e.g. create the extracted collection + `EdgeDefinition`, add an embed/split `FieldExpression`, or merge a 1:1 pair — written through the normal mapping save + `validate_config`, never applied silently. | P11.8 |
| **P11.10** | **Grounding for Phase 10** | Findings are emitted in a form the Phase 10 prompt builder can consume, so the LLM proposal is grounded in deterministic evidence (and the two can cross-check). Phase 11 itself requires no LLM. | P11.1 |

#### Phasing

- **11a (engine + CLI, the deterministic core):** P11.1–P11.3, P11.7. Structural
  detectors + the functional-dependency sampler + CLI, fully unit-testable with a
  fake sampler (canned counts), exactly like `test_fk_inference.py`.
- **11b (more detectors + Studio review):** P11.4–P11.6, P11.8.
- **11c (remediation + grounding):** P11.9, P11.10.

#### Non-functional notes

- **Bounded & resilient.** All sampling uses bounded `LIMIT` queries and degrades
  to name/structure signals on error — the analyzer never blocks or crashes a
  workflow (same contract as `PostgresValueSampler`).
- **Advisory by default.** Findings change nothing until a remediation is
  explicitly accepted and validated.
- **Privacy.** Sampling honours Phase 9 classifications; Restricted/PII columns
  are not sampled.
- **Complements, not replaces.** The deterministic 1:1 Auto-Map remains the
  default starting point; Phase 11 layers advice on top and grounds Phase 10.
- **Out of scope (V1).** Automatic schema rewriting; full functional-dependency
  *mining* (we test *candidate* determinants from keys/heuristics, not all
  column subsets); BCNF/4NF formalism; cross-table deduplication of entities
  (entity resolution).

---

## 6. Future considerations (Phase 7+) -- Exploratory

These ideas are exploratory and represent potential directions, not committed work. Each would require significant design effort.

- **Additional source databases:** MySQL/MariaDB and SQL Server have been added following the `SourceConnector` pattern established in Phase 6; Oracle, SQLite, and others could follow the same way (source-specific schema reader, type map, streaming adapter).
- **Publishing back to external catalogs:** the inverse of Phase 8 — register the migrated ArangoDB graph (and column-level lineage from source to graph) as a governed downstream asset in the connected catalog. A write-path effort, deferred until the Phase 8 read path proves out.
- **Ontology derivation (LLM integration):** *Promoted to a committed phase — see Phase 10: LLM-assisted ontology derivation.* Use a large language model to analyze the source schema and propose an optimized target ArangoDB graph schema for a given domain (vertices vs. edges, implicit relationships, denormalization strategies), reviewed by a human and validated through the existing mapping pipeline.
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
| **Usability & Safety** | **April 2026** | `.env` file and environment variable support (`PG_CONN`, `ARANGO_ENDPOINT`, `ARANGO_DB`, `ARANGO_USER`, `ARANGO_PASSWORD`) via python-dotenv -- credentials no longer required in CLI args. `validate-data` command checks FK referential integrity of dump files before import. Topological import ordering ensures FK targets are loaded before sources; circular FK deps detected and warned. `--since` timestamp filtering for basic incremental streaming. PK-less table warnings during validation and streaming. `.env.example` template. Snowflake integration planned as Phase 6. 373 tests. |
| **CDC Foundation** | **April 2026** | Phase 3 foundation: `ChangeEvent` model, `ArangoDelta`, `TransactionBatch`, `DeltaTransformer`, `CDCHandler`, `ArangoWriter` single-document ops with retry logic. 409 tests. |
| **CDC Listener** | **April 2026** | Phase 3 P3.1 + P3.3: `PGReplicationListener` manages logical replication slots, polls via `pg_logical_slot_get_changes`. Output plugin parsers for `test_decoding` and `wal2json`. Continuous polling loop with graceful shutdown. CLI commands: `cdc-setup`, `cdc-teardown`, `cdc-status`, `cdc-start`. 453 tests. |
| **Conflict Resolution** | **April 2026** | Phase 3 complete (P3.4): Configurable conflict policies (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`). `ConflictResolver` wraps writes with error classification and policy-based resolution. `ConflictLog` for session conflict tracking. `--conflict-policy` CLI option on `cdc-start`. LWW uses `_r2g_lsn` field for per-document LSN tracking. 468 tests. |
| **Kafka Integration** | **April 2026** | Phase 4 complete: DebeziumParser for Debezium JSON envelope, FlatJsonParser for custom producers, KafkaConsumer wraps confluent-kafka with batch polling and at-least-once offset commits, graceful shutdown. kafka-start CLI command. Optional dependency. 502 tests. |

| **Visual Mapper PRD** | **April 2026** | Phase 5b added: Visual Graph Data Mapper & Ingestion Engine. Three epics (Data Catalog Interface, Visual Mapping Interface, Ingestion & Execution Engine) incorporating TigerGraph-inspired split-screen mapping UI, automated introspection, parallel ingestion, job monitoring, and cascading referential integrity. Mapped against existing implementation (catalog, mapping diff, selective reload, Mapping Studio UI). |
| **Expression + Graph-of-Graphs** | **April 2026** | Phase 5c added: Expression Mapping & Graph-of-Graphs UI. Introduces `FieldExpression` first-class data model (fan-in of 1..N source properties into a single target property via AQL / KSQL / Python expressions, identity default), mapping function nodes rendered as circles on connector lines with an inline expression editor, fan-in via drag-and-drop, and mini ER/graph-model visualizations within the source and target panes (FK edges in-pane on the left, edge-collection arrows in-pane on the right). Phase 5d added: ArangoDB-backed data catalog (sources, targets, snapshots, projects, mappings, loads as a named graph). |
| **Phase 5b Execution Engine** | **April 2026** | P5b.3.1 Load trigger, P5b.3.2 SSE progress monitoring, and P5b.3.3 dead-letter queue (`src/r2g/dlq.py`) implemented. Target graph introspection (P5b.1.3) landed via `src/r2g/connectors/arango_reader.py`, `TargetConfig` in the catalog, and `/api/targets*` endpoints. Cascading deletes (P5b.1.4) wired through `remove_source(cascade=True)` and `DELETE /api/sources/{name}?cascade=true`. Settings modal + Auto-Map button closed P5b.2.3 / P5b.2.4. MCP server (`src/r2g/mcp_server.py`) exposes catalog and load operations for agent tooling. Composite foreign keys normalized on `EdgeDefinition` via plural `from_fields` / `to_fields` with round-trip serialization. |
| **UI Architecture Upgrade** | **April 2026** | Phase 5e added and implemented: shared primitives (context menu, keyboard registry, `?` help overlay, floating-card stack); right-click menus on every entity; first-class edges with floating edge editor, drag-to-add, and promote-join-table; floating non-blocking progress card replaces the canvas-blocking overlay; bottom timeline strip for recent loads; complete legend; paint-only lens infrastructure (Topology / Coverage / Validation / Diff) with `1`–`4` shortcuts and header status chip; inline silent validation via `POST /api/projects/{name}/validate-draft` with per-entity badges and tooltip issue lists; a11y and hygiene polish (menu / region / complementary roles, `aria-label`s, Esc pops the top overlay, discoverability copy). 673 tests passing. |
| **In-UI catalog management** | **April 2026** | Phase 5e Epic 8 added: in-UI "+ New source", "+ New target", and "+ New project" floating-card forms wired to the existing `POST /api/sources`, `POST /api/targets`, `POST /api/projects` endpoints; right-click "Remove source…" with cascade-delete confirmation listing dependent projects / snapshots / loads; new Targets panel in the left sidebar with introspect + remove actions; empty-state copy now includes direct CTAs. Closes the "how do I even start" gap so first-time users can go from zero to a working mapping without touching the CLI. |
| **Expression evaluator** | **April 2026** | P5c.1.4 closed: new `src/r2g/expressions.py` ships a safe AQL-subset evaluator (literals, `@bind` refs, arithmetic with null propagation, comparisons, `&&`/`||`/`NOT`, `??`, ternary, and the function set `CONCAT`, `CONCAT_SEPARATOR`, `UPPER`, `LOWER`, `SUBSTRING`, `LENGTH`, `LTRIM`, `RTRIM`, `TRIM`, `TO_STRING`, `TO_NUMBER`, `TO_BOOL`, `CONTAINS`, `COALESCE`). `NodeTransformer` applies compiled `field_expressions` to every row, with identity / unsupported-engine / uncompilable fallbacks. `validate_config` parse-checks expressions and reports unresolved `@bindings`. UI gains a live compile indicator (`/api/expressions/compile`) and a functions-list endpoint (`/api/expressions/functions`), with save blocked on syntactically invalid expressions. 732 tests passing. |
| **Credential encryption at rest** | **April 2026** | Phase 5d non-functional D4 closed for the filesystem catalog: new `src/r2g/security.py` encrypts `SourceConfig.connection_string` and `TargetConfig.password` with Fernet (AES-128-CBC + HMAC-SHA256) using a key sourced from `R2G_SECRET_KEY` env var or a 0600 key file (`~/.r2g/secret.key`). Tagged `enc:v1:` envelope keeps legacy plaintext catalogs readable and transparently upgrades them on next save. UI responses (`GET /api/sources`, `GET /api/targets`) are redacted via `redact_connection_string` / `redact_for_display`; new `r2g secrets init|status|migrate` commands manage the key and force-encrypt existing catalogs. Closes the urgent gap created when Phase 5e.8 surfaced credential capture in the browser. 758 tests passing. |
| **Snowflake introspect-only slice** | **April 2026** | Phase 6 opened with the "thin slice": P6.5 source abstraction (new `r2g.connectors.base.SourceConnector` Protocol + `create_source_connector` factory), P6.1 `SnowflakeConnector` reading `INFORMATION_SCHEMA.TABLES/COLUMNS` plus `SHOW PRIMARY KEYS`/`SHOW IMPORTED KEYS`, and P6.2 Snowflake-aware `DEFAULT_TYPE_MAP` entries (`NUMBER`, `VARIANT`, `ARRAY`, `OBJECT`, `TIMESTAMP_*`, `BINARY`, `VECTOR`, geo types). `snowflake-connector-python` is an optional extra (`pip install 'r2g-arango[snowflake]'`); the UI, MCP server, and `r2g source snapshot` CLI dispatch through the factory based on the stored `source.source_type`, with a 501 + install hint when the extra is missing. Snowflake appears in the "+ New source" dropdown with a Snowflake SQLAlchemy-style connection-string hint. Dump (P6.3), streaming (P6.4), and FK inference (P6.6) remain planned. 785 tests passing. |
| **FK inference** | **April 2026** | P6.6 closed: new `src/r2g/fk_inference.py` provides a pure-Python name-heuristic engine with pluggable value-overlap sampling. Candidates are produced from `{prefix}_id`, `{prefix}id`, `{prefix}_{pkcol}`, non-generic PK-name matches, and a composite-PK pass, filtered by JSON-level type compatibility and ranked by confidence (bonuses for identical data types, identical nullability, and sampler-reported overlap ≥0.5 / ≥0.9; veto on zero overlap). `PostgresValueSampler` runs bounded `LEFT JOIN` queries to score value overlap (PG only this slice). New `POST /api/sources/{name}/infer-fks` endpoint + `GET /api/projects/{name}` lookup, new "Suggest FKs" toolbar button / `i` shortcut / canvas context-menu entry opening a floating card with per-row Accept / Accept all / Dismiss actions that write real `EdgeDefinition`s into the mapping (collision-proof naming preserves existing edges). New `r2g source infer-fks <name> [--sample] [--accept]` CLI command prints a Rich table and optionally writes accepted candidates into a fresh snapshot. 819 tests passing, 34 new. |
| **Temporal graph mode (Phase 5)** | **June 2026** | Phase 5 implemented: `src/r2g/temporal/` (`TemporalConfig` / `TemporalNaming`, `TemporalApplier`, point-in-time / version-history / interval AQL templates in `queries.py`). CDC and Kafka pipelines gain `--temporal` versioned writes (ProxyIn / Entity / ProxyOut + `hasVersion` edges, `created`/`expired` intervals, soft-delete), `--ttl-seconds` retention with sparse TTL + `mdi`-prefixed interval indexes (P5.5/P5.6), and `--smart-field` SmartGraph key prefixes (P5.8). Covered by `test_temporal_applier.py`, `test_temporal_models.py`, `test_temporal_queries.py`. Live-workload validation pending. |
| **Demo sample databases + UI polish** | **June 2026** | Added Chinook and Pagila PostgreSQL samples plus a CSV library-system sample under `docker/` with a `load-samples.sh` helper and `docker-compose.yml` wiring for Friday-demo readiness. UI polish: R2G application rebrand with custom icon (`src/r2g/ui/static/r2g-icon.png`) and ArangoDB color scheme; targets rendered as a cluster→database tree; draggable / resizable inter-frame boundaries; non-ArangoDB-green PK badge color; project edit/delete in-UI (`PATCH`/`DELETE /api/projects/{name}`, P5e.8.3); `_system` database protected from loads with non-`_system` target defaults. |
| **CSV primary-key heuristic** | **June 2026** | `CsvConnector` PK detection (`src/r2g/connectors/csv_source.py`) now recognizes `id` plus `{table}_id` / `{singular(table)}_id` / `{table}id` / `{singular(table)}id` (e.g. `customers.csv` keyed on `customer_id`), gated by a uniqueness + non-null check on the read sample so a non-unique column is never marked a key; header-only files fall back to a name-only match. This unlocks FK-inference *targets* for CSV tables that lack a bare `id` column. 6 new tests in `tests/test_csv_connector.py`. 1000 passing, 6 skipped. |
| **CSV value-overlap FK sampling** | **June 2026** | Extended P6.6 FK inference with `CsvValueSampler` in `src/r2g/fk_inference.py`: reads the two candidate CSV files with Polars (type inference disabled so values compare as raw text, avoiding int/float token mismatches), bounded by a row limit, and returns the fraction of distinct local values present in the foreign column — the same statistic `PostgresValueSampler` computes via `LEFT JOIN`. Resilient (missing file / unreadable column → `None`). Wired into `r2g source infer-fks --sample` and `POST /api/sources/{name}/infer-fks` for CSV sources; Snowflake still falls back to name-only. 9 new tests in `tests/test_fk_inference.py`. 992 passing, 6 skipped. |
| **Configurable hints — menus + buttons (Phase 5e.7.4)** | **June 2026** | Context-menu items gained optional explanatory `hint` tooltips (ⓘ marker, shown on hover / keyboard focus, `role="tooltip"`, viewport-clamped) rendered centrally in `openContextMenu`. The same styled tooltip surface was extended to toolbar / panel buttons via `data-hint` + a delegated `mouseover` / `focusin` handler (placed below the button, replacing the old native `title` tooltips; `aria-label`s retained). A single "Show menu hints" toggle (default on, `localStorage` `r2g.menuHints`) in the `?` help overlay controls both. Static-asset-only change to `src/r2g/ui/static/index.html`. |
| **Naming conventions & rename change-management (Phase 5f)** | **June 2026** | Phase 5f added and implemented. New `src/r2g/naming.py` (`split_identifier` / `convert_identifier` / `apply_naming_convention`) and `NamingConvention` model on `MappingConfig`; `POST /api/projects/{name}/apply-naming` + canvas "Apply naming convention…" form (PascalCase collections, camelCase properties/edges by default). Rename change-management: `Project.loaded_mapping` tracking via `CatalogManager.set_loaded_mapping`; identity-based rename detection in `diff_mappings` (source-table identity for collections, relationship identity for edges) emitting parameterized `ReloadAction`s; hardened `SelectiveReloader` (in-place collection/edge rename, edge reload-from-source, parameter-bound property-rename AQL, named-graph rebuild, idempotency + `_system` guards); `GET /api/projects/{name}/migration-plan` + `POST /api/projects/{name}/migrate`; save-time migration prompt overlay. Reserved-attribute protection (`RESERVED_ATTRIBUTES`) across naming, diff, executor, and `validate_config`. Fixed latent edge `_from`/`_to` + named-graph resolution to use target collection names (`ConfigManager.graph_edge_definitions`, `EdgeTransformer` `from_name`/`to_name`). New `tests/test_naming.py`; expanded diff / executor / config / UI-API tests. 983 passing, 6 skipped. |
| **Post-demo UX feedback (Phase 5g)** | **June 2026** | Added Phase 5g (Planned) capturing first-demo feedback: light-mode default + dark toggle (P5g.1), simplified top bar via an action overflow menu (P5g.2), on-demand Sources/Targets (P5g.3) and Properties/detail (P5g.4) surfaces instead of permanent sidebars, bidirectional source↔target mapping highlight (P5g.5), an IDE-style project explorer (P5g.6), and progressive-disclosure mapping detail with per-table / per-collection / per-edge expand/collapse and relationship-neighbourhood cascade (P5g.7), an open-target-database action (P5g.8), function-node discoverability (P5g.9), and composite / multi-input function nodes with a read-only `_key` (P5g.10). **Implemented** in `src/r2g/ui/static/index.html`; remaining gaps: the P5g.3 drawer defaults open and the P5g.7 "show only unmapped / only edges" filters are not yet built. |
| **Snowflake dump + streaming (Phase 6 close-out)** | **April 2026** | P6.3 + P6.4 closed, completing Phase 6. New `r2g.connectors.session.SourceSession` Protocol (`count_rows`, `stream_rows`, `dump_table_to_csv`, `close`) plus concrete `PostgresSession` (`REPEATABLE READ` + server-side cursor + `COPY TO STDOUT` fast path) and `SnowflakeSession` (`BEGIN`/`COMMIT` snapshot + `cursor.fetchmany` streaming + `csv`-module dump). `SourceConnector` gains an `open_session()` method; `StreamingPipeline` is now fully source-agnostic and opens one session per worker (same consistent-snapshot semantics on both backends). `r2g stream --source <name>` resolves a catalog source by `source_type`; the legacy `--pg-conn` still works. New `r2g source dump <name>` subcommand replaces `r2g dump-tables` for catalog-aware dumps. UI `POST /api/projects/{name}/load` dispatches through `create_source_connector` with a 501 + install hint when the Snowflake extra is missing. Backward-compat constructor shim (`StreamingPipeline(pg_conn_string=…)`) keeps existing call sites working. 845 tests passing, 26 new. |

| **External data catalog integration (Phase 8) — Planned** | **June 2026** | Added Phase 8: connect r2g to external enterprise data catalogs (OpenMetadata, AWS Glue, Atlan, …) as an upstream discovery layer (browse → select → import as a source), distinct from the internal Phase 5d catalog. Backed by a cited research pass on catalog API suitability for "discover-then-connect" (`docs/internal/PLAN-external-data-catalogs.md`); OpenMetadata recommended as the first integration (OSS, official SDK, connection metadata, dockerizable for e2e testing). Market-share ranking flagged as unverified. Implementation + test plan drafted for review; no code yet. Also recorded MySQL/MariaDB + SQL Server source connectors shipped since the April baseline. |
| **External data catalog integration (Phase 8a–8b) — Implemented** | **June 2026** | Shipped 8a (foundation + OpenMetadata) and 8b (Studio UI): `src/r2g/catalogs/` provider abstraction + OpenMetadata REST provider (`httpx`, `openmetadata` extra), an encrypted `CatalogProviderConfig` registry, the `r2g catalog add/list/browse/import-source/remove` CLI, Studio `/api/catalogs*` browse/import endpoints with a left-rail Catalogs panel, and unit + skip-when-unavailable e2e tests. MySQL + SQL Server source connectors landed alongside. 1189 unit tests passing, 82% coverage. |
| **Classification propagation & entitlement-aware loading (Phase 9) — Planned** | **June 2026** | Added Phase 9 on the Phase 8 catalog backbone: a three-tier governance posture — capture & propagate catalog classifications/owners/tiers onto target collections/fields + column-level lineage (P9.1–P9.4, incl. the mosaic = max-sensitivity rule), advise & gate via a pre-load entitlement report + exclude-above-threshold default + transform-at-load masking reusing the field-expression engine (P9.5–P9.6, P9.9), and enable enforcement by emitting a classification manifest + suggested ArangoDB RBAC / OPA / tier-layout artifacts (r2g never enforces) plus classification re-sync (P9.7–P9.8). Lane discipline: r2g carries governance metadata and refuses to silently launder sensitive data, but is not a runtime authz engine. Implementation + test plan drafted (`docs/internal/PLAN-classification-entitlement.md`); no code yet. |
| **LLM-assisted ontology derivation (Phase 10) — Planned** | **June 2026** | Promoted the exploratory "ontology derivation" idea to committed Phase 10: an optional `LLMProvider` abstraction proposes a richer target ontology (vertex-vs-edge, implicit relationships, embed-vs-link, naming) from the introspected schema, which flows through the **same** `validate_config` → mapper-review (`diff_mappings`) → loader path as Auto-Map. Design principles: human-in-the-loop (never auto-applied), schema-grounded (metadata not bulk rows), classification-aware (no egress of Phase-9 Restricted/PII columns), validated/hallucination-resistant, reproducible (temperature 0 + stored provenance), and optional/provider-agnostic (deterministic `generate_default_config` stays the default). Implementation + test plan drafted (`docs/internal/PLAN-llm-ontology-derivation.md`); no code yet. |
| **Denormalization & normal-form analysis (Phase 11) — Planned** | **June 2026** | Added Phase 11: a deterministic (no-LLM) analyzer (`src/r2g/denorm.py`) that detects source-side denormalization — embedded lookups (functional/transitive dependencies via bounded value sampling), repeating column groups, multi-valued columns, redundant reference data, and over-split 1:1 tables — and emits scored, evidence-backed findings with recommended graph remedies (extract vertex / embed array / split / merge). Reuses the `fk_inference.py` machinery (name heuristics + `create_value_sampler` bounded sampling + Suggest-FKs-style review card). Advisory by default (no silent rewrite), classification-aware (no sampling of Phase-9 Restricted/PII columns), and grounds the Phase 10 LLM proposal. Implementation + test plan drafted (`docs/internal/PLAN-denormalization-analysis.md`); no code yet. |

The source files `PRD-gemini.md` and `PRD-notebooklm.md` remain in the repository for reference and are superseded by this file.
