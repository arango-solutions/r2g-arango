# R2G-ETL Pipeline

*Product Requirements Document (PRD) -- Experimental Reference Implementation*

| Field | Value |
| :--- | :--- |
| **Product name** | R2G-ETL Pipeline (Relational to Graph -- Extract, Transform, Load) |
| **Version** | 0.1.0 (experimental) |
| **Date** | Originally drafted December 2025, consolidated April 2026 |
| **Status** | Phases 1--4 implemented and hardened; Phases 5--6 are planned or exploratory |
| **Target users** | Database architects, data engineers, and developers evaluating relational-to-graph migration with ArangoDB |

---

## 1. Goals and objectives

The primary goal of the R2G-ETL Pipeline is an experimental, configurable tool for transforming and loading data from relational schemas into ArangoDB graph schemas. It serves as a reference implementation demonstrating the mechanical mapping patterns. While PostgreSQL is the primary supported source, the architecture is designed to accommodate additional relational sources (see Phase 5: Snowflake integration).

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

The roadmap is organized into six phases: four implemented (MVP through Kafka), one planned (Snowflake), and one exploratory (future sources and advanced features).

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

---

## 4. Technical requirements

| Category | Requirement | Details |
| :--- | :--- | :--- |
| **Architecture** | Modularity | Design so data sources can be swapped (e.g., PostgreSQL replaced by Snowflake or MySQL) without rewriting the whole tool. Currently PostgreSQL-only; Snowflake planned (Phase 5). |
| **Target DB** | ArangoDB | Load via `arangoimport` (file-based, Phase 1) and the ArangoDB HTTP API (streaming/CDC/Kafka, Phases 2--4). |
| **Transformation** | Schema mapping | Configurable prefix mapping for `_from` and `_to` (e.g., `user_1` to `Users/1`). |
| **Data integrity** | Key generation | Correct document `_key` values derived from source primary keys, including composite keys joined by a configurable separator. |
| **Technology stack** | Python | Chosen for ecosystem support (psycopg, python-arango, Polars, Pydantic, structlog, confluent-kafka, python-dotenv). |

### Known constraints

- **Referential integrity is opt-in**: the `validate-data` command checks FK values against PK sets from dump files, but this check is not enforced automatically during import. Orphaned references will still produce edges pointing to non-existent vertices if validation is skipped.
- **Bulk load idempotency**: re-running the streaming pipeline with `--drop-collections` replaces all data. For incremental updates, CDC and Kafka pipelines provide configurable conflict resolution (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`) with at-least-once delivery semantics.
- **Credential handling**: connection parameters can be loaded from `.env` files or environment variables (`PG_CONN`, `ARANGO_ENDPOINT`, etc.), but generated import scripts still contain connection defaults. No integrated secrets management (e.g., HashiCorp Vault).

### Phase 5: Snowflake integration -- Planned

Snowflake is a common data warehouse among R2G users. This phase adds Snowflake as a source alongside PostgreSQL, reusing the existing mapping, transformation, and loading infrastructure.

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P5.1** | **Snowflake schema reader** | Connect to Snowflake via the Snowflake Connector for Python (`snowflake-connector-python`) and introspect `INFORMATION_SCHEMA` to extract tables, columns, primary keys, and foreign key constraints (imported/inferred). Output the same `Schema` model used by PostgreSQL. | P1.1 |
| **P5.2** | **Snowflake type mapping** | Map Snowflake data types (`NUMBER`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP_*`, `VARIANT`, `ARRAY`, `OBJECT`, `GEOGRAPHY`, `GEOMETRY`, etc.) to JSON types. `VARIANT`/`OBJECT` map to JSON objects; `ARRAY` maps to JSON arrays. Extend `DEFAULT_TYPE_MAP` with Snowflake-specific entries. | P1.4 |
| **P5.3** | **Snowflake dump export** | `dump-tables` command variant that uses `COPY INTO @stage` or cursor-based extraction to export Snowflake tables as CSV files. Handle Snowflake-specific CSV quoting and NULL representation. | P5.1 |
| **P5.4** | **Snowflake streaming** | `stream` command variant that reads from Snowflake using the Python connector's cursor (Snowflake does not support server-side cursors like PostgreSQL, but supports `fetch_pandas_all()` / `fetch_arrow_all()` for batched reads). Reuse the ArangoDB writer path. Snowflake's `RESULT_SCAN` or warehouse-level snapshot isolation provides read consistency. | P5.1, P2.3 |
| **P5.5** | **Source abstraction layer** | Refactor the schema reader and streaming pipeline behind a `SourceConnector` protocol/ABC so PostgreSQL and Snowflake (and future sources) share a common interface. CLI commands accept `--source-type pg|snowflake` or auto-detect from connection string format. | P5.1, P5.4 |
| **P5.6** | **Snowflake FK inference** | Snowflake does not enforce foreign key constraints (they are informational only and often absent). Provide a `--infer-fks` option that analyzes column naming conventions (e.g., `user_id` matching `users.id`) and value overlap to suggest FK relationships. Require user confirmation via the mapping config. | P5.1 |

#### Snowflake-specific considerations

- **FK constraints are not enforced in Snowflake.** They can be declared but are informational only. Many Snowflake schemas have no FK metadata at all. The FK inference feature (P5.6) addresses this gap.
- **Semi-structured data.** Snowflake `VARIANT`, `OBJECT`, and `ARRAY` columns can contain nested JSON. These should be preserved as nested structures in ArangoDB documents rather than flattened.
- **Large tables.** Snowflake tables can be very large. The streaming path should support `LIMIT`/`OFFSET` pagination or warehouse-level result caching to manage memory. Arrow-based fetching (`fetch_arrow_all()`) provides the best throughput for large result sets.
- **Authentication.** Snowflake supports multiple auth methods (user/password, key-pair, SSO/OAuth, external browser). The connector should accept standard Snowflake connection parameters: `account`, `user`, `password`, `warehouse`, `database`, `schema`, `role`. These should be loadable from env vars (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.) and `.env` files.
- **Cost implications.** Every query against Snowflake consumes warehouse credits. The schema reader and streaming pipeline should minimize the number of queries. `--dry-run` should clearly report query cost implications.

---

## 6. Future considerations (Phase 6+) -- Exploratory

These ideas are exploratory and represent potential directions, not committed work. Each would require significant design effort.

- **Additional source databases:** MySQL, SQL Server, Oracle, and other relational databases could be added following the same `SourceConnector` pattern established in Phase 5. Each requires a source-specific schema reader, type map, and streaming adapter.
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

The source files `PRD-gemini.md` and `PRD-notebooklm.md` remain in the repository for reference and are superseded by this file.
