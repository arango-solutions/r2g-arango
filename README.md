# Experimental Relational-to-Graph ETL Pipeline

An experimental reference implementation showing how to transform PostgreSQL relational schemas and data into ArangoDB graph structures. Foreign keys become edges, tables become vertex collections, and `arangoimport` scripts are generated for high-performance bulk loading.

This project is intended to guide ArangoDB users through the mechanics of relational-to-graph migration. It is not production software.

See [PRD.md](PRD.md) for the full product requirements document.

## Concepts

Relational databases model relationships implicitly through foreign keys and resolve them at query time via joins. Graph databases model relationships explicitly as first-class edges, enabling direct traversal without joins.

The R2G pipeline applies a mechanical mapping:

- Each **table** becomes an ArangoDB **document collection** (vertices). The table's primary key becomes the document `_key`.
- Each **foreign key** becomes an **edge collection**. For every row in the source table, an edge is created from the source vertex to the target vertex, using the FK value to resolve the `_to` endpoint.
- **Join tables** (many-to-many) become **edges** rather than vertices -- the two FK columns point to the two vertex collections the edge connects.
- **Data types** are coerced from PostgreSQL representations to proper JSON types: integers, floats, booleans, nested JSON for `jsonb` columns, and arrays.

```mermaid
flowchart LR
    PG[(PostgreSQL)] -->|ingest-schema| SJ[schema.json]
    SJ -->|generate-config| MY[mapping.yaml]
    PG -->|"dump-tables"| CSV[".csv per table"]
    CSV --> CSVPATH["generate-csv-import<br/>(file-based)"]
    MY --> CSVPATH
    SJ --> CSVPATH
    CSVPATH --> SH1["import_csv.sh"]
    SH1 -->|"arangoimport --type csv"| ADB[(ArangoDB)]
    PG -->|"stream (direct)"| STREAM["streaming pipeline<br/>server-side cursors"]
    MY --> STREAM
    SJ --> STREAM
    STREAM -->|"HTTP API bulk import"| ADB
```

## Prerequisites

- **Python 3.10+**
- **PostgreSQL** with data you want to migrate (any version with `pg_catalog` support)
- **ArangoDB** instance (tested with 3.11+) with `arangoimport` on your PATH
- **psql** or another tool to export CSV dumps from PostgreSQL

## Features

- **Schema introspection** -- connects to PostgreSQL and extracts tables, columns, primary keys, and foreign keys
- **Mechanical mapping** -- tables become document collections, foreign keys become edge collections, join tables become edges
- **Type coercion** -- PostgreSQL types (integer, boolean, jsonb, arrays, etc.) are converted to proper JSON types
- **YAML-driven configuration** -- auto-generate a default mapping or hand-tune collection names, field renames, include/exclude lists
- **Polars-powered file processing** -- CSV/TSV/GZ dump files processed via Polars for high throughput
- **`arangoimport` script generation** -- produces executable bash scripts that load documents first, then edges, with configurable connection parameters
- **Named graph creation** -- generates arangosh JavaScript to create ArangoDB named graph definitions from edge mappings
- **Structured logging** -- human-readable dev output or JSON for production via structlog
- **CSV-direct import** -- generate `arangoimport --type csv` scripts that import PG CSV dumps directly with `--translate` for key remapping, `--datatype` for type coercion, and collection prefixes for edge `_from`/`_to` construction; no intermediate JSONL needed
- **Mapping visualizer** -- interactive HTML visualization (D3.js force-directed graph) of the relational-to-graph mapping with four views: graph schema, relational schema cards, edge mapping detail, and a mapping editor with YAML export
- **Direct PG streaming** -- stream data directly from PostgreSQL to ArangoDB via the HTTP API with server-side cursors, configurable batch sizes, and REPEATABLE READ snapshot isolation -- no intermediate files; supports parallel streaming with `--workers`
- **Dry-run mode** -- `stream --dry-run` validates connectivity to both PostgreSQL and ArangoDB, reads and transforms all data, but skips writes and graph creation -- reports row counts and sample documents per collection for pre-flight validation
- **Progress bars and throughput** -- Rich progress bars during streaming with real-time row counts; elapsed time and rows/second throughput displayed on completion
- **Retry with backoff** -- transient ArangoDB write failures (connection errors, server overload) are retried with exponential backoff
- **Collection management** -- `--drop-collections` flag drops and recreates target collections before import for idempotent re-runs
- **Table filtering** -- `--include-tables` and `--exclude-tables` on the `stream` command for selective import of large schemas
- **Import error reporting** -- document-level errors from ArangoDB bulk imports are captured, logged, and displayed in the summary table instead of silently dropped
- **Comprehensive type mapping** -- 50+ PostgreSQL types explicitly mapped to JSON types: integer variants, float variants, boolean, JSON/JSONB, UUID, timestamps, intervals, network types, geometric types, and text search types
- **Schema diff** -- `diff-schema` command compares two schema snapshots and reports added/removed tables, column type changes, nullable changes, primary key changes, and foreign key changes; supports `--json` output for scripting
- **Config migration** -- `migrate-config` command auto-updates a mapping YAML when the PostgreSQL schema evolves: adds collections for new tables, adds edges for new FKs, removes edges for dropped FKs, flags orphaned collections, and cleans stale field references and type overrides -- all while preserving user customizations (renames, field mappings, include/exclude lists)
- **Data validation** -- `validate-data` command checks referential integrity of dump files before import: builds PK lookup sets per table and verifies every FK value references an existing PK; reports orphaned references that would produce broken edges
- **Topological import ordering** -- document collections are imported in dependency order (FK targets before FK sources) so that referenced vertices exist before edges are created; circular FK dependencies are detected and warned
- **Environment variable support** -- connection parameters (`PG_CONN`, `ARANGO_ENDPOINT`, `ARANGO_DB`, `ARANGO_USER`, `ARANGO_PASSWORD`) can be set via environment variables or a `.env` file; CLI flags override env vars when both are provided
- **Skip existing** -- `stream --skip-existing` skips collections that already contain data, enabling resumption of partial streaming runs without re-importing completed collections
- **Incremental streaming** -- `stream --since 2026-04-01T00:00:00` filters rows by a timestamp column (auto-detects `updated_at`/`created_at` or use `--since-column`); combine with `--on-duplicate=replace` for basic incremental updates
- **PK-less table safety** -- tables without a primary key are warned during validation and streaming; documents receive auto-generated keys and edges referencing such tables are flagged
- **CDC (Change Data Capture)** -- near real-time PostgreSQL→ArangoDB sync via logical replication. `PGReplicationListener` manages replication slots, polls `pg_logical_slot_get_changes`, parses output via `test_decoding` (built-in) or `wal2json` plugins. `DeltaTransformer` converts row-level changes to graph mutations (document upserts/deletes + edge recalculation). `CDCHandler` orchestrates event processing with transaction grouping and stats tracking. CLI commands: `cdc-setup`, `cdc-teardown`, `cdc-status`, `cdc-start`
- **Composite foreign key support** -- multi-column foreign keys are correctly introspected from `pg_catalog`, represented in mappings, and transformed into composite `_key` / `_from` / `_to` values using a configurable separator
- **Multi-schema support** -- `--pg-schema` option on `ingest-schema`, `dump-tables`, and `stream` commands allows introspection and import from any PostgreSQL schema, not just `public`
- **Automated table dumping** -- `dump-tables` command connects to PostgreSQL and exports each table as a CSV file in one pass
- **Join table auto-detection** -- `generate-config` heuristically identifies junction tables (exactly 2 FKs, no non-structural data columns) and flags them as join tables

## Project structure

```
src/r2g/
├── main.py                     # Typer CLI (17 commands)
├── config_migrate.py           # Config migration when schema evolves
├── data_validator.py           # Referential integrity checker for dump data
├── schema_diff.py              # Schema comparison / structural diff
├── topo_sort.py                # Topological sort for import ordering, cycle detection
├── cdc/                        # Change Data Capture (Phase 3)
│   ├── models.py               # ChangeEvent, ArangoDelta, TransactionBatch
│   ├── parsers.py              # Output plugin parsers (test_decoding, wal2json)
│   ├── pg_listener.py          # PGReplicationListener: slot mgmt, polling loop
│   ├── delta_transformer.py    # Convert CDC events → ArangoDB mutations
│   └── handler.py              # CDCHandler: orchestrate event processing with stats
├── types.py                    # Pydantic models (Schema, Table, MappingConfig, EdgeDefinition, ...)
├── config.py                   # ConfigManager, YAML load/save, PG→JSON type map, join detection
├── log.py                      # structlog setup
├── connectors/
│   ├── postgres.py             # PostgreSQL schema reader via psycopg
│   └── arango_writer.py        # ArangoDB HTTP API writer via python-arango
├── input/
│   └── dump_reader.py          # Polars-based CSV/TSV/GZ reader
├── transformers/
│   ├── node_transformer.py     # Row → ArangoDB document (with type coercion)
│   ├── edge_transformer.py     # Row → ArangoDB edge (FK and join-table modes)
│   └── converter.py            # Re-exports NodeTransformer, EdgeTransformer
├── generators/
│   ├── arangoimport.py         # Bash script generator (JSONL and CSV-direct)
│   └── visualizer.py           # Interactive HTML mapping visualizer + editor
└── streaming/
    └── pipeline.py             # PG → ArangoDB direct streaming pipeline
```

## Installation

```bash
pip install -e .
```

With test dependencies:

```bash
pip install -e ".[test]"
```

With test and dev (lint) dependencies:

```bash
pip install -e ".[test,dev]"
```

## Quick start

### 0. Configure credentials (optional)

Instead of passing connection strings on the command line, create a `.env` file (see `.env.example`):

```bash
cp .env.example .env
# Edit .env with your PostgreSQL and ArangoDB credentials
```

The CLI auto-loads `.env` from the working directory. All connection flags (`--conn`, `--pg-conn`, `--endpoint`, `--database`, `--username`, `--password`) can be set via environment variables (`PG_CONN`, `ARANGO_ENDPOINT`, `ARANGO_DB`, `ARANGO_USER`, `ARANGO_PASSWORD`). CLI flags override env vars.

### 1. Extract schema from PostgreSQL

```bash
r2g ingest-schema --conn "postgresql://user:pass@localhost/mydb" --output schema.json
```

### 2. Generate a default mapping config

```bash
r2g generate-config --schema schema.json --output mapping.yaml
```

This creates a YAML file with one document collection per table and one edge collection per foreign key. Edit it to rename collections, exclude fields, or mark join tables.

### 3. Dump tables to CSV

Use the built-in `dump-tables` command to export all tables at once:

```bash
r2g dump-tables --conn "postgresql://user:pass@localhost/mydb" --output-dir ./dumps
```

Or use `psql` manually (one file per table, filename must match the table name):

```bash
for table in users orders products; do
  psql -d mydb -c "COPY ${table} TO STDOUT WITH CSV HEADER" > dumps/${table}.csv
done
```

### 3a. Visualize the mapping (optional)

```bash
r2g visualize-mapping --schema schema.json --config mapping.yaml --output mapping.html
```

Opens an interactive HTML report in your browser showing the PG-to-graph mapping: draggable graph schema, table cards with PK/FK badges, and edge mapping details.

### 4. Generate CSV-direct import script (preferred)

```bash
r2g generate-csv-import \
  --schema schema.json \
  --config mapping.yaml \
  --data-dir ./dumps \
  --output import_csv.sh \
  --endpoint http://localhost:8529 \
  --database mydb \
  --graph-name my_graph
```

### 4a. Alternative: JSONL transform path

Transform an entire directory of CSV dumps in one pass:

```bash
r2g transform-all \
  --schema schema.json \
  --config mapping.yaml \
  --input-dir ./dumps \
  --output-dir ./output \
  --file-pattern "*.csv"
```

Or transform a single table's nodes or edges:

```bash
r2g transform-nodes --schema schema.json --config mapping.yaml --table users --input dumps/users.csv --output output/users.jsonl
r2g transform-edges --schema schema.json --config mapping.yaml --table orders --input dumps/orders.csv --output output/orders_edges.jsonl
```

Then generate the arangoimport script:

```bash
r2g generate-import \
  --config mapping.yaml \
  --data-dir ./output \
  --output import.sh \
  --endpoint http://localhost:8529 \
  --database mydb \
  --graph-name my_graph
```

This produces an executable `import.sh` (documents first, then edges) and an arangosh graph creation script.

### 5. Load into ArangoDB

**CSV-direct path (step 4):**

```bash
./import_csv.sh
```

**JSONL path (step 4a):**

```bash
./import.sh
```

Override connection details via environment variables (works with the generated scripts):

```bash
ARANGO_ENDPOINT=http://prod:8529 ARANGO_DB=prod_db ARANGO_PASSWORD=secret ./import_csv.sh
```

### Pre-flight: validate data integrity (optional)

```bash
r2g validate-data --schema schema.json --config mapping.yaml --data-dir ./dumps
```

Checks that every FK value in your dump files references an existing PK in the target table. Reports orphaned references that would produce broken edges in ArangoDB.

### Schema evolution: migrating the mapping config

When your PostgreSQL schema changes (new tables, dropped columns, added/removed FKs), update the mapping config automatically:

```bash
# Re-extract the updated schema
r2g ingest-schema --conn "postgresql://user:pass@localhost/mydb" --output schema_v2.json

# Migrate the existing config to match
r2g migrate-config --schema schema_v2.json --config mapping.yaml
```

This preserves all your customizations (collection renames, field mappings, include/exclude lists, type overrides) while adapting to schema changes. Use `--output new_mapping.yaml` to write to a different file, or `--json-report` for machine-readable output.

### Alternative: Direct streaming (no intermediate files)

Skip steps 3-5 entirely and stream data directly from PostgreSQL to ArangoDB:

```bash
r2g stream \
  --pg-conn "postgresql://user:pass@localhost/mydb" \
  --schema schema.json \
  --config mapping.yaml \
  --endpoint http://localhost:8529 \
  --database mydb \
  --batch-size 10000 \
  --graph-name my_graph
```

This uses server-side cursors with REPEATABLE READ isolation for consistent snapshots and bulk-imports via the ArangoDB HTTP API.

Options:

- `--workers 4` -- parallel streaming with per-worker PG + ArangoDB connections. **Note:** each worker opens its own `REPEATABLE READ` transaction; this provides per-worker consistency but not a single global snapshot across all workers. For strict point-in-time consistency, use `--workers 1` (default).
- `--on-duplicate replace` -- ArangoDB duplicate handling strategy (`replace`, `update`, `ignore`, `error`)
- `--include-tables users,orders` -- only stream specified tables (and their edges)
- `--exclude-tables audit_log` -- skip specified tables
- `--drop-collections` -- drop and recreate target collections before import
- `--skip-existing` -- skip collections that already have data (for resuming partial runs)

Add `--dry-run` to preview row counts and sample documents without writing to ArangoDB:

```bash
r2g stream --dry-run \
  --pg-conn "postgresql://user:pass@localhost/mydb" \
  --schema schema.json \
  --config mapping.yaml \
  --endpoint http://localhost:8529 \
  --database mydb
```

## CLI reference

| Command | Description |
|---|---|
| `ingest-schema` | Connect to PostgreSQL and extract schema metadata to JSON |
| `validate-schema` | Validate a schema JSON file against the internal model |
| `inspect-dump` | Preview rows from a CSV/TSV/GZ dump file |
| `generate-config` | Auto-generate a YAML mapping config from a schema file |
| `transform-nodes` | Transform a single table dump into ArangoDB document JSONL |
| `transform-edges` | Transform a single table dump into ArangoDB edge JSONL |
| `transform-all` | Transform all tables and edges in one pass with progress bar |
| `generate-import` | Generate arangoimport bash script and optional graph creation JS |
| `generate-csv-import` | Generate arangoimport script for direct CSV import (no JSONL intermediate) |
| `visualize-mapping` | Generate interactive HTML visualization of the PG-to-graph mapping |
| `dump-tables` | Connect to PostgreSQL and dump each table to a CSV file |
| `validate-config` | Validate mapping config against schema (checks table references, column names, edge definitions) |
| `validate-data` | Check referential integrity of dump files (FK values vs target PKs); reports orphaned references before import |
| `diff-schema` | Compare two schema.json files and report structural changes (tables, columns, types, PKs, FKs); supports `--json` for machine-readable output |
| `migrate-config` | Auto-update a mapping config YAML to match an evolved schema: adds new tables/edges, removes stale edges, flags orphaned collections, cleans dropped-column references; `--json-report` for machine-readable output |
| `cdc-setup` | Create a PostgreSQL logical replication slot for CDC (`--slot`, `--plugin test_decoding\|wal2json`) |
| `cdc-teardown` | Drop a logical replication slot |
| `cdc-status` | Show replication slot metadata (active, LSN positions) |
| `cdc-start` | Start the CDC listener: polls for changes, transforms via mapping config, applies deltas to ArangoDB in near real-time (`--poll-interval`, `--batch-size`, `--create-slot/--no-create-slot`) |
| `stream` | Stream data directly from PostgreSQL to ArangoDB via HTTP API (no intermediate files); supports `--dry-run`, `--pg-schema`, `--drop-collections`, `--workers`, `--include-tables`, `--exclude-tables`, `--skip-existing`, `--on-duplicate`, `--since`, and `--since-column` |

All commands support `--verbose` / `-v` for debug logging and `--json-log` for structured JSON output.

## Mapping configuration

The YAML mapping config controls how PostgreSQL tables map to ArangoDB collections. See [`examples/sample_mapping.yaml`](examples/sample_mapping.yaml) for a commented example.

Key sections:

- **`collections`** -- per-table settings: target collection name, field renames (`field_mappings`), `exclude_fields`, `include_fields`, `is_join_table`
- **`edges`** -- foreign key relationships: edge collection name, from/to vertex collections, from/to fields
- **`type_overrides`** -- force a specific JSON type for a column when auto-detection is wrong
- **`key_separator`** -- character used to join composite primary key values (default: `_`)

## CDC (Change Data Capture)

After an initial full load, use CDC to keep ArangoDB in sync with PostgreSQL changes in near real-time:

```bash
# 1. Create a logical replication slot on PostgreSQL
r2g cdc-setup --pg-conn "postgresql://user:pass@localhost/mydb"

# 2. Start the CDC listener (blocks, Ctrl+C to stop)
r2g cdc-start --pg-conn "postgresql://user:pass@localhost/mydb" \
  schema.json mapping.yaml \
  --endpoint http://localhost:8529 \
  --database mydb \
  --username root --password secret \
  --poll-interval 1.0 --batch-size 1000

# 3. Check slot status
r2g cdc-status --pg-conn "postgresql://user:pass@localhost/mydb"

# 4. Clean up when done
r2g cdc-teardown --pg-conn "postgresql://user:pass@localhost/mydb"
```

The listener supports two PostgreSQL output plugins:
- **`test_decoding`** (default) -- built-in to PostgreSQL, no extensions needed
- **`wal2json`** -- requires the [wal2json](https://github.com/eulerto/wal2json) extension; provides cleaner JSON output

For UPDATE/DELETE events, set `REPLICA IDENTITY FULL` on source tables to capture old row values:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

## Known limitations

This is an experimental reference implementation. The following constraints apply:

- **PostgreSQL only (currently)** -- the schema reader uses `pg_catalog` queries specific to PostgreSQL. Snowflake support is planned (Phase 5 in the PRD). No MySQL, SQLite, or other source support yet.
- **Data validation is opt-in** -- orphaned foreign key references (FK values pointing to non-existent PKs) will produce edges to vertices that don't exist in ArangoDB. Use `validate-data` before import to catch these, but it is not enforced automatically.
- **Basic incremental support only** -- `stream --since` filters rows by a timestamp column for basic incremental updates, but there is no change tracking, CDC, or diff-based processing yet (see Phases 3-4 in the PRD).
- **Self-referential FKs** -- these work but produce edges within the same collection (e.g., `orders.referrer_id -> customers.id` creates `orders_to_customers_referrer_id`). This is correct but may be unexpected.
- **ArangoDB write path** -- the `stream` command connects directly to ArangoDB via python-arango, but the file-based paths (`generate-csv-import`, `generate-import`) still require `arangoimport` installed separately.
- **Credential handling** -- connection strings and passwords appear in CLI arguments and generated scripts. The import script uses environment variable overrides, but the tool has no secrets management.

## Testing

```bash
pytest tests/ -v
```

453 tests covering CLI commands (via typer.testing.CliRunner, including CDC commands), types (including composite FK serialization), schema diff, config migration, data validation (referential integrity with orphan detection), topological sort (dependency ordering, circular FK detection), config validation (including self-referential FKs, duplicate edge naming, and PK-less table warnings), dump reader, node transformer, edge transformer, import generators (JSONL and CSV-direct), visualizer, ArangoDB writer (with retry logic, error surfacing, and single-document CDC ops), CDC models (ChangeEvent, ArangoDelta, TransactionBatch), CDC output plugin parsers (test_decoding text parsing, wal2json JSON parsing), CDC delta transformer (INSERT/UPDATE/DELETE → graph mutations with edge cleanup), CDC handler (event processing, transaction batching, stats tracking, failure handling), CDC replication listener (slot management, polling, graceful shutdown), streaming pipeline (sequential, parallel, table filtering, import errors, skip-existing, topological ordering, since-filtering, PK-less table handling), dry-run mode, progress callbacks, throughput timing, and end-to-end integration tests against live PG + ArangoDB.

To run unit tests only (no Docker required):

```bash
pytest tests/ -m "not integration"
```

To run all tests including integration (requires Docker PG + ArangoDB):

```bash
pytest tests/ -v
```

## Roadmap

Phases 1 and 2 are implemented. See [PRD.md](PRD.md) for the full phased roadmap:

- **Phase 1** -- Table dump file processing (MVP): schema ingestion, JSONL transforms, CSV-direct import, visualizer -- **implemented**
- **Phase 2** -- Direct PostgreSQL streaming (server-side cursors, HTTP API bulk import, REPEATABLE READ snapshots) -- **implemented**
- **Phase 3** -- CDC integration: event models, delta transformer, handler, replication listener, output plugin parsers (`test_decoding`/`wal2json`), continuous polling loop, CLI commands -- **implemented** (conflict resolution pending)
- **Phase 4** -- Kafka consumer (Debezium, transactional ordering)
- **Phase 5** -- Snowflake integration (schema reader, type mapping, streaming, FK inference, source abstraction layer)
- **Phase 6+** -- Additional sources (MySQL, SQL Server), LLM-driven ontology derivation, ArangoRDF, bi-directional sync
