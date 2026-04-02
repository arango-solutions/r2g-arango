# Antigravity: R2G-ETL Pipeline

Automated pipeline for transforming PostgreSQL relational schemas and data into ArangoDB graph structures. Foreign keys become edges, tables become vertex collections, and `arangoimport` scripts are generated for high-performance bulk loading.

See [PRD.md](PRD.md) for the full product requirements document.

## Features

- **Schema introspection** -- connects to PostgreSQL and extracts tables, columns, primary keys, and foreign keys
- **Mechanical mapping** -- tables become document collections, foreign keys become edge collections, join tables become edges
- **Type coercion** -- PostgreSQL types (integer, boolean, jsonb, arrays, etc.) are converted to proper JSON types
- **YAML-driven configuration** -- auto-generate a default mapping or hand-tune collection names, field renames, include/exclude lists
- **Polars-powered file processing** -- CSV/TSV/GZ dump files processed via Polars for high throughput
- **`arangoimport` script generation** -- produces executable bash scripts that load documents first, then edges, with configurable connection parameters
- **Named graph creation** -- generates arangosh JavaScript to create ArangoDB named graph definitions from edge mappings
- **Structured logging** -- human-readable dev output or JSON for production via structlog

## Project structure

```
src/r2g/
├── main.py                     # Typer CLI (8 commands)
├── types.py                    # Pydantic models (Schema, Table, MappingConfig, EdgeDefinition, ...)
├── config.py                   # ConfigManager, YAML load/save, PG→JSON type map
├── log.py                      # structlog setup
├── connectors/
│   └── postgres.py             # PostgreSQL schema reader via psycopg
├── input/
│   └── dump_reader.py          # Polars-based CSV/TSV/GZ reader
├── transformers/
│   ├── node_transformer.py     # Row → ArangoDB document (with type coercion)
│   ├── edge_transformer.py     # Row → ArangoDB edge (FK and join-table modes)
│   └── converter.py            # Re-exports NodeTransformer, EdgeTransformer
└── generators/
    └── arangoimport.py         # Bash script and arangosh JS generator
```

## Installation

Requires Python 3.10+.

```bash
pip install -e .
```

With test dependencies:

```bash
pip install -e ".[test]"
```

## Quick start

### 1. Extract schema from PostgreSQL

```bash
r2g ingest-schema --conn "postgresql://user:pass@localhost/mydb" --output schema.json
```

### 2. Generate a default mapping config

```bash
r2g generate-config --schema schema.json --output mapping.yaml
```

This creates a YAML file with one document collection per table and one edge collection per foreign key. Edit it to rename collections, exclude fields, or mark join tables.

### 3. Transform dump files

Transform an entire directory of CSV dumps (one file per table, filename = table name):

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
r2g transform-nodes --schema schema.json --table users --input dumps/users.csv --output output/users.jsonl
r2g transform-edges --schema schema.json --config mapping.yaml --table orders --input dumps/orders.csv --output output/orders_edges.jsonl
```

### 4. Generate arangoimport script

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

```bash
./import.sh
```

Override connection details via environment variables:

```bash
ARANGO_ENDPOINT=http://prod:8529 ARANGO_DB=prod_db ARANGO_PASSWORD=secret ./import.sh
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

All commands support `--verbose` / `-v` for debug logging and `--json-log` for structured JSON output.

## Mapping configuration

The YAML mapping config controls how PostgreSQL tables map to ArangoDB collections. See [`examples/sample_mapping.yaml`](examples/sample_mapping.yaml) for a commented example.

Key sections:

- **`collections`** -- per-table settings: target collection name, field renames (`field_mappings`), `exclude_fields`, `include_fields`, `is_join_table`
- **`edges`** -- foreign key relationships: edge collection name, from/to vertex collections, from/to fields
- **`type_overrides`** -- force a specific JSON type for a column when auto-detection is wrong
- **`key_separator`** -- character used to join composite primary key values (default: `_`)

## Testing

```bash
pytest tests/ -v
```

108 tests covering types, config, dump reader, node transformer, edge transformer, and import generator.

## Roadmap

Phase 1 (MVP) is implemented. See [PRD.md](PRD.md) for the full phased roadmap:

- **Phase 2** -- Direct PostgreSQL streaming (batched reads, no intermediate files)
- **Phase 3** -- CDC integration (logical decoding, near real-time sync)
- **Phase 4** -- Kafka consumer (Debezium, transactional ordering)
- **Phase 5+** -- LLM-driven ontology derivation, ArangoRDF, bi-directional sync
