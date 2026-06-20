# Changelog

All notable changes to `r2g` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aspires to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **External data catalog — Studio UI import (Phase 8b)**: an "Import from
  catalog" path in the Studio (folder button on the Sources panel) — register a
  catalog, browse its `service → database → schema` tree, and import a
  database/schema as a source, all in the browser. New `/api/catalogs*`
  endpoints (list/add/remove/browse/import-source); catalog tokens redacted in
  responses. Validated end-to-end against a live OpenMetadata 1.13 instance.
- **Inline `$VAR` expansion in connection strings**: `create_source_connector`
  (and the FK sampler + MCP paths) now expand `$VAR` / `${VAR}` references
  *inline* within a DSN, not just whole-string — so catalog-imported sources
  whose credentials are `$R2G_DB_USER` / `$R2G_DB_PASSWORD` placeholders connect
  cleanly. Literal DSNs (no `$`) are unaffected.
- **External data catalog discovery — OpenMetadata (Phase 8a)**
  (`r2g-arango[openmetadata]`): a catalog-agnostic `CatalogProvider` abstraction
  + factory (`r2g.catalogs`) and an OpenMetadata provider that talks to the REST
  API via `httpx` (no heavy SDK). New `r2g catalog add/list/browse/import-source/
  remove` CLI: browse a catalog's `service → database → schema → table` tree (and
  Kafka topics), then import a selected database/schema/topic as a normal r2g
  source ("discover-then-connect"). Catalog provider tokens are encrypted at rest
  alongside source/target secrets. Credentials are **not** read from the catalog —
  imported sources use `$R2G_DB_USER` / `$R2G_DB_PASSWORD` placeholders resolved at
  connect time. Distinct from the internal Phase 5d catalog; read-only. Unit-tested
  with a mocked HTTP layer; a live OpenMetadata e2e (skipped when unavailable) is
  included for field validation.
- **SQL Server source connector** (`r2g-arango[sqlserver]`, pure-Python
  `pymssql`): schema introspection over `INFORMATION_SCHEMA` plus `sys.*`
  catalog views for declared foreign keys (clean composite-FK ordering), a
  batched cursor read session with CSV dump, and a SQL Server FK value-overlap
  sampler. `bit` columns are mapped to boolean; the schema namespace defaults
  to `dbo`. Works everywhere the other relational sources do —
  `source add --type sqlserver` (alias `mssql`), `source snapshot`,
  `source dump`, `source infer-fks --sample`, `stream --source`, the Studio
  UI, and MCP. Verified end-to-end against SQL Server 2022 via a docker-compose
  `sqlserver` service + `tests/integration/test_mssql_e2e.py`, wired into the
  CI integration job.
- **MySQL / MariaDB source connector** (`r2g-arango[mysql]`, pure-Python
  `pymysql`): schema introspection over `information_schema`, a
  consistent-snapshot read session (`REPEATABLE READ` + `START TRANSACTION
  WITH CONSISTENT SNAPSHOT`) with server-side cursor streaming and CSV dump,
  and a MySQL FK value-overlap sampler. Works everywhere PostgreSQL and
  Snowflake do — `source add --type mysql`, `source snapshot`, `source dump`,
  `source infer-fks --sample`, `stream --source`, the Studio UI, and MCP.
  `mariadb://` URLs and the `mariadb` source type alias are accepted.
  Verified end-to-end against MySQL 8.4 via a new docker-compose `mysql`
  service + `tests/integration/test_mysql_e2e.py` (introspection, streaming
  into ArangoDB, dry-run, and live session count/stream/dump), wired into the
  CI integration job.
- **Phase 5 — Temporal graph mode** (immutable-proxy time travel):
  ProxyIn/Entity/ProxyOut vertices with `hasVersion` edges, point-in-time
  AQL query helpers, `--temporal` / `--ttl-seconds` flags on `cdc-start`
  and `kafka-start`, mdi interval index plus sparse TTL index for version
  garbage collection, and `--smart-field` for SmartGraph key prefixes.
- **Phase 5g — Mapper UX refinements**: light-mode default, slim toolbar,
  graph explorer, on-demand panels, progressive disclosure, bidirectional
  highlight, function-node discoverability, composite-key nodes.
- Naming conventions for generated collections/edges with rename
  change-management, plus UI hints and bundled demo data.
- CSV directory and Kafka topic source types alongside PostgreSQL and
  Snowflake.
- Auto-creation of the target database and named graph on load.
- Expression preview and per-batch AQL delegation (Phase 5b/5c close-out).
- `r2g --version` flag; shell completion documented via Typer's built-in
  `--install-completion`.
- CI: coverage gate (`--cov-fail-under`), mypy type-check job, and an
  opt-in Docker-based integration test job.
- Public release preparation: Apache-2.0 LICENSE, NOTICE, CONTRIBUTING,
  CODE_OF_CONDUCT (Contributor Covenant 2.1), SECURITY policy, issue and
  pull-request templates, PyPI Trusted Publisher release workflow.
- `docs/` layout: canonical PRD moved to `docs/PRD.md`; internal planning
  notes moved to `docs/internal/`.

### Changed

- Code-quality remediation: consolidated duplicated Python and JS helpers
  (edge-target map, PG connection factory, table preview, shared
  mapper/explorer UI helpers), removed dead code, unified HTML escapers.
- Partitioned tables collapse into a single logical table in the mapper.
- Major dependencies now declare upper version bounds in
  `pyproject.toml` to prevent silent breaking upgrades.
- PyPI distribution name is `r2g-arango`. The import package and CLI
  command remain `r2g`.
- `pyproject.toml` declares Apache-2.0 license, authors, keywords,
  trove classifiers, and project URLs.

### Fixed

- `drop_collections` no longer fails on collections that are members of a
  named graph.
- Document keys are sanitized to the ArangoDB legal character set.
- Preview SQL injection in table preview; DLQ now wired into streaming.
- Collapsed edge spine connects both endpoint tables; topology legend
  layout; `_key` node legibility.
- The docker-compose ArangoDB healthcheck no longer always reports
  "unhealthy" (the image ships no `curl`); it now uses BusyBox `wget` with a
  Basic-auth header, so `docker compose up --wait` succeeds in the CI
  integration job.

### Security

- Bearer-token auth for non-loopback UI binds (auto-generated token when
  none configured), CORS lockdown (same-origin unless `R2G_CORS_ORIGINS`
  set), and a path jail for mapping-file writes.
- Secret redaction in structured logs, DSN-credential scrubbing in error
  messages, redacted DLQ entries, MCP secret redaction.
- Closed inline-handler DOM XSS in mapper/explorer panes, with a static
  regression-guard test.
- MCP SSE transport now requires a Bearer token (reuses `R2G_API_TOKEN`,
  auto-generated on non-loopback bind); MCP tool errors are scrubbed of DSN
  credentials and `generate_mapping(save_path=...)` is confined to the
  catalog projects directory.
- CSV sources can be confined to a trusted directory tree via the opt-in
  `R2G_CSV_BASE_DIR` environment variable.

## [0.1.0] — pre-release

Initial phased implementation (not yet published to PyPI):

- **Phase 1** — Table dump file processing: schema ingestion, JSONL
  transforms, CSV-direct import via `arangoimport`, interactive HTML
  mapping visualiser.
- **Phase 2** — Direct PostgreSQL streaming with server-side cursors,
  REPEATABLE READ snapshot isolation, HTTP API bulk import, parallel
  workers, retry with backoff, topological ordering, table filtering,
  progress reporting.
- **Phase 3** — Change Data Capture (CDC): logical replication listener
  (`test_decoding` and `wal2json` parsers), delta transformer, handler,
  four conflict-resolution policies (`source_wins`, `last_write_wins`,
  `log_and_skip`, `fail`), CLI commands (`cdc-setup`, `cdc-teardown`,
  `cdc-status`, `cdc-start`).
- **Phase 4** — Kafka integration: Debezium and flat-JSON parsers,
  `confluent-kafka` consumer, `kafka-start` CLI.
- **Phase 5b/5c/5e** — Mapping studio UI (FastAPI + static HTML/JS):
  project catalog, schema introspection, mapping editor with YAML
  export, graph schema lens, edge mapping detail, expression evaluator.
- **Phase 6** — Snowflake source support: `SourceConnector` protocol,
  `SnowflakeConnector` schema introspection, source-agnostic streaming
  pipeline, `r2g source dump` CLI, pure-Python FK inference with
  optional value-overlap sampler.

[Unreleased]: https://github.com/ArthurKeen/r2g-arango/compare/main...HEAD
