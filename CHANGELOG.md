# Changelog

All notable changes to `r2g` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aspires to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Public release preparation: Apache-2.0 LICENSE, NOTICE, CONTRIBUTING,
  CODE_OF_CONDUCT (Contributor Covenant 2.1), SECURITY policy, issue and
  pull-request templates, PyPI Trusted Publisher release workflow.
- `docs/` layout: canonical PRD moved to `docs/PRD.md`; internal planning
  notes moved to `docs/internal/`.

### Changed

- PyPI distribution name is `r2g-arango`. The import package and CLI
  command remain `r2g`.
- `pyproject.toml` declares Apache-2.0 license, authors, keywords,
  trove classifiers, and project URLs.

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
