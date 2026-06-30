# Changelog

All notable changes to `r2g` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aspires to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Enforcement artifacts & classification re-sync (Phase 9c)**: the "enable
  enforcement — emit, don't enforce" layer. `src/r2g/governance.py` gains
  `classification_manifest` (canonical per-collection/edge/field classification
  + lineage + owners + sync timestamp), `suggested_rbac` (per-clearance ArangoDB
  collection read-grants, cumulative: a `confidential` clearance reads
  public/internal/confidential but not restricted), `policy_rego` (a default-deny
  OPA/Rego stub keyed on collection level vs. principal clearance, no `opa`
  dependency), and `tier_layout_recommendation` (group collections by tier with a
  suggested database/prefix per tier). `write_governance_artifacts` emits them all
  under `<project>/governance/`, keyed by **target** (ArangoDB) collection names.
  Surfaced via `r2g entitlements emit <project> [--threshold] [--out] [--tier-layout]
  [--no-rego]`, `POST /api/projects/{name}/governance/emit`, and `emit_governance` /
  `tier_layout` on the load endpoint. `r2g catalog resync-classifications <source>`
  re-pulls classifications from the bound catalog (provenance now stored on
  `SourceConfig`: `catalog_name`, `catalog_asset_fqn`, `classifications_synced_at`),
  refreshes the stored map, re-merges onto the latest snapshot, and reports
  lattice-level drift via the new `diff_classifications` helper (escalations
  called out). The CDC/temporal path carries the policy onto changed rows:
  `cdc-start` / `kafka-start` gain `--govern` (+ `--allow-sensitive`,
  `--sensitivity-threshold`) which applies the same sensitivity gate to the
  mapping so subsequent row changes don't launder newly sensitive columns.
  Retroactively re-gating already-loaded data on a tier rise is intentionally out
  of scope (a serving-layer/backfill concern); r2g advises and gates future
  writes. A gated OpenMetadata classification e2e test ships
  (skip-when-unavailable). r2g emits; the serving layer enforces. **Phase 9 is
  complete (9a + 9b + 9c).**
- **Entitlement report, load gate & masking (Phase 9b)**: the
  advise-and-gate layer on top of the 9a classification carrier. New
  `src/r2g/governance.py` builds an **entitlement report** (each target property,
  its source-column lineage, the mosaic-recomputed sensitivity level, and whether
  it is masked), a default-exclude **threshold gate** (above-threshold *unmasked*
  source columns are added to `exclude_fields` for the run unless `allow_sensitive`
  is set), and a `governance/lineage.json` **manifest** recording each field's
  handling. New `src/r2g/masking.py` supplies `hash`/`tokenize`/`redact`/`nullify`
  masking expressions built on the existing `FieldExpression` engine (hashes are
  AQL-delegated, so they work for every source type) and sentinel-tagged so the
  gate treats a masked field as safe to load. Surfaced via `r2g entitlements
  report <project> [--threshold] [--json]`, `GET /api/projects/{name}/entitlements`,
  and the load gate on `POST /api/projects/{name}/load` (`allow_sensitive`,
  `sensitivity_threshold`; the excluded set is reported, never silently dropped).
  The Mapping Studio gains an **entitlement-report panel** (Actions menu, canvas
  context menu, and the `g` shortcut) listing above-threshold fields with source
  lineage; a paint-only **sensitivity lens** (View as → Sensitivity / press `5`)
  that tints source columns and target properties by mosaic-recomputed tier
  (source-column color = the highest level it feeds) with a legend; and a
  one-click **"mask this field"** target-property context action that writes a
  masking `FieldExpression` (hash / tokenize / redact / nullify) so the field
  clears the load gate. r2g advises and emits; the serving layer enforces.
- **Classification capture & propagate (Phase 9a)**: the governance backbone for
  carrying catalog classifications across the relational→graph boundary. A new
  `Classification` model (`tags`, `tier`, `glossary_terms`, `source`) annotates
  `Column`; `CatalogAsset` and `ResolvedSource` gain `column_classifications`,
  `owners`, and `tier`. The OpenMetadata provider now reads `columns,tags,owners`
  and parses per-column tags, table owners, and the `Tier.*` confidentiality tier
  (glossary terms split out by tag source); `resolve_source` captures
  `table → column → Classification` for table/schema/database assets, best-effort
  so a limited governance API never blocks import. The resolved map persists on
  `SourceConfig.classifications` (with `data_owners`/`data_tier`) at
  `catalog import-source` and is merged onto `Column.classification` at
  `source snapshot`. New `r2g.classification` module supplies the sensitivity
  lattice (`public < internal < confidential < restricted`), an overridable
  tag/tier→level map, `max_sensitivity`/`exceeds_threshold`/`tier_of`, and
  `recompute_mosaic` — the max-of-contributors mosaic rule over fan-in
  properties, vertex collections, and edges. Advise/gate (9b) and
  emit-enforcement artifacts + re-sync (9c) remain; r2g carries governance
  metadata and never acts as a runtime authorization engine.
- **Denormalization analysis — advisory remediation + grounding (Phase 11c)**:
  each finding now carries concrete remediation guidance (`remediation_hint`),
  shown in the CLI (a "Suggested remediation" column), the API (a `hint` field on
  every finding), and the Studio card. Added `summarize_findings_for_prompt`, a
  compact confidence-ranked digest that grounds the (forthcoming) Phase-10
  ontology proposal in deterministic evidence. Mechanical auto-apply is
  intentionally deferred: the recommended remedies (vertex extraction, merge,
  split/combine to array) are not yet representable in the source-table-bound
  mapping + AQL expression model, so r2g advises rather than emit an invalid
  mapping — consistent with the phase's "advise, never silently rewrite".
- **Denormalization analysis — more detectors + Studio card (Phase 11b)**:
  three additional detectors — **multi-valued** columns (delimited lists, via a
  new bounded `delimiter_rate` probe on all four value samplers), **redundant
  reference** data (text columns with a very low distinct ratio, suppressed when
  already explained by an embedded-lookup finding), and **1:1 over-normalization**
  (structural: a table whose entire primary key is also a foreign key to another
  table → recommend merge/embed). Plus a read-only **Studio findings card** in
  the Mapping Studio, reachable from the Actions menu, the canvas right-click
  menu, and the `n` shortcut (clone of the Suggest-FKs card). Advisory only —
  accepting a finding into the mapping arrives in 11c.
- **Denormalization analysis — deterministic core (Phase 11a)**: a new
  `r2g source analyze-denorm <name>` command surfaces *advisory*, evidence-backed
  denormalization findings on a source's latest snapshot. Detects **repeating
  column groups** (`phone1/phone2/phone3`, structural, no sampling) and, with
  `--sample`, **embedded lookups** — a non-key column that functionally determines
  other non-key columns (e.g. `zip → city, state`), recommending extraction into a
  shared vertex. Backed by `src/r2g/denorm.py` (`analyze_denormalization`,
  mirroring `infer_foreign_keys`) and two new bounded probes (`distinct_ratio`,
  `group_single_valued`) added to all four value samplers (PostgreSQL, MySQL,
  SQL Server, CSV). Read-only, classification-aware (`--no-sample-columns` escape
  hatch plus a Phase-9 `is_sampleable` hook), and resilient (probe failures
  degrade to structural signals). `--json` output supported; no schema or data is
  ever rewritten. Also exposed over the API as
  `POST /api/sources/{name}/analyze-denorm` (mirrors `infer-fks`). 11b (remaining
  detectors + Studio review card) and 11c (remediation scaffolding + Phase-10
  grounding) remain.
- **External data catalog — MCP tools (Phase 8b, P8.6)**: five new MCP tools —
  `list_catalogs`, `add_catalog`, `remove_catalog`, `catalog_browse`, and
  `catalog_import_source` — let an agent register a catalog, browse its
  `service → database → schema → table` tree (or search), and import a discovered
  asset as a normal r2g source, mirroring the CLI/UI. Tokens are redacted on read
  and `$ENV_VAR` token references resolve at use time; errors are DSN-scrubbed and
  credentials are never taken from the catalog. `get_asset` is now part of the
  `CatalogProvider` protocol (the CLI, UI, and MCP all rely on it).
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
