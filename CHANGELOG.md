# Changelog

All notable changes to `r2g` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aspires to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Physical types re-based on `relational-schema-analyzer` (Stage 2, step 2).**
  `r2g.types` `Schema`/`Table`/`Column` now subclass RSA's
  `PhysicalSchema`/`Table`/`Column`, and `ForeignKey` is re-exported from RSA, so
  the two projects share one physical-schema type core. `Column` keeps r2g's
  Phase-9 `classification` as a first-class field and a legacy-preserving
  serializer emits r2g's historical shape, so **existing snapshots,
  `~/.r2g/catalog.json`, and saved mappings are byte-identical — no migration**
  (guarded by a new serialization compatibility corpus). RSA-native
  `extra['classification']` is tolerated on input. `relational-schema-analyzer` is
  now a **core dependency** (`>=0.2.0,<0.3.0`); the `[ontology]` extra is retained
  as an empty back-compat alias. See `docs/internal/DESIGN-rsa-compat-layer.md`.
  The `--engine rsa` adapter now passes the r2g `Schema` straight to RSA's analyzer
  (no JSON round-trip), since the types are unified. No behavior change.
- **FK-inference heuristic engine shared with `relational-schema-analyzer`
  (Stage 2, step 4).** `r2g.fk_inference` now imports the inference engine
  (`infer_foreign_keys`, `InferenceOptions`, `InferredForeignKey`, sampler
  protocol) from RSA instead of duplicating ~500 lines of identical heuristics.
  r2g keeps a thin `InferredForeignKey` subclass that adds the ArangoDB-specific
  `to_edge_definition`, and a wrapper that re-wraps RSA's results as that subclass,
  so the public API is unchanged. The concrete value samplers (which carry r2g's
  `sample_values` and are coupled to r2g's connectors) stay in r2g for now and are
  reconciled in step 5. No behavior change (verified by the FK-inference suite).
- **FK/denorm value samplers share RSA's implementations (Stage 2, step 5 —
  sampler de-dup).** `PostgresValueSampler`, `MySQLValueSampler`,
  `SQLServerValueSampler`, and `CsvValueSampler` now subclass RSA's samplers,
  inheriting the FK-overlap query and the Phase-11 denormalization probes
  (`distinct_ratio`/`group_single_valued`/`delimiter_rate`) and adding only r2g's
  `sample_values` probe (used by `r2g.llm.sampling`). This removes ~700 lines of
  duplicated sampler code; the shared connector helpers (URL parsing, driver
  loading, CSV path resolution) are byte-identical, so behavior is unchanged (full
  suite green). The **introspection connectors** themselves remain in r2g: RSA's
  have diverged (enum sampling, provenance, `ordinal`/`is_unique`, duckdb/databricks
  vs r2g's kafka) and return RSA-typed objects, so swapping them is not byte-stable
  without a re-typing + classification-merge + live-DB parity effort — tracked as
  the remainder of steps 5–6.
- **Connector-layer shared helpers + RSA Stage 2 close-out (steps 5–6, descoped).**
  `r2g.connectors.session` now re-exports RSA's byte-identical `SourceSession`
  protocol, and `r2g.connectors.base` re-exports RSA's source-type helpers
  (`expand_env_vars`, `normalize_source_type`, `is_postgresql`/`is_mysql`/
  `is_sqlserver`, `serialize_rows`). The `SourceConnector` protocol stays local so
  its `get_schema()` remains typed to r2g's `Schema` subclass (which the snapshot,
  classification-annotate, and schema-diff paths depend on), as do
  `SUPPORTED_SOURCE_TYPES` (incl. `kafka`) and `create_source_connector`. The
  **introspection connectors and bulk-read sessions are kept in r2g by design** —
  RSA's have diverged and drive r2g's data-migration path — closing the RSA
  dependency reversal (Stage 2): the shared-semantics core (physical types,
  FK-inference engine, value samplers, source-type helpers, session protocol) is
  unified, with RSA a core dependency. A live-DB introspection parity audit is added
  at `tests/integration/test_rsa_introspection_parity.py` for any future revisit. No
  behavior change; import safety for minimal installs preserved (no DB drivers pulled
  by importing the connector base/session).

### Added

- **Serialization compatibility corpus** (`tests/test_serialization_compat.py` +
  frozen fixtures) freezing the on-disk JSON shape of the physical types and
  `MappingConfig`, asserting byte-stability across the RSA type reversal.

## [0.3.0] — 2026-07-05

### Added

- **Deterministic ontology engine via `relational-schema-analyzer` (Phase 10,
  Stage 1)**: `r2g ontology suggest` gains an `--engine rsa` option (and the UI a
  matching **Engine** selector) that derives a conceptual model —
  semantic (PascalCase) collection names, join-table detection, foreign-key
  relationships, and provenance (confidence, detected patterns, fingerprint,
  review flags) — **deterministically and offline** (no rows, no network) using
  the shared [`relational-schema-analyzer`](https://github.com/ArthurKeen/relational-schema-analyzer)
  library (the introspection core originally extracted from r2g). The analyzer's
  tool-contract bundle is converted into an `OntologyProposal` and flows through
  the same validated `proposal_to_mapping` "hallucination gate" as the LLM path,
  so the resulting `MappingConfig` is always schema-valid and loadable. Pass
  `--refine` (UI: **Refine**) to additively LLM-improve the deterministic model.
  Install with the new `r2g-arango[ontology]` extra. Many-to-many join tables are
  surfaced as advisory review notes (loaded as a vertex + FK edges), consistent
  with embed hints.

## [0.2.0] — 2026-07-04

First tagged release. Everything below the 0.1.0 pre-release baseline: temporal
graph mode validated end-to-end, MySQL/MariaDB + SQL Server sources, the
object-centric Mapping Studio (naming conventions, rename change-management,
FK/denormalization suggestions), the ArangoDB-backed catalog, external data
catalog integration (Phase 8), classification propagation & entitlement-aware
loading (Phase 9), LLM-assisted ontology derivation (Phase 10a–c) with
deterministic grounding, denormalization analysis (Phase 11), and a hardened
security posture.

### Added

- **End-to-end validation for temporal graph mode (Phase 5)**: a
  skip-when-unavailable integration test (`tests/integration/test_temporal_e2e.py`)
  that closes the long-standing "field validation against a live temporal
  workload" gap. It drives INSERT → UPDATE → DELETE through `TemporalApplier`
  against a real ArangoDB and asserts the full immutable-proxy time-travel
  contract: ProxyIn/ProxyOut creation, entity versions with `[created, expired)`
  intervals, the two `hasVersion` edges (closed on update), `ttlExpireAt`
  stamping on expiry, the interval (`mdi`/`zkd`/`persistent`) and sparse TTL
  indexes, replay-safety (a duplicated INSERT creates no phantom version), and
  every point-in-time / version-history / interval-overlap / current-version AQL
  template returning correct results at each instant.

- **Deterministic grounding for ontology proposals (P11.10 → Phase 10)**: an
  opt-in `--ground` flag (CLI), `ground` field (`suggest-ontology` API), and
  "Add deterministic denormalization findings as advisory evidence" toggle
  (Studio dialog) that runs the Phase 11 analyzer over the schema and hands its
  findings to the model as advisory evidence, so a proposal is grounded in
  deterministic analysis (e.g. "zip determines city, state — consider a Location
  vertex") rather than name heuristics alone. New `r2g.llm.grounding.build_grounding`
  wraps `analyze_denormalization` + `summarize_findings_for_prompt`; structural
  detectors always run and functional-dependency detectors use the same value
  sampler as `--sample` when available. The grounding block is fence-neutralized
  in the prompt and carries only column names + counts/ratios (never raw values),
  and the **classification gate carries over** — Restricted/PII columns are added
  to the analyzer's `no_sample_columns` so they are never value-sampled while
  grounding. Provenance records whether the proposal was grounded. Tested in
  `tests/test_llm_grounding.py` plus CLI/API wiring cases.

- **LLM ontology enrichment — providers & sampling (Phase 10c)**: broadens the
  Phase-10 seam. **Two new providers behind the same factory:** an Anthropic
  (Claude) provider (`src/r2g/llm/anthropic_provider.py`, Messages API; JSON
  parsed from the response with Markdown-code-fence stripping; key from
  `$ANTHROPIC_API_KEY`) and an **OpenAI-compatible / local** provider for Ollama,
  vLLM, LM Studio, llama.cpp, and hosted compatibles — served by `OpenAIProvider`
  with `require_key=False` + a required `base_url` (the `Authorization` header is
  omitted when no key is present). Factory aliases (`claude`, `local`, `ollama`,
  `vllm`, `lmstudio`, …) resolve to the right provider. **Opt-in,
  classification-filtered value sampling:** `build_schema_digest` now takes a
  `table → column → values` sample map and renders a few bounded,
  injection-neutralized example values per column — **only** for columns below the
  redaction threshold, so Restricted/PII columns stay name-only and are never
  sampled. `r2g.llm.sampling.collect_samples` gathers them via a new bounded
  `sample_values` probe added to the Postgres/MySQL/SQL-Server/CSV value samplers
  (best-effort; redacted columns are skipped entirely). Wired through the CLI
  (`r2g ontology suggest --provider/--model/--api-key/--base-url/--sample/--samples-per-column`),
  the `suggest-ontology` API (`provider`/`base_url`/`sample`/`samples_per_column`),
  and the Studio dialog (provider select, base-URL field, "sample non-sensitive
  columns" toggle); provenance records whether and how many columns were sampled.
  A **`R2G_LLM_LIVE`-gated live smoke test** (`tests/test_llm_live.py`) exercises
  the real provider round-trip and asserts a valid `MappingConfig`; new
  network-free unit tests in `tests/test_llm_providers.py`,
  `tests/test_llm_sampling.py`, and added cases in `tests/test_llm_prompt.py` /
  `tests/test_cli_ontology.py`.

- **LLM ontology review & apply in the Studio (Phase 10b)**: brings Phase-10a's
  proposal into the Mapping Studio. Two UI-server endpoints:
  `POST /api/projects/{name}/suggest-ontology` (read-only — returns the structured
  proposal, the resulting *validated* candidate mapping, a `diff_mappings` diff vs
  the current mapping, and validation/provenance notes) and
  `POST /api/projects/{name}/apply-ontology` (rebuilds an accepted, possibly-subset
  proposal through the same `proposal_to_mapping` gate and returns an **editable
  draft** — nothing is persisted server-side, exactly like `apply-naming`). The
  Studio gains a "Suggest model (AI)" entry in the Actions menu, the canvas
  context menu, and an `m` keyboard shortcut; it opens a floating review panel
  listing each proposed relationship / collection change / property rename with its
  rationale and confidence and a **per-item checkbox**. "Apply selected" sends only
  the checked items, loads the returned draft into the mapper edit state, and marks
  the project dirty so the user Saves through the normal path (which offers an
  in-place migration when the target is already loaded). API key is sourced from
  the environment (`$OPENAI_API_KEY`, or a `$ENV_VAR` reference in the request) and
  never persisted or echoed. Context-menu-primary, overlay panel, no new route.
  Tested with a network-free fake provider (`tests/test_ui_api.py::TestSuggestOntologyApi`).

- **LLM-assisted ontology derivation (Phase 10a)**: an *optional* path where a
  model **proposes** a richer target ontology — the LLM proposes, the
  deterministic pipeline disposes. New `src/r2g/llm/` package mirroring
  `r2g.catalogs`: an `LLMProvider` Protocol + lazy-importing `create_llm_provider`
  factory (`base.py`); a REST-over-`httpx` `OpenAIProvider` in
  JSON-object/`temperature=0` mode reading `$OPENAI_API_KEY` from the environment
  (`openai_provider.py`); a **metadata-only** prompt builder (`prompt.py`) that
  redacts Phase-9 Restricted/PII columns to name-only (never sampled), fences
  schema text as untrusted data with fence/`\`\`\`` neutralization (prompt-injection
  hardening), and enforces a hard token budget; and `proposal_to_mapping`
  (`ontology.py`) — the **hallucination gate** that starts from the Auto-Map
  baseline, applies each proposed collection/edge/rename behind a guard mirroring
  `validate_config`, drops references to non-existent tables/columns (recorded in
  notes), de-dupes restated FK edges, rejects reserved-attribute rename targets,
  and guarantees the result always passes `validate_config` (worst case:
  Auto-Map-equivalent). Embed suggestions surface as advisory notes only (no
  mechanical apply in V1). CLI `r2g ontology suggest <project> [--domain]
  [--provider] [--model] [--api-key] [--apply] [--yes] [--json]` prints the
  proposal, a `diff_mappings` diff against the current mapping, and
  validation/provenance notes; nothing is written without `--apply` (with
  confirmation), which also drops a `llm-ontology-provenance.json` sidecar.
  Shipped as the optional `r2g-arango[llm]` extra (added to `[all]`); no LLM
  dependency or network call unless a suggestion is invoked. Fully unit-tested
  with a network-free fake provider (`tests/test_llm_base.py`,
  `test_llm_prompt.py`, `test_ontology_proposal.py`, `test_cli_ontology.py`).
  The Studio diff-review/apply UX (10b) and opt-in sampling / more providers
  (10c) follow.

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

[Unreleased]: https://github.com/ArthurKeen/r2g-arango/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ArthurKeen/r2g-arango/releases/tag/v0.2.0
