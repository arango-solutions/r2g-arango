# Plan & Decision: relational-schema-analyzer dependency reversal (Phase 10, Stage 2)

> **Status: DEFERRED (July 2026).** Companion to PRD §"Phase 10: Ontology
> derivation". Stage 1 (the deterministic `--engine rsa` ontology engine, shipped
> in r2g-arango 0.3.0) is done. Stage 2 — the *dependency reversal* in which r2g
> imports its introspection core back from
> [`relational-schema-analyzer`](https://github.com/ArthurKeen/relational-schema-analyzer)
> (RSA) and deletes the duplicated modules — is **deferred**. This document
> records *why* (the two codebases have diverged in both directions), and the
> concrete, migration-aware path that would unblock it.

## 1. Background

RSA was extracted **from** r2g: its introspection core (`types`, `connectors`,
`fk_inference`, `schema_diff`, `topo_sort`, typemap/join-heuristic) began as a
lift of r2g's, with `Schema` renamed to `PhysicalSchema` (a `Schema` alias is
kept). RSA's own IMPLEMENTATION-PLAN Phase 5 anticipates the reversal:

> **Reverse the dependency.** `r2g` ends up depending on this library, not vice
> versa. … add dependency, replace embedded modules with imports/shims, delete
> duplicated code, wire conceptual schema into `MappingConfig` generation.

Stage 1 realized the *wiring* half (via a JSON round-trip bridge in
`src/r2g/rsa_ontology.py`) without touching r2g's own modules. Stage 2 is the
*de-duplication* half. The premise of a clean, behavior-preserving reversal is
that the modules are still substantially identical. **They are not.**

## 2. Divergence inventory (evidence, July 2026)

### 2.1 `types.py` — the blocking divergence

r2g's and RSA's physical types have diverged in **both** directions:

| Field / model | r2g | RSA | Impact |
| :-- | :-- | :-- | :-- |
| `Column.classification` (Phase 9 governance) | **present** (`Optional[Classification]`) | **absent** | Adopting RSA's `Column` **drops classification** — breaks Phase 9. |
| `Column.is_unique / default / comment / ordinal` | absent | present | RSA enriches; r2g never persisted these. |
| `Column.type_category` (computed) | absent | present (computed field) | Changes serialized shape. |
| `ForeignKey.is_unique` | absent | present | Cardinality hint; benign but shape-changing. |
| `Table.indexes / check_constraints / unique_constraints / schema_name / is_view / comment` | absent | present | RSA enrichment; not in r2g snapshots. |
| `PhysicalSchema.source` (`SourceProvenance`) | absent (r2g `Schema` has `tables` only) | present | Shape difference. |
| ArangoDB models (`MappingConfig`, `CollectionMapping`, `EdgeDefinition`, `FieldExpression`, `NamingConvention`) + `RESERVED_ATTRIBUTES` | present (r2g-owned) | intentionally absent | Stay in r2g regardless. |

**Blast radius:** `from r2g.types import …` appears in **37 source files**;
`classification` / `Classification` is used in **15 source modules** (catalog
snapshot merge, redaction, entitlement load-gate, LLM prompt redaction, the
sampling gate, masking, etc.).

**Why this blocks a "refactor".** Switching r2g to RSA's `Column` is not a code
move — it is a **persisted-data-shape migration**. Snapshot JSON, the
ArangoDB-backed catalog records, and saved mapping configs would all change
shape, and dozens of tests assert exact `model_dump()`. Dropping `classification`
is a non-starter; keeping it requires RSA's `Column` to carry it.

### 2.2 `fk_inference.py` — diverged both ways

- RSA renamed `InferredForeignKey.to_edge_definition()` (returns `EdgeDefinition`)
  → `to_foreign_key()` (returns `ForeignKey`). r2g has **5 callers** of the
  `EdgeDefinition`-returning form.
- r2g **added** `sample_values()` to its value samplers (Phase 10c), consumed by
  `llm/sampling.py`; RSA does **not** have it.
- Type-map import differs (`r2g.config.pg_type_to_json_type` vs RSA
  `typemap.pg_type_to_json_type`).

### 2.3 `connectors/*` — diverged both ways

- r2g-only: `arango_reader`, `arango_writer`, `kafka_source`.
- RSA-only: `duckdb_source`, `databricks_source`; different `create_connector`
  factory + `SUPPORTED_SOURCE_TYPES`; **no Kafka**.

r2g's connectors also feed the Phase 9 snapshot path that merges `classification`
onto columns; RSA connectors return classification-free RSA types.

### 2.4 Genuinely identical (safe) modules

`topo_sort.py` and `schema_diff.py` differ from RSA **only** in the module
docstring and the `from r2g.types import Schema` → `from .types import Schema`
import line. They are duck-typed (touch only `.tables`, `.foreign_keys`,
`.columns`, `.primary_key`), so either copy runs against either type set.

## 3. Decision

**Defer the wholesale dependency reversal.** Do **not** delete/replace r2g's
`types`, `fk_inference`, or `connectors` on the current divergence.

Rationale:
- It would break Phase 9 (`classification`) or force a persisted-format migration.
- De-duping only the two safe modules (`topo_sort`, `schema_diff`, ~140 LOC)
  would promote RSA from an **optional `[ontology]` extra to a hard core
  dependency** for every install, while every large duplicated module remains —
  a poor cost/benefit trade and a new coupling for negligible gain.
- Stage 1's round-trip bridge already delivers the user-visible value
  (deterministic ontology derivation) without the coupling.

**The real unit of work is type-model reconciliation, not code shuffling.**

## 4. Path to unblock (future Stage 2)

Ordered; each step is independently shippable.

1. **Upstream RSA: governance/extra passthrough on `Column`.** Add an optional,
   paradigm-neutral extension point to RSA's `Column` (and `Table`) — either a
   first-class `classification: Optional[dict]` or a generic
   `extra: dict[str, Any] = {}` that survives round-trips. Release as RSA 0.2.0.
   This lets r2g's Phase 9 data live on RSA types without RSA taking a governance
   opinion.
2. **r2g compatibility layer.** Introduce `r2g.types` re-exports backed by RSA
   types, mapping `Schema = PhysicalSchema` and re-adding `classification` via the
   passthrough. Keep the ArangoDB models (`MappingConfig` et al.) and
   `RESERVED_ATTRIBUTES` in r2g. Gate on **byte-stable `model_dump()`** for
   existing snapshots (add a serialization compat test corpus first). **Design:
   [`DESIGN-rsa-compat-layer.md`](DESIGN-rsa-compat-layer.md)** (thin r2g subclasses
   over RSA types with a legacy-preserving serializer; zero-migration first landing;
   two open decisions pending sign-off).
3. **Snapshot/catalog migration.** If the serialized shape changes, add a
   read-time upgrader (tolerate old + new) and a one-shot migration for stored
   catalog records; never require users to re-snapshot.
4. **`fk_inference` reconciliation.** ✅ *Engine done.* Import the heuristic engine
   (`infer_foreign_keys`, `InferenceOptions`, `InferredForeignKey`, sampler
   protocol) from RSA; keep a thin r2g `InferredForeignKey` subclass adding
   `to_edge_definition` (the ArangoDB analogue of RSA's `to_foreign_key`) and a
   wrapper that re-wraps RSA's results. The concrete value samplers (which carry
   r2g's `sample_values` and are coupled to r2g's connectors) are deferred to step 5
   so sampler + connector parity is validated together.
5. **Connector strategy (incl. sampler de-dup).** Decide per-connector: import
   shared read-only introspectors (postgres/mysql/mssql/snowflake/csv) from RSA
   behind r2g shims that layer on the classification merge; **keep** r2g-only
   connectors (`arango_*`, `kafka_source`) local. Fold in the value samplers here —
   either land `sample_values` upstream in RSA or subclass RSA's samplers — since
   the MySQL/SQL-Server/CSV samplers depend on connector URL parsers / resolvers.
   RSA-only sources (duckdb, databricks) are opt-in follow-ons for r2g if desired.
6. **Delete duplicates + flip the dependency.** Only once 1–5 are green against
   r2g's full suite: delete the now-shimmed modules and make RSA a normal
   dependency (core or a required extra), shipping re-export shims for a
   deprecation window.

**Safe interim (optional, not recommended alone):** if any de-dup is wanted
before reconciliation, `topo_sort`/`schema_diff` are the only behavior-preserving
candidates — but only worth doing bundled with step 6, to avoid a hard RSA
dependency for ~140 LOC.

## 5. Test gates (whenever Stage 2 proceeds)

- Full r2g suite green (`pytest -m "not integration"`), `ruff`, `mypy`.
- A **serialization compatibility corpus**: load pre-Stage-2 snapshot/catalog/
  mapping JSON and assert it still loads and round-trips.
- Phase 9 end-to-end (classification propagation, redaction, load gate) unchanged.
- Live introspection integration (Postgres/MySQL) parity vs current connectors.

## 6. Status summary

- **Stage 1:** DONE (r2g-arango 0.3.0) — deterministic `--engine rsa`.
- **Stage 2:** COMPLETE (descoped) — the drift-prone shared-semantics core (physical
  types, FK-inference engine, value samplers, source-type helpers, the
  `SourceSession` protocol) is unified with RSA; the introspection connectors +
  bulk-read sessions are **intentionally kept local** (see step 5/6 below). RSA is a
  core dependency. No further reversal work is scheduled unless the introspection
  reuse is revisited.
  - **Step 1 (RSA `extra` passthrough):** DONE — RSA 0.2.0 shipped
    (Column/Table `extra`, serialized-when-non-empty); r2g `[ontology]` now
    requires `>=0.2.0`.
  - **Step 2 (compat layer):** DONE — see
    [`DESIGN-rsa-compat-layer.md`](DESIGN-rsa-compat-layer.md). `r2g.types`
    `Schema`/`Table`/`Column` now subclass RSA's `PhysicalSchema`/`Table`/`Column`
    (with a legacy-preserving serializer → byte-stable snapshots, guarded by the
    serialization compat corpus); `ForeignKey` is re-exported from RSA; RSA is a
    core dependency. Gates green (corpus + full non-integration suite + ruff +
    mypy). Zero data migration.
  - **Step 3 (`rsa_ontology.py` round-trip removal):** DONE — r2g `Schema` is passed
    straight to RSA's analyzer (no `model_dump_json`/`model_validate_json` bridge);
    RSA adapter + ontology CLI/UI tests green (incl. real end-to-end golden bundle).
  - **Step 4 (`fk_inference` engine reconciliation):** DONE (engine) — r2g
    `fk_inference` imports the heuristic engine (`infer_foreign_keys`,
    `InferenceOptions`, `InferredForeignKey`, sampler protocol) from RSA instead of
    duplicating ~500 identical lines. r2g keeps a thin `InferredForeignKey` subclass
    adding `to_edge_definition` (ArangoDB) plus a wrapper that re-wraps RSA's
    results, so the public API is unchanged (FK-inference suite green, no behavior
    change). The concrete value samplers (which carry r2g's `sample_values` and are
    coupled to r2g's connectors) stay in r2g and are folded into **step 5** with the
    connector reconciliation, where connector parity can be validated together.
  - **Step 5 (connector strategy — sampler de-dup):** DONE (samplers) — r2g's
    `PostgresValueSampler`/`MySQLValueSampler`/`SQLServerValueSampler`/
    `CsvValueSampler` now subclass RSA's samplers (inheriting the FK-overlap query
    and the Phase-11 denorm probes) and add only r2g's `sample_values` probe,
    removing ~700 duplicated lines. The shared connector helpers (URL parsers,
    driver loaders, CSV path resolution) are byte-identical, so behavior is
    unchanged (full non-integration suite + ruff + mypy green). **Introspection
    connectors remain deferred:** RSA's `postgres`/`mysql`/`mssql`/`snowflake`/`csv`
    connectors have diverged 100–160 lines each (enum sampling, `SourceProvenance`,
    `ordinal`/`is_unique`, duckdb/databricks vs r2g's kafka) and return RSA-typed
    objects with enrichment fields that r2g's serializers drop. A swap therefore
    can't be a plain drop-in: it needs a dedicated effort to wrap RSA's introspectors,
    re-type results to r2g's `Schema`/`Table`/`Column` subclasses, re-apply the Phase-9
    classification merge, and validate parity against live Postgres/MySQL/SQL-Server
    (integration-gated). Note: the live-DB audit below shows that, once re-typed into
    r2g's `Schema` shape, RSA's output is currently byte-identical to r2g's for the
    tested schemas — so the enrichment drop is the *only* observed difference; the
    remaining work is the wrapper + a broader parity corpus, not a semantic reconciliation.

    **Descope decision (Stage 2 close-out):** the introspection connectors + bulk-read
    sessions stay in r2g by design. What *was* provably safe to share has been shared:
    `src/r2g/connectors/session.py` now re-exports RSA's byte-identical `SourceSession`
    protocol, and `src/r2g/connectors/base.py` re-exports RSA's source-type helpers
    (`expand_env_vars`, `normalize_source_type`, `is_postgresql`/`is_mysql`/
    `is_sqlserver`, `serialize_rows`) while keeping the `SourceConnector` protocol local
    (its `get_schema` is typed to r2g's `Schema` subclass), plus `SUPPORTED_SOURCE_TYPES`
    (with `kafka`) and `create_source_connector`. An integration-marked parity audit
    (`tests/integration/test_rsa_introspection_parity.py`) can quantify the introspection
    divergence against live DBs if reuse is ever revisited.

    **Live-DB parity audit (expanded corpus, 2026-07-07).** The audit
    (`tests/integration/test_rsa_introspection_parity.py`) was run against the
    docker-compose stack across a deliberately edge-case-heavy corpus. For every case,
    RSA's introspection output was re-validated into r2g's `Schema` shape and compared
    to r2g's. The comparison **asserts** parity on the *shared* tables (column names,
    primary keys) and **records** membership differences (objects only one side returns)
    and per-column/FK diffs.

    | Case | Source | Shared tables (r2g / rsa) | Columns | FKs | Diffs |
    |---|---|---|---|---|---|
    | PostgreSQL | northwind | 14 / 14 | 92 | 13 | none |
    | PostgreSQL | chinook | 11 / 11 | 64 | 11 | none |
    | PostgreSQL | pagila | 22 / **30** | 129 | 39 | **RSA also returns 8 objects** (7 views + 1 materialized view) |
    | MySQL | shop | 4 / 4 | 13 | 3 | none |
    | SQL Server | shop | 4 / 4 | 13 | 3 | none |
    | CSV | csv_demo | 4 / 4 | 18 | 0 | none |

    **Findings.**
    - On every **shared base table** — including pagila's edge cases (ENUM `mpaa_rating`,
      `DOMAIN` types, `text[]` arrays, `tsvector`, composite PKs `film_actor`/`film_category`,
      and the partitioned `payment` table) — r2g and RSA agree **exactly** on column names,
      types, nullability, primary keys, and declared foreign keys. Zero column/PK/FK diffs
      across the whole corpus. The 100–160 line code divergence manifests only as RSA
      enrichment fields (`SourceProvenance`, `ordinal`, `is_unique`, …) that r2g's
      serializers drop.
    - **The one real divergence is object membership:** RSA's Postgres introspector also
      returns **views and materialized views** (pagila: `actor_info`, `customer_list`,
      `film_list`, `nicer_but_slower_film_list`, `rental_by_category` [matview],
      `sales_by_film_category`, `sales_by_store`, `staff_list`), whereas r2g introspects
      **base tables only**. A naive reuse of RSA's introspector would therefore start
      surfacing views as loadable collections. This is the concrete gap any future reuse
      must close (filter to base tables, or make view inclusion opt-in).

    So the descope stands: reuse remains a wrapper effort — re-type into r2g's `Schema`,
    re-apply the Phase-9 classification merge, **and filter RSA's view/matview membership
    to match r2g** — validated by this audit corpus (still to extend to `snowflake` and
    `kafka`, which are untested here). The audit test is retained to re-run on demand.
  - **Step 6** (delete remaining duplicates + flip to a normal dependency): resolved by
    the descope — RSA became a core dependency in step 2, and the safe session/base
    shares above are the only connector-layer de-dup. The introspection connectors are
    **not** deleted (kept local by design), so no re-export shims are needed. Removing
    the empty `[ontology]` extra alias remains a separate, optional cleanup.
