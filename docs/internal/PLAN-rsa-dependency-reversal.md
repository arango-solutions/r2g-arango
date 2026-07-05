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
   existing snapshots (add a serialization compat test corpus first).
3. **Snapshot/catalog migration.** If the serialized shape changes, add a
   read-time upgrader (tolerate old + new) and a one-shot migration for stored
   catalog records; never require users to re-snapshot.
4. **`fk_inference` reconciliation.** Reconcile `to_edge_definition`↔
   `to_foreign_key` (keep an r2g adapter that builds `EdgeDefinition` from RSA's
   `ForeignKey`) and land `sample_values` upstream (or keep an r2g sampler
   subclass). Then import the heuristics from RSA.
5. **Connector strategy.** Decide per-connector: import shared read-only
   introspectors (postgres/mysql/mssql/snowflake/csv) from RSA behind r2g shims
   that layer on the classification merge; **keep** r2g-only connectors
   (`arango_*`, `kafka_source`) local. RSA-only sources (duckdb, databricks) are
   opt-in follow-ons for r2g if desired.
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
- **Stage 2:** DEFERRED — blocked on RSA `Column` governance/extra passthrough
  (step 1) and a persisted-shape migration; not a pure refactor.
