# Plan A — identify keys a source does not declare

**Status:** A1–A5 built and verified live 2026-10-02; release of RSA 0.8.1 pending · **Supersedes in part:** the Cursor plan
`rsa-connector-consolidation` (split into Plan A here and Plan B below).

## Why

FK inference only targets columns already known to be keys: a primary key or a UNIQUE
constraint (RSA `fk_inference._build_candidate_key_index`). Snowflake declares keys rarely
and never enforces them, so a typical Snowflake schema gives inference **no targets and
therefore no relationships**. Today the only remedy is a hand-written key overlay (P6.8).

Three defects stand between r2g and finding keys on its own. All three were reproduced on
2026-10-02:

1. **Uniqueness is lost on save.** r2g's `Table` serializer persists `name, columns,
   primary_key, foreign_keys, is_partitioned, partition_of` — no `unique_constraints`. A
   unique key present when a snapshot is captured is gone after the catalog reloads it, and
   any FK inferred from it disappears too. This includes UNIQUE keys declared in a *reviewed
   overlay*, so it is a live defect in P6.8 as shipped (the bundled demo overlay declares
   only PKs and FKs, which is why its tests pass).
2. **r2g's connectors never read declared UNIQUE constraints.** None of r2g's connectors set
   `unique_constraints`; RSA's Postgres, MySQL, SQL Server and Snowflake connectors do. This
   is Plan B's to fix (see below).
3. **Nothing proposes keys from the data.** No PK profiling exists in r2g or RSA, and r2g has
   value samplers for Postgres, MySQL, SQL Server and CSV but not Snowflake.

## Plan A — steps

| # | Where | What | Done when |
|---|---|---|---|
| A1 ✅ | r2g | Persist `unique_constraints` (written only when non-empty, so existing snapshots stay byte-identical). Mark snapshots with `schema_format_version`: absent = 1 = "cannot hold UNIQUE keys", 2 = "keeps what the capture produced". A *storage* version only -- it does not claim the capture looked for UNIQUE keys; that becomes true per connector in Plan B (corrected in PR #12 review). | An overlay-declared UNIQUE survives save + reload and still yields its inferred FK; the compat corpus is unchanged |
| A2 ✅ | r2g | Pin the declared-UNIQUE gap in the RSA parity test against RSA's **raw** output, instead of normalising both sides through r2g's serializer (which is how the gap stayed hidden). | The test states the exact current difference and fails when Plan B closes it |
| A3a ✅ RSA #4 | RSA | Read UNIQUE *indexes* (not only UNIQUE constraints) as candidate keys. Postgres often expresses uniqueness this way: pagila has three such indexes, e.g. `store.manager_staff_id`, and RSA records no indexes at all, so inference cannot see them. Pinned in `tests/integration/test_declared_uniqueness_parity.py`. | The pinned assertion flips; inference finds a natural-key FK backed only by a unique index -- **and it survives a catalog reload.** r2g's `Table` serializer is an allowlist and drops `indexes`, so A3a must either express unique indexes as `unique_constraints` (already persisted) or extend the serializer the way A1 did; otherwise the A1 bug returns for indexes (PR #12 review) |
| A3 ✅ RSA #5 | RSA | Cost-governed Snowflake value sampler, modelled on the BigQuery sampler design in RSA's `PLAN-bigquery.md` (dry-run/limit gating, sampling, a per-session query budget). | Conformance tests pass; a live run stays under its budget |
| A4 ✅ RSA #6 | RSA | PK candidate profiler: a column or column set is a candidate when it is never empty and its values are distinct, in a sample and then confirmed. Each candidate carries its evidence. | Deterministic tests, with mutation checks |
| A5 ✅ RSA #7 + r2g | RSA + r2g | Emit candidates as a **draft overlay** for review. `r2g source suggest-keys` writes it; a person edits it; the existing `--key-overlay` applies it. Nothing is applied automatically. | Draft → review → apply round-trips through existing P6.8 code |
| A6 | both | Acceptance: on the constraint-free Customer 360 data, compare the draft with the bundled reviewed overlay (5 PKs, 6 FKs). | Precision/recall reported; misses explained |

**A6, measured live 2026-10-02** through the real CLI (`suggest-keys` -> `set-key-overlay
--reviewed` -> `snapshot`) on the constraint-free Customer 360 data: the snapshot's keys match
the hand-reviewed overlay exactly -- 5/5 primary keys, 6/6 foreign keys, no false proposals, all
at value overlap 1.00, in 13 warehouse queries. Caveat: a 5-table demo with under 10 rows per
table; the profiler correctly marks that as weak evidence. A larger, messier schema is the next
real test.

**Found along the way, both fixed in RSA:** MySQL reported a unique index with a functional part
(`UNIQUE (ext, (lower(note)))`) as a key on `ext` alone (A3a); `naming.singularize` ignored
upper-case words, so `CUSTOMERS` never matched `CUSTOMER` (A4). Also noted, not fixed: the
Postgres/MySQL/SQL Server value samplers compare against an arbitrary slice of the foreign
table, which scores a valid FK into a large table near zero.

RSA work ships as a patch release (0.8.1); r2g raises its minimum. RSA's planned 0.9.0 is
BigQuery, so the patch must be cut before that lands or from a 0.8 branch.

## Plan B — read declared keys; consolidate introspection

The Cursor plan's connector migration (CSV, MySQL, SQL Server, PostgreSQL, Snowflake
introspection delegated to RSA), minus the storage contract that A1 now covers. It also
closes defect 2. Because it reverses the recorded decision in
`DESIGN-rsa-compat-layer.md` §10 step 6 ("introspection connectors kept local by design"),
it must say so and why.

## Decisions taken

- **Drafts, not auto-apply.** Suggested keys are written for human review; a wrong
  identity key creates duplicate vertices that surface much later.
- **Profiling lives in RSA,** per the recorded scope decision of 2026-10-02 (shared-memory
  `r2g_data-model_20261002_005837`).
