# PLAN — Federation Forge: the reverse generator (walking skeleton)

**Status:** S1 walking skeleton shipped (Postgres, r2g #1); **S2 dialects shipped**
(snowflake / clickhouse / arango behind the same seam — see "S2 dialects" below)
**Commissioned by:** contextual-data-fabric ADR-0006 (accepted 2026-09-08),
which assigns r2g the generator core (its D-4): r2g owns the ontology→schema
mapping machinery in both directions, so the *reverse* direction — generate a
physical schema and synthetic data **from** an ontology — lands here.
**Consumer:** the fabric's M15 orchestration (shape descriptors, partitioning
across engines, expected-catalog/golden computation) stays in
contextual-data-fabric. r2g ships only the per-dialect seam:

```
generate(ontology, dialect, seed) -> {ddl, loader, rows}
```

## Why (one paragraph)

Every correctness claim the fabric makes is validated against one hand-built
corpus. ADR-0006's insight: the estate already owns ontology↔schema mapping in
the forward direction (introspect → Auto-Map → CSI). Run it in reverse and
every generated federation is born with its ground truth attached — the
generating ontology IS the expected aligned ontology. The core correctness
property is the roundtrip through the REAL forward pipeline (never through
forge-internal shortcuts):

```
introspect(generate(O)) ≡ O        (ADR-0006 D-3)
```

with `≡` defined honestly: equality up to CC-12 naming normalization and
declared-constraint availability.

## Decisions (r2g-side)

### F-1 · The ontology input is the CSI v1 conceptual model

ADR-0006's shape descriptor names an `ontology.ttl`, but r2g has never parsed
OWL/TTL and the estate's only *validated* ontology interchange contract is CSI
v1 (`schemas/csi_v1.schema.json`), which `arango-schema-analyzer` already
produces in reverse. The forge therefore takes a **conceptual ontology** shaped
exactly like `conceptualModel` in CSI v1 — entities, properties, relationships
— with one extension the skeleton needs: each property carries a JSON `type`
(`integer | float | boolean | string`), which CSI already permits (the fabric's
`arango-cmf.json` emits it today). TTL→conceptual-model conversion is the
fabric orchestration's job when the descriptor lands (S3); if the team wants
TTL ingestion in r2g instead, that is a one-module follow-up, not a rework.
**Flagged for review in the skeleton PR.**

### F-2 · Naming is the declared inverse of CC-12, asserted through the real normalizer

`csi.owl_entity_name` / `csi.owl_property_name` are lossy and non-injective,
so the generator declares one canonical physical spelling and the roundtrip
asserts through the *normalizer*, never against a hoped-for literal:

- class `UsageMetric` → table `pluralize(convert_identifier(name, "snake"))`
  = `usage_metrics`; property `healthScore` → column `health_score`;
- the test asserts `owl_entity_name(generated_table) == input_class` and
  `owl_property_name(generated_column) == input_property` — the exact
  functions the forward CSI emitter runs.

Skeleton guard: the generator **refuses** an ontology whose class names don't
survive its own roundtrip (`owl_entity_name(pluralize(snake(C))) != C`, e.g.
singularize's naive trailing-`s` rule) — fail loudly at generate time, never
at compare time.

### F-3 · One canonical Postgres type per JSON type

`config.DEFAULT_TYPE_MAP` is many-to-one, so the inverse picks one
roundtrip-stable spelling per JSON type and the comparison runs through
`pg_type_to_json_type` on both sides:

| conceptual `type` | generated DDL | introspects back as | JSON type again |
|---|---|---|---|
| `integer` | `bigint` | `bigint` | `integer` |
| `float` | `double precision` | `double precision` | `float` |
| `boolean` | `boolean` | `boolean` | `boolean` |
| `string` | `text` | `text` | `string` |

Temporal/decimal/uuid categories (RSA's `normalized_type_category`) are still
out — see S2-5 for why; the four types cover every property in the fabric's
live CSIs. Each S2 dialect declares its own physical spelling per JSON type
(`dialects/<name>.py`); the comparison still runs through
`pg_type_to_json_type` on the introspected side.

### F-4 · Keys and relationships: declared constraints first

Every entity gets `id bigint PRIMARY KEY`. Every conceptual relationship
`fromEntity → toEntity` becomes a real `FOREIGN KEY` column
`<singular_snake(toEntity)>_id` on the from-table referencing `<to-table>(id)`
— so Auto-Map re-derives the edge as `<from>_to_<to>` and the CSI names it
back to the input's lowerCamel relationship type. The ADR's
constraint-stripped variant (keys recovered by inference OR reported absent —
never silently wrong) is exercised live for the first time by the `clickhouse`
dialect (S2-3, where no FK syntax exists at all); *injecting* it into dialects
that could declare keys is the S3 denormalizer's job.

### F-5 · Data synthesis: once, seeded, spine-safe (ADR-0006 D-2/D-5)

`random.Random(seed)` end to end; a descriptor re-run must be byte-identical.
Rows are synthesized per entity in relationship-topological order
(`topo_sort`), and every FK value is drawn from the already-synthesized parent
`id`s — join-spine agreement by construction. Values are type-driven and
deliberately naive (the skeleton's "naive synthesis"); vocabularies,
cardinality shaping, and statistics arrive with the fabric's descriptor knobs.

### F-6 · Collision-free by construction (skeleton)

`csi._resolve_label_collisions` actively mutates conceptual models (qualify
renames; `roles` drops PK-collision labels), so a colliding ontology cannot
roundtrip verbatim. The skeleton generator refuses input ontologies where two
entities share a property label; the roundtrip integration test emits with the
default policy and asserts `provenance.labelCollisions == []`. Deliberate
collisions are a *denormalizer feature* (the injected-collision report is the
expected artifact) — S3.

## S2 dialects — the other three launch-set targets behind one seam

ADR-0006 D-4 names Postgres, Snowflake-SQL, ClickHouse-SQL, and Arango
collections as the launch set. `generate()` keeps its signature; internally it
is now three pieces so a dialect is a *projection*, never a re-derivation:

```
plan_schema(O)            -> SchemaPlan   (canonical snake_case tables/columns + roles; pure fn of O)
synthesize_rows(plan, s)  -> rows         (random.Random(seed) once; keyed by canonical names)
DIALECTS[d].render_*(…)   -> ddl, loader  (the only dialect-specific code)
```

`r2g.forge.dialects` is the registry (`DIALECTS`, `get_dialect`,
`SUPPORTED_DIALECTS`). Adding a dialect = one module following
`dialects/base.py` + one registry line; the shared unit tests parametrize over
the registry, so type-table completeness and the D-2 row-identity property are
asserted for every dialect automatically.

### S2-1 · Rows are byte-identical across dialects (ADR-0006 D-2)

`rows` is the pre-partition dataset and is keyed by *canonical* names
(`accounts.account_name`) in every dialect; the Snowflake loader spells them
`ACCOUNTS.ACCOUNT_NAME` at emit time, the Arango loader adds `_key` at load
time. `tests/test_forge_dialects.py` asserts the JSON of `rows` is identical
across all four dialects for the same `(ontology, seed, rows_per_entity)`.

### S2-2 · `snowflake`: unquoted UPPERCASE, declared-but-unenforced keys

Identifiers are emitted unquoted, so Snowflake folds them to UPPERCASE — the
spelling real customer schemas have and the one the forward pipeline handles
(`singularize`/`convert_identifier` are case-insensitive since #2:
`ACCOUNTS` → `Account`, `ACCOUNT_NAME` → `accountName`). Quoting lower-case
names would also round-trip but exercises a spelling no real schema has.
`PRIMARY KEY`/`FOREIGN KEY` are declared; Snowflake records them without
enforcing them, and RSA's `SnowflakeConnector` reads them back via
`SHOW PRIMARY KEYS` / `SHOW IMPORTED KEYS` — the declared path F-4 wants.

Types: `NUMBER(38,0)` / `FLOAT` / `BOOLEAN` / `VARCHAR`. **Known, pinned gap:**
Snowflake's only integer type is `NUMBER(38,0)` and `INFORMATION_SCHEMA`
reports it as bare `NUMBER`, which `pg_type_to_json_type` maps to `float`
(the connector does not carry `NUMERIC_SCALE`). The roundtrip test names this
in `KNOWN_TYPE_GAPS` and fails if the gap either widens or silently closes —
D-3's "never silently wrong", applied to types. Closing it is an RSA + r2g
follow-up (report scale; scale-0 `NUMBER` → `integer`).

Roundtrip: `tests/integration/test_forge_roundtrip_snowflake.py` — throwaway
schema `FORGE_RT_<seed>_<pid>_<hex>` in the real account, dropped in `finally`;
skips with a reason when creds are unset **or when the role may not
`CREATE SCHEMA`** (the fabric's `CDF_RO` cannot; set `SNOWFLAKE_FORGE_ROLE`).
RSA's connector is URL-driven and cannot carry a private-key path, so the
fixture shims `snowflake.connector.connect` for *auth only*; every
introspection statement is RSA's.

### S2-3 · `clickhouse`: the first live constraint-stripped shape

ClickHouse has no FK syntax at all. The dialect emits `MergeTree … ORDER BY
(id)` (so `system.columns.is_in_primary_key` surfaces the PK) and records the
FK *intent* as a column `COMMENT 'forge:foreign-key -> accounts(id)'` plus
loader header comments. Types `Int64`/`Float64`/`Bool`/`String`, properties
`Nullable(…)`.

**Caveat, stated plainly:** RSA has **no ClickHouse connector**. The roundtrip
therefore introspects through **r2g's own `ClickHouseConnector`** (system
tables into RSA's `PhysicalSchema` shape — the real forward leg the fabric uses
for ClickHouse), then runs `rsa_ontology` and Auto-Map + CSI over it. D-3's
two-branch contract is asserted both ways: `get_schema()` reports FKs *absent*
(never invented from the comment); r2g's `infer_foreign_keys` and RSA's
baseline both *recover* exactly the planned edges; with no keys applied,
Auto-Map derives **no** edge rather than a wrong one. Runs against the new
compose `clickhouse` service (HTTP 8124, so a developer's other stack on 8123
survives; `CLICKHOUSE_DSN` overrides) and in CI.

### S2-4 · `arango`: a manifest and a loader script, not SQL

One document collection per entity (`accounts`), one edge collection per
relationship named exactly as the forward Auto-Map derives it
(`contacts_to_accounts`, `_from`/`_to` on the spine ids), documents = rows with
`_key = str(id)`, edges carry only `_key/_from/_to`. `ForgeArtifacts` keeps its
field names but reinterprets them: `ddl` is a JSON collection manifest
(`forge.collections.json`: collections, edge projections, a named graph
`forge`), `load_sql` a standalone python-arango loader (`forge.load.py`, env
driven, idempotent). `project_documents(manifest, rows)` is the same projection
as a library call; a unit test execs the script in isolation and checks the two
agree.

Roundtrip: `tests/integration/test_forge_roundtrip_arango.py` loads by
*running the generated script*, then `arangodb-schema-analyzer`'s
`AgenticSchemaAnalyzer` (deterministic baseline, no LLM) under both the
`auto` and `collection` strategies reproduces the ontology up to the CC-12
normalizers (ASA reports `account_name` and `CONTACTS_TO_ACCOUNTS`). ASA's
baseline does not type properties (everything is `string`), so type fidelity
is asserted on the SQL dialects only.

### S2-5 · Temporal stays out (for now)

The brief lists `TIMESTAMP_NTZ` / `DateTime64`, but the forward map collapses
every temporal type to `string`, so a `temporal` conceptual type has no honest
roundtrip today. Adding one is a forward-pipeline change first (a distinct JSON
type in `DEFAULT_TYPE_MAP` and the CSI), then a one-line entry per dialect.

## Deliverables (S1 PR)

| # | Artifact | Where |
|---|---|---|
| 1 | This plan | `docs/internal/PLAN-federation-forge.md` |
| 2 | Generator core: `ForgeOntology` loader/validator, `generate(ontology, dialect="postgres", seed)` → `ForgeArtifacts(ddl, load_sql, rows)` | `src/r2g/forge.py` |
| 3 | CLI: `r2g forge generate --ontology o.json --seed 421 --out-dir …` (thin body, function-local imports, validate-before-try) | `main.py` sub-app |
| 4 | Unit tests: determinism (byte-identical re-run), naming inverse through the real `owl_*` normalizers, type inverse through `pg_type_to_json_type`, FK spine agreement, refusal cases | `tests/test_forge.py`, `tests/test_cli_forge.py` |
| 5 | Live roundtrip: temp PG schema ← DDL+load, `PostgresConnector.get_schema` → `generate_default_config` → `mapping_to_csi` → compare to input ontology (D-3, real pipeline, no forge shortcuts) | `tests/integration/test_forge_roundtrip.py` |

Non-goals here: partitioner/denormalizer/goldens (S3),
scale knobs (S4), any change to `MappingConfig`/`Schema` serialization
(byte-stability guard stays untouched), PRD phase-table entry (goes through
`/prd-sync` with the user, not a silent edit).

## The honest-fidelity caveat (from recon, worth keeping visible)

`MappingConfig` sits between schema and CSI and carries derived decisions
(join-table flags, edge names). The generator emits physical schemas and the
re-introspection re-derives those decisions independently via Auto-Map —
*those two derivations agreeing is the actual content of the fidelity claim*.
That is exactly what ADR-0006 wants tested (the estate is in the loop), and it
is why the roundtrip test compares CSI conceptual models, not intermediate
artifacts.
