# Implementation & Test Plan: Denormalization & Normal-Form Analysis (PRD Phase 11)

> **Status: PLANNED (June 2026).** Detailed companion to PRD §"Phase 11:
> Denormalization & normal-form analysis". A **deterministic** (no-LLM) analyzer
> that detects source-side denormalization and advises on better graph models.
>
> **Thesis.** `ConfigManager.generate_default_config` maps every table 1:1 to a
> document collection and cannot see that a table carries an *embedded lookup*, a
> *repeating group*, a *multi-valued column*, or that two tables are an over-split
> 1:1 pair. Phase 11 detects these with the **same rules+sampling machinery
> already proven in `fk_inference.py`** and surfaces scored, evidence-backed
> *findings* with recommended graph remedies. It advises; it never rewrites the
> schema or data silently. It is also the deterministic *grounding* for the
> Phase 10 LLM proposal.

## 1. Goal & scope

**Goal.** Given a snapshot, detect denormalization smells and recommend a graph
remedy per finding (extract a vertex, embed an array, split a multi-valued
column, merge a 1:1 pair), with confidence + concrete evidence — then let the
user accept (scaffold the mapping) or dismiss.

**In scope (V1).**
- A deterministic analyzer engine (structural/name core + optional bounded value
  sampler), parallel to `infer_foreign_keys`.
- Detectors: repeating groups (P11.2), functional/transitive dependencies =
  embedded lookups (P11.3), redundant reference data (P11.4), multi-valued
  columns (P11.5), 1:1 over-normalization (P11.6).
- CLI + API + Studio review card; opt-in remediation scaffolding; Phase 10
  grounding.

**Explicitly out of scope (V1).**
- **Automatic schema rewriting** — findings change nothing until explicitly
  accepted and validated.
- **Full FD mining** — we test *candidate* determinants (keys, heuristic
  prefixes), not all 2^n column subsets.
- **BCNF/4NF formalism**; **entity resolution** / cross-table dedup.
- **Sending data anywhere** — sampling is local + bounded + classification-aware.

**Relationship to prior phases.**
- **`fk_inference.py`** is the template and the reuse target: the `DenormFinding`
  dataclass mirrors `FkCandidate`; `analyze_denormalization` mirrors
  `infer_foreign_keys`; the `create_value_sampler` factory + bounded-`LIMIT`
  query pattern (`PostgresValueSampler` / `MySQLValueSampler` /
  `SQLServerValueSampler` / `CsvValueSampler`) is extended with a small
  group-by/distinct probe.
- **`generate_default_config`** stays the 1:1 default; remediation scaffolding
  edits the resulting `MappingConfig` through the normal save + `validate_config`.
- **Suggest FKs UI** (P6.6) is the exact UX template for the findings card.
- **Phase 9 classifications** gate which columns may be value-sampled.
- **Phase 10** consumes findings as grounding (and vice-versa).

## 2. Detectors (deterministic signals)

| Kind | Signal | Sampling? | Recommended remedy |
| :--- | :--- | :--- | :--- |
| `repeating_group` | Numbered/suffixed column families (`phone1/phone2`, `addr_line_1..3`) or repeated typed sets | No (name+type) | Child collection or embedded array |
| `embedded_lookup` (FD/2NF/3NF) | A non-key column **determines** other non-key columns: group by candidate determinant → dependents single-valued per group | Yes | Extract `{determinant + dependents}` into a shared vertex; link by edge |
| `redundant_reference` | Co-varying column set with few distinct combinations relative to rows | Yes (distinct ratio) | Extract a lookup/vertex |
| `multi_valued` | Consistent delimiters in a text column (`"a,b,c"`) | Yes (delimiter rate) | Split into array / child collection |
| `one_to_one` | Two tables strict 1:1 on identical/shared key | Yes (cardinality) | Merge / embed instead of two collections + edge |

**Candidate determinants for FD probing (P11.3)** are bounded: PK columns,
columns matching `*_id` / `*_code` / `*_key`, and low-cardinality columns from a
cheap distinct-count probe — never the full subset lattice. Each finding records
the exact probe and counts as evidence.

## 3. Architecture (grounded in the current codebase)

### 3.1 Engine (P11.1)

New `src/r2g/denorm.py`:
```text
DenormFinding (dataclass)
  kind: str                  # repeating_group | embedded_lookup | ...
  table: str
  columns: list[str]         # the involved columns (e.g. determinant + dependents)
  recommended_action: str    # extract_vertex | embed_array | split | merge
  confidence: float
  evidence: str              # human-readable, with sampled counts/examples

AnalyzeOptions (dataclass): sample, sample_limit, min_confidence, classifications
analyze_denormalization(schema, options, sampler=None) -> list[DenormFinding]
```
Pure-Python structural detectors (repeating groups, 1:1 by key shape) run with no
sampler. The FD / redundant / multi-valued detectors call an optional sampler.

### 3.2 Sampler extension (P11.3–P11.6)

Extend the existing sampler seam (`create_value_sampler`,
`src/r2g/fk_inference.py:1035`) with bounded probes used by the analyzer (one
small `LIMIT` CTE per probe, resilient → `None` on error):
- `group_single_valued(table, determinant_cols, dependent_col) -> float` —
  fraction of determinant groups with exactly one dependent value (FD strength).
- `distinct_ratio(table, cols) -> float` — distinct combos / rows.
- `delimiter_rate(table, col, delim) -> float` — fraction of sampled values
  containing the delimiter.
- `pair_is_1to1(table_a, key_a, table_b, key_b) -> bool` — cardinality check.

These reuse each connector's existing connection/limit machinery
(PG/MySQL/SQLServer/CSV), so all four sources are covered as they already are for
FK inference. CSV uses Polars (type-inference-off) as in `CsvValueSampler`.

### 3.3 Entry points (P11.7, P11.8)

- **CLI** (`src/r2g/main.py`): `r2g source analyze-denorm <name> [--sample]
  [--sample-limit N] [--min-confidence 0.4] [--json]` → Rich table of findings
  (kind, table, columns, action, confidence pill, evidence). Read-only.
- **API** (`src/r2g/ui/server.py`): `POST /api/sources/{name}/analyze-denorm`
  returning scored findings (+ the snapshot lookup, mirroring
  `POST /api/sources/{name}/infer-fks`).
- **UI** (`index.html`): a findings floating card cloned from the Suggest-FKs card
  (`.fk-suggest-*` styles) — confidence pill, evidence line, per-row Accept /
  Dismiss + Dismiss all; opened from the Actions / canvas context menu.

### 3.4 Remediation scaffolding (P11.9)

Accepting a finding edits the project `MappingConfig` and saves through the normal
path (+ `validate_config`), never silently:
- `embedded_lookup` / `redundant_reference` → add the extracted
  `CollectionMapping` + an `EdgeDefinition` (collision-proof naming as in FK
  accept), and `exclude_fields` the moved columns on the origin collection.
- `multi_valued` → add a `FieldExpression` that splits the column into an array
  (engine-appropriate), or scaffold a child collection.
- `repeating_group` → scaffold a child collection keyed back to the parent.
- `one_to_one` → mark the pair for merge (embed the secondary's fields).

### 3.5 Phase 10 grounding (P11.10)

`analyze_denormalization` output is serializable into the Phase 10 prompt digest
(`src/r2g/llm/prompt.py`) so the LLM proposal is grounded in deterministic
evidence; conversely a Phase 10 embed/extract suggestion can be checked against
Phase 11 findings. Phase 11 has **no** LLM dependency.

## 4. Implementation milestones (file-level)

**11a — engine + CLI (deterministic core)**
1. `src/r2g/denorm.py` — `DenormFinding`, `AnalyzeOptions`,
   `analyze_denormalization`; structural detectors (repeating group, 1:1 shape)
   + FD detector (sampler-driven).
2. `src/r2g/fk_inference.py` — add the bounded probe methods to each sampler +
   surface via `create_value_sampler` (or a sibling factory).
3. `src/r2g/main.py` — `source analyze-denorm` command.
4. Classification-aware column filtering (skip Phase-9 Restricted/PII; until
   Phase 9 lands, a no-op hook + a `--no-sample-columns` escape hatch).

**11b — more detectors + Studio review**
5. `src/r2g/denorm.py` — redundant-reference, multi-valued, refine 1:1.
6. `src/r2g/ui/server.py` — `POST /api/sources/{name}/analyze-denorm`.
7. `src/r2g/ui/static/index.html` — findings card (clone Suggest-FKs) + action.

**11c — remediation + grounding**
8. Accept → mapping scaffolding (collection/edge/expression) via the save path.
9. `src/r2g/llm/prompt.py` hook to include findings (Phase 10).

Docs at each step: README (analysis section), CHANGELOG, PRD status.

## 5. Test plan

Mirrors `test_fk_inference.py`: a **fake sampler** returning canned probe values
drives the sampling detectors deterministically; structural detectors need no
sampler. No live DB in unit tests.

### 5.1 Unit tests (no network)
- `tests/test_denorm.py`
  - **repeating_group**: `phone1/phone2/phone3`, `addr_line_1..3` detected;
    unrelated numeric-suffixed names (`md5`, `sha256`) not misfired.
  - **embedded_lookup (FD)**: with a fake sampler reporting `group_single_valued
    ≈ 1.0` for `zip → city,state`, a finding is emitted with the right
    determinant/dependents + evidence; near-0 → no finding; veto/threshold via
    `min_confidence`.
  - **redundant_reference**: low distinct-ratio → finding; high → none.
  - **multi_valued**: high delimiter rate → finding; punctuation-in-prose → none.
  - **one_to_one**: 1:1 key shape + cardinality → merge finding; 1:N → none.
  - **resilience**: sampler returning `None`/raising → structural signals still
    returned, no crash.
  - **classification gate**: columns marked Restricted are not passed to the
    sampler.
- `tests/test_cli.py` (extend) — `source analyze-denorm` output + `--json` +
  `--min-confidence`.
- `tests/test_ui_api.py` (extend) — `POST /api/sources/{name}/analyze-denorm`
  returns findings; accept → valid `MappingConfig` (passes `validate_config`).
- `tests/test_fk_inference.py` (extend) — the new bounded probe methods on each
  sampler (mocked drivers, as today).

### 5.2 Integration / e2e
- Extend the existing connector e2e (`tests/integration/test_*_e2e.py`): seed a
  table with an embedded lookup (`zip → city,state`) and a multi-valued column;
  `analyze-denorm --sample` against the **real** PG/MySQL → assert the expected
  findings. Auto-skips when the DB/Arango is unavailable (existing `requires_*`).

### 5.3 Verification gates (unchanged)
`ruff check src/ tests/`, `mypy src/r2g`, `pytest -m "not integration"
--cov=r2g --cov-fail-under=80`, and the Dockerized integration job.

## 6. Risks & open questions (for review)
1. **False positives on FD.** A determinant can look functional in a bounded
   sample but not globally. *Mitigation:* sample-as-evidence framing + confidence
   + human accept; never auto-apply. Confirm default `min_confidence` and sample
   size.
2. **Candidate-determinant explosion.** Restricting probes to keys + heuristic +
   low-cardinality columns keeps it bounded; confirm the heuristic set.
3. **Multi-valued delimiter ambiguity.** Commas in free text vs. real lists —
   require a high, consistent delimiter rate + uniform token shape; confirm
   thresholds.
4. **Classification ordering.** 11a ships before Phase 9 lands; default to a hook
   that samples all columns *unless* the user excludes them (`--no-sample-columns`),
   and wire the Phase-9 gate when available. Confirm acceptable.
5. **Remediation correctness.** Auto-scaffolding an extracted vertex + edge must
   produce a mapping that round-trips through `validate_config`; keep V1
   scaffolding conservative (advisory text + minimal scaffold) and expand later.
6. **Sampler cost.** Group-by probes are heavier than FK overlap; enforce the
   bounded `LIMIT` and a per-run probe budget.

## 7. Recommendation
Build **11a** first: the engine + the two highest-value, lowest-risk detectors
(repeating groups — structural, zero sampling; embedded-lookup FD — the flagship)
+ the CLI, all exercised against a fake sampler. It immediately improves model
quality (catching embedded lookups the 1:1 Auto-Map turns into redundant
properties) with no schema rewriting and no LLM. Then **11b** for the remaining
detectors + the Studio review card, and **11c** for opt-in remediation scaffolding
and Phase 10 grounding. Keep the discipline explicit: r2g detects and advises with
evidence; the user decides; the deterministic pipeline validates and loads.
