# Implementation & Test Plan: Classification Propagation & Entitlement-Aware Loading (PRD Phase 9)

> **Status: PLANNED (June 2026).** This is the detailed companion to PRD
> §"Phase 9: Classification propagation & entitlement-aware loading". It builds
> directly on the Phase 8 external-catalog backbone (`src/r2g/catalogs/`) shipped
> in 8a–8b.
>
> **Thesis.** Copying relational data into ArangoDB creates a new system of
> record with none of the source's access controls, and graph denormalization
> creates *new* sensitivity (the mosaic effect). r2g is a **migration tool, not
> a runtime authorization engine**. Its job is to **carry governance metadata
> across the relational→graph boundary** and **refuse to silently launder
> sensitive data** — not to enforce access at query time. Everything below is
> additive; the existing migration path is untouched except where it gains
> annotations and an opt-out gate.

## 1. Goal & scope

**Goal.** When a source is imported from a connected catalog, capture its
column-level classifications / owners / tiers, propagate them onto the target
graph (collection- and field-level annotations + column→property lineage),
**advise** the user before loading anything sensitive (with masking and
exclude-by-default), and **emit** the metadata a serving layer needs to enforce
(manifest + suggested RBAC / policy / tier layout).

**In scope (V1).**
- Column-level classification capture from OpenMetadata (the only provider with
  e2e coverage today).
- A classification *carrier* threaded `resolve_source → ResolvedSource →
  SourceConfig → Schema.Column → MappingConfig → target`.
- Mosaic recomputation (max-sensitivity over contributors).
- A pre-load entitlement report + threshold gate + transform-at-load masking.
- A classification manifest + suggested-RBAC / OPA / tier-layout artifacts.
- A UI sensitivity lens + entitlement panel.

**Explicitly out of scope (V1).**
- **Runtime enforcement.** r2g never sits in the query-time authz path.
- **Pulling engine GRANTs / RLS / column-masking rules.** These do not map
  across engines or to graph consumers; the catalog's tag/tier/owner layer is
  the portable carrier.
- **Mapping catalog policy → ArangoDB users automatically.** We *suggest* RBAC;
  binding to real identities is a serving-layer / IdP concern.
- **Writing classifications back to the catalog.** That is the Phase 8 write-path
  effort, still deferred.

**Relationship to prior phases.** Phase 8 supplies discovery + `CatalogAsset`.
Phase 5c supplies the `FieldExpression` engine reused for masking. Phase 5e
supplies the paint-only lens infrastructure reused for the sensitivity lens.
Phase 5d (filesystem catalog + Fernet) supplies the persistence + redaction the
classification carrier rides on.

## 2. The core problem (carry into every decision)

1. **Mosaic effect.** A vertex assembled from three tables, or an edge spanning
   two, can reveal a combined picture no single source column did. Classification
   on a combined entity is **recomputed as the max of contributors**, never
   blindly inherited.
2. **Staleness.** A one-time copy snapshots policy at migration time; source
   policy drift will not propagate. CDC/temporal mode can carry classification
   *changes* but not arbitrary policy. Plan for periodic re-sync.
3. **Identity mismatch.** Source DB roles ≠ ArangoDB users ≠ end-user SSO
   identities. Entitlements are ultimately expressed against org IdP groups,
   which is exactly why the catalog's identity-agnostic tag/tier/owner layer is
   the right carrier — not GRANTs.
4. **Lane discipline.** Artifact and command names must make explicit that r2g
   *advises and emits*, and the serving layer *enforces*.

## 3. Architecture (grounded in the current codebase)

### 3.1 Capture: column-level classification (P9.1)

Today `OpenMetadataProvider` already requests `fields=tags` on
database/schema/table entities and stores **asset-level** `tags` on
`CatalogAsset` (`src/r2g/catalogs/openmetadata.py`, `_asset_from_entity`). It
does **not** request column tags. Extend:

- `get_asset` for tables: request `fields=columns,tags,owners` (OpenMetadata
  returns per-column `tags`, `description`, and `glossaryTerms`; table `owners`;
  the `Classification`/`Tag` `mutuallyExclusive` + confidentiality tier live
  under tag FQNs like `PII.Sensitive`, `Tier.Tier1`).
- New normalized model in `src/r2g/catalogs/base.py`:
  ```text
  Classification (pydantic)
    tags: list[str]            # tag FQNs, e.g. "PII.Sensitive", "PersonalData.Personal"
    tier: str | None           # confidentiality tier FQN if present
    glossary_terms: list[str]
    source: str = "catalog"    # provenance of the classification
  ```
- Extend `CatalogAsset` with `column_classifications: dict[str, Classification]`
  (column name → classification) and `owners: list[str]`, `tier: str | None` at
  asset level. Keep the existing `tags` for back-compat.

Provider stays REST-over-`httpx`; the new reads are mocked in unit tests exactly
like the existing `test_openmetadata_provider.py`.

### 3.2 Carrier: thread classifications through the pipeline (P9.2)

This is the backbone; nothing in tiers 2–3 works without it.

1. **`ResolvedSource`** (`catalogs/base.py`): add
   `column_classifications: dict[str, dict[str, Classification]]` keyed
   `table → column → Classification`, plus `owners` / `tier`. Currently
   `ResolvedSource` drops tags — stop dropping them.
2. **`SourceConfig`** (`src/r2g/catalog.py`): persist an optional
   `classifications` blob (the resolved map) so it survives import and is
   available at snapshot/mapping/load time without re-querying the catalog.
   Encrypted-at-rest layer is unaffected (classifications are not secrets, but
   ride the same JSON catalog).
3. **`Column`** (`src/r2g/types.py`): add an optional
   `classification: Optional[Classification] = None`. Populated when a snapshot
   is taken for a catalog-imported source by merging
   `SourceConfig.classifications` onto the introspected columns
   (`r2g source snapshot`). Sources not imported from a catalog simply have
   `None` everywhere — fully backward compatible.
4. **Sensitivity lattice** — new `src/r2g/classification.py`:
   ```text
   SENSITIVITY_ORDER = ["public", "internal", "confidential", "restricted"]
   tier_of(classification) -> str            # map tag/tier FQNs -> a lattice level
   max_sensitivity(items) -> str             # mosaic rollup
   exceeds_threshold(level, threshold) -> bool
   ```
   A small, configurable tag→level map (defaults for common OpenMetadata tags:
   `PII.*`/`PHI.*` → `restricted`, `Tier.Tier1` → `confidential`, …) with an
   override hook in project config.

### 3.3 Propagate: target annotation + lineage (P9.3, P9.4)

- **Mosaic recompute (P9.4)** at mapping-build time over:
  - fan-in `FieldExpression.sources` (a target property's level = max of its
    source columns),
  - vertex collections (collection level = max of mapped/kept fields),
  - edges (edge level = max of endpoints + edge-carried fields).
  Implemented in `classification.py` operating on `MappingConfig` + annotated
  `Schema`.
- **Target annotations.** The transformers (`src/r2g/transformers/
  node_transformer.py`, `edge_transformer.py`) optionally stamp governance
  attributes on emitted documents — `_classification`, `_sensitivity`,
  `_source_owner` — gated by a project flag (off by default to avoid bloating
  every document; on for governed loads). Collection-level classification is
  written to a **sidecar metadata collection** `r2g_governance` (one doc per
  collection/edge: tier, owners, source lineage) since ArangoDB has no native
  per-collection metadata slot. The DLQ/manifest pattern from Phase 5b is the
  template.
- **Lineage manifest.** A machine-readable
  `<project>/governance/lineage.json`: each `source table.column → graph
  property/edge`, with the column's classification, the mosaic-recomputed entity
  level, and any masking applied (P9.6). This is the auditable record of what
  crossed the boundary.

### 3.4 Advise & gate (P9.5, P9.6)

- **`r2g entitlements report <project> [--threshold confidential]`**: walks the
  mapping + annotated schema, lists every mapped field at/above the threshold
  with source lineage and recomputed entity level. UI: a floating
  entitlement-report panel (P9.9).
- **Gate in `r2g load` / `POST /api/projects/{name}/load`**: above-threshold
  fields are **excluded by default** (added to `CollectionMapping.exclude_fields`
  for the run) unless the user passes `--allow-sensitive` / confirms in the UI,
  or has masked them. A dry-run prints the report and the exclude set without
  loading.
- **Transform-at-load masking (P9.6)**: reuse the existing `FieldExpression`
  engine. Provide helper expressions / templates — `HASH`, `TOKENIZE`, `REDACT`
  (constant), `NULLIFY` — and a one-click "mask this field" in the mapper that
  writes the appropriate `FieldExpression`. Masking choice is recorded in the
  lineage manifest, and a masked field is no longer treated as above-threshold
  for the gate.

### 3.5 Enable enforcement — emit, don't enforce (P9.7, P9.8)

On a governed load, emit (under `<project>/governance/`):
- **`classification-manifest.json`** — the canonical per-collection/edge/field
  classification + lineage (superset of the report).
- **`suggested-rbac.json`** — per-tier ArangoDB collection grants (Enterprise
  collection-level RBAC), as a *recommendation* the operator applies.
- **`policy.rego`** (optional) — an OPA/Rego stub keyed on collection +
  `_sensitivity` for app/proxy enforcement.
- **Tier-layout recommendation** — when `--tier-layout` is set, suggest (or
  generate) separate collections/databases/graphs per sensitivity tier so coarse
  collection-RBAC can bite.

**Re-sync (P9.8)**: `r2g catalog resync-classifications <project>` re-pulls
classifications from the bound catalog and refreshes `SourceConfig` +
annotations; CDC/temporal writes carry classification *changes* on changed rows.
A `classifications_synced_at` timestamp is surfaced (CLI + UI) so staleness is
visible.

### 3.6 UI (P9.9)

- **Classification lens** — a new paint-only `LensType` (per
  `/.cursor/rules/ui-architecture.mdc`): colors collections/edges/fields by
  sensitivity tier, with a legend stating the tier→color mapping and the mosaic
  rule. Switched via canvas **View As** (no relayout, no new route).
- **Entitlement panel** — a floating, dismissible panel (not a route) showing the
  pre-load report and feeding the load gate; context-menu-primary, consistent
  with the workspace contract.

### 3.7 Dependencies

None new for 9a–9b (OpenMetadata classification reads use the existing
`httpx`/`openmetadata` extra; masking uses the existing expression engine). 9c's
OPA/Rego output is a templated string artifact — no `opa` dependency.

## 4. Implementation milestones (file-level)

**9a — capture & propagate (the backbone; fully unit-testable)**
1. `src/r2g/catalogs/base.py` — `Classification` model; extend `CatalogAsset`
   (`column_classifications`, `owners`, `tier`) and `ResolvedSource`.
2. `src/r2g/catalogs/openmetadata.py` — request `columns,tags,owners`; populate
   column classifications in `get_asset` / `_asset_from_entity` / `resolve_source`.
3. `src/r2g/classification.py` — sensitivity lattice, tag→level map,
   `max_sensitivity`, threshold helpers, mosaic recompute over `MappingConfig`.
4. `src/r2g/catalog.py` — persist `SourceConfig.classifications`.
5. `src/r2g/types.py` — `Column.classification`.
6. `src/r2g/main.py` — merge classifications onto the snapshot in
   `source snapshot`; carry through `catalog import-source`.
7. `src/r2g/transformers/{node,edge}_transformer.py` — optional governance
   attributes + collection-level metadata emission (flag-gated).
8. Lineage manifest writer (new `src/r2g/governance.py` or fold into
   `classification.py`).

**9b — advise & gate**
9. `src/r2g/main.py` — `entitlements report` command; load-time threshold gate
   (`--allow-sensitive`, dry-run).
10. `src/r2g/ui/server.py` + `index.html` — entitlement panel + sensitivity lens;
    `GET /api/projects/{name}/entitlements`.
11. Masking helpers + mapper "mask this field" (`index.html` + expression
    templates; validation in the existing `validate_config`).

**9c — enable enforcement**
12. `src/r2g/governance.py` — classification manifest, `suggested-rbac.json`,
    `policy.rego`, tier-layout recommendation; wired into the load path
    (`--emit-governance`, `--tier-layout`).
13. `src/r2g/main.py` — `catalog resync-classifications`; CDC/temporal carry of
    classification changes.

Docs at each step: README (governance section + commands), CHANGELOG, PRD status.

## 5. Test plan

Mirrors the existing strategy: mocked-HTTP unit tests + a skip-when-unavailable
OpenMetadata e2e, gated on CI coverage (`--cov-fail-under=80`).

### 5.1 Unit tests (no network)
- `tests/test_classification.py` — sensitivity lattice ordering; tag→level map +
  overrides; `max_sensitivity` / mosaic recompute over fan-in, vertex, and edge
  cases; threshold comparison; masked-field demotion.
- `tests/test_openmetadata_provider.py` (extend) — column tags/owners/tier parsed
  from a mocked `tables?fields=columns,tags,owners` payload; `resolve_source`
  carries `column_classifications`; no secrets leak.
- `tests/test_catalog_registry.py` (extend) — `SourceConfig.classifications`
  round-trips through save/load.
- `tests/test_transformers_*` — governance attributes stamped only when enabled;
  collection-metadata docs written; identity path unchanged when disabled.
- `tests/test_entitlements.py` — `entitlements report` output; load gate excludes
  above-threshold fields by default; `--allow-sensitive` overrides; masked fields
  pass the gate.
- `tests/test_governance.py` — manifest / suggested-RBAC / Rego / tier-layout
  artifact shape; lineage manifest correctness incl. mosaic levels + masking.
- `tests/test_ui_api.py` (extend) — `GET /api/projects/{name}/entitlements`;
  lens metadata; redaction of owner contacts where appropriate.

### 5.2 Integration / e2e (OpenMetadata)
- Extend `tests/integration/test_openmetadata_e2e.py`: seed the OM
  `DatabaseService` (pointing at the test Postgres/MySQL) **with column tags**
  (`PII.Sensitive`, `Tier.Tier1`), then:
  1. `catalog import-source` → assert `SourceConfig.classifications` populated,
  2. `source snapshot` → assert `Column.classification` set on tagged columns,
  3. build a mapping with fan-in → assert mosaic recompute (entity = max),
  4. `entitlements report` → assert tagged fields listed,
  5. governed `load` with `--emit-governance` → assert manifest + annotations in
     ArangoDB (sidecar `r2g_governance`) and excluded/masked sensitive fields.

### 5.3 Verification gates (unchanged)
`ruff check src/ tests/`, `mypy src/r2g`, `pytest -m "not integration"
--cov=r2g --cov-fail-under=80`, and the Dockerized integration job.

## 6. Risks & open questions (for review)
1. **Tag→sensitivity mapping is org-specific.** The default map
   (`PII/PHI → restricted`, `Tier.Tier1 → confidential`, …) must be overridable
   per project. Confirm the default lattice and the override surface (project
   config vs. a global `r2g secrets`-style store).
2. **Annotation placement.** Per-document `_classification` bloats storage and
   couples governance to data. Proposed: collection-level metadata in a sidecar
   `r2g_governance` collection by default, per-document attributes only when
   explicitly enabled. Confirm.
3. **Mosaic policy beyond max.** "Max of contributors" is the safe default; some
   orgs want combination rules (e.g. quasi-identifiers that aggregate to PII).
   V1 ships max + a documented extension point; confirm that is enough.
4. **Default-exclude vs. default-warn.** Exclude-above-threshold-by-default is the
   safe stance but changes load behavior for governed sources. Confirm the
   threshold default (`confidential`?) and that non-catalog sources are
   unaffected.
5. **Tier-based physical layout** (separate collections/DBs/graphs per tier) is a
   large mapping/load change. V1 *recommends* the layout; *generating* it may
   slip to a 9c follow-up.
6. **Re-sync semantics.** How aggressively to re-pull (manual command vs.
   scheduled) and whether a re-sync that *raises* a field's tier should retro-gate
   already-loaded data (warn-only in V1).
7. **Owner PII.** Catalog owners may be individuals; redact contact details in
   API/log output via the existing redaction layer.

## 7. Recommendation
Build **9a (capture & propagate)** first and in full — it is the data-model
thread everything else depends on, it is entirely unit-testable against the
mocked OpenMetadata seam, and it already delivers value (a governed, annotated
graph + lineage) without any new enforcement surface. Then **9b (advise & gate)**
to make the migration *refuse to silently launder* sensitive data and offer
masking. Defer **9c (enforcement artifacts + re-sync)** until 9a/9b prove out,
and keep the lane discipline explicit throughout: r2g advises and emits; the
serving layer enforces.
