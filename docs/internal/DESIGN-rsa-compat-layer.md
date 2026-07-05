# Design: r2g ↔ relational-schema-analyzer compatibility layer

> **Scope.** This is the design for the **compatibility layer** — ADR
> `PLAN-rsa-dependency-reversal.md` **step 2** (introduce `r2g.types` re-exports
> backed by RSA types) and the **step 3** migration concerns it forces. It is the
> concrete follow-on to the shipped **step 1** (RSA 0.2.0's `extra` passthrough on
> `Column`/`Table`). It does **not** cover deleting the duplicate `fk_inference` /
> `connectors` modules or flipping the hard dependency (ADR steps 4–6); those are
> gated on this layer landing green.
>
> **Status: PROPOSED.** Requires sign-off on the two open decisions in §9 before
> implementation.

## 1. Goal & non-goals

**Goal.** Make r2g's physical type model (`Schema`/`Table`/`Column`/`ForeignKey`)
*be* RSA's (`PhysicalSchema`/`Table`/`Column`/`ForeignKey`) — so the two projects
share one introspection type set — **without**:

- dropping r2g's Phase-9 `Column.classification` governance data;
- changing the on-disk shape of existing snapshots, `~/.r2g/catalog.json`, or saved
  `MappingConfig` files (the ADR's "byte-stable `model_dump()`" gate);
- breaking any of the ~37 modules that `from r2g.types import …` or the ~15 that
  touch `classification`.

**Non-goals (this step).**

- Deleting r2g's `fk_inference.py` / `connectors/*` (ADR steps 4–6). We *reconcile
  the type boundary only*, keeping those modules but making them speak the shared
  types.
- Adopting RSA's *richer* serialized column shape (`is_unique`, `ordinal`,
  `type_category`, …) into persisted r2g artifacts. That is an optional later
  enrichment (§7, Strategy 2), not required to unify the types.
- Moving the ArangoDB models (`MappingConfig`, `CollectionMapping`,
  `EdgeDefinition`, `FieldExpression`, `NamingConvention`) or `RESERVED_ATTRIBUTES`
  out of r2g. They are r2g-owned forever.

## 2. The central problem

Today r2g's `Column` is a 5-field model with **no custom serializer**, so it dumps:

```json
{ "name": "email", "data_type": "text", "is_nullable": true,
  "is_primary_key": false, "classification": null }
```

RSA's `Column` (0.2.0) dumps a different shape — extra enrichment fields plus the
computed `type_category`, `classification` **absent**, and (new) `extra` omitted
when empty:

```json
{ "name": "email", "data_type": "text", "is_nullable": true,
  "is_primary_key": false, "default": null, "comment": null,
  "ordinal": null, "type_category": "string" }
```

Naively aliasing `Column = rsa.Column` therefore (a) **loses `classification`** and
(b) **changes every persisted column's bytes**. `classification` lives on:

- every `Column` inside every `SchemaSnapshot.schema` embedded in
  `~/.r2g/catalog.json` (persisted via `Catalog.model_dump(mode="json")`);
- `SourceConfig.classifications: dict[str, dict[str, Classification]]` (a separate
  resolved map, stamped onto columns at `source snapshot` by
  `classification.apply_*`);
- standalone `Schema.save_to_file()` snapshot JSON.

So this is a **type-model reconciliation with a persisted-shape contract**, not a
rename. The design below preserves both the type unification *and* the byte shape.

## 3. Chosen approach — thin r2g subclasses over RSA types, legacy-preserving serialization

Keep a small `r2g.types` module, but back the physical types with RSA:

```python
# r2g/types.py  (sketch — see §4 for the serialization contract)
from relational_schema_analyzer.types import (
    Column as _RsaColumn,
    Table as _RsaTable,
    ForeignKey as ForeignKey,          # re-export: r2g's FK is a compatible subset
    PhysicalSchema as _RsaSchema,
)

class Classification(BaseModel): ...   # unchanged, stays in r2g

class Column(_RsaColumn):
    """RSA's column + r2g's Phase-9 governance classification."""

    classification: Optional[Classification] = None   # first-class field (see note)

    @model_validator(mode="before")   # tolerate RSA-native `extra['classification']`
    @classmethod
    def _accept_classification_from_extra(cls, data): ...

    @model_serializer(mode="wrap")    # emit r2g's historical 5-key shape
    def _serialize(self, handler): ...

class Table(_RsaTable):
    columns: list[Column]              # narrow the element type to r2g's Column

class Schema(_RsaSchema):
    tables: dict[str, Table] = {}
    def save_to_file(self, path): ...
    @classmethod
    def load_from_file(cls, path): ...

# ArangoDB models + ForeignKey stay in r2g. ForeignKey is a byte-identical
# superset in RSA, so it is re-exported (`from rsa.types import ForeignKey`).
```

> **Implementation note (as built).** `classification` landed as a **first-class
> field** on the `Column` subclass rather than a property over
> `extra['classification']`. This is simpler, avoids pydantic property-setter
> friction, and keeps the ~15 governance modules' `col.classification` reads/writes
> unchanged. RSA's `extra` passthrough (step 1) is still honored on **input** (the
> `_accept_classification_from_extra` validator lifts it onto the field) so
> RSA-native producers round-trip, and it remains the storage mechanism if/when
> Strategy 2 persists the RSA-native shape.

Why subclasses, not bare aliases:

- **Preserves `classification` as a first-class attribute** (`col.classification`)
  so the ~15 governance modules keep working unchanged, while the *storage* is the
  RSA `extra` passthrough — exactly what step 1 shipped for.
- **Lets r2g control serialization** (§4) to keep bytes stable.
- **`isinstance(col, rsa.Column)` is true**, so RSA's own functions (typemap,
  baseline, fk heuristics) accept r2g columns directly — no JSON round-trip. This is
  what makes the eventual steps 4–6 (import RSA's `fk_inference`/heuristics) possible.

### Rejected alternatives

- **Bare alias `Column = rsa.Column` + adopt RSA shape.** Cleanest type story but
  drops `classification` and rewrites every persisted artifact → forces a hard
  migration and rewrites dozens of dump-assertion tests. Deferred to §7 Strategy 2,
  not the first landing.
- **Keep r2g's own `Column`, adapter only at the boundary.** That is Stage 1
  (`rsa_ontology.py`'s JSON bridge) — no unification, so steps 4–6 never unlock.
- **Composition (`Column` *has-a* `rsa.Column`).** Loses `isinstance`, so RSA code
  paths still need conversion shims. Strictly worse than subclassing for our goal.

## 4. Serialization contract (byte-stability)

The compat `Column` overrides serialization to emit **exactly today's r2g shape**,
and to load **both** the legacy and the RSA-native shapes. This is the crux that
makes the change a no-op on disk.

```python
class Column(_RsaColumn):
    @model_serializer(mode="wrap")
    def _serialize(self, handler):
        # Emit r2g's historical 5-key shape; hide RSA enrichment + `extra`.
        clf = self.classification
        return {
            "name": self.name,
            "data_type": self.data_type,
            "is_nullable": self.is_nullable,
            "is_primary_key": self.is_primary_key,
            "classification": clf.model_dump() if clf else None,
        }

    @model_validator(mode="before")
    @classmethod
    def _accept_legacy_and_native(cls, data):
        # Legacy: top-level `classification` → extra['classification'].
        # Native/RSA: `extra['classification']` already present. Tolerate both;
        # ignore RSA enrichment keys on read (they were never in r2g snapshots).
        ...
```

Consequences:

- `Column.model_dump()` / `model_dump_json()` are **byte-identical** to today for
  every existing column (including the `"classification": null` default). **Zero
  migration** of `catalog.json`, snapshots, or mapping files (§6 confirms this).
- RSA enrichment (`is_unique`, `ordinal`, `type_category`, …) is available
  **in-memory** (e.g. for the `--engine rsa` path, which benefits from it) but is
  **not persisted** by r2g — matching the current contract.
- `Table` / `Schema` need the same treatment **only if** their RSA supersets change
  the dump; `Table` gains `extra` + enrichment fields, so `Table` also gets a
  wrap-serializer that emits its legacy keys (`name`, `columns`, `primary_key`,
  `foreign_keys`, `is_partitioned`, `partition_of`). `ForeignKey` is already a
  compatible subset and can be re-exported as-is (verify with the corpus).

**This contract is the single most important invariant and is enforced by the
compat corpus in §8.**

## 5. Dependency positioning (Open Decision 1)

`r2g.types` is imported by ~37 modules across the core (pipeline, catalog,
connectors, governance). If it imports `relational_schema_analyzer` at module load,
**RSA becomes a hard core dependency of every r2g install** — it can no longer be
just the `[ontology]` extra.

RSA's *core* (`relational_schema_analyzer.types`) only needs `pydantic`, so the
runtime weight is negligible; its heavy bits (connectors: duckdb, snowflake,
databricks; owl; mcp) are its own optional extras and are **not** pulled in by
importing `types`. Recommendation:

- Promote `relational-schema-analyzer` to a **core runtime dependency** in
  `pyproject.toml` (`>=0.2.0,<0.3.0`), and **remove** the `[ontology]` extra (or
  keep it as an empty back-compat alias for one release).
- Keep RSA's connector/owl/mcp extras out of r2g's default install.

This is a deliberate, ADR-anticipated coupling ("promote RSA … to a hard core
dependency"). It is acceptable **because** we are unifying types (high value), not
just de-duping ~140 LOC. **Needs sign-off** (§9).

**Import-safety verified.** RSA's `__init__` eagerly imports several submodules,
but they lazy-import their heavy optional deps: `connectors/__init__` loads only
`.base`/`.session` (concrete drivers like `psycopg` are imported inside
`create_connector`), and `owl_export`/`providers`/`tool` import `rdflib`/`openai`/
`anthropic`/`mcp` inside functions. RSA's only core deps are `pydantic` +
`jsonschema`; the sole other top-level third-party import reachable from
`import relational_schema_analyzer.types` is `polars`, already an r2g core
dependency. So `import r2g.types` works on a minimal r2g install (adds only the
light `jsonschema` transitive dep).

## 6. Migration & back-compat

With Strategy 1 (§4) the serialized shape is unchanged, so **no data migration is
required**. The read path still needs to be *tolerant* for forward-compat:

- **Read-time upgrader (defensive):** the `_accept_legacy_and_native` validator
  accepts columns that already carry `extra['classification']` (in case a future
  RSA-native producer writes them) and ignores unknown RSA enrichment keys. This
  makes `catalog.json` / snapshots readable regardless of which shape wrote them.
- **`SourceConfig.classifications`** (the separate resolved map) is untouched — it
  is not a `Column` and keeps its `dict[str, dict[str, Classification]]` shape.
- **No forced re-snapshot**, ever. If we later opt into Strategy 2 (persist RSA
  shape), that release adds a one-shot `catalog.json` rewrite behind a version bump,
  with the tolerant reader already in place from this step.

## 7. Follow-on enrichment (Strategy 2, optional, later)

Once the compat layer + corpus are green, a *separate* opt-in release may persist
RSA's richer column shape (enrichment + `extra['classification']`) to give r2g
snapshots `is_unique`/`ordinal`/etc. That release would:

1. Drop the legacy-preserving serializer (let RSA's own serializer run).
2. Bump a catalog schema version and add the one-shot upgrader.
3. Update the dump-assertion tests to the new shape.

Explicitly **out of scope** here; listed so the serializer in §4 is understood as a
*compatibility shim with a defined exit*, not permanent.

## 8. Test gates

Land nothing until all are green (`pytest -m "not integration"`, `ruff`, `mypy`):

1. **Serialization compat corpus (new, write first).** Freeze real artifacts
   *before* the change: a `catalog.json` with classified + unclassified columns,
   several `Schema.save_to_file` snapshots (partitioned tables, composite FKs,
   PII/tier classifications), and saved `MappingConfig`s. Assert, after the change:
   - `Schema.load_from_file(old).model_dump_json() == old_bytes` (byte-stable);
   - `CatalogStore` loads the frozen `catalog.json` and re-saves identical bytes;
   - `col.classification` round-trips (tags, tier, glossary_terms, source).
2. **Full existing suite unchanged** — especially every test asserting exact
   `Column`/`Schema`/`Table` `model_dump()`.
3. **Phase-9 end-to-end** — classification stamp → redaction → LLM prompt
   redaction → sampling gate → load gate all behave identically.
4. **`--engine rsa` path** — `rsa_ontology.py` simplifies (no JSON round-trip; pass
   the r2g `Schema` straight into RSA since it now *is* a `PhysicalSchema`); its 9
   tests plus the golden bundle stay green.
5. **`isinstance(r2g_column, rsa.Column)`** holds and RSA functions accept r2g types.

## 9. Decisions (signed off — July 2026)

1. **RSA becomes a core dependency.** (§5) Promote `relational-schema-analyzer` to a
   core runtime dependency (`>=0.2.0,<0.3.0`); keep RSA's connector/owl/mcp extras
   optional. The `[ontology]` extra becomes an empty back-compat alias for one
   release. *(Rejected: keeping it an extra with a vendored fallback base class —
   fragile conditional base class.)*
2. **Subclass + legacy-preserving serialization now.** (§3–§4) Zero-migration first
   landing; RSA's richer serialized shape (Strategy 2, §7) is a later opt-in.
   *(Rejected: biting the persisted-shape migration up front.)*

## 10. Proposed implementation order (once §9 is signed off)

1. ✅ **DONE.** Write the **compat corpus** (§8.1) against *current* code; commit the
   frozen fixtures + byte-stability tests (they pass today, guarding the refactor).
2. ✅ **DONE.** Land the **`r2g.types` subclass facade** (§3–§4) + dependency change
   (§5). Gates §8.2–§8.5 green: compat corpus (7), full non-integration suite
   (1429 passed, 1 skipped), `ruff`, `mypy` (whole package). No dump fallout; two
   pydantic field-narrowing `type: ignore[assignment]` (invariance) added.
3. ✅ **DONE.** Simplify `rsa_ontology.py` to drop the JSON round-trip (§8.4). r2g
   `Schema` (an RSA `PhysicalSchema` subclass) is now passed straight into
   `analyzer.analyze(schema)`; RSA reads the shared physical fields and ignores
   r2g's `classification`. RSA adapter + ontology CLI/UI tests (108, incl. the real
   end-to-end golden bundle) green; `ruff`/`mypy` clean.
4. ✅ **DONE (engine).** Reconcile `fk_inference` (ADR step 4): `r2g.fk_inference`
   imports the heuristic engine (`infer_foreign_keys`, `InferenceOptions`,
   `InferredForeignKey`, sampler protocol) from RSA and keeps only a thin
   `InferredForeignKey` subclass adding `to_edge_definition` (the ArangoDB analogue
   of RSA's `to_foreign_key`) plus a wrapper re-wrapping RSA's results. Because r2g's
   `Schema` is an RSA `PhysicalSchema` subclass it is passed straight through. The
   FK-inference suite (52) + downstream denorm/CLI/UI/sampling suites green; `ruff`/
   `mypy` clean. Value samplers stay in r2g (they carry `sample_values` and depend on
   r2g connectors) and are folded into step 5.
5. ✅ **DONE (samplers).** Sampler de-dup (ADR step 5): r2g's four value samplers now
   subclass RSA's (inheriting the FK-overlap query + Phase-11 denorm probes) and add
   only `sample_values`, removing ~700 duplicated lines. The shared connector helpers
   (URL parsers, driver loaders, `resolve_csv_table_path`) are byte-identical, so
   behavior is unchanged (full suite + ruff + mypy green). The **introspection
   connectors** stay in r2g: RSA's have diverged (enum sampling, `SourceProvenance`,
   `ordinal`/`is_unique`, duckdb/databricks vs kafka) and emit RSA-typed objects with
   enrichment fields r2g's serializers drop, so swapping them is not byte-stable
   without a re-type + classification-merge + live-DB parity effort.
6. Then ADR step 6 (reconcile the introspection connectors, then delete remaining
   duplicates and flip the dependency) as separate, independently-shippable PRs.

## 11. Risks & rollback

- **Hidden dump-shape assumptions** beyond the corpus (e.g. a test comparing
  serialized column dicts we didn't fixture). *Mitigation:* the corpus + running the
  full suite in §8.2; the wrap-serializer is defined by r2g so any drift is fixable
  in one place.
- **`extra` collision.** Only r2g writes `extra['classification']`; document
  `extra` as consumer-namespaced. Low risk.
- **RSA minor bumps changing `Column`/`Table`.** The `<0.3.0` cap + the compat
  corpus catch shape drift on upgrade.
- **Rollback:** the change is contained to `r2g/types.py` + `pyproject.toml` (+ the
  `rsa_ontology.py` simplification). Reverting the commit restores the standalone
  types with no data changes, since disk bytes never changed.
