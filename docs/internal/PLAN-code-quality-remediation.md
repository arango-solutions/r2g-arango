# Code-Quality Remediation Plan

> Internal working doc. Re-audited **2026-06-05** against the current codebase
> (post Phase 5g UI work). Supersedes the ad-hoc plan from the original audit.
> Baseline: **1000 passing, 6 skipped, 80% line coverage** (`pytest --cov`).

## How this plan was produced

A coverage run plus three read-only audits (security; duplication & dead code;
documentation drift). The first remediation pass already landed as commit
`a3be84b` ("Code-quality pass…"); the items below reflect what is **still open**
plus **new** findings introduced by the large UI/feature growth since then.

Legend — Effort: S (≲1h) / M (≲half day) / L (multi-day). Risk: chance of
breaking behaviour.

---

## 0. Already done (commit `a3be84b`)

- [x] SQL injection in table preview fixed (`ui/server.py`, `mcp_server.py`) — snapshot allowlist + `psycopg.sql.Identifier`. Re-verified safe.
- [x] MCP credential redaction for source/target tools + resources. Re-verified on primary paths.
- [x] `catalog.json` written `0600`. Re-verified (`catalog.py:147-149`).
- [x] Deleted dead `transformers/converter.py`. Confirmed gone.
- [x] Wired `DeadLetterQueue` into `StreamingPipeline` (PRD P5b.3.3).
- [x] First doc-drift pass (test count, UI port, Phase 5 status, CDC/secrets claims).

---

## 1. Security (highest priority)

### Still open from prior audit
- [x] **HIGH — Unauthenticated mutating REST API + `CORS allow_origins=["*"]`** — **DONE (step 2b, `create_app`).** Local-first auth: loopback bind + no token = open; non-loopback bind or `R2G_API_TOKEN` set ⇒ Bearer token required on all `/api` routes (except `/api/health` and SSE `/stream`, gated by the unguessable load id). Non-loopback bind with no env token auto-generates one (printed by the CLI). CORS `*` removed → same-origin only unless `R2G_CORS_ORIGINS` is set. UI `apiFetch` sends the token and prompts once on 401. +3 tests.
- [x] **HIGH — Path traversal** via unconstrained `mapping_config_path` — **DONE (step 2b).** API now derives the persisted path server-side as `<catalog>/projects/<name>/mapping.yaml`; a client-supplied path is only *read* (validated as `MappingConfig`) to seed contents, never used as a write target. Project names validated (no separators/`..`). +2 tests. **NOTE:** MCP `save_path` (`mcp_server.py`) is still unjailed — folds into the MCP-auth item below.
- [x] **MED — SSE / status leak full Python tracebacks** — **DONE (step 1).** Traceback stripped from client events; logged server-side only.
- [x] **MED — No log redaction** of secrets/DSNs — **DONE (steps 2a).** `log._redact_secrets` structlog processor masks secret-named fields and scrubs `scheme://user:pass@` from any value; `security.scrub_dsn_credentials`. +tests.

### New findings
- [ ] **HIGH — Server-side SSRF** via target introspect and source snapshot. **Decision (step 2):** connecting to user-chosen DB endpoints (incl. localhost) is the product's purpose, so IP-blocking would break normal use; the control is **auth** (now in place for non-loopback binds). RFC1918/link-local blocking intentionally NOT added. Revisit if a hosted/multi-tenant mode ships.
- [x] **HIGH — Unauthenticated destructive ops** (load, drops, migrate) — **covered by the auth item (step 2b)** for non-loopback binds / token mode.
- [x] **MED — DOM XSS** in mapper panes — **DONE (steps 2a + 2c).** 2a unified `escHtml` (quote-safe, null-safe). 2c: (1) escaped the fully-raw source-pane injections (`t.name`/`c.name`/`c.type` into attrs, ids, and text) and the raw expression snippet in connector props; (2) converted every inline handler that interpolated an identifier into a JS string literal (`fn('${name}')`) to read from an HTML-escaped `data-*` attribute via `this.dataset` / `this.closest(...).dataset` — closing the entity-decode-before-JS vector that escaping alone can't fix. Context menus were already safe (`data-idx` + `addEventListener`, escaped labels). Regression guard: `TestStaticAssetSafety.test_no_inline_handler_js_string_interpolation`.
- [x] **MED — Verbose exception strings** returned to clients — **DONE (steps 1, 2a):** DSN credentials scrubbed from `detail` on DB-connect endpoints via `_safe_detail`; preview already returns a generic message.
- [x] **MED — DLQ PII exposure** — **DONE (step 1):** `/load/{id}/errors` row values redacted (`_redact_dlq_entry`), field names + metadata preserved.
- [ ] **MED — MCP has no auth** (`mcp_server.py`, SSE mode network-exposed via `main.py:1705-1729`); error responses return raw `str(e)`. **Fix:** token auth, bind localhost, sanitize errors. **Effort M, Risk Low.**
- [ ] **MED — CSV source arbitrary directory read** (`csv_source.py:93-107`): connection string is any path; reads all `*.csv` there. **Fix:** restrict to configured base dir. **Effort S, Risk Low.**
- [ ] **LOW — DoS / rate-limit gaps** on expression compile/preview and `infer-fks?sample=true` (unauthenticated, expensive); preview-modal header/title not escaped; `~/.r2g` dir + DLQ files use default perms; `openProjectDatabase` opens user-controlled endpoint (`index.html:1922-1928`, already `noopener`). **Effort S each, Risk Low.**

### Verified safe (no action)
- No `eval`/`exec` in app code (expression engine uses a closed AST walker, `expressions.py:16-20,505`); no `subprocess`/`shell=True`; preview SQL not bypassable; secrets encrypted at rest (Fernet).

---

## 2. Duplication

### Still open from prior audit
- [ ] `target_by_source` dict build — canonical helper exists (`config.py:263-276`) but **4 inline copies** remain (`cdc/delta_transformer.py:80-89`, `main.py:215-227`, `main.py:363-375`, `streaming/pipeline.py:392-401`). **Fix:** `ConfigManager.target_by_source_table()` + `edge_transformer()` helpers. **Effort M, Risk Med.**
- [ ] Source-connector dispatch — improved (factory in `connectors/base.py`) but **legacy direct `PostgresConnector`** bypasses remain in `main.py`. **Effort S, Risk Low.**
- [x] FK value-sampler dispatch duplicated → **DONE:** `fk_inference.create_value_sampler(...)`, used by both `ui/server.py` and `main.py`.
- [x] CSV extension tuple + table→file resolution duplicated → **DONE:** shared `CSV_EXTENSIONS` + `resolve_csv_table_path()` in `csv_source.py`; `fk_inference` imports them.
- [x] `_singularize`/`_pluralize` heuristics diverged → **DONE:** consolidated as `naming.pluralize` / `naming.singularize` (union heuristic: `ses`/`ches`/`shes`/`xes`/`zes`); both callers reuse them.
- [x] `SUPPORTED_SOURCE_TYPES` vs `KNOWN_TYPES` maintained separately → **DONE:** `catalog.add_source` imports `SUPPORTED_SOURCE_TYPES` (empty/unknown still rejected).
- [x] `_serialize_rows` byte-identical in `ui/server.py` and `mcp_server.py` → **DONE:** shared `connectors.base.serialize_rows`.

### New (Python)
- [x] `_redact_source`/`_redact_target` identical in `ui/server.py` and `mcp_server.py` → **DONE:** moved to `security.redact_source_dump` / `redact_target_dump`; both import as `_redact_source`/`_redact_target`.
- [ ] Postgres table-preview logic duplicated (`ui/server.py` vs `mcp_server.py`) → shared `preview_postgres_table()` (keep identifier validation centralized). **M/Med.**
- [x] Source-type defaulting / PG-alias checks → **DONE (sampler sites):** `connectors.base.normalize_source_type()` + `is_postgresql()` added and used by the factory, catalog, and both FK-sampler call sites. Remaining scattered `in ("postgresql","postgres","pg")` checks can adopt them opportunistically.
- [ ] Python `_resolve_target` vs JS `_resolveProjectTarget` → expose `GET /api/projects/{name}/target-url`. **M/Low.**

### New (JS, `index.html`)
- [x] Dual HTML escapers `escHtml` vs `_htmlEscape` — **DONE (step 2a):** `escHtml` now delegates to the null-safe, quote-escaping `_htmlEscape`, so all ~80 sites are hardened. (Inline event-handler injection still needs the `dataset` refactor — see the XSS item.)
- [ ] `Object.entries(editState.collections).find(([k,c]) => (c.sourceTable||k)===t)` ~12 sites → `_collEntryForSourceTable()`. **S/Low.**
- [ ] FK shape normalization `fk.columns||[fk.column]` / `foreign_table||foreignTable` 5+ sites → `_normalizeFk()`. **S/Low.**
- [ ] `_key` read-only toast/tooltip copy in 3 blocks → `_keyReadOnlyMessage()`. **S/Low.**
- [ ] `drawSourceGraphEdges`/`drawTargetGraphEdges` near-identical → `drawPaneGraphEdges({...})`. **M/Med.**
- [ ] 15 `_menu*` context-menu builders repeat item/copy/inspect patterns (~300 lines) → `menuItem()`, `menuCopy()`, `menuEditExpression()`. **L/Low.**

---

## 3. Dead / unreachable code

### Python
- [x] `TargetGraphSchema` — **DELETED** (types.py).
- [x] `TypeMapping` — **DELETED** (types.py).
- [x] `HasVersionDirection` — **DELETED** (temporal/models.py + `__init__` export + now-unused `Enum` import).
- [x] `inspect.signature` cascade shim — **REMOVED**; calls `catalog.remove_source(name, cascade=cascade)` directly.
- [x] four `hasattr(catalog, …)` target guards — **REMOVED** (methods always exist on `CatalogManager`).

### JS (`index.html`)
- [x] **REGRESSION — `saveMapping` migration prompt** — fixed in step 1 (re-added `maybePromptMigration()`), and now folded into a single canonical `saveMapping`.
- [x] `executeLoad` duplicate copies + unused `_orig*` consts — **DELETED**.
- [x] `closeProgressView()` (zero callers) and the bypassed `showProgressView` alias — **DELETED**.
- [x] End-of-file monkey-patch block — **FOLDED** into authoring-time definitions: side-effects (`markDirty`, lens/badge repaint, `_wireTargetCardEdgeDrag`, post-load `clearDirty`/`loadBottomTimeline`/`setLens`/`requestValidation`) inlined into `toggleField`/`editCollection`/`editEdgeName`/`saveExpressionEditor`/`resetExpressionEditor`/`renderMapper`/`selectProject`; `executeLoad`/`saveMapping` promoted to single declarations. Block removed.

---

## 4. Test coverage (80% overall)

Raise coverage on the weak modules (current → suggested focus):
- [ ] `main.py` **46%** — CLI command handlers largely untested (biggest gap: 598 missed stmts). **L.**
- [ ] `connectors/postgres.py` **51%** — connector/session paths. **M.**
- [ ] `mcp_server.py` **59%** — tool handlers + redaction/error paths. **M.**
- [ ] `selective_reload.py` **67%** — reload executor. **M.**
- [ ] `ui/server.py` **73%** — error branches, SSE, introspect. **M.**
- [ ] `input/dump_reader.py` **76%**, `node_transformer.py` **77%**, `streaming/pipeline.py` **78%**. **S-M.**

---

## 5. Documentation drift

### Status mismatches (highest value — cheap, high signal)
- [ ] **PRD Phase 5g still "Planned" but implemented.** Flip header (`PRD.md:487`), status table (line 10), §3 intro (line 86), document history (line 571), and items **P5g.1–P5g.10** to **Implemented**. Note two **partials**: P5g.3 explorer defaults **open** (not collapsed); P5g.7 optional "show only unmapped / only edges" **filters not built**. **S/Low.**
- [ ] **Test count** README (`513`) and PRD history (`567`): 998 → **1006 collected (1000 unit + 6 integration)**. **S.**
- [ ] **PRD P5c.1.5 AQL delegation** "Not started" → **Done** for streaming (`node_transformer.py:34-87`, `pipeline.py:227-361`, tests exist). **S.**
- [ ] **PRD P5c.2.5 expression editor** "preview/highlight deferred" → **Done** (`index.html:3632-3709,3488-3514`, `ui/server.py:135`). **S.**
- [ ] **Package name** `r2g[…]` → `r2g-arango[…]` across PRD (`47,128,195,563`). **S.**

### README gaps
- [ ] CLI reference table stops at `stream`; missing `ui`, `mcp`, `secrets *`, `source *`, `project *`, `history`, `mapping-diff`, `selective-reload`. **M.**
- [ ] Quick start `stream` omits catalog `--source` path; `r2g source dump` under-documented vs legacy `dump-tables`. **S.**
- [ ] Stale reference to deleted `transformers/converter.py` (`README.md:131`); project-structure tree outdated; mermaid + prerequisites still PostgreSQL-only; roadmap lacks Phase 5g. **S-M.**
- [ ] CDC section omits `--temporal`/`--ttl-seconds`/`--smart-field` flags. **S.**

### Internal docs
- [ ] `docs/internal/PLAN-mapping-ui-catalog-reingest.md` heavily stale (502/845/673 test counts, `reload --changes-only` → `selective-reload`, sidebar wording, package name). Add archive banner or bulk-update. **S.**

---

## Recommended order

1. **Quick correctness + truthfulness wins (½ day, low risk):**
   fix the `saveMapping` migration-prompt regression; strip SSE/status tracebacks;
   redact DLQ responses + verbose `detail=str(e)`; flip PRD Phase 5g / P5c statuses
   and the 1006 test count. *(Ships visible value, no behaviour risk.)*
2. **Security hardening (highest priority, 1–2 days):**
   API auth + CORS lockdown (covers destructive-ops exposure) → path jail →
   SSRF allowlist → log redaction → DOM XSS pass (pairs with the escaper merge).
3. **Dead-code removal (low risk, fast):**
   delete server shims, unused types (`TargetGraphSchema`/`TypeMapping`/`HasVersionDirection`),
   shadowed JS `executeLoad`/`closeProgressView`/`_orig*`; fold the JS monkey-patch block.
4. **Deduplication (incremental):**
   `_serialize_rows`, `_redact_*`, FK sampler factory, CSV path helper, source-type
   normalizer, `SUPPORTED_SOURCE_TYPES`; then `target_by_source` helper, Postgres
   preview helper, JS escaper unification + small JS helpers; menu-builder refactor last.
5. **Docs completeness (after code settles):**
   README CLI table + multi-source quick start; internal PLAN archive banner.
6. **Coverage (ongoing):**
   target `main.py`, `postgres.py`, `mcp_server.py`, `selective_reload.py`, `ui/server.py`.

**Rationale:** Step 1 is cheap and removes a live behaviour gap + doc lies before
the next demo. Step 2 is the only class of issue that is remotely exploitable and
should precede any public/non-localhost deployment. Steps 3–4 reduce the surface
that keeps regenerating bugs (e.g. the monkey-patch regression). Docs and coverage
trail the code so they don't churn against in-flight changes.
