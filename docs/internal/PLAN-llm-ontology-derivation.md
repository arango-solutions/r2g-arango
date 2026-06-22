# Implementation & Test Plan: LLM-Assisted Ontology Derivation (PRD Phase 10)

> **Status: PLANNED (June 2026).** Detailed companion to PRD §"Phase 10:
> LLM-assisted ontology derivation". Promotes the long-standing exploratory
> "ontology derivation (LLM integration)" idea into a committed, testable phase.
>
> **Thesis.** r2g already derives a target graph deterministically
> (`ConfigManager.generate_default_config`) and lets users refine it in the
> mapper. Phase 10 adds an *optional* path where an LLM **proposes** a richer
> ontology from the introspected schema. The LLM never touches the graph: its
> output is a candidate `MappingConfig` that flows through the **same**
> `validate_config` → `diff_mappings` review → loader path as every other
> mapping. **The LLM proposes; the deterministic pipeline disposes.**

## 1. Goal & scope

**Goal.** "Describe my domain, suggest the graph." Given a snapshot, let a
domain-aware model recommend which tables are vertices vs. edges, surface
implicit/undeclared relationships, suggest embed-vs-link and naming
improvements — then let a human review the proposal as a diff and apply the parts
they want, with full validation.

**In scope (V1).**
- An `LLMProvider` abstraction + OpenAI provider (lazy import, optional extra).
- A schema-grounded, **metadata-only** prompt builder (no bulk row data) with an
  optional user domain hint.
- A structured ontology proposal → candidate `MappingConfig`, validated against
  the real schema.
- Human-in-the-loop review via the existing `diff_mappings` machinery; CLI + API
  + Studio entry points.
- Provenance (model/prompt/params/response) and guardrails (privacy,
  determinism, cost budget).

**Explicitly out of scope (V1).**
- **Autonomous mapping** — nothing is saved/loaded without explicit review.
- **LLM at load time** — field expressions remain user-authored; the model only
  shapes the *mapping*, never transforms data during ingestion.
- **Fine-tuning / training** on customer schemas.
- **Natural-language querying** of the resulting graph.
- **Sending bulk data** to the model; value sampling is opt-in and
  classification-filtered (10c).

**Relationship to prior phases.**
- **`generate_default_config`** (`src/r2g/config.py:317`) is the seam: the LLM
  output is converted into a `MappingConfig` of the *same shape* this function
  returns, so everything downstream (`validate_config`, mapper, `diff_mappings`,
  loader) is unchanged. Auto-Map stays the default and the fallback.
- **`validate_config`** (`src/r2g/config.py:163`) is the hallucination gate.
- **`diff_mappings`** (Phase 5f) renders the proposal as an accept/reject diff.
- **Phase 9 classifications** gate what may be sent to an external model.
- **Phase 8 `CatalogProvider`** is the structural template for `LLMProvider`
  (Protocol + factory + lazy import + optional extra).

## 2. Core risks (carry into every decision)

1. **Hallucination.** Models invent tables/columns. *Mitigation:* every proposed
   object is validated against the real `Schema`; unknown references are dropped
   or flagged with reasons, never loaded.
2. **Nondeterminism.** *Mitigation:* `temperature=0`, structured (JSON-schema)
   output, stored provenance; proposals are reproducible and auditable.
3. **Privacy / data egress.** Schema names and (optional) samples leave the
   environment. *Mitigation:* metadata-only by default; opt-in sampling is bounded
   and **excludes Phase-9 Restricted/PII columns**; the feature is fully optional
   and absent unless invoked.
4. **Prompt injection via schema text.** Malicious table/column names/comments
   could try to steer the model. *Mitigation:* schema-derived text is treated as
   data (delimited/escaped), the system prompt is fixed, and output is validated
   structurally regardless of model "instructions".
5. **Cost / latency.** *Mitigation:* a hard token budget, compact schema
   serialization, surfaced cost/latency, and no background calls.
6. **Provider lock-in.** *Mitigation:* `LLMProvider` Protocol; OpenAI first,
   Anthropic / local OpenAI-compatible endpoints behind the same seam.

## 3. Architecture (grounded in the current codebase)

### 3.1 `LLMProvider` abstraction (P10.1)

New module `src/r2g/llm/` (parallel to `catalogs/` and `connectors/`):

```text
LLMProvider (Protocol)                 # mirrors catalogs/base.py CatalogProvider
  provider_type: str
  propose_ontology(request: OntologyRequest) -> OntologyProposal

create_llm_provider(provider_type, *, model=None, api_key=None, params=None) -> LLMProvider
SUPPORTED_LLM_TYPES = ("openai",)      # anthropic / local to follow
```

- `src/r2g/llm/openai_provider.py` — `OpenAIProvider`, **REST-over-`httpx`** (same
  rationale as the OpenMetadata provider: simple structured-output call, trivial
  to mock, tiny dependency) *or* the official `openai` SDK behind a lazy import.
  Uses JSON-schema / structured-output mode so the response parses deterministically.
- API key via the `$ENV_VAR` convention (`$OPENAI_API_KEY`) resolved at call time
  through the existing env/`r2g secrets` path — never persisted in plaintext.

### 3.2 Prompt builder (P10.2)

New `src/r2g/llm/prompt.py`:
- `build_schema_digest(schema: Schema, *, include_samples=False, classifications=None) -> str`
  — a compact, delimited description of tables, columns (name, type, nullability),
  PKs, and accepted/inferred FKs. Optional **domain hint** string from the user.
- **Redaction-aware:** when Phase 9 classifications are present, Restricted/PII
  columns are omitted (or name-only) and never sampled. Schema text is escaped and
  fenced so it cannot act as instructions (injection hardening).
- A fixed system prompt instructing the model to return the structured
  `OntologyProposal` (vertex/edge designation, implicit relationships,
  embed-vs-link, name suggestions, with a short rationale per item).

### 3.3 Proposal → `MappingConfig` (P10.3)

New `src/r2g/llm/ontology.py`:
```text
OntologyRequest  (pydantic): schema_digest, domain_hint, options
OntologyProposal (pydantic): collections[], edges[], renames[], embeds[], notes[]
                             each item carries `rationale` + `confidence`
proposal_to_mapping(proposal, schema) -> tuple[MappingConfig, list[str]]
```
- Convert the proposal into a `MappingConfig` (reusing `CollectionMapping` /
  `EdgeDefinition` / `FieldExpression`), then run `validate_config(schema, cfg)`.
- **Repair/drop loop:** references to non-existent tables/columns are removed and
  recorded in the returned notes; the result is always a *valid* `MappingConfig`
  (worst case: equivalent to Auto-Map). The model never produces something the
  loader can't run.

### 3.4 Review & apply (P10.4)

- Compute `diff_mappings(current_or_automap, proposed)` and return it; the Studio
  renders the existing diff UI with per-item accept/reject. Apply writes only the
  accepted subset via the normal save path (project marked dirty). Nothing is
  auto-saved or auto-loaded.

### 3.5 Entry points (P10.5, P10.7)

- **CLI** (`src/r2g/main.py`, new `ontology` Typer group):
  `r2g ontology suggest <project> [--domain "…"] [--provider openai] [--model …]
  [--sample] [--apply]` → prints the proposal + validation notes (Rich), applies
  only on `--apply`.
- **API** (`src/r2g/ui/server.py`): `POST /api/projects/{name}/suggest-ontology`
  (body: domain hint, options) → `{ proposal, mapping, diff, notes, provenance }`.
- **UI** (`index.html`): an Actions / canvas entry "Suggest model (AI)"; opens a
  floating review panel (the diff) with accept/reject — context-menu-primary, no
  new route, per the UI architecture contract.

### 3.6 Provenance (P10.6)

Persist on the project (catalog): provider, model, prompt hash (or prompt),
parameters, raw response, token usage / cost, latency, timestamp. Default
`temperature=0`. Surfaced in the CLI output and the review panel.

### 3.7 Dependencies

New optional extra, upper-bounded like the rest:
`llm = ["httpx>=0.27.0,<1.0"]` (REST path) or `["openai>=1.0,<2.0"]` (SDK path);
added to `[all]`. No dependency or network call unless a suggestion is invoked.

## 4. Implementation milestones (file-level)

**10a — grounded proposal, structure-only (the testable core)**
1. `src/r2g/llm/__init__.py`, `base.py` — `LLMProvider` Protocol,
   `OntologyRequest` / `OntologyProposal`, `create_llm_provider`,
   `SUPPORTED_LLM_TYPES`.
2. `src/r2g/llm/openai_provider.py` — `OpenAIProvider` (lazy import, structured
   output, `$OPENAI_API_KEY` via env/secrets, token budget).
3. `src/r2g/llm/prompt.py` — schema digest + domain hint + classification-aware
   redaction + injection hardening.
4. `src/r2g/llm/ontology.py` — `proposal_to_mapping` + validate/repair against
   `validate_config`.
5. `src/r2g/main.py` — `ontology` group (`suggest [--apply]`); provenance print.
6. `pyproject.toml` — `llm` extra (+ `all`), keywords.

**10b — review & apply in the Studio**
7. `src/r2g/ui/server.py` — `POST /api/projects/{name}/suggest-ontology` returning
   proposal + `diff_mappings` diff + notes + provenance.
8. `src/r2g/ui/static/index.html` — "Suggest model (AI)" action + floating diff
   review panel with per-item accept/reject; apply via the existing save path.

**10c — enrichment**
9. Opt-in, classification-filtered value sampling in the prompt builder; richer
   embed-vs-link / denormalization suggestions; additional providers
   (Anthropic / local OpenAI-compatible) behind the same factory.

Docs at each step: README (AI section + `llm` extra), CHANGELOG, PRD status.

## 5. Test plan

Mirrors the connector/catalog strategy: a **fake `LLMProvider`** (canned
structured responses, no network) for unit tests, gated on CI coverage; no live
LLM call in CI.

### 5.1 Unit tests (no network)
- `tests/test_llm_base.py` — factory dispatch per `provider_type`; unknown type
  raises; `OntologyRequest`/`OntologyProposal` validation; lazy-import + missing
  extra → `ImportError` with a pip hint.
- `tests/test_llm_prompt.py` — schema digest shape; domain hint inclusion;
  **classification redaction** (Restricted/PII columns absent / name-only and
  never sampled); injection hardening (schema text fenced/escaped); token budget
  enforced.
- `tests/test_ontology_proposal.py` — `proposal_to_mapping` builds a valid
  `MappingConfig`; **hallucinated** tables/columns dropped + reported;
  vertex/edge/embed mapping correctness; result always passes `validate_config`
  (degrades to Auto-Map-equivalent in the worst case); determinism with a fixed
  fake response.
- `tests/test_cli_ontology.py` — `CliRunner` over `ontology suggest` /
  `--apply` with a fake provider: proposal printed; `--apply` writes only on
  confirm; provenance persisted.
- `tests/test_ui_api.py` (extend) — `POST /api/projects/{name}/suggest-ontology`
  returns proposal + diff + notes; API-key/secret never echoed; respects
  classifications.

### 5.2 Integration / manual
- **No live-LLM CI e2e** (cost, nondeterminism, secrets) — handled like Snowflake
  / Glue: a fake provider in CI plus a manual, opt-in live smoke test
  (`R2G_LLM_LIVE=1`, real `$OPENAI_API_KEY`) that asserts the round trip yields a
  *valid* mapping (not specific content).

### 5.3 Verification gates (unchanged)
`ruff check src/ tests/`, `mypy src/r2g`, `pytest -m "not integration"
--cov=r2g --cov-fail-under=80`.

## 6. Risks & open questions (for review)
1. **Structured-output format.** Confirm OpenAI structured-output / JSON-schema
   mode + model (e.g. a current `gpt-*`); pin and re-verify at build time (the
   API surface moves fast).
2. **REST vs SDK.** REST-over-`httpx` keeps deps tiny and mocking trivial (as with
   OpenMetadata); the official SDK is friendlier for structured output. Pick one
   for 10a.
3. **Classification dependency.** 10a ships before Phase 9 lands; until then,
   default to **metadata-only, no sampling**, and treat *all* columns as
   non-sendable for sampling. Confirm this ordering is acceptable.
4. **How much schema to send.** Large schemas may exceed context/budget; confirm
   chunking/summarization strategy and the token budget default.
5. **Proposal richness vs. trust.** Embed-vs-link and denormalization are the
   highest-value but riskiest suggestions; gate them behind clear rationale +
   confidence and keep them rejectable per item.
6. **Provider/key UX.** Confirm key sourcing (`$OPENAI_API_KEY` env vs.
   `r2g secrets`) and whether per-project model/provider config is needed.

## 7. Recommendation
Build **10a** first: the `LLMProvider` seam + metadata-only prompt + structured
proposal → **validated** `MappingConfig`, exercised entirely against a fake
provider. It delivers the core value (a richer, *valid* proposed mapping) with no
data-egress and full reproducibility, and it slots into the existing
`generate_default_config` → `validate_config` → mapper seam without touching the
loader. Then **10b** for the Studio diff-review/apply UX, and **10c** for opt-in
sampling and more providers. Throughout, keep the discipline explicit: the LLM
proposes, the human reviews, and the deterministic pipeline validates and loads.
