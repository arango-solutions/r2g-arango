# Implementation & Test Plan: External Data Catalog Integration (PRD Phase 8)

> **Status: Phase 8a IMPLEMENTED (June 2026); 8b–8d planned.** This plan
> proposes how r2g connects to external enterprise data catalogs
> (OpenMetadata, AWS Glue, Atlan, …) and uses them as an upstream **discovery**
> layer. It is the detailed companion to PRD §"Phase 8: External data catalog
> integration".
>
> **8a shipped (decisions confirmed with product):** whole-database/schema
> import granularity; relational + Kafka source kinds; credentials supplied by
> the user via `$ENV_VAR` / `r2g secrets` (not read from the catalog).
> Delivered: `r2g.catalogs` (`CatalogProvider` + factory), the OpenMetadata
> provider, a `CatalogProviderConfig` registry with encrypted tokens, the
> `r2g catalog add/list/browse/import-source/remove` CLI, unit tests (mocked
> HTTP) + a skip-when-unavailable live e2e.
>
> **Implementation note — REST over SDK.** The OpenMetadata provider talks to
> the REST API directly via `httpx` (the `openmetadata` extra) rather than the
> heavyweight `openmetadata-ingestion` SDK: the reads we need are simple, the
> dependency footprint is tiny, and the HTTP layer is trivially mocked in unit
> tests. 8b (UI/MCP), 8c (Glue), 8d (Atlan/DataHub) remain as below.

## 1. Goal & scope

**Goal.** Let an r2g user browse a connected data catalog, pick a
database / schema / table (or Kafka topic / file collection) they have access
to, and register it as an r2g migration source — instead of hand-typing
connection details. r2g still connects to the underlying data store itself to
run the migration ("discover-then-connect").

**In scope (V1):** *read-only* discovery against external catalogs; resolving a
catalog asset into an r2g `SourceConfig`; CLI + UI + MCP entry points.

**Explicitly out of scope (V1):**
- Writing/publishing back to the catalog (registering the ArangoDB graph +
  lineage as a downstream asset). Tracked as a future write-path effort.
- Treating the catalog as the migration transport. r2g always connects
  directly to the source DB; the catalog only supplies *where it is*.
- Pulling credentials out of catalogs (they are masked on read — see §3).

**Relationship to Phase 5d.** Phase 5d is r2g's *own internal* catalog
(sources/projects/mappings, optionally ArangoDB-backed). Phase 8 is about
*external* catalogs. They share nothing structurally except that an imported
source lands in the internal catalog as a normal `SourceConfig`.

## 2. Research landscape (cited; 2026-06)

Source of truth: a deep-research pass (fan-out web search → fetch → adversarial
verification). Findings that survived verification, with confidence:

| Catalog | OSS? | Read API | Connection metadata? | Python SDK | Verdict for r2g |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **OpenMetadata** | Yes | REST (`/api/v1/...`), FQN/ID lookups | **Yes** — host/port, db name, SSL on `DatabaseService` (creds masked) | Official, type-safe (`openmetadata-ingestion`) | **First integration** — high confidence |
| **AWS Glue Data Catalog** | No (AWS) | AWS API / `boto3` | **Yes** — `Connection` objects store creds/URI/VPC | `boto3` | Second — high confidence; AWS-only |
| **Atlan** | No | REST + search | Partial (asset-centric) | Official `pyatlan` (API-token auth, 400+ types) | Third (commercial breadth) — high confidence on API |
| **DataHub** | Yes | Pull-ingest + (read API **unverified**) | Entity-aspect model; connection exposure unclear | Claimed, **refuted in verification** | Candidate — re-validate read API first |
| **Collibra** | No | GraphQL "Knowledge Graph" (BETA) | **Likely governance-only** (no connection metadata) | — | Low priority for discover-then-connect |

**Common metadata model** (supports a catalog-agnostic abstraction): every
catalog converges on `Source/Service → Database/Asset → Schema → Table →
Column`, plus tags/classification and lineage/relationships.

### Caveats (must carry into any decision)
1. **Market share is UNVERIFIED.** The research did not confirm any
   Gartner/Forrester position, revenue, customer count, or GitHub-star figure
   (the market-penetration search agents stalled). The ordering above is by
   **API suitability + testability, not adoption.** If market share matters to
   the call, that needs a dedicated follow-up.
2. **Credentials are masked on read** across catalogs. r2g reads
   host/type/db-name for discovery; the user supplies credentials at connect
   time. This is a *feature* for our threat model, not a gap.
3. **DataHub read API and Unity Catalog OSS** API claims were refuted/abstained
   — do not assume those capabilities without re-checking.
4. SDK versions / endpoints move fast (e.g. `pyatlan` 9.7.6 dated 2026-06-16;
   OpenMetadata docs moved v1.11→v1.12). Pin versions; re-verify at build time.

Primary sources: OpenMetadata SDK & schema docs (`docs.open-metadata.org`,
`openmetadatastandards.org`), AWS Glue connections docs (`docs.aws.amazon.com/glue`),
`github.com/atlanhq/atlan-python` + `pypi.org/project/pyatlan`, DataHub docs
(`docs.datahub.com`).

## 3. Architecture (grounded in the current codebase)

The integration mirrors the proven `SourceConnector` pattern. New code is
additive; the existing migration path is untouched downstream of "import".

### 3.1 New abstraction — `CatalogProvider`

New module `src/r2g/catalogs/base.py` (note plural `catalogs/`, parallel to
`connectors/`):

```text
CatalogProvider (Protocol)            # mirrors connectors/base.py SourceConnector
  provider_type: str
  endpoint: str
  list_data_sources()    -> list[CatalogAsset]      # services / top-level systems
  list_children(parent)  -> list[CatalogAsset]      # db → schema → table descent
  search(query, limit)   -> list[CatalogAsset]
  resolve_source(asset)  -> ResolvedSource          # the discover→connect bridge

CatalogAsset (pydantic)               # normalized, catalog-agnostic
  provider, fqn, kind {service|database|schema|table|topic|file_collection},
  name, source_type (mapped to r2g's: postgresql|mysql|sqlserver|snowflake|kafka|csv|None),
  connection_hint {host, port, database, extra}   # NO secrets
  tags: list[str]; parent_fqn: str | None

ResolvedSource (pydantic)
  source_type: str
  connection_template: str    # e.g. "postgresql://$PG_USER:$PG_PASSWORD@host:5432/db"
  source_params: dict         # csv/kafka extras when relevant
  notes: str                  # e.g. "credentials not provided by catalog"
```

Factory in the same module:
```text
create_catalog_provider(provider_type, endpoint, *, token=None, params=None) -> CatalogProvider
SUPPORTED_CATALOG_TYPES = ("openmetadata", "glue", "atlan", "datahub")
```
Lazy imports per provider so optional SDKs aren't required unless used —
exactly like `create_source_connector`.

### 3.2 Catalog registry (persistence)

New `CatalogProviderConfig` in `src/r2g/catalog.py` (sibling of `SourceConfig`):
`name, provider_type, endpoint, token (encrypted), params, created/updated`.
- The `token` is encrypted at rest with the **existing** `CredentialCipher`
  (Fernet) in `security.py` and redacted in API/log output via the existing
  `redact_*` helpers. No new secret machinery.
- `CatalogManager` gains `add_catalog / list_catalogs / get_catalog /
  remove_catalog`, following the existing source/target methods.

### 3.3 The discover-then-connect bridge

`resolve_source(asset)` produces a `ResolvedSource` whose
`connection_template` uses r2g's existing `$ENV_VAR` convention (already
supported in `_resolve_conn_string`). `import-source` then creates a normal
`SourceConfig` via the existing `add_source`, and **everything downstream is
unchanged**: `source snapshot`, FK inference, mapping, `stream`.

Mapping `catalog source_type → r2g source_type` is a small table per provider
(OpenMetadata `Mysql`/`Postgres`/`Mssql`/`Snowflake`/`Kafka` → r2g canonical
types via the existing `normalize_source_type`).

### 3.4 Entry points

- **CLI** (`src/r2g/main.py`, new `catalog_app` Typer group, mirroring
  `source_app`):
  - `r2g catalog add --name <n> --type openmetadata --endpoint <url> [--token $OM_TOKEN]`
  - `r2g catalog list`
  - `r2g catalog browse <name> [--search <q>] [--path <fqn>]` → Rich tree/table
  - `r2g catalog import-source <name> <asset-fqn> --as <source-name>` → registers a source
  - `r2g catalog remove <name>`
- **UI** (`src/r2g/ui/server.py` + `ui/static/index.html`): "Import from
  catalog" in the **+ New source** form — pick a registered catalog → searchable
  tree → selection pre-fills host/type/db, leaving credentials to the user.
  New endpoints: `GET/POST /api/catalogs`, `GET /api/catalogs/{name}/browse`,
  `POST /api/catalogs/{name}/import-source`.
- **MCP** (`src/r2g/mcp_server.py`): `list_catalogs`, `catalog_browse`,
  `catalog_import_source` tools (errors scrubbed via the existing `_safe_error`).

### 3.5 Dependencies (`pyproject.toml`)

New optional extras, upper-bounded like the rest:
`openmetadata = ["openmetadata-ingestion>=1.5,<2.0"]`,
`glue = ["boto3>=1.34,<2.0"]`, `atlan = ["pyatlan>=9.0,<10.0"]`. Add to `all`.
(Confirm exact OpenMetadata SDK package name/extra at build time — docs moved
recently.)

## 4. Implementation milestones (file-level)

**8a — foundation + OpenMetadata (the testable core)**
1. `src/r2g/catalogs/__init__.py`, `base.py` — Protocol, `CatalogAsset`,
   `ResolvedSource`, `create_catalog_provider`, `SUPPORTED_CATALOG_TYPES`.
2. `src/r2g/catalogs/openmetadata.py` — `OpenMetadataProvider` (list services,
   descend db→schema→table, search, resolve_source). Lazy SDK import with a
   pip-install hint (same pattern as `snowflake.py` / `mysql.py`).
3. `catalog.py` — `CatalogProviderConfig` + `CatalogManager` CRUD + Fernet
   encryption of `token`.
4. `main.py` — `catalog_app` group (`add/list/browse/import-source/remove`).
5. `pyproject.toml` — `openmetadata` extra (+ `all`), keywords.

**8b — UI + MCP**
6. `ui/server.py` + `index.html` — catalog registry + browse + import path.
7. `mcp_server.py` — three catalog tools.

**8c — AWS Glue**
8. `src/r2g/catalogs/glue.py` — `GlueProvider` via `boto3`; resolve `Connection`
   objects into `ResolvedSource`. `glue` extra.

**8d — Atlan / DataHub**
9. `src/r2g/catalogs/atlan.py` (`pyatlan`); DataHub only after read-API
   re-validation.

Docs at each step: README sources/extras matrix, CHANGELOG, CONTRIBUTING.

## 5. Test plan

Mirrors the connector test strategy already in the repo (mocked-SDK unit tests
+ Dockerized e2e that skips when unavailable).

### 5.1 Unit tests (no network; mock the SDK/HTTP) — gate on CI coverage
- `tests/test_catalogs_base.py` — factory dispatch for each `provider_type`;
  unknown type raises; `CatalogAsset` / `ResolvedSource` validation; the
  `catalog source_type → r2g source_type` mapping.
- `tests/test_openmetadata_provider.py` — with a **fake OpenMetadata SDK/HTTP**
  (same approach as `test_mysql_connector.py`'s fake driver): `list_data_sources`
  returns services; `list_children` descends db→schema→table; `search` filters;
  `resolve_source` builds the right `connection_template` and **omits secrets**;
  masked-credential handling (notes set, env-var template emitted).
- `tests/test_catalog_registry.py` — `CatalogManager.add/list/get/remove`;
  token encrypted at rest (round-trips through `CredentialCipher`); redaction in
  serialized output.
- `tests/test_cli_catalog.py` — `CliRunner` over `catalog add/list/browse/
  import-source`, asserting an imported asset becomes a real `SourceConfig`
  (collaborators mocked, like `test_cli_runtime.py`).
- UI/MCP endpoint tests in the existing `test_ui_api.py` / `test_mcp_server.py`
  style (incl. token redaction in responses).

### 5.2 Integration / e2e (OpenMetadata is the OSS, dockerizable win)
- Add an `openmetadata` service to `docker-compose.yml` (OM ships Docker
  images / compose). Seed it by **registering a `DatabaseService` that points
  at the existing `postgres` (or `mysql`) test container**, so the catalog
  describes a database r2g can actually reach.
- `tests/integration/test_openmetadata_e2e.py` (auto-skips when OM/Arango
  unavailable, per the existing `requires_*` pattern):
  1. register the catalog (`catalog add`),
  2. `browse` → assert the seeded service / database / schema / tables appear,
  3. `import-source` a table's parent database → assert a `SourceConfig` with
     the right `source_type` + host/db (no secrets),
  4. `source snapshot` the imported source against the **real** Postgres/MySQL
     (supplying creds via env) → assert tables/PKs/FKs match — proving the full
     discover-then-connect loop end-to-end, all OSS.
- Wire the OM service + `OPENMETADATA_*` env into the CI `integration` job.
- **Glue/Atlan/DataHub: no CI e2e** (need cloud accounts) — unit-tested with
  mocked SDKs + manual field validation, exactly as Snowflake is handled.

### 5.3 Verification gates (unchanged from current practice)
`ruff check src/ tests/`, `mypy src/r2g`, `pytest -m "not integration"
--cov=r2g --cov-fail-under=80`, and the Dockerized integration job.

## 6. Risks & open questions (for review)
1. **Market-share unknown.** If the catalog *choice* must be justified by
   adoption (not just API fit), commission a focused market-penetration check
   before committing the order. Current recommendation (OpenMetadata-first) is
   optimised for *fastest credible, testable integration*.
2. **OpenMetadata SDK surface/packaging** moved recently — confirm the exact
   pip package, version, and read-API entry points at build start.
3. **DataHub read API** must be re-validated before any DataHub work.
4. **Credential UX.** Confirm the intended flow: catalog supplies host/db, user
   supplies creds via `$ENV_VAR` / `r2g secrets`. (Assumed here.)
5. **File collections / Kafka.** OM models messaging (Kafka) and storage
   services; decide whether V1 covers only relational + Kafka, or also object
   stores → r2g CSV/file sources.
6. **Scope of "select a database."** Confirm the selection granularity users
   want (whole database vs. individual tables vs. a saved subset).

## 7. Recommendation
Build **8a (OpenMetadata foundation)** first: it is the only option that is
simultaneously OSS, SDK-backed, connection-metadata-bearing, and Docker-testable
end-to-end, so it delivers the full discover-then-connect loop with real CI
coverage at the lowest risk. Add **Glue** second for enterprise/AWS reach
(unit-tested + field-validated), then **Atlan/DataHub**. Defer catalog
write-back (lineage publishing) until the read path proves out.
