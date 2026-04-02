# R2G-ETL Pipeline

*Product Requirements Document (PRD) -- Experimental Reference Implementation*

| Field | Value |
| :--- | :--- |
| **Product name** | R2G-ETL Pipeline (Relational to Graph -- Extract, Transform, Load) |
| **Version** | 0.1.0 (experimental) |
| **Date** | Originally drafted December 2025, consolidated April 2026 |
| **Status** | Phase 1 implemented; Phases 2--5 are planned or exploratory |
| **Target users** | Database architects, data engineers, and developers evaluating relational-to-graph migration with ArangoDB |

---

## 1. Goals and objectives

The primary goal of the R2G-ETL Pipeline is an experimental, configurable tool for transforming and loading data from PostgreSQL relational schemas into ArangoDB graph schemas. It serves as a reference implementation demonstrating the mechanical mapping patterns.

### Key objectives

| Objective | Detail |
| :--- | :--- |
| **Automation** | Eliminate manual spreadsheet-based mapping and script generation for initial data migration. |
| **Flexibility** | Support multiple ingestion paths: flat files (implemented), direct connection, CDC, and Kafka (planned). |
| **Schema management** | Ingest PostgreSQL schema and maintain metadata for mapping to target ArangoDB graph topologies (property graph, labeled property graph). |
| **Scalability** | Use `arangoimport` for efficient, high-volume bulk loading. |
| **Synchronization** | Support synchronizing the relational system through live stream processing of delta changes (planned; see Section 3). |

---

## 2. Solution overview

The product is a multi-phased pipeline that reads PostgreSQL relational schema, applies a defined mapping, and generates data in a form suitable for ArangoDB's `arangoimport` tool.

### Core components

| Component | Function |
| :--- | :--- |
| **Schema reader** | Connects to PostgreSQL to read and parse schema metadata: tables, columns, primary keys, and foreign keys. |
| **Metadata store** | Persists the ingested PostgreSQL schema and the user-defined target ArangoDB ontology/schema as JSON and YAML files. |
| **Mapping engine** | Applies transformation logic: tables to document collections; foreign keys to edge collections (PK/FK values to `_from` / `_to` with collection prefixes). |
| **Data egress / import generator** | Emits JSON Lines suitable for `arangoimport` and generates executable bash import scripts. |

### Relational-to-graph mapping logic

The transformation is largely mechanical and can be described in three layers (these are **mapping** concerns, not the same as project Phases 1--4 in Section 3):

1. **Transliteration (structural mapping)**
   - Each relational **table** maps to an ArangoDB **document collection**.
   - The table **primary key** feeds document **`_key`** (or another agreed unique identifier).
   - Each **foreign key** relationship maps to an **edge collection**: traverse the dependent table and map PK/FK values to ArangoDB **`_from`** and **`_to`**, with correct collection prefixes.
   - Table columns become document properties, with appropriate JSON type conversion.

2. **Join tables**
   Join tables that implement many-to-many relationships in the relational model are modeled as **edges** in the graph. The two FK columns become the `_from` and `_to` endpoints.

3. **Normalization**
   Categorical attributes (e.g., country codes) may be normalized into dedicated **vertex collections** with connecting edges when richer category data or reuse across entities is required.

### Edge cases in mapping

The mechanical mapping handles several non-trivial patterns:

- **Self-referential FKs** (e.g., `employees.manager_id -> employees.id`): produces edges within the same vertex collection. This is correct graph modeling but may surprise users expecting separate collections.
- **Multiple FKs to the same table** (e.g., `orders.customer_id` and `orders.referrer_id` both referencing `customers`): each FK produces a separate edge collection, named `{source}_to_{target}` with a `_{fk_column}` suffix to disambiguate.
- **Nullable FKs**: rows where the FK value is NULL are silently skipped (no edge is created). This is intentional -- a NULL FK means "no relationship."
- **Tables with no primary key**: these cannot produce meaningful `_key` values. The tool will fail if it encounters a table without a PK that is mapped as a document collection.

The following patterns are **not yet handled**:

- **Composite foreign keys** (multi-column FKs): untested and likely broken.
- **Circular FK dependencies** (table A references B, B references A): will produce valid edges but import ordering may need manual adjustment.
- **Inheritance patterns** (single-table inheritance, table-per-type): no special handling; each table is mapped independently.
- **Polymorphic associations**: not supported.

---

## 3. Project phases and requirements

The roadmap is organized into four implementation phases, from MVP through Kafka-backed change streams.

### Phase 1: Table dump file processing (MVP) -- Implemented

| ID | Requirement | Description | Status |
| :--- | :--- | :--- | :--- |
| **P1.1** | **Schema ingestion** | Connect to PostgreSQL (credentials/URL) and read table, column, PK, and FK definitions. | Done |
| **P1.2** | **Metadata storage** | Store ingested PostgreSQL schema and user-defined target ArangoDB schema (ontology) as metadata. | Done |
| **P1.3** | **Dump file input** | Accept flat-file dumps (e.g., CSV, TSV, GZ) of individual PostgreSQL tables. | Done |
| **P1.4** | **Node transformation** | Transform dump rows into ArangoDB document form with type coercion for `arangoimport`. | Done |
| **P1.5** | **Edge transformation** | Build edge collections by cross-referencing PKs and FKs; map to `_from` and `_to` including collection prefixes. | Done |
| **P1.6** | **`arangoimport` script generation** | Emit executable shell scripts to run `arangoimport` for all generated document and edge files. | Done |

### Phase 2: Direct PostgreSQL connection and streaming -- Planned

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P2.1** | **Direct read interface** | Establish direct, persistent connections to the live PostgreSQL database. | P1.1 |
| **P2.2** | **Batched data extraction** | Read data in controlled batches (e.g., server-side cursors) to bound memory use. | P2.1 |
| **P2.3** | **Streaming import** | Stream transformed data to ArangoDB via the HTTP API without requiring intermediate files on disk. | P2.2, P1.4, P1.5 |
| **P2.4** | **Snapshotting logic** | Support a full initial load with consistent snapshot semantics. | P2.3 |

### Phase 3: Change Data Capture (CDC) integration -- Planned

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P3.1** | **CDC hook/listener** | Integrate with PostgreSQL CDC (e.g., logical decoding via `pgoutput`) to capture INSERT, UPDATE, and DELETE events. | P2.1 |
| **P3.2** | **Delta transformation** | Map captured changes to ArangoDB replace/insert/delete operations. | P1.4, P1.5 |
| **P3.3** | **Live stream processing** | Continuously apply deltas to ArangoDB for near real-time synchronization. | P3.2, P2.3 |
| **P3.4** | **Conflict resolution** | Handling when updates touch nodes and edges in conflicting ways. Requires design work to define conflict policies (last-write-wins, source-of-truth priority, etc.). | P3.3 |

### Phase 4: Kafka integration -- Exploratory

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P4.1** | **Kafka producer/connector** | Connect to an external CDC pipeline (e.g., Debezium or similar) that streams PostgreSQL changes to Kafka topics. | P3.1 |
| **P4.2** | **Kafka consumer** | Subscribe to the relevant PostgreSQL change topics. | P4.1 |
| **P4.3** | **Kafka message transformation** | Parse messages (e.g., Avro, JSON) and apply the R2G mapping. | P4.2, P3.2 |
| **P4.4** | **Transactional ordering** | Apply changes to ArangoDB in the same sequential order as in the Kafka log. | P4.3 |

---

## 4. Technical requirements

| Category | Requirement | Details |
| :--- | :--- | :--- |
| **Architecture** | Modularity | Design so data sources can be swapped (e.g., PostgreSQL replaced by MySQL) without rewriting the whole tool. Currently PostgreSQL-only. |
| **Target DB** | ArangoDB | Load via `arangoimport` (implemented) and/or the ArangoDB HTTP API (planned for Phase 2). |
| **Transformation** | Schema mapping | Configurable prefix mapping for `_from` and `_to` (e.g., `user_1` to `Users/1`). |
| **Data integrity** | Key generation | Correct document `_key` values derived from source primary keys, including composite keys joined by a configurable separator. |
| **Technology stack** | Python | Chosen for ecosystem support (psycopg, python-arango, Polars, Pydantic, structlog). |

### Known constraints

- **`public` schema only**: the schema reader queries `information_schema` filtered to `table_schema = 'public'`. Multi-schema databases are not supported without manual schema file editing.
- **No referential integrity validation**: the tool does not verify that FK values actually reference existing PKs. Orphaned references will produce edges pointing to non-existent vertices in ArangoDB.
- **No idempotency guarantees**: re-running the pipeline with `--overwrite` replaces all data. There is no merge, diff, or conflict resolution for repeated loads.
- **Credential handling**: connection strings and passwords appear in CLI arguments. Generated import scripts use environment variable overrides (`ARANGO_ENDPOINT`, `ARANGO_PASSWORD`, etc.) but the tool has no integrated secrets management.

---

## 5. Future considerations (Phase 5+) -- Exploratory

These ideas are exploratory and represent potential directions, not committed work. Each would require significant design effort.

- **Ontology derivation (LLM integration):** Use a large language model to analyze the PostgreSQL schema and propose an optimized target ArangoDB graph schema for a given domain. This could suggest which tables should be vertices vs. edges, identify implicit relationships, and recommend denormalization strategies. Feasibility has improved significantly with current model capabilities.
- **ArangoRDF integration:** Emit data compatible with ArangoRDF so RDF, property graph, and labeled property graph representations can be selected as needed. Requires understanding the target use case (SPARQL queries, knowledge graphs, etc.) to choose the right representation.
- **Bi-directional synchronization:** Propagate changes from ArangoDB back to PostgreSQL. This is an extremely complex problem involving conflict resolution, schema evolution, and transactional consistency across two fundamentally different data models. Should be considered only if a concrete use case demands it.

---

## Document history

| Version | Date | Notes |
| :--- | :--- | :--- |
| Draft (Gemini-structured source) | December 2025 | Initial PRD with phased requirements P1.1--P4.4, technical requirements, and Phase 5+ items. |
| Narrative supplement (NotebookLM source) | December 2025 | Overlapping content with expanded relational-to-graph mapping (transliteration, join tables, normalization) and synchronization framing. |
| **Consolidated PRD** | **April 2026** | Single authoritative document. Gemini structure and requirement IDs preserved; NotebookLM mapping logic merged; conversational phrasing removed. Scope clarified as experimental reference implementation. Status columns added to phase tables. Edge cases, known constraints, and security notes added. "Antigravity" branding removed. |

The source files `PRD-gemini.md` and `PRD-notebooklm.md` remain in the repository for reference and are superseded by this file.
