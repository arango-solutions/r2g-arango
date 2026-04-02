# Antigravity: R2G-ETL Pipeline

*Product Requirements Document (PRD)*

| Field | Value |
| :--- | :--- |
| **Product name** | Antigravity R2G-ETL Pipeline (Relational to Graph — Extract, Transform, Load) |
| **Version** | 1.0 (consolidated) |
| **Date** | Originally drafted December 2025, consolidated April 2026 |
| **Product owner** | [Name/Team] |
| **Target users** | Database architects, data engineers, and developers using ArangoDB and PostgreSQL |

---

## 1. Goals and objectives

The primary goal of the Antigravity R2G-ETL Pipeline is a robust, configurable, and scalable automated tool for transforming and loading data from PostgreSQL relational schemas into ArangoDB graph schemas.

### Key objectives

| Objective | Detail |
| :--- | :--- |
| **Automation** | Eliminate manual spreadsheet-based mapping and script generation for initial data migration. |
| **Flexibility** | Support multiple ingestion paths: flat files, direct connection, Change Data Capture (CDC), and Kafka. |
| **Schema management** | Ingest PostgreSQL schema and maintain metadata for mapping to target ArangoDB graph topologies (property graph, labeled property graph). |
| **Scalability** | Use `arangoimport` for efficient, high-volume bulk loading. |
| **Synchronization** | Support synchronizing the relational system through live stream processing of delta changes (phased; see Section 3). |
| **Future-proofing** | Lay groundwork for advanced features such as LLM-driven ontology derivation (Phase 5+). |

---

## 2. Solution overview

The product is a multi-phased pipeline that reads PostgreSQL relational schema, applies a defined mapping, and generates or streams data in a form suitable for ArangoDB’s `arangoimport` tool and related APIs.

### Core components

| Component | Function |
| :--- | :--- |
| **Schema reader** | Connects to PostgreSQL to read and parse schema metadata: tables, columns, primary keys, and foreign keys. |
| **Metadata store** | Persists the ingested PostgreSQL schema and the user-defined target ArangoDB ontology/schema (e.g., internal ArangoDB collections or files). |
| **Mapping engine** | Applies transformation logic: tables → document collections; foreign keys → edge collections (PK/FK values → `_from` / `_to` with collection prefixes). |
| **Data egress / import generator** | Emits JSON Lines or CSV suitable for `arangoimport`, and/or drives streaming import paths. |

### Relational-to-graph mapping logic

The transformation is largely mechanical and can be described in three layers (these are **mapping** concerns, not the same as project Phases 1–4 in Section 3):

1. **Transliteration (structural mapping)**  
   - Each relational **table** maps to an ArangoDB **document collection**.  
   - The table **primary key** feeds document **`_key`** (or another agreed unique identifier).  
   - Each **foreign key** relationship maps to an **edge collection**: traverse the dependent table and map PK/FK values to ArangoDB **`_from`** and **`_to`**, with correct collection prefixes.  
   - Table columns become document properties, with appropriate JSON type conversion.

2. **Join tables**  
   Join tables that implement many-to-many relationships in the relational model are modeled as **edges** in the graph.

3. **Normalization**  
   Categorical attributes (e.g., country codes) may be normalized into dedicated **vertex collections** with connecting edges when richer category data or reuse across entities is required.

This relational-to-graph mapping supports efficient traversal in ArangoDB by representing declared foreign keys as directed edges, reducing repeated join-heavy access patterns for connected workloads and multi-hop queries, while bulk load paths preserve throughput via `arangoimport` and explicit key/edge semantics.

---

## 3. Project phases and requirements

The roadmap is organized into four implementation phases, from MVP through Kafka-backed change streams.

### Phase 1: Table dump file processing (minimum viable product — MVP)

| ID | Requirement | Description | Deliverable |
| :--- | :--- | :--- | :--- |
| **P1.1** | **Schema ingestion** | Connect to PostgreSQL (credentials/URL) and read table, column, PK, and FK definitions. | Internal representation of PostgreSQL schema metadata. |
| **P1.2** | **Metadata storage** | Store ingested PostgreSQL schema and user-defined target ArangoDB schema (ontology) as metadata. | Metadata store in place. |
| **P1.3** | **Dump file input** | Accept flat-file dumps (e.g., CSV, TSV) of individual PostgreSQL tables. | Tool can parse and process dump files. |
| **P1.4** | **Node transformation** | Transform dump rows into ArangoDB document form for `arangoimport` into document collections. | Document JSON Lines (or equivalent) for `arangoimport`. |
| **P1.5** | **Edge transformation** | Build edge collections by joining/cross-referencing PKs and FKs across **different** table dumps; map to `_from` and `_to` including collection prefixes. | Edge JSON Lines (or equivalent) for `arangoimport`. |
| **P1.6** | **`arangoimport` script generation** | Emit shell commands/scripts to run `arangoimport` for all generated document and edge files. | Runnable import scripts for a full load. |

### Phase 2: Direct PostgreSQL connection and streaming

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P2.1** | **Direct read interface** | Establish one or more direct, persistent connections to the live PostgreSQL database. | P1.1 |
| **P2.2** | **Batched data extraction** | Read data in controlled batches (e.g., `LIMIT`/`OFFSET` or cursor-based reads) to bound memory use. | P2.1 |
| **P2.3** | **Streaming import** | Stream transformed data to `arangoimport` or an ArangoDB API without requiring intermediate files on disk. | P2.2, P1.4, P1.5 |
| **P2.4** | **Snapshotting logic** | Support a full initial load of the relational dataset (snapshot semantics as defined by the implementation). | P2.3 |

### Phase 3: Change Data Capture (CDC) integration

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P3.1** | **CDC hook/listener** | Integrate with PostgreSQL CDC (e.g., logical decoding via `pgoutput`) to capture INSERT, UPDATE, and DELETE events. | P2.1 |
| **P3.2** | **Delta transformation** | Map captured changes to ArangoDB PATCH (update/replace) or INSERT/DELETE operations. | P1.4, P1.5 |
| **P3.3** | **Live stream processing** | Continuously apply deltas to ArangoDB for near real-time synchronization. | P3.2, P2.3 |
| **P3.4** | **Conflict resolution** | Basic handling when updates touch nodes and edges in conflicting ways (policy TBD). | P3.3 |

### Phase 4: Kafka integration

| ID | Requirement | Description | Pre-requisite |
| :--- | :--- | :--- | :--- |
| **P4.1** | **Kafka producer/connector** | Connect to an external pipeline (e.g., Debezium) that streams PostgreSQL changes to Kafka topics. | P3.1 |
| **P4.2** | **Kafka consumer** | Subscribe to the relevant PostgreSQL change topics. | P4.1 |
| **P4.3** | **Kafka message transformation** | Parse messages (e.g., Avro, JSON) and apply the R2G mapping. | P4.2, P3.2 |
| **P4.4** | **Transactional ordering** | Apply changes to ArangoDB in the same sequential order as in the Kafka log. | P4.3 |

---

## 4. Technical requirements

| Category | Requirement | Details |
| :--- | :--- | :--- |
| **Architecture** | Modularity | Design so data sources can be swapped (e.g., PostgreSQL replaced by MySQL later) without rewriting the whole tool. |
| **Target DB** | ArangoDB | Load via `arangoimport` and/or the ArangoDB HTTP API. |
| **Transformation** | Schema mapping | Configurable prefix mapping for `_from` and `_to` (e.g., `user_1` → `Users/1`). |
| **Data integrity** | Key generation | Correct document `_key` values derived from source primary keys. |
| **Technology stack** | Implementation language | Prefer Python or Go for ecosystem support (PostgreSQL, Kafka, ArangoDB clients). |

---

## 5. Future considerations (Phase 5+)

- **Ontology derivation (LLM integration):** Use a large language model to analyze the PostgreSQL schema and propose an optimized target ArangoDB graph schema for a given domain.
- **ArangoRDF integration:** Emit data compatible with ArangoRDF so RDF, property graph, and labeled property graph representations can be selected as needed.
- **Bi-directional synchronization:** Propagate changes from ArangoDB back to PostgreSQL where required.

---

## Document history

| Version | Date | Notes |
| :--- | :--- | :--- |
| Draft (Gemini-structured source) | December 2025 | Initial PRD with phased requirements P1.1–P4.4, technical requirements, and Phase 5+ items. |
| Narrative supplement (NotebookLM source) | December 2025 | Overlapping content with expanded relational-to-graph mapping (transliteration, join tables, normalization) and synchronization framing. |
| **Consolidated PRD** | **April 2026** | **Single authoritative document:** Gemini structure and requirement IDs preserved; NotebookLM mapping logic merged; conversational phrasing and LaTeX-style notation removed; title set to *Antigravity: R2G-ETL Pipeline*. |

The source files `PRD-gemini.md` and `PRD-notebooklm.md` remain in the repository for reference and are superseded by this file for product decisions.
