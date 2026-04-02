The comprehensive Product Requirements Document (PRD) for converting relational database data (specifically PostgreSQL) into a knowledge graph inside ArangoDB is titled the **Antigravity: Relational to Graph ETL (R2G-ETL) Pipeline**.

The primary objective of this pipeline is to create a robust, configurable, and **scalable automated tool** for transforming data from PostgreSQL relational schemas into ArangoDB graph schemas, leveraging high-performance ingestion methods like `arangoimport`,,.

## 🚀 Product Requirements Document (PRD): Antigravity R2G-ETL Pipeline

### 1. Goals and Objectives
The core goal is the automated transformation and loading of PostgreSQL data into an ArangoDB graph.

| Objective | Detail |
| :--- | :--- |
| **Automation** | Eliminate manual spreadsheet-based mapping and script generation for initial data migration. |
| **Scalability** | **Utilize `arangoimport` for efficient, high-volume data loading**,. |
| **Flexibility** | Support various data ingestion methods, including flat files, direct connection, Change Data Capture (CDC), and Kafka. |
| **Synchronization**| **Be capable of synchronizing the relational system** through live stream processing of delta changes. |
| **Schema Management**| Automatically ingest PostgreSQL schema and maintain metadata for mapping to target ArangoDB graph topologies. |

### 2. Solution Overview and Core Components
The R2G-ETL Pipeline is designed to read the PostgreSQL schema, apply predefined mapping logic, and generate data in a format optimized for ArangoDB’s bulk import tools.

| Component | Functionality | Relevance to High Performance |
| :--- | :--- | :--- |
| **Schema Reader** | Connects to PostgreSQL to read and parse the metadata (tables, columns, Primary Keys (PKs), Foreign Keys (FKs)),. | Provides the necessary structure for mechanical, high-speed mapping. |
| **Mapping Engine** | Applies transformation logic to convert the relational model to the graph model. | Maps the logical relationships for eventual fast traversal in ArangoDB,. |
| **Metadata Store** | Stores the ingested PostgreSQL schema definitions and the user-defined target ArangoDB ontology,. | Critical for maintaining data consistency across synchronization cycles. |
| **Data Egress/Import Generator** | Converts transformed data into **JSON Lines or CSV files suitable for `arangoimport`**,. | **Directly addresses the high-performance ingestion requirement** by generating optimized bulk loading scripts/commands,. |

#### Relational-to-Graph Mapping Logic
The transformation process is inherently mechanical and focuses on three phases:

1.  **Transliteration (Phase 1 Logic):**
    *   Each relational **Table** is mapped to an ArangoDB **Document Collection**,.
    *   The Table **Primary Key** can be used as the Document **`_key`** (or unique identifier),.
    *   Each **Foreign Key Constraint** is mapped to an **Edge Collection**,.
    *   The Foreign Key relationships are converted into edges by iterating through the dependent table, where the PK and FK values are mapped to the ArangoDB system properties **`_from`** and **`_to`**,,.
    *   Table attributes become document properties, maintaining JSON type conversion.
2.  **Join Tables (Phase 2 Logic):** Join Tables, which resolve many-to-many relationships in relational systems, are modeled as **Edges** in the graph,.
3.  **Normalization (Phase 3 Logic):** Categorical attributes (e.g., Country Code) can be normalized into their own **Vertex Collections** with connecting Edges, especially if additional information about these categories is required.

### 3. Synchronization Requirements (Phased Development)
To meet the requirement for synchronization, the PRD outlines a staged approach, moving from batch processing (MVP) to continuous streaming (Kafka/CDC).

| Phase ID | Requirement Focus | Description |
| :--- | :--- | :--- |
| **Phase 1** (MVP) | Static Dump Files | Accepts flat file dumps (CSV/TSV), transforms records into ArangoDB document format (nodes), generates edges by cross-referencing PKs/FKs across files, and generates **`arangoimport` scripts**,. |
| **Phase 2** | Direct Streaming | Establishes **direct, persistent connections** to the live PostgreSQL database and reads data in controlled batches. It must stream the transformed data directly to `arangoimport` or the ArangoDB API **without intermediate files**. |
| **Phase 3** | **Change Data Capture (CDC)** | Integrates with a PostgreSQL CDC mechanism (e.g., Logical Decoding via `pgoutput`) to capture **INSERT, UPDATE, and DELETE events**. It transforms these delta changes into ArangoDB `PATCH` (update/replace) or `INSERT`/`DELETE` operations, achieving **near real-time synchronization**,. |
| **Phase 4** | **Kafka Integration** | Implements a **Kafka Consumer** to subscribe to PostgreSQL change topics (e.g., streamed via Debezium),. The tool must parse Kafka messages, apply the R2G mapping, and ensure that changes are applied to ArangoDB in the correct **sequential/transactional order**. |

### 4. Technical Requirements and Future Proofing

The implementation must adhere to strict technical specifications to ensure performance and integrity:

*   **Target DB:** Must utilize `arangoimport` or the ArangoDB HTTP API for data loading.
*   **Key Generation:** Must correctly generate document `_key` values using the source relational primary keys.
*   **Transformation:** Must support configurable prefix mapping (e.g., prefixing `_from` and `_to` attributes with the collection name, like `Users/1`).
*   **Architecture:** The R2G-ETL tool should be modular to allow easy replacement of the PostgreSQL data source with others like MySQL in the future.

#### Future Considerations for Knowledge Graph Evolution
The PRD also outlines Phase 5+ requirements focused on enhancing the knowledge graph capabilities, leveraging advancements in modeling:

*   **Ontology Derivation:** Integrating a Large Language Model (LLM) to analyze the PostgreSQL schema and generate an optimized target **ontology** (ArangoDB graph schema) tailored for a specific knowledge domain.
*   **ArangoRDF Integration:** Developing the capacity to generate the target data in a format compatible with **ArangoRDF**, allowing for the dynamic selection of RDF, Property Graph, and Labeled Property Graph representations.
*   **Bi-directional Synchronization:** Implementing the ability to propagate changes made in ArangoDB back to the PostgreSQL database.

The conversion from the relational model to the graph structure, as described in this PRD, facilitates the benefits inherent in graph databases, such as enabling efficient querying of connected data by eliminating costly table joins and speeding up multi-hop queries,,,. This pipeline aims to achieve speed by leveraging bulk import tools while maintaining data accuracy through explicit Foreign Key translation into directed Edges,.