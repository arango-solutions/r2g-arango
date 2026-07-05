# r2g-arango — Relational-to-Graph Ontology for ArangoDB

**Derive a graph ontology from your relational schema and project data onto it in ArangoDB — shape the ontology visually, move data three ways, keep it in sync, and govern what you migrate.**

*Open source · Apache 2.0 · `pip install r2g-arango` · Python 3.10+ · Experimental / educational reference implementation*

---

## The problem

Foreign keys are relationships trapped as implicit joins. Migrating to a graph is easy to start and hard to finish: correct type/key handling, real modeling decisions, moving data at scale, staying in sync, and not leaking sensitive columns on the way out.

## What r2g does

Derives a graph **ontology** from a relational schema — **tables → entity (document) collections, foreign keys → relationships (edges), join tables → edges, primary keys → `_key`** — as a deterministic, reproducible baseline you then refine, load data onto, and keep current.

---

## Key capabilities

| | |
|---|---|
| **Broad sources** | PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, CSV directories, Kafka topics — via a common connector layer. |
| **Correct mapping** | Composite/multi-column FKs, 50+ type coercions, multi-schema, partitions, PK-less-table safety, join-table auto-detection. |
| **Ontology Studio** | FastAPI web UI to shape the ontology on a single graph canvas: right-click context menus, lenses (Topology / Coverage / Validation / Diff / Sensitivity), Auto-Map, Suggest FKs, naming conventions. |
| **Three load paths** | Batch `arangoimport` script generation · direct HTTP streaming (server-side cursors, parallel workers, dry-run) · CDC via logical replication + Kafka/Debezium. |
| **Change management** | Schema diff, config migration that preserves customizations, selective reload, and in-place migration of a loaded graph. |
| **Data governance** | Catalog-sourced classifications (OpenMetadata), sensitivity gate at load, transform-at-load masking, and emitted RBAC/OPA enforcement artifacts. |
| **AI ontology assistant** | *Optional.* An LLM **proposes** a richer ontology (entity-vs-relationship, implicit relationships, better names); you review per-item and apply. |

---

## How it works (3 steps)

1. **Introspect** — connect to a source and snapshot its schema (tables, keys, FKs, and — with a catalog bound — classifications).
2. **Shape the ontology** — auto-derive a baseline ontology, then refine it on the canvas (or ask the AI for a proposal); validate before anything is saved.
3. **Load & sync** — stream or script the initial load in dependency order, then keep it current with CDC.

```
Source DB ──introspect──▶ schema ──auto-derive──▶ ontology ──▶ validate ──▶ load ──▶ ArangoDB
                                     ▲                                        ▲
                              ontology studio / AI proposal              CDC keeps it in sync
```

---

## What makes it different

- **Determinism first.** A mechanical baseline underpins everything; the clever features *advise*, they don't silently rewrite.
- **The LLM proposes; the pipeline disposes.** AI suggestions are validated against the real schema, metadata-only, PII-redacted, and human-reviewed per item — never auto-applied, never able to load a hallucinated table.
- **Governance built in.** Classifications flow from catalog → mapping → load; sensitive fields are gated or masked by default.
- **Human-in-the-loop everywhere.** Drafts, diffs, and reviews; nothing hits the database without an explicit save.

---

## Get started

```bash
pipx install 'r2g-arango[postgres,ui]'
r2g source add shop postgresql "$PG_CONN"
r2g source snapshot shop
r2g ui        # open the Mapping Studio
```

**Tech:** Python · Typer CLI · FastAPI + web UI · psycopg / python-arango · Polars · structlog · optional `[llm]`, `[kafka]`, `[snowflake]`, `[openmetadata]` extras.

**Links:** GitHub `ArthurKeen/r2g-arango` · full roadmap in `docs/PRD.md` · License: Apache 2.0

> Useful for evaluating relational-to-graph migration with ArangoDB and as a starting point for production pipelines — not itself production-hardened software.
