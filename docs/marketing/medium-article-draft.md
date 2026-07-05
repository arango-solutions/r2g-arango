# From Schema to Ontology: Turning Relational Data into an ArangoDB Graph with r2g

*A deep dive into r2g-arango — an open-source toolkit that derives a graph **ontology** from your relational schema and projects PostgreSQL, MySQL, SQL Server, and Snowflake data onto it in ArangoDB, with a visual ontology studio, change-data-capture sync, data-governance guardrails, and an optional "LLM proposes, the pipeline disposes" ontology assistant.*

> **Draft — Medium article.** Suggested read time ~9 minutes. Placeholders for images/screenshots are marked `[IMAGE: …]`.

---

## The problem: your relationships are hiding in plain sight

Relational databases are brilliant at storing data and stubborn about relationships. A foreign key *is* a relationship — but it's an implicit one, reconstructed at query time by joins. Ask a relational database a graph-shaped question ("show me every account within three hops of this one that shares a device fingerprint") and you get a pile of self-joins, a query planner sweating, and a latency graph that curves the wrong way.

Graph databases flip that. Relationships become **first-class edges** you traverse directly, no joins required. [ArangoDB](https://arangodb.com) goes a step further: it's multi-model, so your documents, graph, and search all live in one engine.

So the migration sounds simple: tables → vertices, foreign keys → edges. And mechanically, it *is* simple. But that mechanical projection isn't the goal — the goal is a good **ontology**: the set of entity types, relationship types, and properties that describe your domain as a graph. Deriving that ontology from a schema, and keeping data faithfully projected onto it, is where the real work lives:

- Doing the mechanical mapping **correctly** — composite keys, join tables, 50+ column types, partitions, PK-less tables.
- Shaping the **ontology** (not every table is an entity; not every FK is a meaningful relationship; some tables are edges, some are best embedded).
- Actually **moving the data** onto that ontology — once, continuously, or incrementally.
- Keeping the target **in sync** as the source changes.
- Not **leaking sensitive columns** into your shiny new graph along the way.

`r2g` (packaged as **`r2g-arango`**) is an open-source, Apache-2.0 toolkit that tackles all of the above. Let me walk through it.

[IMAGE: r2g Mapping Studio — relational source fields wired to a target graph model]

---

## 1. The baseline ontology (the mechanical part, done right)

At its core, r2g derives a deterministic **baseline ontology** from the schema:

- Each **table** becomes a **document collection**; the primary key becomes the document `_key`.
- Each **foreign key** becomes an **edge collection**; every row produces an edge from source vertex to target vertex.
- **Join tables** (two FKs, no payload) collapse into **edges** rather than vertices — the many-to-many becomes a real relationship.
- **Types are coerced** from source representations into proper JSON: integers, floats, booleans, nested `jsonb`, arrays, UUIDs, timestamps, network and geometric types — 50+ mappings in total.

It handles the fiddly cases that break naïve scripts: **composite/multi-column** foreign keys (turned into composite `_key`/`_from`/`_to` with a configurable separator), **multi-schema** sources, PostgreSQL **declarative partitions** (children collapse into their parent by default), and **PK-less tables** (warned, with auto-generated keys).

Crucially, this baseline ontology is *derived*, not hand-written — one function turns an introspected schema into a default mapping. That determinism becomes the safety net for everything fancier later.

---

## 2. Refining the ontology is a judgment call — so make it visual

A 1:1 mirror of your relational schema is a fine *starting ontology* and a poor *destination*. `order_items` is probably an edge with properties. A lookup table might be better embedded than linked. Two tables might be an inheritance hierarchy. Turning the baseline into a domain ontology is a modeling decision — one best made where you can see the graph.

r2g ships an **ontology / mapping Studio** — a FastAPI server with a single-canvas web UI — so that shaping happens on the graph itself:

- A **graph canvas** shows source tables and target collections/edges with the mappings drawn between them.
- **Lenses** repaint the same graph to answer different questions: *Topology*, *Coverage* (what's mapped), *Validation* (what's broken), *Diff* (what changed), and *Sensitivity* (what's classified).
- **Right-click context menus** on nodes, edges, and the canvas drive the actions — approve, rename, create relationships, mask a field — keeping you on the graph instead of bouncing through wizards.
- **One-click helpers**: Auto-Map everything, Suggest foreign keys (by name and value overlap), Apply a naming convention (PascalCase collections, camelCase properties), and analyze denormalization smells.

[IMAGE: The Studio with the Validation lens active and a floating detail panel open]

Every edit is a *draft*. You review, validate, and save — nothing touches the database until you say so.

---

## 3. Three ways to move the data

Different jobs want different pipelines, so r2g gives you three:

1. **Batch ETL / script generation.** Generate executable `arangoimport` scripts (JSONL or CSV-direct with `--translate`/`--datatype`) that load documents first, then edges, in topological order so referenced vertices always exist before edges reference them.
2. **Direct streaming.** Stream straight from the source to ArangoDB over the HTTP bulk API with server-side cursors, configurable batch sizes, REPEATABLE-READ snapshot isolation, and parallel `--workers` — no intermediate files. There's a `--dry-run` that validates connectivity, transforms everything, and reports counts and sample documents without writing.
3. **CDC / Kafka sync.** Near-real-time PostgreSQL→ArangoDB sync via **logical replication** (`test_decoding` or `wal2json`), plus a **Kafka** consumer that understands Debezium. Row changes become graph mutations (document upserts/deletes + edge recalculation) with configurable conflict policies (`source_wins`, `last_write_wins`, `log_and_skip`, `fail`).

Along the way you get the operational niceties you'd otherwise write yourself: retry-with-backoff on transient failures, progress bars with throughput, per-document error reporting, table include/exclude filters, `--skip-existing` resumption, and incremental `--since` streaming.

---

## 4. Schemas evolve — change management is built in

Migrations aren't a one-shot event. r2g treats schema drift as a first-class concern:

- **`diff-schema`** compares two snapshots (added/removed tables, type changes, nullability, PK/FK changes).
- **`migrate-config`** updates your mapping YAML when the source changes — adding collections/edges for new tables and FKs, removing edges for dropped FKs, flagging orphans — *while preserving your customizations*.
- **Selective reload** and **in-place migration** apply just the delta to an already-loaded graph (rename collections, reload moved edges, rebuild the named graph) instead of a full reload.

---

## 5. Don't launder sensitive data into your graph

A migration is also a great way to accidentally copy PII into a new, less-governed system. r2g's governance layer (built to *advise*, while the serving layer *enforces*) closes that gap:

- **Catalog integration** pulls column-level **classifications**, owners, and tiers from an external catalog (OpenMetadata today) at snapshot time.
- A **sensitivity lattice** rolls those classifications up across the mapping (a "mosaic": an entity is as sensitive as its most sensitive contributor).
- At load time, a **sensitivity gate** excludes above-threshold, unmasked fields by default (explicit opt-in to include), and **transform-at-load masking** (hash, tokenize, redact, nullify) lets you carry a column safely.
- r2g can **emit enforcement artifacts** — a classification manifest, suggested ArangoDB RBAC grants, and an OPA/Rego policy stub — so the database and policy layers can actually enforce what the pipeline advised.

The Studio surfaces all of this: a *Sensitivity* lens tints classified fields, an entitlement report lists what's exposed, and masking is a right-click away.

---

## 6. The AI layer: the LLM proposes, the deterministic pipeline disposes

Here's the part I'm most excited about — and the part most likely to be done badly elsewhere.

r2g's newest capability is **LLM-assisted ontology derivation**. You point it at a schema and (optionally) describe your domain, and a model **proposes** a richer target graph: which tables are really vertices vs. edges, implicit/undeclared relationships, and clearer collection/property names — each suggestion carrying a rationale and a confidence score.

The design principles are strict, because "let an LLM design your database" is a trap:

- **The LLM never writes to the graph.** Its output is a candidate mapping that flows through the *exact same* validation → review → load path as every other mapping. The deterministic Auto-Map stays the default and the fallback.
- **Hallucination-proof by construction.** Every proposed collection, edge, and rename is validated against the real schema. Anything that references a table or column that doesn't exist is dropped and reported. In the worst case, the result degrades to the plain mechanical mapping — it can *never* load something that isn't there.
- **Metadata-only, and privacy-aware.** Only schema metadata is sent — never bulk rows — and columns your catalog flagged as Restricted/PII are redacted to name-only and never sampled. The prompt is injection-hardened: schema text is fenced and treated as untrusted data.
- **Human-in-the-loop.** In the Studio, a "Suggest model (AI)" action opens a review panel where you accept or reject each suggestion **per item**. Apply the ones you want; they land as an editable draft you still have to Save.
- **Reproducible.** Temperature 0, structured output, stored provenance (model, parameters, timestamp).

It's optional — an extra you install only if you want it — and it never phones home unless you invoke it. "Describe my domain, suggest the graph," with none of the "please don't hallucinate my schema" anxiety.

[IMAGE: "Suggest model (AI)" review panel with per-item accept/reject checkboxes]

---

## The philosophy tying it together

Three ideas run through the whole toolkit:

1. **Determinism first.** Everything is grounded in a mechanical, reproducible baseline. The clever features (AI, denormalization analysis) *advise*; they don't silently rewrite.
2. **Human-in-the-loop.** Drafts, diffs, and reviews everywhere. Nothing hits the database without an explicit save.
3. **Safety and honesty.** Referential-integrity checks, topological ordering, credential scrubbing in logs, classification-aware egress — and a README that's candid that this is an educational, experimental reference implementation, not production-hardened software.

---

## Getting started

`r2g-arango` is on PyPI, with opt-in extras so you only install the connectors and UI you need:

```bash
# CLI + Postgres + the mapping studio
pipx install 'r2g-arango[postgres,ui]'

# Point it at a database, introspect, and open the studio
r2g source add shop postgresql "$PG_CONN"
r2g source snapshot shop
r2g ui
```

From there: Auto-Map, refine on the canvas (or ask the AI for a proposal), validate, and stream into ArangoDB. Want it continuous? Wire up CDC. Worried about PII? Bind a catalog and turn on the sensitivity gate.

- **Source & docs:** the GitHub repo (`ArthurKeen/r2g-arango`)
- **License:** Apache 2.0
- **Full roadmap:** `docs/PRD.md` in the repo

If you're evaluating a relational-to-graph move onto ArangoDB — or you just want a well-documented reference for *how* such a pipeline is built — clone it, run it, and tell me where it hurts.

---

*Built in the open. Feedback, issues, and PRs welcome.*
