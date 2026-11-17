# Course Pack — Advanced Techniques With Apache Spark Declarative Pipeline

> STATUS: VETTED (auto-generated from the course structure on 2025-06-05)

This pack is the structural ground truth for the Teaching Assistant: what the course
teaches, in what order, and where each concept is anchored. Pair it with
`links.md` (docs paths), `faq.md` (Q&A), `misconceptions.md`, and
`best_practices.md`. Cloud default for links: **AWS**.

---

## 1. Concept map (teaching order)

**Module 0 — Course Overview and prerequisites**
- Position the course: *advanced design patterns* for Spark Declarative Pipelines (SDP), not SDP fundamentals.
- Confirm the six prerequisites: prior SDP experience (ideally *Build Data Pipelines with Apache Spark Declarative Pipelines*), Delta Lake fundamentals, streaming concepts (micro-batch, checkpointing, event time), solid SQL (joins, `MERGE`, `CASE WHEN`), ability to read/run Python in notebooks, Unity Catalog basics (catalogs, schemas, tables, volumes).
- Environment requirements: Serverless compute **environment Version 4**, a per-learner Unity Catalog catalog (`labuser…` in Vocareum, otherwise **Create Catalog** permission), and the **Simulated Retail Customer Data** Marketplace share (`dbacademy_retail` in Vocareum, else set `your_marketplace_share_catalog_name`).

**Module 1 — Lecture: flows, multi-flow ingestion, expectations, liquid clustering**
- A **flow** = query + target; the basic unit of work in a pipeline. Targets are **streaming tables** or **materialized views**.
- Processing modes: **incremental** (checkpoint-driven) vs **full refresh** (checkpoint discarded, target overwritten).
- **Default flow** (created by `CREATE OR REFRESH STREAMING TABLE … AS SELECT`) vs **explicit flows** (`CREATE FLOW … AS INSERT INTO … BY NAME SELECT`, or `@dp.append_flow` in Python) when one target needs multiple writers.
- Why multi-flow beats `UNION`: per-flow checkpoints, independent progress, failure isolation, add a source without a full refresh.
- Checkpoint semantics keyed by **flow name** — renaming a flow creates a new checkpoint and re-reads from the beginning.
- **Expectations**: `CONSTRAINT … EXPECT (…)` with WARN (default), `ON VIOLATION DROP ROW`, `ON VIOLATION FAIL UPDATE`.
- **Liquid clustering**: `CLUSTER BY (cols)` vs `CLUSTER BY AUTO` as the replacement for partitioning and `ZORDER`.

**Module 2 — Demo: multi-flow ingestion into one bronze table**
- Three subsidiary feeds (Bright Home, Lumina Sports, Northstar Outfitters) in CSV/JSON land in **one** bronze streaming table via three named flows.
- Schema standardization per flow: Auto Loader read, project to a common column list/order, alias source-specific names so `INSERT INTO … BY NAME` matches the target.
- Keep bronze permissive (STRING + raw payload); cast and clean in **silver** where expectations validate the result.
- Apply **liquid clustering** to the ingestion targets; apply **Unity Catalog tags** to bronze/silver/gold objects for discoverability and governance.
- Build gold **materialized views** and inspect the refresh type (incremental vs full recompute) in the Pipelines UI.

**Module 3 — Lecture: multiplex ingestion, VARIANT, Delta sinks, Iceberg/UniForm**
- **Multiplex** source: one landing location carries many event domains with different schemas → *ingest once, fan out by type*.
- **`VARIANT`** for semi-structured payloads instead of a union-of-all-schemas struct; downstream tables extract only what they need.
- **Delta sinks** (`dp.create_sink(...)` + `@dp.append_flow(target="sink")`) for append-only streaming writes to targets the pipeline does **not** own — no expectations, no full refresh, not a managed dataset.
- **Delta UniForm**: Iceberg metadata written alongside the Delta table so Iceberg readers read the same Parquet files — no copy, no export job.

**Module 4 — Demo: multiplex fan-out, Delta sink, Iceberg read**
- One bronze streaming table reads the mixed stream as-is into `VARIANT`.
- Separate silver streaming tables filter by event type into domain tables (store operations, marketing, logistics).
- Publish the logistics output to an external consumer through a **Delta sink**, then enable **UniForm** so Iceberg-compatible engines can read it.

**Module 5 — Lecture: CDC and slowly changing dimensions**
- Change data capture concepts; **SCD Type 1** (overwrite, current state only) vs **SCD Type 2** (close old row, insert new version, point-in-time history).
- `AUTO CDC INTO` as the declarative replacement for hand-written `MERGE`.

**Module 6 — Demo: `AUTO CDC INTO` with SCD Type 2**
- Required parts: target streaming table created separately, `FROM STREAM(source)`, `KEYS (…)`, `SEQUENCE BY <ordering column>`; optional `APPLY AS DELETE WHEN`, `COLUMNS * EXCEPT (…)`, `STORED AS SCD TYPE 2`.
- Querying SCD2: generated `__START_AT` / `__END_AT`; current rows are `__END_AT IS NULL` (used by the gold "current customers" materialized view); historical analysis uses the validity window.
- Deleted-customers view from keys whose latest change is a delete; `max_by` to pick an attribute from the most recent record per key.

**Module 7 — Lecture: advanced data quality**
- Expectation design: NULL-tolerant constraints and SQL three-valued logic (a comparison against `NULL` is `NULL`, not `TRUE`, so the row fails).
- **Quarantine pattern** vs `DROP ROW`: apply the same condition set twice (as written for valid, inverted for quarantine) so nothing is lost and the failing rule is recorded.
- **Schema evolution** resilience: ingest as STRING, pin types with Auto Loader `schemaHints`, capture surprises in `_rescued_data`.

**Module 8 — Demo: quarantine, violation modes, schema evolution**
- Run 1: clean baseline. Run 2: adds quality issues **and two new columns** — pipeline keeps running; unexpected content appears in `_rescued_data`.
- Compare per-constraint pass/fail counts in the Pipelines UI **data quality** panel across the two runs.

**Module 9 — Lab: multi-source e-commerce pipeline (graded)**
- Learners combine the patterns end to end: multi-flow bronze ingestion from several sources, silver with expectations/quarantine, gold materialized views, clustering and tags.

**Module 99 — Summary and Next Steps**
- Recap of the patterns; out-of-scope list; documentation and learning-path pointers.

---

## 2. Where each topic is taught

| Concept | Module | Self-paced equivalent |
| --- | --- | --- |
| Prerequisites, environment, Marketplace share | Module 0 — Course Overview | Self-paced: Course Overview / Get Started |
| Flow = query + target; streaming table vs materialized view | Module 1 — Lecture | Self-paced: Flows and pipeline objects |
| Incremental processing vs full refresh; checkpoints keyed by flow name | Module 1 — Lecture; reinforced in Module 2 | Self-paced: Pipeline updates and refresh modes |
| Default flow vs explicit flow (`CREATE FLOW`, `@dp.append_flow`) | Module 1 — Lecture; Module 2 — Demo | Self-paced: Multiple flows into one target |
| Multi-flow vs `UNION` trade-off | Module 1 — Lecture | Self-paced: Multiple flows into one target |
| Schema standardization with `INSERT INTO … BY NAME` | Module 2 — Demo | Self-paced: Multi-source bronze ingestion |
| Auto Loader options (`schemaHints`, `_rescued_data`) | Module 2 — Demo; Module 8 — Demo | Self-paced: Ingestion and schema evolution |
| Liquid clustering: `CLUSTER BY (cols)` vs `CLUSTER BY AUTO` | Module 1 — Lecture; Module 2 — Demo | Self-paced: Table layout and clustering |
| Unity Catalog tags on pipeline objects | Module 2 — Demo | Self-paced: Governance for pipeline output |
| Expectations and the three violation modes | Module 1 — Lecture; Module 7/8 | Self-paced: Expectations and data quality |
| Incremental vs full-recompute materialized views | Module 2 — Demo (refresh type in Pipelines UI) | Self-paced: Materialized views |
| Multiplex pattern (ingest once, fan out by type) | Module 3 — Lecture; Module 4 — Demo | Self-paced: Multiplex ingestion |
| `VARIANT` for semi-structured payloads | Module 3 — Lecture; Module 4 — Demo | Self-paced: Semi-structured data |
| Delta sinks (`dp.create_sink`, append-only, unmanaged target) | Module 3 — Lecture; Module 4 — Demo | Self-paced: Pipeline sinks |
| Delta UniForm / Iceberg reads | Module 3 — Lecture; Module 4 — Demo | Self-paced: Interoperability with Iceberg |
| SCD Type 1 vs Type 2 decision | Module 5 — Lecture | Self-paced: CDC and SCDs |
| `AUTO CDC INTO` clauses (`KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`, `STORED AS SCD TYPE 2`) | Module 5 — Lecture; Module 6 — Demo | Self-paced: AUTO CDC |
| Querying SCD2 (`__START_AT`, `__END_AT`, `max_by`) | Module 6 — Demo | Self-paced: AUTO CDC |
| NULL-tolerant constraints and three-valued logic | Module 7 — Lecture; Module 8 — Demo | Self-paced: Advanced expectations |
| Quarantine pattern (inverse-logic twin table) | Module 7 — Lecture; Module 8 — Demo | Self-paced: Quarantine and remediation |
| Schema evolution across two runs; `_rescued_data` inspection | Module 8 — Demo | Self-paced: Schema evolution |
| End-to-end pattern assembly (graded) | Module 9 — Lab | Self-paced: Capstone / knowledge check |
| Out-of-scope boundaries and next steps | Module 99 — Summary and Next Steps | Self-paced: Summary and next steps |

---

## 3. Notebooks (teaching order)

1. **0 — Course Overview** — agenda, prerequisites, environment and Marketplace-share requirements, Serverless environment Version 4.
2. **1 — Lecture: Flows, Multi-Flow Ingestion, Expectations, Liquid Clustering** — flow anatomy, default vs explicit flows, processing modes, violation modes, clustering.
3. **2 — Demo: Multi-Flow Ingestion with Clustering, Tags and Data Quality** — three subsidiary feeds into one bronze streaming table; silver casting; gold materialized views; `CLUSTER BY`; Unity Catalog tags.
4. **3 — Lecture: Multiplex Ingestion, VARIANT, Delta Sinks and Iceberg** — multiplex rationale, `VARIANT`, sink semantics, UniForm.
5. **4 — Demo: Multiplex Fan-Out, Delta Sink and Iceberg Read** — one bronze `VARIANT` table, per-domain silver tables, Delta sink for logistics, UniForm enablement.
6. **5 — Lecture: Change Data Capture and Slowly Changing Dimensions** — CDC concepts, SCD Type 1 vs Type 2, `AUTO CDC INTO` overview.
7. **6 — Demo: AUTO CDC INTO with SCD Type 2** — required clauses, `__START_AT`/`__END_AT`, current vs historical gold views, deleted-customers view, `max_by`.
8. **7 — Lecture: Advanced Data Quality** — NULL-tolerant expectations, quarantine pattern, schema-evolution strategy.
9. **8 — Demo: Quarantine, Violation Modes and Schema Evolution** — two-run design (clean baseline, then quality issues + two new columns), data quality metrics, `_rescued_data`.
10. **9 — Lab: Multi-Source E-Commerce Pipeline** — graded; assemble multi-flow bronze, validated silver with quarantine, gold materialized views, clustering and tags.
11. **99 — Summary and Next Steps** — recap, scope boundaries, documentation links, learning path and certification pointers.

*Supporting (not taught, skip in TA answers): `Version Info` and `Includes/Classroom-Setup-*` classroom-setup notebooks (they create the catalog, schemas, volumes and source data; `build_user_catalog_name` with `catalog_forced` lets a learner reuse an existing catalog).*

---

## 4. Scope

**In scope:** advanced *design patterns* inside Spark Declarative Pipelines — flows and multi-flow ingestion, checkpoint behavior and refresh modes, expectations (WARN / DROP ROW / FAIL UPDATE) and the quarantine pattern, liquid clustering, Unity Catalog tags on pipeline objects, multiplex ingestion with `VARIANT`, Delta sinks, Iceberg reads via Delta UniForm, `AUTO CDC INTO` with SCD Type 1 and Type 2, schema evolution with `schemaHints` and `_rescued_data`, and reading pipeline behavior in the Pipelines UI.

**Out of scope (redirect, don't improvise):** SDP fundamentals (prerequisite course), Delta Lake internals, Structured Streaming APIs outside pipelines, orchestration and scheduling with **Lakeflow Jobs**, CI/CD with **Declarative Automation Bundles** and Git folders, cost and capacity tuning, and writing to Iceberg from external engines. Point to the documentation in *Summary and Next Steps* and the data engineering learning path.

---

## 5. Sealed solutions

Graded work in this course — the **Module 9 e-commerce lab** and any "your turn" / TODO cells inside the demo notebooks — is **sealed**. The TA never pastes a working answer: no completed flow definitions, no finished `AUTO CDC INTO` statement, no corrected constraint set, no filled-in lab cells.

Reason: these artifacts are the assessment. Writing them for the learner removes the exact reasoning the module is built to teach (which target a flow should write to, where a cast belongs, whether a rule should WARN or FAIL UPDATE).

Instead the TA uses a **hint ladder**, one rung at a time, and stops as soon as the learner is unblocked:
1. Point at the module and concept that governs the decision (e.g. "explicit flows, Module 1").
2. Name the observable to check (per-flow record counts, refresh type, data quality panel, `_rescued_data`, setup variables such as `my_vol_path`).
3. Ask a diagnostic question ("do your projected columns match the target column list that `INSERT INTO … BY NAME` requires?").
4. Describe the shape of the fix in words, never the code.

Factual platform questions (what a clause does, what UniForm writes, what `__END_AT IS NULL` means) are answered directly — sealing applies to graded answers, not to product facts.
