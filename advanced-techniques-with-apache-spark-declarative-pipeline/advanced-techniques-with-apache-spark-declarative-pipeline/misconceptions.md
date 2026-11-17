# Misconceptions — Advanced Techniques With Apache Spark Declarative Pipeline

> STATUS: VETTED (auto-generated from the course structure and SME/instructor input on 2025-06-10) — a maintainer should re-read and VET before use.

## How the TA uses this file

Each row is a **false premise** a learner may carry into a question. When a learner's
question embeds one of these premises, the TA should **correct the premise first**, in one
or two sentences, and only then answer the question that was actually asked. Do not answer
"around" a false premise — a learner who keeps it will hit the same wall again in the next
module.

Rules:

- Correct the premise, then teach. Never just say "that's wrong."
- Anchor the correction to where it is taught in this course (the **Anchor lesson** column)
  so the learner can go re-watch or re-run it.
- If the learner's false premise is about something **out of scope** for this course
  (Lakeflow Jobs, Declarative Automation Bundles, Iceberg writes, cost tuning), correct it
  briefly and redirect to `links.md` Section B rather than teaching it here.
- If correcting the premise would reveal a **graded lab answer** (Module 9), correct the
  concept generically and keep the lab's specific implementation sealed — hints only.
- These are teaching corrections, not product guarantees. For product behavior, prefer the
  vetted rows in `faq.md` Section 1 and the docs paths in `links.md`.

## Flows and multi-flow

| False premise | What's actually true | Anchor lesson |
| --- | --- | --- |
| "A flow is just another name for a table." | A **flow** is the unit of work and equals **query plus target**. The query says what to read and how to transform; the target is where results land (a streaming table or a materialized view). A table is the target, not the flow. | M1 Lecture — Flows, M2 Demo |
| "More flows means more tables." | Many flows can write into **one** target. That is the whole point of the multi-flow pattern: three subsidiary feeds (Bright Home, Lumina Sports, Northstar Outfitters) converge on a single bronze streaming table. | M1 Lecture, M2 Demo — multi-flow ingestion |
| "`CREATE OR REFRESH STREAMING TABLE ... AS SELECT` has no flow, it's just a query." | That statement creates exactly one **default flow** that owns the table. You only move to **explicit flows** when a target needs more than one writer — declare the table without a query, then add named flows (`CREATE FLOW ... AS INSERT INTO ... BY NAME SELECT`, or `@dp.append_flow` in Python). | M1 Lecture — default vs explicit flows |
| "`UNION` of three sources is the same thing as three flows." | A `UNION` is **one** query, therefore one flow and one checkpoint. Adding or changing a source usually forces a **full refresh** of the whole table, and one bad source blocks all of them. Multi-flow gives each source its own checkpoint, independent progress, and failure isolation. | M1 Lecture, M2 Demo — multi-flow vs UNION |
| "I can add a fourth subsidiary by editing the existing flow's query." | Add a **new named flow**. Editing an existing flow's query changes what that checkpoint is responsible for and typically means a full refresh; adding a flow lets the new feed backfill independently while existing flows keep their position. | M2 Demo, M9 Lab (concept only) |
| "Flows in one pipeline run in a fixed order I control." | Flows are **declarative**. The pipeline resolves dependencies from the dataset graph and runs independent flows concurrently. You express order through dependencies (bronze → silver → gold), not through statement order in the notebook. | M1 Lecture, M2 Demo — flow graph in the Pipelines UI |

## Checkpoints and full refresh

| False premise | What's actually true | Anchor lesson |
| --- | --- | --- |
| "Renaming a flow is cosmetic." | A checkpoint is keyed by the **flow name**. Rename the flow and you get a brand-new, empty checkpoint, so the flow re-reads the source from the beginning — which **duplicates rows** in an append target. Change logic with a deliberate full refresh, not by renaming. | M1 Lecture — checkpoints, M2 Demo (live rename) |
| "Full refresh is a harmless retry — it's the pipeline equivalent of re-running a cell." | **Full refresh discards the checkpoint and overwrites the target.** It reprocesses all source data (cost and time), and it only "loses nothing" if the source still holds all history. Treat it as a deliberate, logic-change operation, not a retry. | M1 Lecture — processing modes |
| "Deleting a flow removes the rows it wrote." | Removing a flow stops future writes but **does not delete rows it already appended**. Cleaning up previously-appended data is a separate, deliberate action. | M1 Lecture — checkpoints and flow lifecycle |
| "Incremental and full refresh are pipeline-level settings I pick once." | They are **processing modes per flow update**. Incremental uses the checkpoint to process only new records; full refresh discards it and reprocesses everything. You can full-refresh selected tables rather than the whole pipeline. | M1 Lecture — incremental vs full refresh |

## Expectations and data quality

| False premise | What's actually true | Anchor lesson |
| --- | --- | --- |
| "`EXPECT (quantity > 0)` also filters out nulls." | SQL three-valued logic makes `NULL > 0` evaluate to `NULL`, not `TRUE`, and an expectation only passes on `TRUE` — so null rows **violate** the constraint. Write it NULL-tolerant (`quantity IS NULL OR quantity > 0`) and add a separate explicit `NOT NULL` expectation so each rule reports its own violation count. | M7 Lecture, M8 Demo — NULL-tolerant constraints |
| "An `EXPECT` with no `ON VIOLATION` clause drops the bad rows." | No clause means **WARN**: the row **is written** and the violation is counted in the Pipelines UI data quality metrics. `ON VIOLATION DROP ROW` discards failing rows; `ON VIOLATION FAIL UPDATE` stops the update. | M1 Lecture, M7 Lecture — violation modes |
| "`ON VIOLATION DROP ROW` and the quarantine pattern are the same thing." | `DROP ROW` **silently discards** bad records — they cannot be audited or repaired. The **quarantine pattern** keeps everything by applying the same condition set twice, once as written for the valid table and once with **inverse logic** for a quarantine table, and records which rule failed so you have a remediation queue. | M7 Lecture, M8 Demo — quarantine |
| "Expectations clean the data for me." | Expectations **validate and route**; they do not repair. Cleaning, casting, and standardization are transformation work you write in silver. Expectations then tell you whether that work succeeded. | M7 Lecture, M8 Demo |
| "A `FAIL UPDATE` failure means the pipeline is broken." | It usually means the constraint **did its job** on genuinely bad data. Decide whether the rule or the data is wrong, fix in code, and reserve `FAIL UPDATE` for contract-breaking rules such as a missing primary key. | M7 Lecture, M8 Demo; instructor troubleshooting guidance |
| "Expectations work on anything the pipeline writes, including sinks." | Expectations apply to **pipeline-managed datasets** (streaming tables, materialized views). A **sink** is not a pipeline-managed dataset, so you cannot attach expectations to it or full refresh it like a table. | M3 Lecture, M4 Demo — Delta sinks |

## Ingestion, schema, and VARIANT

| False premise | What's actually true | Anchor lesson |
| --- | --- | --- |
| "Cast and clean in bronze so downstream tables get good types." | Keep **bronze permissive** — strings and the raw payload — and do type casting and business cleanup in **silver**, where expectations can validate the result. Casting too early means one malformed value can fail ingestion for an entire file. | M8 Demo; instructor troubleshooting guidance |
| "A new column in the source will break my pipeline." | Ingesting as `STRING`, pinning only the types you care about with Auto Loader `schemaHints`, and keeping the `_rescued_data` column lets new columns land automatically. In the Module 8 demo, Run 2 adds two new columns and the pipeline keeps running, with the unexpected content visible in `_rescued_data`. | M7 Lecture, M8 Demo — schema evolution |
| "`_rescued_data` is an error column — if it's populated, something failed." | It is a **capture** column for data that did not match the inferred schema (new fields, type mismatches). Populated `_rescued_data` is a signal to inspect and, if the field matters, add a `schemaHints` entry — not evidence of failure. | M8 Demo — Run 2 comparison |
| "For a multiplexed source I should define a struct covering every event schema." | Ingest the payload as **`VARIANT`**: one open, efficiently-encoded column that does not need the union of every schema and does not break when a producer adds a field. A wide struct becomes mostly nulls and needs a code change per new field. | M3 Lecture, M4 Demo — VARIANT |
| "The multiplex pattern means one stream per event type reading the landing zone." | The pattern is **ingest once, fan out by type**: one bronze streaming table reads the mixed stream as-is, then separate downstream streaming tables filter by event type. One reader per type means many concurrent streams scanning the same files, far more checkpoints, and much higher cost. | M3 Lecture, M4 Demo — multiplex |

## Sinks, UniForm, and clustering

| False premise | What's actually true | Anchor lesson |
| --- | --- | --- |
| "A sink is just a streaming table I named differently." | A managed **streaming table** is owned by the pipeline — it appears in lineage and can be full refreshed. A **sink** targets something the pipeline does **not** own, is **append-only**, is not a pipeline-managed dataset, and takes no expectations. Use it to publish to external consumers. | M3 Lecture, M4 Demo — Delta sinks |
| "Delta UniForm converts the table to Iceberg / makes a second copy." | UniForm writes **Iceberg metadata alongside** the Delta table so Iceberg-compatible engines read the **same underlying Parquet files**. There is no second copy and no export job; the pipeline keeps writing Delta. | M3 Lecture, M4 Demo — UniForm |
| "UniForm lets external Iceberg engines write to my table." | The course scope is **Iceberg reads** over the Delta table via UniForm metadata. Writing to Iceberg from external engines is out of scope — redirect to `links.md` Section B. | M3 Lecture; M99 Summary (scope) |
| "Liquid clustering is just a new name for partitioning, or needs `ZORDER` alongside it." | **Liquid clustering replaces** partitioning and `ZORDER` with an adaptive layout you can change **without rewriting the table**. Do not combine it with partitioning or `ZORDER` on the same table. | M1 Lecture, M2 Demo — liquid clustering |
| "`CLUSTER BY AUTO` is always the safe choice." | `CLUSTER BY AUTO` is right when query patterns are **unknown or evolving** and you want Databricks to choose and adjust keys from observed workloads. When you know the dominant filter or join columns (a date plus a store or customer id), name them explicitly — and keep the key count small. | M1 Lecture, M2 Demo |

## CDC and SCD

| False premise | What's actually true | Anchor lesson |
| --- | --- | --- |
| "`AUTO CDC INTO` creates the target table for me." | The target **streaming table must be created separately** with `CREATE OR REFRESH STREAMING TABLE`. `AUTO CDC INTO` then applies change events to it, supplying `FROM STREAM(source)`, `KEYS (...)`, and `SEQUENCE BY`. | M5 Lecture, M6 Demo — AUTO CDC INTO |
| "`SEQUENCE BY` is optional ordering nicety." | It is how the pipeline resolves **late and out-of-order events**. Without a correct sequencing column (timestamp or version), a stale change event can overwrite a newer one. | M5 Lecture, M6 Demo |
| "`AUTO CDC INTO` defaults to keeping history." | The default is **SCD Type 1** — the row is overwritten and only current state is kept. You must ask for history with `STORED AS SCD TYPE 2`. | M5 Lecture, M6 Demo |
| "SCD Type 2 is strictly better than Type 1." | Type 2 keeps every version, which you need for point-in-time reporting and audit — but it costs more storage and forces **every** query to state whether it wants current or historical rows. Type 1 is correct when history has no business value (fixing a typo in an address). | M5 Lecture — choosing SCD type |
| "In an SCD Type 2 table, `SELECT *` gives me current customers." | It gives you **all versions**. Current rows are the ones where `__END_AT IS NULL` — exactly the filter the gold "current customers" materialized view uses. Historical analysis reads all rows and uses `__START_AT`/`__END_AT` as a validity window. | M6 Demo — querying SCD2 |
| "I have to hand-write `MERGE` logic for CDC in a pipeline." | `AUTO CDC INTO` **replaces** hand-written MERGE for applying change events, including deletes via `APPLY AS DELETE WHEN` and column pruning via `COLUMNS * EXCEPT (...)`. | M5 Lecture, M6 Demo |

## Materialized views and pipeline behavior

| False premise | What's actually true | Anchor lesson |
| --- | --- | --- |
| "Materialized views always refresh incrementally." | A materialized view refreshes incrementally only when its query is **expressible incrementally** (selects, filters, joins, supported aggregations over change-tracking Delta sources). Non-deterministic expressions, unsupported functions, and some window or nested constructs force a **full recompute**. Check the refresh type reported in the Pipelines UI. | M1 Lecture, M2 Demo — incremental MVs |
| "Materialized views and streaming tables are interchangeable targets." | Streaming tables are for **incremental append/CDC** ingestion with checkpoints; materialized views are for **declaratively recomputed** results. Which one you pick changes refresh semantics, cost, and whether full refresh is meaningful. | M1 Lecture — flows and targets |
| "Everything I need to see is in the notebook output." | The teaching payoff is in the **Pipelines UI**: the flow graph with three flows converging on one bronze table, per-flow record counts, per-MV refresh type, and the data quality panel with per-constraint pass/fail counts. | M2, M4, M6, M8 Demos; instructor delivery guidance |
| "Unity Catalog tags change how the data is stored or queried." | Tags are **metadata only** — key-value labels on catalogs, schemas, tables, and columns that make pipeline output discoverable and filterable in Catalog Explorer. They do not change query results or performance. | M2 Demo — governance tags |

## Scope and environment

| False premise | What's actually true | Anchor lesson |
| --- | --- | --- |
| "This course will show me how to schedule and deploy the pipeline." | This is an **advanced patterns** course. Orchestration with **Lakeflow Jobs**, deployment with **Declarative Automation Bundles** and Git folders, cost/capacity tuning, and Iceberg writes from external engines are out of scope — see `links.md` Section B and M99. | M0 Overview, M99 Summary and Next Steps |
| "This course teaches SDP from scratch." | It assumes the prerequisite course: `CREATE OR REFRESH STREAMING TABLE`, basic constraints, the Pipelines UI, plus Delta Lake fundamentals, streaming concepts, solid SQL, Python-reading comfort, and Unity Catalog basics. Learners without these will struggle from the first demo. | M0 Overview — prerequisites |
| "Any serverless compute version will run these notebooks." | Use the **serverless environment version the notebooks were tested on (Version 4)**; other versions may not support everything shown. | M0 Overview; instructor environment guidance (DRAFT — perishable) |
| "If bronze returns no rows, the pipeline code is wrong." | The most common cause is the **Marketplace share catalog name** — the Simulated Retail Customer Data share is `dbacademy_retail` in the Academy lab environment, otherwise learners set `your_marketplace_share_catalog_name` themselves. Print setup variables and `LIST` the volume before blaming the pipeline. | M0/Setup; instructor lab-support guidance (DRAFT) |
| "These notebooks are safe to run in any workspace." | They create catalogs, schemas, volumes, and pipelines — run them only in **development or sandbox** workspaces, stop updates once the graph and metrics are reviewed, and clean up pipelines and course schemas at the end. | M0 Overview, M99 Summary; instructor cleanup guidance (POLICY) |

## Maintenance

- Last generated: 2025-06-10. Re-vet whenever the notebooks change, when `ldp/*`
  documentation paths move, or when a module is added or renumbered.
- Rows derived from instructor input (serverless version, Marketplace catalog name, lab
  blockers) track `faq.md` Section 2 and are **perishable** — re-confirm against the current
  course release before asserting them.
- Keep this file premise-shaped. New product facts belong in `faq.md`; new "do this instead"
  guidance belongs in `best_practices.md`.
