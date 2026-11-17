# Product FAQ — Advanced Techniques With Apache Spark Declarative Pipeline

This FAQ has three tiers. **Tier status is load-bearing** — the TA must treat each
section differently and must never present a DRAFT answer as settled product fact.

| Section | Source | Status |
|---|---|---|
| 1. Product FAQ (SME / PM reviewed) | SME-supplied Q&A for this course | `STATUS: VETTED` |
| 2. Instructor-sourced delivery FAQ | Instructor questions about running the course | `STATUS: DRAFT — PENDING INSTRUCTOR REVIEW (2025-11-05)` |
| 3. Course-derived learner FAQ | Derived from the en_us notebooks (what learners get wrong) | `STATUS: DRAFT — PENDING INSTRUCTOR REVIEW (2025-11-05)` |

**Cloud prefixes** — every `Docs source` value below is a *path*. Append it to the
prefix for the learner's cloud (classroom default is AWS):

- AWS — `https://docs.databricks.com/aws/en/`
- Azure — `https://learn.microsoft.com/en-us/azure/databricks/`
- GCP — `https://docs.databricks.com/gcp/en/`

> **Maintainer-only column.** The `Provenance (maintainer-only)` column in Section 1
> exists for FAQ upkeep. It is **never** shown to a learner, never quoted, and never
> used as a citation. Learner-facing citations come from the `Docs source` column only.

---

## Section 1 — Product FAQ (SME / PM reviewed)

> **STATUS: VETTED**
> Reviewed SME/PM answers. The TA may answer these directly and confidently, citing
> the `Docs source` path appended to the learner's cloud prefix.

| Question | Vetted answer | Docs source (append to cloud prefix) | Provenance (maintainer-only) |
|---|---|---|---|
| What exactly is a "flow" in Spark Declarative Pipelines? | A flow is the basic unit of work in a pipeline and equals **query plus target**: the query defines what to read and how to transform it; the target is where results land — a streaming table or a materialized view. Each flow reads, processes and writes independently, which gives modular updates and failure isolation. Flows run in one of two processing modes: **incremental**, which uses a checkpoint to process only new records, or **full refresh**, which discards the checkpoint and reprocesses everything. | `ldp/flows` | SME Q1 (Flows) — course SME intake, 2025-11-05 |
| When do I need an explicit flow instead of the default flow? | Every `CREATE OR REFRESH STREAMING TABLE ... AS SELECT` creates a single **default flow** that owns the table. Switch to **explicit flows** when one target needs more than one writer: declare the table without a query, then add named flows with `CREATE FLOW ... AS INSERT INTO ... BY NAME SELECT` in SQL, or the `@dp.append_flow` decorator in Python. This is the pattern the multi-flow demo uses to land Bright Home, Lumina Sports and Northstar Outfitters data into one bronze streaming table. | `ldp/flows` | SME Q2 (Default vs explicit flows) — course SME intake, 2025-11-05 |
| Why is the multi-flow pattern preferred over a `UNION` of sources? | With `UNION`, all sources are one query and therefore **one flow and one checkpoint**, so adding or changing a source usually forces a full refresh of the whole table. With multi-flow, each source has its own named flow and its own checkpoint: flows progress independently, a failure in one source does not block the others, and you can add a new subsidiary feed by adding a flow without reprocessing existing history. | `ldp/flows` | SME Q3 (Multi-flow vs UNION) — course SME intake, 2025-11-05 |
| How do flow checkpoints behave, and what accidentally resets them? | Each flow stores a checkpoint keyed by the **flow name**, so the flow resumes exactly where it stopped. Three things to watch: (1) renaming a flow creates a brand-new checkpoint and the flow re-reads the source from the beginning, which can duplicate data in an append target; (2) a **full refresh** discards the checkpoint and overwrites the target; (3) removing a flow does **not** delete the rows it already appended. Change transformation logic with a deliberate full refresh, not by renaming flows. | `ldp/flows` | SME Q4 (Checkpoints and full refresh) — course SME intake, 2025-11-05 |
| My sources are CSV and JSON with different column names and types — where do I reconcile them? | Reconcile **per flow in bronze**: each flow selects from its own volume with Auto Loader and projects to one common column list and order, aliasing source-specific names so `INSERT INTO ... BY NAME` matches the target schema. Keep bronze permissive (strings plus the raw payload) and do type casting and business cleanup in **silver**, where expectations can validate the result. Casting too early in bronze lets a single bad value fail ingestion for a whole file. | `ingestion/cloud-object-storage/auto-loader/schema` | SME Q5 (Schema standardization) — course SME intake, 2025-11-05 |
| When should I use `CLUSTER BY AUTO` versus naming clustering keys? | **Liquid clustering** replaces partitioning and `ZORDER` with an adaptive layout you can change without rewriting the table. Use `CLUSTER BY AUTO` when query patterns are unknown or evolving and you want Databricks to choose and adjust keys from observed workloads. Name keys explicitly with `CLUSTER BY (col1, col2)` when you know the dominant filter or join columns (for example a date plus a store or customer id), and keep the key count small. | `delta/clustering` | SME Q6 (Liquid clustering) — course SME intake, 2025-11-05 |
| What do the three expectation violation modes actually do? | `CONSTRAINT name EXPECT (condition)` with no `ON VIOLATION` clause is **WARN**: the row is written and the violation is counted in the Pipelines UI data quality metrics. `ON VIOLATION DROP ROW` writes only passing rows and discards the rest. `ON VIOLATION FAIL UPDATE` stops the update on the first violating row. Use WARN for observability, DROP ROW for clean serving tables, and FAIL UPDATE only for contract-breaking rules such as a missing primary key. | `ldp/expectations` | SME Q7 (Expectations) — course SME intake, 2025-11-05 |
| How do I get gold materialized views to refresh incrementally instead of recomputing? | A materialized view refreshes incrementally when its query is **expressible incrementally** — typically selects, filters, joins and supported aggregations over Delta sources that track changes. Non-deterministic expressions, unsupported functions, and certain window or nested constructs force a full recompute. Check the refresh type reported for the materialized view in the Pipelines UI and simplify the query if you expected incremental and got full recompute. | `optimizations/incremental-refresh` | SME Q8 (Incremental materialized views) — course SME intake, 2025-11-05 |
| Why does the course apply Unity Catalog tags to bronze, silver and gold objects? | Tags are key-value labels on catalogs, schemas, tables and columns in **Unity Catalog** that make pipeline output discoverable and governable: tag by medallion layer, data owner, subsidiary or sensitivity, then search and filter on those tags in Catalog Explorer. The demo sets tags as part of pipeline development so governance is not a bolt-on later. Tags are metadata only — they do not change query results or performance. | `database-objects/tags` | SME Q9 (Governance) — course SME intake, 2025-11-05 |
| What is the multiplex pattern, and why not just create one stream per event type? | In a **multiplex** source, one landing location carries events for many domains (store operations, marketing, logistics) with different schemas. The pattern is **ingest once, fan out by type**: one bronze streaming table reads the mixed stream as-is, then separate downstream streaming tables filter by event type into domain-specific tables. One reader per event type would mean many concurrent streams scanning the same files, far more checkpoints, and much higher cost. | `ldp/flows` | SME Q10 (Multiplex pattern) — course SME intake, 2025-11-05 |
| Why ingest the multiplexed payload as `VARIANT` instead of defining a struct schema? | `VARIANT` stores semi-structured JSON in a single open, efficiently encoded column, so the bronze table needs no union of every event schema and does not break when a producer adds a field. Downstream silver tables extract and cast only the fields their domain needs, keeping fan-out logic local to each table. A wide struct covering all event types becomes mostly nulls and needs a code change for every new producer field. | `sql/language-manual/data-types/variant-type` | SME Q11 (VARIANT type) — course SME intake, 2025-11-05 |
| When do I use a Delta sink instead of a managed streaming table? | Managed **streaming tables** are the default: the pipeline owns them, shows them in lineage, and can full refresh them. A **sink** is for streaming writes to a target the pipeline does not own — how the multiplex demo publishes logistics data for external consumption. Declare it in Python with `dp.create_sink(...)` and feed it with `@dp.append_flow(target="sink_name")`. Sink writes are **append-only**, and because the sink is not a pipeline-managed dataset you cannot apply expectations to it or full refresh it like a table. | `ldp/sinks` | SME Q12 (Delta sinks) — course SME intake, 2025-11-05 |
| What does Delta UniForm do, and how does it give Iceberg readers access? | **Delta UniForm** writes Iceberg metadata alongside the Delta table so Iceberg-compatible engines read the same underlying Parquet files — there is **no second copy of the data** and no export job. Enable it as a table property on the Delta table; external readers then resolve the table through its Iceberg metadata. In the demo this exposes the live logistics output for cross-platform analytics while the pipeline keeps writing Delta. | `delta/uniform` | SME Q13 (Iceberg reads with UniForm) — course SME intake, 2025-11-05 |
| SCD Type 1 or Type 2 — how do I choose? | **Type 1** overwrites the row so the table holds only current state: simple, compact, right when history has no business value (correcting a typo in an address). **Type 2** keeps every version by closing the old row and inserting a new one — needed for point-in-time reporting, auditability, and questions like "which region was this customer in last quarter". Type 2 costs more storage and makes every query state whether it wants current or historical rows. | `ldp/cdc` | SME Q14 (CDC and SCDs) — course SME intake, 2025-11-05 |
| What are the required parts of an `AUTO CDC INTO` statement? | `AUTO CDC INTO` applies change events to a target **streaming table** and replaces hand-written MERGE logic. You supply: (1) the target streaming table, created separately with `CREATE OR REFRESH STREAMING TABLE`; (2) `FROM STREAM(source)`; (3) `KEYS (...)` for the business key; (4) `SEQUENCE BY` an ordering column (timestamp or version) so late or out-of-order events land correctly; optionally (5) `APPLY AS DELETE WHEN` for delete events, `COLUMNS * EXCEPT (...)` to drop operational columns, and `STORED AS SCD TYPE 2` to keep history instead of the default Type 1 overwrite. | `ldp-ref/sql-ref-auto-cdc-into` | SME Q15 (AUTO CDC INTO) — course SME intake, 2025-11-05 |
| How do I query an SCD Type 2 table for current versus historical state? | With `STORED AS SCD TYPE 2` the pipeline adds start and end timestamp columns `__START_AT` and `__END_AT`. Current rows are those where `__END_AT IS NULL` — exactly the filter the gold "current customers" materialized view uses. Historical analysis reads all rows and uses the start/end values as a validity window. The demo also builds a deleted-customers view from rows whose latest change was a delete, and uses `max_by` to pick an attribute from the most recent record per key. | `ldp/cdc` | SME Q16 (Querying SCD2) — course SME intake, 2025-11-05 |
| Why does my `EXPECT` constraint drop rows that look valid? | Because SQL three-valued logic makes a comparison against `NULL` evaluate to `NULL`, not `TRUE`, and an expectation passes only on `TRUE`. So `EXPECT (quantity > 0)` silently fails every row where `quantity` is `NULL`. Write **NULL-tolerant** constraints when a missing value is acceptable — `EXPECT (quantity IS NULL OR quantity > 0)` — and add a separate explicit `NOT NULL` expectation for columns that truly must be populated, so each rule reports its own violation count. | `ldp/expectations` | SME Q17 (NULL-tolerant constraints) — course SME intake, 2025-11-05 |
| How does the pipeline absorb new source columns without a code change? | Two things make bronze resilient: ingesting all columns as **STRING** so a type surprise cannot fail the load, and using Auto Loader `schemaHints` to pin the types you do care about plus the `_rescued_data` column to capture anything that does not match the inferred schema. New columns then land automatically and silver casts and validates only what it needs. In the data quality demo, Run 2 adds two new columns and the pipeline keeps running, with the unexpected content visible in `_rescued_data`. | `ingestion/cloud-object-storage/auto-loader/schema` | SME Q18 (Schema evolution) — course SME intake, 2025-11-05 |
| How is the quarantine pattern different from `ON VIOLATION DROP ROW`? | `DROP ROW` silently discards bad records, so they cannot be audited or repaired. The **quarantine pattern** keeps everything: the same condition set is applied twice — once as written for the valid silver table, once with **inverse logic** for a quarantine table — so each record goes exactly one way and nothing is lost. The quarantine table also records which rule failed, giving you a remediation queue, while per-constraint violation counts remain visible in the Pipelines UI data quality metrics. | `ldp/expectations` | SME Q19 (Quarantine pattern) — course SME intake, 2025-11-05 |
| What does this course not cover, and where should I go next? | This is an advanced *patterns* course for Spark Declarative Pipelines. It does not teach SDP fundamentals (the prerequisite course covers `CREATE OR REFRESH STREAMING TABLE`, basic constraints and the Pipelines UI), Delta Lake internals, Structured Streaming APIs outside pipelines, orchestration with **Lakeflow Jobs**, CI/CD with **Declarative Automation Bundles**, cost and capacity tuning, or Iceberg **writes** from external engines. For next steps use the Databricks docs for declarative pipelines, CDC, expectations, clustering and UniForm, plus the Databricks training and certification catalog for the data engineering learning path. | `ldp/` | SME Q20 (Scope boundaries) — course SME intake, 2025-11-05 |

---

## Section 2 — Instructor-sourced delivery FAQ

> **STATUS: DRAFT — PENDING INSTRUCTOR REVIEW (2025-11-05)**
> Sourced from instructor delivery questions for this course. The TA may use these to
> help an instructor plan or unblock a classroom, but must present them as draft
> guidance, not settled product behavior. Rows flagged `PERISHABLE` (versions, lab
> environment details) must be re-verified against the current course build before use.

### Domain: Prerequisites and environment

| Question | Draft answer | Docs source | Confidence | Flag |
|---|---|---|---|---|
| What should learners already know before day one? | Six prerequisites from the Course Overview: (1) prior Spark Declarative Pipelines experience, ideally "Build Data Pipelines with Apache Spark Declarative Pipelines", including `CREATE OR REFRESH STREAMING TABLE`, constraints and the Pipelines UI; (2) Delta Lake fundamentals; (3) streaming concepts — micro-batch, checkpointing, event time; (4) solid SQL including joins, `MERGE`, `CASE WHEN`; (5) comfort reading and running Python in notebooks; (6) Unity Catalog basics — catalogs, schemas, tables, volumes. Learners who have never built a pipeline will struggle from the first demo. | `ldp/` | High | — |
| What must be in place in the workspace before the first demo? | Confirm three things: (1) learners select the **Serverless environment version the notebooks were tested on (Version 4)** — other versions may not support everything shown; (2) each learner has a Unity Catalog catalog — pre-created in a Databricks Academy Vocareum workspace as `labuser12345`, while other workspaces need **Create Catalog** permission so setup can create a `labuser_<username>` catalog; (3) the **Simulated Retail Customer Data** Marketplace share is installed, available as `dbacademy_retail` in Vocareum, otherwise learners install it themselves with a unique catalog name and set `your_marketplace_share_catalog_name`. Run every classroom setup cell yourself in a dry run first. | `compute/serverless/dependencies` | High | PERISHABLE |
| What cleanup and cost guidance should I give? | These notebooks create catalogs, schemas, volumes and pipelines, so run them only in **development or sandbox** workspaces. Serverless pipelines keep incurring cost while an update runs: stop updates once the graph and metrics have been reviewed, and do not leave continuous mode on. At the end of the course delete the pipelines and drop the course schemas (multiplex, CDC, data quality, and the lab bronze/silver/gold schemas); in Vocareum the lab environment is reclaimed for you. Avoid unnecessary full refreshes, which reprocess all source data. | `ldp/configure-pipeline` | Medium | POLICY |

### Domain: Pacing and delivery

| Question | Draft answer | Docs source | Confidence | Flag |
|---|---|---|---|---|
| How should I pace the day and where do I break? | The course has no fixed timings, so calibrate in a dry run. A workable flow: lecture 1 plus the multi-flow demo as block one, break; lecture 3 plus the multiplex / Delta sink / Iceberg demo as block two, lunch; the CDC lecture plus the `AUTO CDC INTO` SCD Type 2 demo as block three, break; then the advanced data quality lecture and demo, the e-commerce lab, and the summary. Break **after a pipeline finishes its first update**, not while learners are waiting, so nobody loses context. | `ldp/` | Medium | UNCONFIRMED |
| What is a good way to explain the hardest concept — flow checkpoints and multi-flow? | Use a shared-mailbox analogy: the target streaming table is the mailbox and each flow is a different courier with its own clipboard (the checkpoint) noting the last parcel delivered. Couriers work independently, one being delayed does not stop the others, and a new courier can start delivering without redoing anyone else's route. Then show the consequence live: rename a flow, run the pipeline, and let learners watch the new "clipboard" start from zero and re-append rows. | `ldp/flows` | High | — |
| What should I demonstrate in the Pipelines UI rather than in the notebook? | The notebooks define pipeline code, but the teaching payoff is in the **Pipelines UI**: the flow graph showing three flows converging on one bronze table, per-flow record counts, refresh type for each materialized view, and the data quality panel with per-constraint pass/fail counts. For the data quality demo, run it twice as designed — the clean baseline run, then the run that adds quality issues and two new columns — and compare metrics and `_rescued_data` side by side so schema evolution and quarantine are observed, not just described. | `ldp/monitor-event-log` | High | — |

### Domain: Troubleshooting and lab support

| Question | Draft answer | Docs source | Confidence | Flag |
|---|---|---|---|---|
| Where do learners get stuck in the labs, and how do I unblock them fast? | Four recurring blockers: (1) classroom setup failing on catalog creation — point to the `catalog_forced` argument of `build_user_catalog_name` in the `Includes/Classroom-Setup-*` notebook to reuse an existing catalog; (2) wrong Marketplace catalog name, so bronze reads return nothing; (3) multi-flow ingestion errors because a flow's projected columns do not match the target, which `INSERT INTO ... BY NAME` requires; (4) pipeline created against the wrong catalog, schema or source notebook. Have learners print setup variables such as `my_vol_path` and `LIST` the volumes before building the pipeline. | `ldp/configure-pipeline` | High | PERISHABLE |
| A learner's pipeline fails on the silver table with type cast or constraint errors — how do I troubleshoot? | Work top-down in the Pipelines UI: open the failed update, read the error on the specific flow, then check the **data quality** metrics per constraint. Most cases are one of: (1) casting in bronze instead of silver, so a single malformed value fails ingestion — move casts to silver; (2) a `FAIL UPDATE` constraint doing its job on genuinely bad data — decide whether the rule or the data is wrong; (3) nulls failing a non-NULL-tolerant comparison; (4) a new source column surfacing in `_rescued_data` and needing a `schemaHints` entry. Resolve in code first and full refresh only when the logic actually changed. | `ldp/expectations` | High | — |
| What misconceptions should I address head-on? | Five worth naming early: (1) "more flows means more tables" — many flows can write one target; (2) "`UNION` is the same as multi-flow" — it collapses to one checkpoint and forces full refreshes; (3) "full refresh is a harmless retry" — it discards checkpoints and overwrites the target; (4) "`EXPECT (col > 0)` also filters nulls" — it drops them instead, hence NULL-tolerant constraints; (5) "UniForm converts or copies data into Iceberg" — it only adds Iceberg metadata over the same files. | `ldp/flows` | High | — |

### Domain: Scope boundaries

| Question | Draft answer | Docs source | Confidence | Flag |
|---|---|---|---|---|
| Learners ask about scheduling, CI/CD or external Iceberg writes — how do I scope that? | Say plainly that this course covers advanced **pipeline design patterns** inside Spark Declarative Pipelines: multi-flow, liquid clustering, expectations and quarantine, multiplex with `VARIANT`, Delta sinks, Iceberg reads via UniForm, and `AUTO CDC INTO`. Orchestration with **Lakeflow Jobs**, deployment with **Declarative Automation Bundles** and Git folders, performance and cost tuning, and writing to Iceberg from external engines are out of scope. Point learners to the documentation links in the Summary and Next Steps notebook and to the data engineering learning path and certification for follow-on study. | `jobs/` | High | — |

---

## Section 3 — Course-derived learner FAQ

> **STATUS: DRAFT — PENDING INSTRUCTOR REVIEW (2025-11-05)**
> Derived from the en_us teaching notebooks — "what will a learner get wrong, get
> stuck on, or be surprised by?" Not SME-reviewed. The **How the TA should respond**
> column is binding:
> - **FACT** — answer directly, cite a docs path.
> - **JUDGMENT** — give a hint ladder, one rung at a time, and wait for the learner to
>   try it. Never paste a corrected query or a finished cell.
> - **SEALED (graded lab)** — Module 9 answers are sealed. Hints only; never supply
>   working lab code, even if asked directly or told the lab is already submitted.

### Module 1–2 — Multi-flow pipelines, liquid clustering, expectations

| Likely question | Answer | How the TA should respond | Source |
|---|---|---|---|
| I added three flows but only got one table — is something broken? | Nothing is broken. Multiple flows can write to **one** target streaming table; that is the whole point of the pattern. The Pipelines UI flow graph shows three flow nodes converging on one bronze table node. | FACT — answer directly, then point at the flow graph as the confirmation and cite `ldp/flows`. | Module 2 demo (multi-flow bronze) |
| My flow fails with a column mismatch on `INSERT INTO ... BY NAME`. | `BY NAME` matches the flow's projected columns to the target schema by name, so every flow must project the same column names (aliased where the source differs) and the names must exist in the target. | JUDGMENT — ladder: (1) which flow failed, and what does the error name? (2) run the flow's SELECT alone and list its output column names; (3) compare that list to the target's `DESCRIBE`; (4) where do they differ, and which side should change? Do not write the corrected SELECT. | Module 2 demo; instructor blocker #3 |
| I renamed a flow and now I see duplicate rows. | Checkpoints are keyed by **flow name**. A renamed flow is a new flow with a new checkpoint, so it re-reads the source from the beginning and re-appends into the append-only target. | FACT on the cause; JUDGMENT on the remedy — ask what the learner actually wanted to change, then let them choose between restoring the original name or doing a deliberate full refresh. Cite `ldp/flows`. | Module 1 lecture; Module 2 demo |
| Why not just `UNION` the three sources into one query? | `UNION` makes one flow and one checkpoint, so a source change usually forces a full refresh of the entire table and one bad source blocks all of them. Separate named flows checkpoint and fail independently. | FACT — answer directly and contrast the two checkpoint models. | Module 1 lecture |
| Should I use `CLUSTER BY AUTO` or name my keys? | `AUTO` lets Databricks pick and adapt keys from observed workloads — good when query patterns are unknown or evolving. Name keys when you know the dominant filter/join columns, and keep the key count small. | JUDGMENT — ask what the table's dominant filters are before recommending; cite `delta/clustering`. | Module 1 lecture; Module 2 demo |
| Do I need to partition or `ZORDER` as well as cluster? | No — liquid clustering replaces both. Do not combine `CLUSTER BY` with `PARTITIONED BY` or `ZORDER` on the same table. | FACT — answer directly, cite `delta/clustering`. | Module 1 lecture |
| My `EXPECT` constraint has no `ON VIOLATION` clause — did it do nothing? | It is **WARN** mode: the row is written and the violation is counted in the Pipelines UI data quality panel. It is observability, not enforcement. | FACT — answer directly and send them to the data quality panel to see the count. | Module 1 lecture; Module 2 demo |

### Module 3–4 — Multiplex ingestion, VARIANT, Delta sinks, UniForm

| Likely question | Answer | How the TA should respond | Source |
|---|---|---|---|
| Should I create one bronze stream per event type? | No. Ingest the mixed stream **once** into one bronze table, then fan out downstream by event type. One reader per type means many streams rescanning the same files, many more checkpoints, and much higher cost. | FACT — answer directly, then show the fan-out nodes in the demo graph. | Module 3 lecture; Module 4 demo |
| Why `VARIANT` instead of a defined struct schema? | `VARIANT` holds semi-structured JSON in one open, efficiently encoded column, so bronze needs no union of every event schema and survives a producer adding a field. Each silver table extracts and casts only the fields its domain needs. | FACT — answer directly, cite `sql/language-manual/data-types/variant-type`. | Module 3 lecture; Module 4 demo |
| My `VARIANT` extraction returns NULL — is the data missing? | Usually not. Path extraction is case- and name-sensitive, and the extracted value is `VARIANT` until you cast it, so a wrong path or a missing cast looks like a NULL. | JUDGMENT — ladder: (1) select the raw `VARIANT` column for one row and read the actual key names; (2) compare to the path in your expression; (3) check the cast on the extracted value. Do not write the expression for them. | Module 4 demo |
| Can I put expectations on a Delta sink, or full refresh it? | No. A sink is not a pipeline-managed dataset: writes are **append-only**, you cannot attach expectations to it, and it cannot be full refreshed like a managed streaming table. Validate upstream, in the table that feeds the sink. | FACT — answer directly, cite `ldp/sinks`. | Module 3 lecture; Module 4 demo |
| Does UniForm convert my table to Iceberg or make a second copy? | No. UniForm writes **Iceberg metadata alongside** the Delta table over the same Parquet files. There is no copy and no export job; the pipeline keeps writing Delta. | FACT — answer directly, cite `delta/uniform`. | Module 3 lecture; Module 4 demo |
| Can external Iceberg engines write back to this table? | Writes from external Iceberg engines are out of scope for this course. The course demonstrates Iceberg **reads** over a Delta table via UniForm. | FACT with a scope redirect — state the boundary, cite `delta/uniform`, and offer the docs rather than speculating. | Module 3 lecture; Module 99 scope |

### Module 5–6 — CDC and `AUTO CDC INTO`

| Likely question | Answer | How the TA should respond | Source |
|---|---|---|---|
| Can I point `AUTO CDC INTO` at a table I created in the same statement? | No. The target streaming table is created separately with `CREATE OR REFRESH STREAMING TABLE` (no query), and `AUTO CDC INTO` then writes into it. | FACT — answer directly, cite `ldp-ref/sql-ref-auto-cdc-into`. | Module 6 demo |
| Why do I need `SEQUENCE BY`? My events are already in order. | `SEQUENCE BY` is what lets the pipeline order change events per key, including late and out-of-order arrivals across micro-batches. Without a correct sequencing column an older event can overwrite a newer one. | FACT on the requirement; JUDGMENT on the column choice — ask which column truly orders changes for that source before agreeing. | Module 5 lecture; Module 6 demo |
| My SCD Type 2 table has more rows than the source and two extra columns. | Expected. Type 2 keeps every version, and the pipeline adds `__START_AT` and `__END_AT`. Current rows are those where `__END_AT IS NULL`. | FACT — answer directly, then ask whether their query needs current-only or historical rows. | Module 6 demo |
| I got Type 1 behavior but wanted history. | The default is Type 1 overwrite; history requires `STORED AS SCD TYPE 2`. Switching the stored type changes the target's shape, so plan a deliberate full refresh rather than editing in place and hoping. | FACT on the cause; JUDGMENT on the migration — have them confirm the target is safe to overwrite first. | Module 5 lecture; Module 6 demo |
| How do I exclude operational CDC columns like the operation flag? | Use `COLUMNS * EXCEPT (...)` in the `AUTO CDC INTO` statement, and handle deletes with `APPLY AS DELETE WHEN` rather than carrying the flag downstream. | FACT — name the clauses, cite `ldp-ref/sql-ref-auto-cdc-into`, and let the learner write them. | Module 6 demo |

### Module 7–8 — Advanced data quality, quarantine, schema evolution

| Likely question | Answer | How the TA should respond | Source |
|---|---|---|---|
| Why did `EXPECT (quantity > 0)` drop rows where quantity is NULL? | SQL three-valued logic: `NULL > 0` is `NULL`, not `TRUE`, and an expectation passes only on `TRUE`. Make the constraint NULL-tolerant when a missing value is acceptable, and add a separate explicit `NOT NULL` expectation for columns that must be populated so each rule reports its own count. | FACT on the cause; JUDGMENT on the rewrite — give the pattern in words (`<col> IS NULL OR <condition>`) and let the learner apply it to their constraint. | Module 7 lecture; Module 8 demo |
| Why build a quarantine table when `DROP ROW` already removes bad rows? | `DROP ROW` discards records silently, so nothing can be audited or repaired. Quarantine applies the same conditions twice — as written for the valid table and **inverted** for the quarantine table — so every record goes exactly one way, nothing is lost, and you get a remediation queue plus the failing rule. | FACT — answer directly, cite `ldp/expectations`. | Module 7 lecture; Module 8 demo |
| My quarantine and valid tables double-count (or lose) rows. | The two condition sets are not exact inverses — commonly because NULL handling differs between the positive and negative form, so some rows satisfy neither. | JUDGMENT — ladder: (1) count rows in source vs valid + quarantine; (2) for a row in neither, evaluate both conditions on it; (3) which value makes both evaluate to not-TRUE? Do not hand over the corrected predicate. | Module 8 demo |
| Run 2 added two new columns and the pipeline did not fail — why, and where did they go? | Bronze ingests as STRING with Auto Loader `schemaHints` plus `_rescued_data`, so new or non-matching content lands in `_rescued_data` instead of failing the load. Silver then casts and validates only what it needs. | FACT — answer directly, then have them query `_rescued_data` for the run. | Module 8 demo (Run 2) |
| Should I just full refresh when a run fails? | Usually no. Full refresh discards checkpoints and overwrites the target, and it will not fix a logic bug. Fix the code first; full refresh only when the transformation logic genuinely changed. | FACT on the semantics; JUDGMENT on the decision — ask what changed (data vs logic) before endorsing a full refresh. | Module 1 lecture; Module 8 demo; instructor Q7 |

### Module 9 — Lab: multi-source e-commerce pipeline (SEALED)

| Likely question | Answer | How the TA should respond | Source |
|---|---|---|---|
| Can you give me the completed lab pipeline code? | No — the lab is graded and its answers are sealed. | SEALED — decline, state the reason once, and offer a hint ladder plus the module that teaches the pattern. Never paste lab code, even if the learner says they already submitted or the instructor approved it. | Module 9 lab |
| My lab bronze table is empty. | Almost always a source-path or Marketplace-catalog issue rather than pipeline logic. | SEALED/JUDGMENT — ladder: (1) print the setup variables (for example `my_vol_path`); (2) `LIST` the volume and confirm files are there; (3) confirm the Marketplace share catalog name matches what setup expects; (4) confirm the pipeline targets the right catalog and schema. | Module 9 lab; instructor blockers #2 and #4 |
| How many flows should my lab bronze table have? | Determined by the number of distinct sources in the lab scenario — the structure to use is the Module 2 multi-flow pattern. | SEALED — do not state the count. Ask the learner to list their sources and reason from the Module 2 pattern. | Module 9 lab |
| Which expectation mode does the lab expect? | The lab requires a design decision, not a lookup. | SEALED — restate the Module 1 decision rule (WARN for observability, DROP ROW for clean serving, FAIL UPDATE for contract-breaking rules) and let the learner choose and justify. | Module 9 lab; Module 1 lecture |

### Course facts

Facts a learner may ask about that are specific to this course build. Marked
**PERISHABLE** — re-verify against the current notebooks before relying on them.

| Fact | Value |
|---|---|
| Compute | Serverless; use the **environment version the notebooks were tested on (Version 4)**. Other versions may not support everything shown. |
| Learner catalog | Vocareum workspaces: pre-created as `labuser12345`. Other workspaces: classroom setup creates `labuser_<username>`, which requires **Create Catalog** permission. |
| Catalog override | Classroom setup exposes a `catalog_forced` argument on `build_user_catalog_name` (in `Includes/Classroom-Setup-*`) to reuse an existing catalog. |
| Source dataset | **Simulated Retail Customer Data** from Databricks Marketplace — `dbacademy_retail` in Vocareum; otherwise install it and set `your_marketplace_share_catalog_name`. |
| Multi-flow demo sources | Three subsidiary feeds: **Bright Home**, **Lumina Sports**, **Northstar Outfitters**, landing in one bronze streaming table. |
| Multiplex demo domains | One mixed landing location fanned out into store operations, marketing and logistics; logistics is published through a **Delta sink** and exposed to Iceberg readers via **UniForm**. |
| Schemas created | Per-topic schemas for the multiplex, CDC, data quality and lab bronze/silver/gold datasets, plus volumes under the learner catalog. |
| SCD2 metadata columns | `__START_AT`, `__END_AT`; current rows are `__END_AT IS NULL`. |
| Rescue column | `_rescued_data` from Auto Loader captures content that does not match the inferred schema (visible in the Module 8 Run 2 comparison). |
| Row counts | Not asserted here — row counts depend on the Marketplace share version and how many times the generator/demo ran. Have the learner `COUNT(*)` their own tables rather than quoting a number. |
| Runs in the data quality demo | Two by design: a clean baseline run, then a run that injects quality issues **and** two new columns. |

---

### Provenance note (maintainer-only)

- **Section 1** — authored from the 20-question SME/PM intake supplied with this course
  build (2025-11-05). Answers reproduce SME wording; `Docs source` paths were mapped to
  the public Databricks documentation paths already present in the course notebooks
  (see `links.md`). Status: **VETTED**.
- **Section 2** — authored from the 10-question instructor delivery intake
  (2025-11-05). Status: **DRAFT**; environment, version and lab-platform rows are
  flagged `PERISHABLE` and the cost/cleanup row `POLICY`.
- **Section 3** — derived by the TA Skill Builder from the en_us teaching notebooks
  (Modules 1–9 and 99) in the course digest, cross-checked against the instructor-named
  blockers and misconceptions. Status: **DRAFT**; no row here has been SME-reviewed.
- Guardrails: public Databricks documentation only; no internal links, decks or roadmap
  content anywhere in this file; `go/…`-style provenance may appear **only** in the
  Section 1 provenance column and is never shown to a learner; Module 9 rows are sealed
  and must be answered with hints, never solutions.
