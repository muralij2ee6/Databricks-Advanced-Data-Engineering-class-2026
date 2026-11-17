# Best Practices — Advanced Techniques With Apache Spark Declarative Pipeline

> STATUS: VETTED (auto-generated from the course structure on 2025-06-16) — these practices are
> drawn from the patterns the course notebooks actually demonstrate and from the SME FAQ. VET
> before use in any customer-facing or assessment context.

**How the TA uses this file.** When a learner asks "what's the right way to…", "is this a good
idea?", or shows code that works but is fragile, answer with the *practice plus its reason* — the
reason is what transfers to their own pipelines. Each bullet below is written as
`<Practice>. Reason: <why>. Prevents: <failure>. (taught: <source>)`.

Three rules for how these are delivered:

1. **Never use a practice to hand over a lab answer.** For Module 9 (the e-commerce lab) and any
   graded exercise, state the practice as a *principle* and let the learner apply it. Do not write
   the flow, the constraint, or the `AUTO CDC INTO` statement for them.
2. **Reason before syntax.** A learner who knows *why* each flow gets its own checkpoint can
   design a multi-flow pipeline; one who only has the snippet cannot.
3. **Practices are advisory, not policy.** Where a practice touches cost, cleanup, or workspace
   permissions, defer to the learner's own platform team and to the public docs
   (see `links.md`) rather than asserting what their organization allows.

---

## 1. Flows and multi-flow design

- **Use the default flow for the simple case and switch to explicit flows only when one target needs more than one writer.** Reason: `CREATE OR REFRESH STREAMING TABLE … AS SELECT` already creates a single default flow that owns the table, so explicit flows add naming and wiring you only need for fan-in. Prevents: unnecessary boilerplate, and the confusion of a table that has both an inline query and added flows. (taught: Module 1 lecture; Module 2 demo)

- **Declare the target streaming table *without* a query when you intend to attach multiple named flows to it.** Reason: explicit flows own the writes; a query on the table definition would create a competing default flow for the same target. Prevents: "table already has a flow" style failures and ambiguity about which writer owns the target. (taught: Module 2 demo, multi-flow bronze table)

- **Prefer multi-flow fan-in over a `UNION` of sources when the sources are independent feeds.** Reason: each named flow gets its own checkpoint and progresses on its own, so sources can be added, paused, or recovered individually. Prevents: one `UNION` query collapsing to a single checkpoint, where adding or changing a source forces a full refresh of the whole table. (taught: Module 1 lecture; Module 2 demo with Bright Home, Lumina Sports, Northstar Outfitters)

- **Give each flow a stable, descriptive name tied to its source (for example one name per subsidiary feed) and treat the name as part of the contract.** Reason: the checkpoint is keyed by flow name, so the name is what lets a flow resume where it stopped. Prevents: a rename silently creating a brand-new checkpoint that re-reads the source from the beginning and duplicates rows in an append target. (taught: Module 1 lecture; Module 2 demo)

- **Change transformation logic with a deliberate full refresh, never by renaming a flow.** Reason: a full refresh is the supported, visible way to reprocess; a rename produces the same reprocessing as an accident, with no record of intent. Prevents: duplicate rows plus an unexplained jump in record counts that is hard to diagnose after the fact. (taught: Module 1 lecture; SME FAQ Q4)

- **Add a new source feed by adding a flow, not by editing the existing flows' query.** Reason: existing flows keep their checkpoints and their history, and only the new flow does any backfill. Prevents: reprocessing the full history of established feeds just to onboard one new subsidiary. (taught: Module 2 demo)

- **Project every flow to the same column list, order, and names before `INSERT INTO … BY NAME`.** Reason: `BY NAME` matches on column name, so each flow must alias source-specific names to the target's names. Prevents: the single most common lab failure — an ingestion error because one flow's projected columns do not match the target schema. (taught: Module 2 demo; instructor FAQ Q3)

- **Treat the flow graph in the Pipelines UI as the primary design review artifact.** Reason: the graph shows at a glance whether three flows really converge on one bronze table and what each flow's record count is. Prevents: shipping a pipeline whose topology differs from the one the author intended. (taught: Module 2 demo; instructor FAQ Q8)

---

## 2. Medallion layering, casting, and schema resilience

- **Keep bronze permissive: ingest as `STRING` (or as the raw payload) and defer type casting to silver.** Reason: a layer whose job is "capture everything" must not be able to reject a file because one value looks wrong. Prevents: a single malformed value failing ingestion for an entire file, and the learner-visible symptom of a bronze table that will not load at all. (taught: Module 1 lecture; Module 8 demo; SME FAQ Q5, Q18)

- **Do the type casting, business cleanup, and validation together in silver.** Reason: silver is where expectations can see the cast result and report violations per constraint. Prevents: casts that fail invisibly in bronze with no data quality metric to explain what was lost. (taught: Module 8 demo; instructor FAQ Q7)

- **Use Auto Loader `schemaHints` to pin the types you genuinely care about, and leave the rest inferred.** Reason: hints give you determinism on the important columns without surrendering the flexibility that lets new columns land automatically. Prevents: either extreme — a fully hand-written schema that breaks on every producer change, or a fully inferred schema that silently changes a column's type between runs. (taught: Module 8 demo; SME FAQ Q18)

- **Always keep and actively monitor the `_rescued_data` column.** Reason: it is where anything that did not match the inferred schema is captured, which makes schema drift observable instead of lossy. Prevents: new or malformed source fields disappearing without a trace, and the "where did my column go?" investigation later. (taught: Module 8 demo, Run 2 adds two new columns)

- **Standardize column names and ordering per flow in bronze, not downstream.** Reason: reconciling naming at the point of ingestion keeps each source's quirks local to its own flow. Prevents: a sprawling `CASE`/alias block in silver that has to know about every source's historical naming. (taught: Module 2 demo; SME FAQ Q5)

- **Verify the source location before building the pipeline — print the setup variables and `LIST` the volume.** Reason: a bronze table reading the wrong volume or the wrong Marketplace catalog returns zero rows and looks like a logic bug. Prevents: twenty minutes of debugging a correct pipeline that is simply pointed at an empty path. (taught: classroom setup; instructor FAQ Q3)

---

## 3. Expectations and data quality

- **Choose the violation mode from the business consequence: `WARN` for observability, `DROP ROW` for clean serving tables, `FAIL UPDATE` only for contract-breaking rules.** Reason: the three modes differ in what happens to the row and to the update, not in how the condition is written. Prevents: a nice-to-have rule stopping a production update, or a contract-breaking rule being quietly tolerated. (taught: Module 7 lecture; SME FAQ Q7)

- **Reserve `ON VIOLATION FAIL UPDATE` for rules like a missing primary key, where continuing would corrupt downstream state.** Reason: failing the update is the strongest signal available and should mean "this data is unusable". Prevents: alert fatigue and learners disabling the rule because it fires on ordinary dirty data. (taught: Module 7 lecture)

- **Write NULL-tolerant constraints whenever a missing value is acceptable — for example `EXPECT (quantity IS NULL OR quantity > 0)`.** Reason: SQL three-valued logic makes a comparison against `NULL` evaluate to `NULL`, and an expectation passes only on `TRUE`. Prevents: silently dropping or flagging every row where the column is simply absent. (taught: Module 7 lecture; Module 8 demo; SME FAQ Q17)

- **Declare a separate, explicit `NOT NULL`-style expectation for columns that truly must be populated.** Reason: one rule per concern means each gets its own violation count in the data quality panel. Prevents: a single compound constraint whose failure count tells you nothing about *which* problem occurred. (taught: Module 7 lecture; SME FAQ Q17)

- **Give every constraint a specific, human-readable name.** Reason: the constraint name is what appears in the Pipelines UI data quality metrics and in the quarantine table's failure reason. Prevents: a metrics panel full of indistinguishable rules that nobody can act on. (taught: Module 7 lecture; Module 8 demo)

- **Use the quarantine pattern instead of `DROP ROW` when bad records need to be audited or repaired.** Reason: applying the same condition set twice — as written for the valid table and with inverse logic for the quarantine table — routes each record exactly one way and loses nothing. Prevents: discarded rows that cannot be investigated, re-driven, or reported on. (taught: Module 7 lecture; Module 8 demo; SME FAQ Q19)

- **Keep the quarantine condition as the strict logical inverse of the valid-table condition, and watch NULL handling on both sides.** Reason: if neither branch is NULL-tolerant, NULL rows match *neither* branch and vanish from both tables. Prevents: a silent record leak in exactly the pattern designed to prevent record loss. (taught: Module 8 demo)

- **Record which rule failed in the quarantine table so it functions as a remediation queue.** Reason: a quarantine table without a failure reason is just a pile of rejected rows. Prevents: a manual re-derivation of why each record was quarantined. (taught: Module 8 demo; SME FAQ Q19)

- **Run a data quality change twice — a clean baseline, then the run with the new issues — and compare the metrics side by side.** Reason: the value of expectations is in the delta between runs, which is where schema evolution and quarantine become observable rather than described. Prevents: learners accepting the pattern on faith without ever seeing a violation count move. (taught: Module 8 demo, two-run design; instructor FAQ Q8)

---

## 4. Multiplex ingestion, VARIANT, and fan-out

- **Ingest a multiplexed landing location once into a single bronze streaming table, then fan out by event type downstream.** Reason: one reader per event type would mean many concurrent streams scanning the same files, many more checkpoints, and much higher cost. Prevents: a pipeline whose cost scales with the number of event types rather than with the volume of data. (taught: Module 3 lecture; Module 4 demo; SME FAQ Q10)

- **Store the mixed semi-structured payload as `VARIANT` in bronze rather than as a union-of-everything struct.** Reason: `VARIANT` holds open JSON in one efficiently encoded column, so bronze does not break when a producer adds a field. Prevents: a wide, mostly-null struct schema that needs a code change for every new producer field. (taught: Module 3 lecture; Module 4 demo; SME FAQ Q11)

- **Extract and cast only the fields each domain's silver table needs, at that table.** Reason: it keeps fan-out logic local, so the store-operations table does not have to know about logistics fields. Prevents: one central extraction layer that every team must coordinate changes through. (taught: Module 4 demo)

- **Filter by event type in the downstream streaming table, not in the bronze reader.** Reason: bronze's contract is "the mixed stream as-is"; filtering there would make the raw layer lossy and force a full refresh whenever the filter changed. Prevents: discovering that an event type was never captured and that the source files have since aged out. (taught: Module 3 lecture; Module 4 demo)

- **Validate `VARIANT` extraction results explicitly rather than assuming a path exists.** Reason: an absent path yields a NULL, which looks identical to a present-but-empty field. Prevents: a silver table full of nulls that is mistaken for a source data problem. (taught: Module 4 demo; Module 7 lecture)

---

## 5. Sinks, UniForm, and table layout

- **Default to pipeline-managed streaming tables; use a sink only when the target is not owned by the pipeline.** Reason: managed tables appear in lineage, support expectations, and can be full-refreshed — a sink gives up all three. Prevents: losing data quality enforcement and refresh control on a target that could simply have been a table. (taught: Module 3 lecture; Module 4 demo; SME FAQ Q12)

- **Treat sink writes as append-only and design for that up front.** Reason: a sink is not a pipeline-managed dataset, so there is no full refresh and no expectation enforcement to fall back on. Prevents: building a correction strategy that assumes you can reprocess the target. (taught: Module 4 demo; SME FAQ Q12)

- **Expose a Delta table to Iceberg readers with UniForm instead of exporting a second copy.** Reason: UniForm writes Iceberg metadata alongside the same underlying Parquet files, so there is one copy of the data and no export job to schedule or reconcile. Prevents: a duplicate dataset that drifts from the Delta source and doubles storage. (taught: Module 3 lecture; Module 4 demo; SME FAQ Q13)

- **Use liquid clustering in place of partitioning and `ZORDER` on new tables.** Reason: the layout is adaptive and the clustering keys can be changed without rewriting the table. Prevents: locking a table into a partition scheme that becomes wrong as query patterns change. (taught: Module 1 lecture; Module 2 demo)

- **Use `CLUSTER BY AUTO` when query patterns are unknown or still evolving.** Reason: Databricks chooses and adjusts the keys from observed workloads, which beats a guess made before any queries exist. Prevents: hand-picked keys that reflect the designer's assumptions rather than real filters. (taught: Module 1 lecture; SME FAQ Q6)

- **Name clustering keys explicitly — and keep the list short — when you already know the dominant filter or join columns.** Reason: a small, well-chosen key set such as a date plus a store or customer identifier gives the clearest data skipping. Prevents: diluting the layout benefit across too many keys. (taught: Module 1 lecture; SME FAQ Q6)

---

## 6. CDC and slowly changing dimensions

- **Choose SCD Type 1 when only the current state has business value, and Type 2 when history does.** Reason: Type 1 overwrites and stays compact; Type 2 keeps every version so point-in-time and audit questions can be answered. Prevents: either an unanswerable "which region was this customer in last quarter?" or the storage and query complexity of history nobody needs. (taught: Module 5 lecture; SME FAQ Q14)

- **Prefer `AUTO CDC INTO` over hand-written `MERGE` logic for applying change events.** Reason: the declarative form handles ordering, deletes, and SCD bookkeeping that hand-rolled merges get wrong. Prevents: subtly incorrect history from a MERGE that does not account for out-of-order events. (taught: Module 5 lecture; Module 6 demo; SME FAQ Q15)

- **Always set `SEQUENCE BY` to a genuine ordering column such as an event timestamp or version.** Reason: it is what lets late or out-of-order change events land in the correct position instead of overwriting newer state. Prevents: a stale update clobbering the current row, with no error to signal it. (taught: Module 5 lecture; Module 6 demo; SME FAQ Q15)

- **Create the target streaming table separately with `CREATE OR REFRESH STREAMING TABLE` before pointing `AUTO CDC INTO` at it.** Reason: the CDC statement applies changes to an existing target; it is not a table definition. Prevents: the common "target does not exist" failure in the CDC demo and lab. (taught: Module 6 demo; SME FAQ Q15)

- **Use `COLUMNS * EXCEPT (…)` to strip operational CDC columns from the target.** Reason: the operation and sequencing columns are instructions to the pipeline, not attributes of the business entity. Prevents: downstream consumers filtering on, or accidentally reporting, CDC plumbing columns. (taught: Module 6 demo)

- **Declare delete handling explicitly with `APPLY AS DELETE WHEN` when the source emits delete events.** Reason: without it, a delete event is treated as an ordinary upsert and the row stays current. Prevents: records that were deleted at the source silently remaining active in the target. (taught: Module 5 lecture; Module 6 demo)

- **Filter SCD Type 2 current state with `__END_AT IS NULL`, and make every downstream query state whether it wants current or historical rows.** Reason: a Type 2 table holds multiple rows per key by design, so an unqualified read multiplies records. Prevents: inflated counts and duplicated aggregates in gold tables built over SCD2 sources. (taught: Module 6 demo, gold current-customers view; SME FAQ Q16)

- **Use `max_by` (or an equivalent latest-record-per-key construct) rather than a plain aggregate when you need an attribute from the most recent change per key.** Reason: it picks the value associated with the maximum sequence rather than the maximum value of the attribute itself. Prevents: a gold view that reports the largest value ever seen instead of the current one. (taught: Module 6 demo)

---

## 7. Materialized views and refresh behavior

- **Keep materialized view queries expressible incrementally — selects, filters, joins, and supported aggregations over Delta sources.** Reason: incremental refresh is a property of the query, not a setting you can turn on. Prevents: a gold layer that silently full-recomputes on every update and costs far more than expected. (taught: Module 1 lecture; SME FAQ Q8)

- **Check the reported refresh type for each materialized view in the Pipelines UI after a change.** Reason: it is the only reliable confirmation that a query you believe is incremental actually refreshed incrementally. Prevents: a non-deterministic expression or unsupported function quietly downgrading the view to full recompute. (taught: Module 1 lecture; instructor FAQ Q8; SME FAQ Q8)

- **Avoid non-deterministic expressions in materialized view definitions.** Reason: they make incremental maintenance impossible, so the engine must recompute from scratch. Prevents: an unexplained cost and duration increase that is hard to attribute to a one-line query change. (taught: SME FAQ Q8)

---

## 8. Governance, environment, and cost hygiene

- **Apply Unity Catalog tags to bronze, silver, and gold objects as part of pipeline development, not as a later cleanup project.** Reason: tags by medallion layer, owner, subsidiary, or sensitivity make pipeline output discoverable and filterable in Catalog Explorer from day one. Prevents: a governance backfill across hundreds of tables once nobody remembers who owns what. (taught: Module 2 demo; SME FAQ Q9)

- **Remember that tags are metadata only — never use them as a substitute for access control.** Reason: tags do not change query results, performance, or permissions. Prevents: a false sense of protection on sensitive columns that are still readable. (taught: SME FAQ Q9)

- **Run these notebooks only in a development or sandbox workspace.** Reason: they create catalogs, schemas, volumes, and pipelines. Prevents: classroom artifacts appearing in a production catalog. (taught: course overview; instructor FAQ Q9)

- **Pin the serverless environment version the notebooks were tested on (Version 4 for this course) before the first demo.** Reason: other versions may not support everything the notebooks demonstrate. Prevents: a feature-not-available error that looks like a code bug. (taught: course overview; instructor FAQ Q2 — PERISHABLE, re-verify against the current course release)

- **Stop a pipeline update once the graph and metrics have been reviewed, and do not leave continuous mode on in a classroom or sandbox.** Reason: serverless pipelines keep incurring cost while an update runs. Prevents: an idle pipeline accumulating cost overnight with nobody watching it. (taught: instructor FAQ Q9)

- **Treat full refresh as an expensive, deliberate operation — resolve problems in code first.** Reason: a full refresh discards the checkpoint, reprocesses all source data, and overwrites the target. Prevents: using full refresh as a reflexive retry and paying to reprocess history that was already correct. (taught: Module 1 lecture; instructor FAQ Q7, Q9; SME FAQ Q4)

- **Delete pipelines and drop the course schemas at the end of the course.** Reason: the multiplex, CDC, data quality, and lab bronze/silver/gold schemas and their pipelines persist until removed. Prevents: orphaned pipelines and storage in a shared workspace. (taught: instructor FAQ Q9 — in a managed lab environment the sandbox is reclaimed automatically)

- **Troubleshoot top-down in the Pipelines UI: failed update → the specific failed flow → the error → the per-constraint data quality metrics.** Reason: the UI localizes the failure to one flow and one rule far faster than re-reading notebook code. Prevents: rewriting a working transformation because the actual cause was a constraint or a source path. (taught: Module 2, 4, 6, 8 demos; instructor FAQ Q7)

---

## Scope reminder

This course teaches advanced *design patterns inside* Spark Declarative Pipelines. Orchestration and
scheduling with **Lakeflow Jobs**, deployment with **Declarative Automation Bundles** and Git
folders, performance and cost tuning, Structured Streaming APIs outside pipelines, Delta Lake
internals, and writing to Iceberg from external engines are **out of scope** — acknowledge the
question, give the one-line principle if there is one, and point to the adjacent documentation in
`links.md` Section B rather than improvising depth the course does not cover.

---

*Maintenance: practices sourced from the Module 1–9 lecture and demo notebooks, the Summary and
Next Steps notebook, the vetted SME FAQ, and instructor delivery notes. Re-verify the environment
version, catalog naming, and cleanup items against the current course release each term. Public
Databricks documentation only — see `links.md`.*
