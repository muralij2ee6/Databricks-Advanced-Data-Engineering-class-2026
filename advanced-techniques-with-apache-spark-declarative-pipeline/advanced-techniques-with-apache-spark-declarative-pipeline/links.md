# Course Links — Advanced Techniques With Apache Spark Declarative Pipeline

> STATUS: VETTED (extracted from the en_us notebooks and link-checked on 2026-06-11)

## How to build a URL from this file

This file stores **doc paths**, not full URLs, so the TA can append a path to the
prefix for the learner's cloud. The classroom cloud for this course is **AWS**;
use the matching prefix if a learner says they are on Azure or GCP.

| Cloud | Prefix | Example (`ldp/flows`) |
|---|---|---|
| AWS (classroom default) | `https://docs.databricks.com/aws/en/` | `https://docs.databricks.com/aws/en/ldp/flows` |
| Azure | `https://learn.microsoft.com/en-us/azure/databricks/` | `https://learn.microsoft.com/en-us/azure/databricks/ldp/flows` |
| GCP | `https://docs.databricks.com/gcp/en/` | `https://docs.databricks.com/gcp/en/ldp/flows` |

Section C entries are **full URLs** (blogs, product pages, training catalog) and
are cloud-independent — do not prepend a cloud prefix to them.

---

## Section A — Links in the course notebooks

These paths appear in, or are directly anchored to, the teaching notebooks.
"Taught in" names the module where the TA should send the learner for context.

### A1. Spark Declarative Pipelines — core concepts and flows

| Topic | Path | Taught in |
|---|---|---|
| Declarative pipelines overview (product docs home) | `ldp/` | 0 Course Overview, 99 Summary |
| Pipeline concepts and terminology | `ldp/concepts` | 1 Lecture — Multi-flow pipelines |
| Flows: query + target, processing modes | `ldp/flows` | 1 Lecture, 2 Demo |
| Append flows (`CREATE FLOW` / `@dp.append_flow`) | `ldp/append-flow` | 1 Lecture, 2 Demo, 4 Demo |
| Streaming tables in pipelines | `ldp/streaming-tables` | 1 Lecture, 2 Demo |
| Materialized views in pipelines | `ldp/materialized-views` | 1 Lecture, 2 Demo, 6 Demo |
| Develop pipeline code (SQL and Python) | `ldp/develop-pipelines` | 2 Demo, 9 Lab |
| Configure a pipeline (catalog, schema, source) | `ldp/configure-pipeline` | 2 Demo, 9 Lab |
| Pipeline updates and full refresh semantics | `ldp/updates` | 1 Lecture, 2 Demo |
| SQL language reference for pipelines | `ldp/sql-ref` | 2 Demo, 6 Demo, 8 Demo |
| Python language reference for pipelines | `ldp/python-ref` | 4 Demo |
| Use Unity Catalog with pipelines | `ldp/unity-catalog` | 2 Demo, 9 Lab |

### A2. Data quality — expectations and quarantine

| Topic | Path | Taught in |
|---|---|---|
| Manage data quality with pipeline expectations | `ldp/expectations` | 1 Lecture, 7 Lecture, 8 Demo |
| Expectation patterns (quarantine, portable rules) | `ldp/expectation-patterns` | 7 Lecture, 8 Demo |
| Pipeline observability and data quality metrics | `ldp/observability` | 2 Demo, 8 Demo |
| Pipeline event log | `ldp/event-log` | 8 Demo |

### A3. CDC and slowly changing dimensions

| Topic | Path | Taught in |
|---|---|---|
| `AUTO CDC INTO` — apply change data capture | `ldp/auto-cdc` | 5 Lecture, 6 Demo |
| CDC concepts and SCD Type 1 vs Type 2 in pipelines | `ldp/cdc` | 5 Lecture, 6 Demo |
| Delta change data feed (upstream change sources) | `delta/delta-change-data-feed` | 5 Lecture |

### A4. Ingestion — Auto Loader, multiplex, VARIANT

| Topic | Path | Taught in |
|---|---|---|
| Auto Loader overview | `ingestion/cloud-object-storage/auto-loader/` | 2 Demo, 4 Demo, 8 Demo |
| Auto Loader schema inference, `schemaHints`, `_rescued_data` | `ingestion/cloud-object-storage/auto-loader/schema` | 7 Lecture, 8 Demo |
| Load data with declarative pipelines | `ldp/load` | 2 Demo, 4 Demo |
| `VARIANT` data type | `sql/language-manual/data-types/variant-type` | 3 Lecture, 4 Demo |
| Query semi-structured / variant data | `semi-structured/variant` | 4 Demo |
| Unity Catalog volumes (course source files) | `volumes/` | 2 Demo, 4 Demo, 9 Lab |

### A5. Sinks, Iceberg interoperability, table layout

| Topic | Path | Taught in |
|---|---|---|
| Pipeline sinks (`dp.create_sink`) | `ldp/sinks` | 3 Lecture, 4 Demo |
| Delta UniForm — read Delta tables as Iceberg | `delta/uniform` | 3 Lecture, 4 Demo |
| Iceberg support and Iceberg v3 features | `iceberg/iceberg-v3` | 3 Lecture, 4 Demo |
| Liquid clustering (`CLUSTER BY`, `CLUSTER BY AUTO`) | `delta/clustering` | 1 Lecture, 2 Demo |

### A6. Governance and environment

| Topic | Path | Taught in |
|---|---|---|
| Unity Catalog tags on catalogs, schemas, tables, columns | `data-governance/unity-catalog/tags` | 2 Demo |
| Unity Catalog overview | `data-governance/unity-catalog/` | 0 Course Overview |
| Serverless compute release notes / environment versions | `release-notes/serverless/` | 0 Course Overview (Version 4 requirement) |
| Databricks Marketplace — install a data share | `marketplace/` | 0 Course Overview (Simulated Retail Customer Data) |

---

## Section B — Adjacent public docs (not in the notebooks, safe to offer)

Use these when a learner asks a reasonable follow-on question that the course
deliberately does not cover. Always say the topic is out of scope first, then link.

| Topic | Path | Why a learner asks |
|---|---|---|
| Lakeflow Jobs — orchestration and scheduling | `jobs/` | "How do I run this pipeline nightly?" |
| Trigger a pipeline from a job task | `jobs/pipeline-task` | Scheduling the course pipelines |
| Databricks Asset Bundles / declarative deployment | `dev-tools/bundles/` | CI/CD, promoting pipelines across workspaces |
| Git folders in the workspace | `repos/` | Source-controlling pipeline notebooks |
| Delta Lake table optimization and `OPTIMIZE` | `delta/optimize` | "Do I still need OPTIMIZE with liquid clustering?" |
| Delta Lake table properties | `delta/table-properties` | Enabling UniForm as a table property |
| Structured Streaming concepts (outside pipelines) | `structured-streaming/` | Checkpoints, micro-batch, watermarks |
| Compute requirements for serverless pipelines | `compute/serverless/` | Environment version and dependency questions |
| Catalog Explorer — search and filter by tag | `catalog-explorer/` | Finding tagged medallion objects |
| SQL `max_by` and aggregate functions | `sql/language-manual/sql-ref-functions-builtin` | Gold SCD2 "latest attribute per key" view |
| Data quality monitoring (Lakehouse Monitoring) | `lakehouse-monitoring/` | "Can I alert on expectation failures?" |

---

## Section C — Blogs, product pages, and training (full URLs)

| Resource | URL | Use for |
|---|---|---|
| Spark Declarative Pipelines product page | https://www.databricks.com/product/data-engineering | Positioning / "what is SDP" |
| Apache Spark Declarative Pipelines (Apache Spark project) | https://spark.apache.org/ | Open-source context for the declarative API |
| Databricks blog — engineering and product announcements | https://www.databricks.com/blog | Feature announcements, UniForm/Iceberg posts |
| Databricks training and certification catalog | https://www.databricks.com/learn/training/home | Follow-on courses, the data engineering learning path, next-step course selection (99 Summary) |
| Databricks documentation home | https://docs.databricks.com/ | Fallback entry point when the cloud is unknown |

---

## Link guardrails

- **Public Databricks sources only.** Databricks documentation, `databricks.com`
  product/blog/training pages, and `spark.apache.org`. Nothing else.
- **No internal sources, ever.** No internal shortlinks or redirector URLs, internal
  wikis, Confluence, Google Docs, decks, roadmap or enablement material, field-only
  assets, or unreleased feature notes in any learner-facing answer. Internal
  provenance tags live only in the maintainer-only column of `faq.md` Section 1 and
  are never shown to a learner.
- **No competitor or third-party sources.** Do not link Snowflake, AWS/GCP/Azure
  native-service docs, Stack Overflow, Medium, GitHub gists, or vendor blogs — even
  if they answer the question better. If public Databricks docs do not cover it,
  say so and hand the learner to the instructor.
- **Never invent a URL.** Only emit a path that appears in this file. If a learner
  needs something not listed, describe the doc by name ("search the Databricks docs
  for *pipeline expectations*") rather than guessing a path or a deep anchor.
- **Match the learner's cloud.** Default to the AWS prefix for this classroom;
  switch prefixes only when the learner states a different cloud. Do not mix
  prefixes within one answer.
- **Course-version drift.** `ldp/*` paths correspond to the declarative-pipelines
  docs the notebooks reference. If a path 404s for a learner, point them at
  `ldp/` and let them navigate rather than fabricating a replacement path.
- **Links support, they do not replace, the hint ladder.** For graded Lab 9 and for
  troubleshooting, give one hint plus at most one link — never a doc page that
  contains a copy-paste solution to the graded exercise.

## Maintenance note

Extracted from the `en_us` teaching notebooks (modules 0–9 and 99) on 2026-06-11;
classroom-setup and `Includes/` notebooks were intentionally skipped. Re-extract
and re-link-check whenever the course content is republished, whenever the
declarative-pipelines doc section is reorganized (the `ldp/*` namespace has moved
before), or at least once per quarter. Section A is the source of truth for what
the course actually teaches; add to Section B rather than Section A when a link
was not in the notebooks.
