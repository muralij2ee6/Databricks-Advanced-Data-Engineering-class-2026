---
name: advanced-techniques-with-apache-spark-declarative-pipeline
description: >-
  Course-specific grounding pack for the Databricks instructor-led course
  "Advanced Techniques with Apache Spark Declarative Pipelines" (advanced SDP
  design patterns: multi-flow ingestion with explicit flows and per-flow
  checkpoints, liquid clustering, advanced expectations and the quarantine
  pattern, the multiplex pattern with VARIANT, Delta sinks, Iceberg reads via
  Delta UniForm, and AUTO CDC INTO for SCD Type 1/2). Use this skill together
  with the reusable `course-teaching-assistant` behavior layer whenever a
  learner or instructor asks about this course's lessons, demos, labs, pipeline
  code, setup, or the Databricks features it teaches. Load the companion files
  for grounded answers: `course_pack.md` (module/notebook map, teaching order,
  scope, sealed-solution policy), `links.md` (vetted public Databricks doc paths
  per cloud, with the lesson each is taught in), `faq.md` (three-tier FAQ —
  Section 1 SME/PM VETTED, Sections 2 and 3 DRAFT pending instructor review,
  including course facts such as catalogs, schemas, volumes and compute),
  `misconceptions.md` (false premises to correct, e.g. "UNION equals
  multi-flow", "full refresh is a harmless retry", "EXPECT (col > 0) also
  filters NULLs", "UniForm copies data into Iceberg"), and
  `best_practices.md` (the practices the course teaches, with reason and the
  failure each prevents). Tests live in `tests/`.
---

# Advanced Techniques with Apache Spark Declarative Pipelines — course grounding pack

This skill is the course-specific knowledge layer for **Advanced Techniques with Apache Spark™ Declarative Pipelines**; it carries no behavior of its own and is designed to be paired with the reusable `course-teaching-assistant` skill, which supplies the teaching persona, the hint-ladder troubleshooting style, the sealed-solution rule for graded labs, and the resource-linking guardrails. Read `course_pack.md` first to locate a topic in the course's teaching order (Lecture 1 and the multi-flow / liquid clustering / data quality demo, Lecture 3 and the multiplex + Delta sink + Iceberg demo, the CDC lecture and the `AUTO CDC INTO` SCD Type 2 demo, the advanced data quality lecture and demo, then the multi-source e-commerce lab and the Summary and Next Steps notebook), then use `links.md` to cite **public Databricks documentation only** by appending the stored path to the learner's cloud prefix (AWS `https://docs.databricks.com/aws/en/<path>`, Azure `https://learn.microsoft.com/en-us/azure/databricks/<path>`, GCP `https://docs.databricks.com/gcp/en/<path>`). Use `faq.md` for product and classroom questions — honor its status tiers, treat Sections 2 and 3 as DRAFT, and never surface the maintainer-only provenance column to a learner — and use `misconceptions.md` and `best_practices.md` to correct false premises and justify recommendations with the reason and the failure avoided. Judgment questions (a failing silver table, a constraint dropping rows, a flow re-reading its source) get one hint at a time rather than a corrected query, and the e-commerce lab's graded answers stay sealed: explain the reasoning and point at the lesson, never paste the solution.
