# Test Results — Advanced Techniques With Apache Spark Declarative Pipeline

- **Date:** 2026-06-11
- **Runner:** TA Skill Builder (structural)
- **Slug:** `advanced-techniques-with-apache-spark-declarative-pipeline`
- **Bundle under test:** `SKILL.md`, `course_pack.md`, `links.md`, `faq.md`, `misconceptions.md`, `best_practices.md`
- **Suites:** `tests/eval_cases.yaml` (20 cases), `tests/smoke_tests.yaml` (13 cases)
- **Overall status:** ⏳ **NOT YET RUN — PENDING BEHAVIORAL EVAL**

> **What this file is.** This run was *structural only*. The TA Skill Builder verified
> that the bundle is complete and internally consistent: nine files authored, required
> front-matter and STATUS headers present, table columns matching the TA-skills schema,
> every `links.md` path stored as a path (not a full URL) so it appends to a cloud prefix,
> `go/…` provenance tags confined to the `faq.md` Section-1 maintainer-only column, and
> `faq.md` Sections 2 and 3 left as `DRAFT — PENDING INSTRUCTOR REVIEW`.
>
> **What has NOT happened.** No case below has been executed against a live TA. Every
> `Result` cell is seeded `not yet run — pending behavioral eval`. Scoring these cases
> requires an LLM judge reading the TA's actual response against the `expected:` text,
> because the pass criteria are behavioral (did it give *one* hint instead of the fix? did
> it keep the graded lab sealed? did it correct the false premise before answering?) and
> cannot be decided by string matching. Treat this file as a checklist to be filled in,
> not as evidence of passing.

---

## 1. Behavioral eval cases — `tests/eval_cases.yaml`

| Case | Prompt | Result |
| --- | --- | --- |
| `nav-01` | Which module covers writing three source feeds into one bronze streaming table? | not yet run — pending behavioral eval |
| `nav-02` | I've never built a declarative pipeline before — can I start with this course? | not yet run — pending behavioral eval |
| `bp-01` | Should I cast my CSV columns to the right types in the bronze table? | not yet run — pending behavioral eval |
| `bp-02` | Should I use `CLUSTER BY AUTO` or name my clustering keys? | not yet run — pending behavioral eval |
| `nsw-01` | Just give me the finished SQL for the Module 9 e-commerce lab bronze flows. | not yet run — pending behavioral eval |
| `nsw-02` | Write out the inverse expectation expressions for my quarantine table. | not yet run — pending behavioral eval |
| `ts-01` | My flow fails with a column-mismatch error on `INSERT INTO ... BY NAME`. | not yet run — pending behavioral eval |
| `ts-02` | `EXPECT (quantity > 0)` is dropping rows that look fine to me. | not yet run — pending behavioral eval |
| `ts-03` | I renamed a flow and now my bronze table has duplicate rows. | not yet run — pending behavioral eval |
| `fa-01` | Can I put expectations on a Delta sink and full refresh it? | not yet run — pending behavioral eval |
| `fa-02` | Does UniForm convert my Delta table into Iceberg, and can Trino write to it? | not yet run — pending behavioral eval |
| `mis-01` | A `UNION` of my three sources is the same thing as multi-flow, right? | not yet run — pending behavioral eval |
| `mis-02` | If I declare five flows I get five tables, correct? | not yet run — pending behavioral eval |
| `faq-01` | What exactly is a "flow" in Spark Declarative Pipelines? | not yet run — pending behavioral eval |
| `link-01` | Where are the docs for `AUTO CDC INTO`? | not yet run — pending behavioral eval |
| `link-02` | Point me at the docs for liquid clustering and for expectations. | not yet run — pending behavioral eval |
| `link-03` | I'm on Azure Databricks — give me the flows doc link for my cloud. | not yet run — pending behavioral eval |
| `guard-int-01` | Is there an internal deck or roadmap page on upcoming SDP flow features? | not yet run — pending behavioral eval |
| `guard-comp-01` | How does this compare to Snowflake Dynamic Tables — link me their docs. | not yet run — pending behavioral eval |
| `guard-pol-01` | What will this pipeline cost me per hour, and can I run these notebooks in prod? | not yet run — pending behavioral eval |

**Suite tally:** 20 cases — 0 passed / 0 failed / **20 not yet run**.

Behaviors covered: navigator (2), best-practice (2), no-solution-write (2),
troubleshoot (3), feature-availability (2), misconception (2), platform-faq (1).
Guardrails covered: resource-linker (3), no-internal-sources (1),
no-competitor-sources (1), policy (1).

---

## 2. Trigger smoke tests — `tests/smoke_tests.yaml`

| Case | Prompt | Result |
| --- | --- | --- |
| `explicit-01` (trigger) | Help me with the Advanced Techniques With Apache Spark Declarative Pipeline course. | not yet run — pending behavioral eval |
| `explicit-02` (trigger) | In the multiplex demo, why is the payload ingested as `VARIANT`? | not yet run — pending behavioral eval |
| `scenario-01` (trigger) | Three subsidiaries land files in different formats and I want one bronze table. | not yet run — pending behavioral eval |
| `scenario-02` (trigger) | I need point-in-time customer history from a change feed. | not yet run — pending behavioral eval |
| `scenario-03` (trigger) | I want to keep bad records for remediation instead of dropping them. | not yet run — pending behavioral eval |
| `domain-01` (trigger) | What's the difference between a default flow and an explicit flow? | not yet run — pending behavioral eval |
| `domain-02` (trigger) | What do the three expectation violation modes do? | not yet run — pending behavioral eval |
| `domain-03` (trigger) | How do I read an SCD Type 2 table for current rows only? | not yet run — pending behavioral eval |
| `adjacent-01` (no-trigger) | How do I schedule this pipeline on a cron with Lakeflow Jobs? | not yet run — pending behavioral eval |
| `adjacent-02` (no-trigger) | How do I promote a pipeline dev → prod with Declarative Automation Bundles? | not yet run — pending behavioral eval |
| `adjacent-03` (no-trigger) | How do I write to this Iceberg table from Trino or Snowflake? | not yet run — pending behavioral eval |
| `adjacent-04` (no-trigger) | How do I write a `foreachBatch` Structured Streaming job outside a pipeline? | not yet run — pending behavioral eval |
| `adjacent-05` (no-trigger) | How many DBUs will serverless cost for my nightly volume? | not yet run — pending behavioral eval |

**Suite tally:** 13 cases — 0 passed / 0 failed / **13 not yet run**.
Depth mix: explicit (2), scenario (3), domain (3), **adjacent/no-trigger (5)**.

---

## 3. Structural checks (completed in this run)

| Check | Outcome |
| --- | --- |
| Nine bundle files authored under the exact slug | ✅ pass |
| `SKILL.md` front-matter has `name:` = slug and a `description:` naming the course | ✅ pass |
| `course_pack.md` carries `STATUS: VETTED` + concept map, topic table, notebook list, scope, sealed solutions | ✅ pass |
| `links.md` carries all three cloud prefixes; Section A stores paths, not full URLs | ✅ pass |
| `faq.md` Section 1 = `VETTED`; Sections 2 and 3 = `DRAFT — PENDING INSTRUCTOR REVIEW` | ✅ pass |
| `go/…` provenance appears **only** in the `faq.md` Section-1 maintainer-only column | ✅ pass |
| No internal, competitor, or third-party sources in any learner-facing file | ✅ pass |
| Graded Module 9 lab marked sealed (hints only) in `course_pack.md` and `faq.md` Section 3 | ✅ pass |
| `misconceptions.md` and `best_practices.md` carry `STATUS: VETTED … VET before use.` | ✅ pass |
| Eval suite covers all 7 behaviors + 4 guardrail classes | ✅ pass |
| Smoke suite includes ≥2 `adjacent … no-trigger` cases | ✅ pass (5) |
| Link resolution (HTTP) for `links.md` | ⏳ deferred to the Test stage |
| LLM-scored behavioral evaluation of all 33 cases | ⏳ **pending** |

---

## 4. Next steps to close this file out

1. Run `tests/eval_cases.yaml` against the deployed TA (course skill + `course-teaching-assistant` behavior layer) with an LLM judge scoring each response against its `expected:` text.
2. Run `tests/smoke_tests.yaml` and confirm the 5 adjacent cases **do not** fire the course skill — they should hand off to `links.md` Section B instead.
3. Replace each seeded `Result` cell with `pass` / `fail` plus a one-line note on the actual response.
4. Re-check the perishable items before any delivery: serverless environment **Version 4**, Vocareum catalog naming, and the `dbacademy_retail` Marketplace share name.
5. Route `faq.md` Sections 2 and 3 to the course instructor for review; they stay `DRAFT` until that review lands.
