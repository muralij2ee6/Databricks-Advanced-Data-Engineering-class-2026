# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">08 Demo - Privacy Evidence with System Tables</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/common/databricks_academy_logo.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:flex; align-items:center; gap:14px; flex-wrap:wrap; font-family:sans-serif; border-left:6px solid #4299E0; background:#F8F9FC; border-radius:6px; padding:9px 16px; margin:4px 0;">
# MAGIC   <span style="font-size:12pt; font-weight:700; letter-spacing:0.5px; color:#1B5162;">THE DATA PRIVACY PROCESS</span>
# MAGIC   <span style="color:#C7D0D4;">|</span>
# MAGIC   <span style="display:flex; align-items:center; gap:8px; font-size:11pt; color:#90A5B1;">
# MAGIC     <span>&#9675; Identify</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Protect</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Manage</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span style="color:#0b2026; font-weight:700;"><span style="color:#FFAB00;">&#9679;</span> Verify</span>
# MAGIC   </span>
# MAGIC   <span style="font-size:10.5pt; color:#618794; margin-left:auto;">step 4 of 4</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Privacy Evidence with System Tables
# MAGIC #### Duration: 20 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC An auditor asks you to prove two things about one person: that a sensitive column is governed and that the erasure they requested actually happened. Every control in this course ended by showing it worked. This notebook assembles that evidence into one place. You'll run the queries first, then assemble the answer into one compact evidence row.
# MAGIC
# MAGIC After the evidence row, you'll recap the identify, protect, manage and verify model through the evidence each phase leaves. The Summary and Next Steps lecture that follows covers where to go next.
# MAGIC
# MAGIC > The scope evidence (classification and lineage) reads the shared **priv_demo** estate. The erasure ran earlier on the isolated **erasure_bronze** and **erasure_silver** copies, which mirror **customer_profile** and **customer_enriched**, so the shared estate the other demos read was never changed. Those copies are staged in the already-erased state and the request register as already verified, because here you're the auditor after the fact.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you'll be able to:
# MAGIC
# MAGIC 1. **Explain** which privacy question each system table and `information_schema` answers and what each one doesn't prove on its own.
# MAGIC 2. **Apply** column and table lineage to follow a sensitive column from its classification to the downstream tables it reaches.
# MAGIC 3. **Construct** one evidence trail for a request by joining classification, lineage, query history and audit, correlated by the statement id the register records and by actor, table and time.
# MAGIC 4. **Evaluate** the assembled evidence honestly, separating what it proves from what it only lets you infer.
# MAGIC 5. **Recap** the identify, protect, manage and verify model through the evidence each phase leaves.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .au-eyebrow { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; margin: 8px 0 12px; }
# MAGIC .au-req { display: flex; align-items: center; gap: 18px; border-radius: 10px; padding: 18px 22px; background: #1B5162; box-shadow: 0 2px 10px rgba(27,49,57,0.08); flex-wrap: wrap; }
# MAGIC .au-req-badge { flex: 0 0 auto; background: rgba(255,255,255,0.14); color: #fff; border: 1px solid rgba(255,255,255,0.35); border-radius: 8px; font-family: monospace; font-size: 13.5pt; font-weight: 700; padding: 8px 14px; }
# MAGIC .au-req-text { flex: 1 1 320px; color: #F9F7F4; font-size: 15pt; line-height: 1.45; }
# MAGIC .au-flow { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }
# MAGIC .au-step { flex: 1 1 150px; min-width: 150px; background: #fff; border: 1px solid #EEEDE9; border-top: 5px solid var(--st); border-radius: 8px; padding: 12px 14px; }
# MAGIC .au-step-head { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
# MAGIC .au-step-n { flex: 0 0 auto; display: inline-flex; align-items: center; justify-content: center; width: 22px; height: 22px; border-radius: 50%; background: var(--st); color: #fff; font-size: 11pt; font-weight: 700; }
# MAGIC .au-step-t { font-weight: 700; font-size: 13.5pt; color: #0b2026; }
# MAGIC .au-step-s { font-size: 11pt; font-weight: 700; color: var(--st); margin-left: auto; font-family: monospace; }
# MAGIC .au-step-p { font-size: 12.5pt; color: #4b5c63; line-height: 1.4; margin-top: 6px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="au-eyebrow">The audit request</div>
# MAGIC <div class="au-req">
# MAGIC   <div class="au-req-badge">DSR-2026-0412</div>
# MAGIC   <div class="au-req-text">An auditor asks you to prove, for one customer, that their sensitive data is governed and that the erasure they requested actually happened. Assemble the answer into one evidence row.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="au-eyebrow">The path this demo takes</div>
# MAGIC <div class="au-flow">
# MAGIC   <div class="au-step" style="--st:#618794;"><div class="au-step-head"><span class="au-step-n">1</span><span class="au-step-t">Map the sources</span><span class="au-step-s">B</span></div><div class="au-step-p">Which system table answers which question and what it does not prove on its own.</div></div>
# MAGIC   <div class="au-step" style="--st:#00A972;"><div class="au-step-head"><span class="au-step-n">2</span><span class="au-step-t">Follow the column</span><span class="au-step-s">C</span></div><div class="au-step-p">Trace national_id from its classification through lineage to where it reaches.</div></div>
# MAGIC   <div class="au-step" style="--st:#4299E0;"><div class="au-step-head"><span class="au-step-n">3</span><span class="au-step-t">Join the trail</span><span class="au-step-s">D</span></div><div class="au-step-p">Line up the statement with the governed write across query history and audit.</div></div>
# MAGIC   <div class="au-step" style="--st:#FFAB00;"><div class="au-step-head"><span class="au-step-n">4</span><span class="au-step-t">Validate</span><span class="au-step-s">E</span></div><div class="au-step-p">Confirm the subject is absent from every in-scope table.</div></div>
# MAGIC   <div class="au-step" style="--st:#2272B4;"><div class="au-step-head"><span class="au-step-n">5</span><span class="au-step-t">Assemble</span><span class="au-step-s">F</span></div><div class="au-step-p">Pull it into one compact evidence row, keyed to the request.</div></div>
# MAGIC   <div class="au-step" style="--st:#1B5162;"><div class="au-step-head"><span class="au-step-n">6</span><span class="au-step-t">Close the course</span><span class="au-step-s">G</span></div><div class="au-step-p">Recap the identify, protect, manage, verify model, then point to what is next.</div></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">Before you begin</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   <ul style="margin:0; padding-left:20px;">
# MAGIC     <li style="margin-bottom:8px;"><strong>This demo reads snapshots, not live system tables.</strong> Five deterministic point-in-time snapshots (<strong>snap_classification_results</strong>, <strong>snap_table_lineage</strong>, <strong>snap_column_lineage</strong>, <strong>snap_audit</strong>, <strong>snap_query_history</strong>) mirror the shape of the system tables, so the demo runs the same every time. System tables lag from minutes to a couple of hours, so a live scan wouldn't have the rows yet.</li>
# MAGIC     <li style="margin-bottom:8px;"><strong>The live tables redact what the snapshot shows.</strong> In <code>system.query.history</code> the <code>statement_text</code> returns <code>&lt;REDACTED&gt;</code> unless you're an admin or in the <code>databricks_pii_access</code> group. The snapshot keeps it so Section D can show the actual erasure SQL and its request-id tag; a live read by a non-privileged user would not.</li>
# MAGIC     <li style="margin-bottom:0;"><strong>This is the last demo.</strong> Section G recaps the course through the evidence each step left. The Summary and Next Steps lecture that follows collects the documentation, the topics left out on purpose and where to go next.</li>
# MAGIC   </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select Serverless Compute</strong>
# MAGIC   <div style="color:#333;" class="req-compute">
# MAGIC   <style>.req-compute img { width: 40%; height: auto; }</style>
# MAGIC
# MAGIC Before starting this notebook, select the required compute environment listed below.
# MAGIC
# MAGIC - **Serverless Compute, Version 6** for this notebook.
# MAGIC - A **Serverless SQL warehouse** (2X-Small is enough) for the SQL you'll run across the course.
# MAGIC
# MAGIC ![Serverless Select](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/common/select-serverless-v6.png)
# MAGIC <br></br>
# MAGIC   - How to select an environment version:
# MAGIC [AWS](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies#-select-an-environment-version) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/serverless/dependencies#-select-an-environment-version)
# MAGIC
# MAGIC **NOTE:** This demo is read only. Every cell is a `SELECT` over the staged snapshots and the estate, so it runs on serverless and never changes the shared data.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the setup below. It resolves your per-user catalog, ensures the shared **priv_demo** estate exists, builds the five deterministic evidence snapshots the demo reads, applies the governed **sensitivity_level** tag to **customer_profile** and stages the already-erased **erasure_bronze** and **erasure_silver** copies plus the verified **erasure_requests** register that this notebook checks against.
# MAGIC
# MAGIC If your workspace can't create catalogs, open `./Includes/Classroom-Setup-08` and set `FORCED_CATALOG` to an existing catalog first.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-08

# COMMAND ----------

from pyspark.sql import functions as F

# Re-bind the names this notebook reads so every cell is explicit about its source. This demo is read only.
CATALOG = my_catalog
DEMO_SCHEMA = "priv_demo"
SUBJECT_ID = 4212  # the internal id the subject_key resolves to
REQUEST_ID = "DSR-2026-0412"  # the request the auditor is checking
SUBJECT_KEY = "SUBJ-7C4A9E"  # pseudonymous; the register holds no raw PII


def _t(name):
    return f"`{CATALOG}`.`{DEMO_SCHEMA}`.{name}"


CLASS_SNAP = _t("snap_classification_results")  # what sensitive data exists
TABLE_LINEAGE = _t("snap_table_lineage")  # where it flowed (tables)
COLUMN_LINEAGE = _t("snap_column_lineage")  # where it flowed (columns)
AUDIT = _t("snap_audit")  # who did what
QUERY_HIST = _t("snap_query_history")  # which statement ran
REGISTER = _t("erasure_requests")  # the request control plane
REGISTER_STMTS = _t("erasure_request_statements")  # the register's per-statement action log
BRONZE = _t("erasure_bronze")  # already erased (verification)
SILVER = _t("erasure_silver")  # already erased (verification)

print(f"Catalog: {CATALOG}")
print(f"Assembling evidence for request {REQUEST_ID} (subject_key {SUBJECT_KEY}, customer_id {SUBJECT_ID}).")
print("All sources are read from the staged snapshots and the estate. Nothing is changed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Which Source Answers Which Question
# MAGIC
# MAGIC Before running anything, here's the map: which source you go to for each question and what that source doesn't prove on its own.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="text-align:center; margin-bottom: 12px;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700;">Each source answers one question. Together they make one evidence row.</div>
# MAGIC   <div style="font-size: 14pt; color: #5E7077; margin-top: 4px;">System tables and information_schema, correlated for a single request.</div>
# MAGIC </div>
# MAGIC <div style="background:#FFFFFF; border-radius:8px; padding:12px; box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <svg width="100%" viewBox="0 0 980 420" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="ev-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- six source boxes -->
# MAGIC   <rect x="24" y="20" width="252" height="50" rx="8" fill="#4299E0" fill-opacity="0.10" stroke="#4299E0" stroke-width="1.4"/>
# MAGIC   <text x="150" y="40" text-anchor="middle" font-size="12.5" font-weight="700" fill="#0b2026">what sensitive data exists</text>
# MAGIC   <text x="150" y="58" text-anchor="middle" font-size="11" fill="#5E7077" font-family="monospace">data_classification.results</text>
# MAGIC   <rect x="24" y="84" width="252" height="50" rx="8" fill="#00A972" fill-opacity="0.10" stroke="#00A972" stroke-width="1.4"/>
# MAGIC   <text x="150" y="104" text-anchor="middle" font-size="12.5" font-weight="700" fill="#0b2026">where it flowed</text>
# MAGIC   <text x="150" y="122" text-anchor="middle" font-size="11" fill="#5E7077" font-family="monospace">access.*_lineage</text>
# MAGIC   <rect x="24" y="148" width="252" height="50" rx="8" fill="#FF5F46" fill-opacity="0.10" stroke="#FF5F46" stroke-width="1.4"/>
# MAGIC   <text x="150" y="168" text-anchor="middle" font-size="12.5" font-weight="700" fill="#0b2026">who did what</text>
# MAGIC   <text x="150" y="186" text-anchor="middle" font-size="11" fill="#5E7077" font-family="monospace">access.audit</text>
# MAGIC   <rect x="24" y="212" width="252" height="50" rx="8" fill="#FFAB00" fill-opacity="0.14" stroke="#FFAB00" stroke-width="1.4"/>
# MAGIC   <text x="150" y="232" text-anchor="middle" font-size="12.5" font-weight="700" fill="#0b2026">which statement ran</text>
# MAGIC   <text x="150" y="250" text-anchor="middle" font-size="11" fill="#5E7077" font-family="monospace">query.history</text>
# MAGIC   <rect x="24" y="276" width="252" height="50" rx="8" fill="#618794" fill-opacity="0.14" stroke="#618794" stroke-width="1.4"/>
# MAGIC   <text x="150" y="296" text-anchor="middle" font-size="12.5" font-weight="700" fill="#0b2026">current tags and grants</text>
# MAGIC   <text x="150" y="314" text-anchor="middle" font-size="11" fill="#5E7077" font-family="monospace">information_schema</text>
# MAGIC   <rect x="24" y="340" width="252" height="50" rx="8" fill="#98102A" fill-opacity="0.10" stroke="#98102A" stroke-width="1.4"/>
# MAGIC   <text x="150" y="360" text-anchor="middle" font-size="12.5" font-weight="700" fill="#0b2026">table changes</text>
# MAGIC   <text x="150" y="378" text-anchor="middle" font-size="11" fill="#5E7077" font-family="monospace">DESCRIBE HISTORY</text>
# MAGIC   <!-- converging lines to the hub -->
# MAGIC   <path d="M276 45 L556 200" fill="none" stroke="#5E7077" stroke-width="1.3" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M276 109 L556 205" fill="none" stroke="#5E7077" stroke-width="1.3" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M276 173 L556 210" fill="none" stroke="#5E7077" stroke-width="1.3" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M276 237 L556 215" fill="none" stroke="#5E7077" stroke-width="1.3" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M276 301 L556 220" fill="none" stroke="#5E7077" stroke-width="1.3" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M276 365 L556 225" fill="none" stroke="#5E7077" stroke-width="1.3" marker-end="url(#ev-arrow)"/>
# MAGIC   <!-- the hub: one evidence row -->
# MAGIC   <rect x="560" y="158" width="222" height="118" rx="10" fill="#1B5162" stroke="#0b2026" stroke-width="1.5"/>
# MAGIC   <text x="671" y="192" text-anchor="middle" font-size="15.5" font-weight="700" fill="#FFFFFF">One evidence row</text>
# MAGIC   <text x="671" y="216" text-anchor="middle" font-size="11.5" fill="#F9F7F4">request &middot; scope &middot; actor</text>
# MAGIC   <text x="671" y="234" text-anchor="middle" font-size="11.5" fill="#F9F7F4">action &middot; completion &middot; verification</text>
# MAGIC   <text x="671" y="258" text-anchor="middle" font-size="11.5" fill="#FFAB00">keyed to one request</text>
# MAGIC   <!-- result -->
# MAGIC   <path d="M782 217 L806 217" fill="none" stroke="#5E7077" stroke-width="1.6" marker-end="url(#ev-arrow)"/>
# MAGIC   <rect x="808" y="185" width="150" height="64" rx="10" fill="#00A972" fill-opacity="0.12" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <text x="883" y="212" text-anchor="middle" font-size="22pt" font-weight="800" fill="#00A972">&check;</text>
# MAGIC   <text x="883" y="238" text-anchor="middle" font-size="12.5" font-weight="700" fill="#0b2026">the control worked</text>
# MAGIC   <text x="490" y="410" text-anchor="middle" font-size="13" fill="#0b2026">No source is the whole audit. You assemble the proof by lining the sources up for one request.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .fc-card { background: #F9F7F4; border-radius: 10px; padding: 20px 24px; border-top: 8px solid #1B5162; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .fc-title { font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 6px; }
# MAGIC .fc-intro { font-size: 14pt; line-height: 1.55; color: #0b2026; margin-bottom: 14px; }
# MAGIC .fc-item { display: grid; grid-template-columns: 260px 1fr; gap: 20px; padding: 11px 0; border-bottom: 1px solid #EEEDE9; align-items: start; }
# MAGIC .fc-item:last-child { border-bottom: none; }
# MAGIC .fc-key { font-size: 14pt; font-weight: 700; line-height: 1.35; color: #2272B4; font-family: monospace; overflow-wrap: anywhere; }
# MAGIC .fc-val { font-size: 14pt; color: #0b2026; line-height: 1.5; }
# MAGIC .fc-val .no { color: #98102A; font-weight: 600; }
# MAGIC @media (max-width: 640px) { .fc-item { grid-template-columns: 1fr; gap: 4px; } }
# MAGIC </style>
# MAGIC
# MAGIC <div class="fc-card">
# MAGIC   <div class="fc-title">Which source answers which question</div>
# MAGIC   <div class="fc-intro">Go to one source per question. Each row also names what that source doesn't settle by itself, so you don't overclaim from it.</div>
# MAGIC   <div class="fc-item"><div class="fc-key">system.data_classification.results</div><div class="fc-val">What sensitive data exists: the personal-data columns and their classes. <span class="no">Doesn't</span> tell you where the data went or who touched it.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">system.access.column_lineage<br>system.access.table_lineage</div><div class="fc-val">Where it flowed: the tables and columns a source propagated to. <span class="no">Doesn't</span> confirm a row was deleted, only where it could have spread.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">system.access.audit</div><div class="fc-val">Who did what: the actor, action, object and time of an operation. <span class="no">Doesn't</span> prove the data is physically gone, only that an action was recorded.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">system.query.history</div><div class="fc-val">Which statement ran: the SQL text, type, status and timing. <span class="no">Doesn't</span> capture UI or API actions that weren't SQL statements.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">information_schema</div><div class="fc-val">Current state: the tags and grants in effect right now. <span class="no">Doesn't</span> show history, only the present.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">DESCRIBE HISTORY</div><div class="fc-val">Table changes: the operations on one table, with metrics. <span class="no">Doesn't</span> cover other tables, external copies or non-Delta systems. Shown live in Demo 07 and the erasure lab; this demo reads the other five sources from snapshots.</div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Reading the map</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Two lineage tables, one question.** `column_lineage` and `table_lineage` both answer "where it flowed," at different grains. You use column lineage to prove a specific identifier reached a downstream table and table lineage for the overall chain.
# MAGIC - **Audit and query history overlap but differ.** A SQL `DELETE` shows up in both. Query history has the statement, the actor and the timing. The audit log has the governed write that authorized it, a credential vend for the table. UI and API actions that aren't SQL show up in the audit log but not in query history.
# MAGIC - **The "doesn't" column is the point.** Each source is strong evidence for its own question and weak or silent for the others. Naming the limit keeps you from claiming, for example, that an audit row proves physical deletion.
# MAGIC - **`information_schema` is present tense.** It answers "what is the tag right now," which is exactly what you want when proving a column is currently governed, but it's not a history.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-callout { border-left: 6px solid #FF5F46; border-radius: 8px; padding: 16px 22px; background: rgba(255,95,70,0.10); font-size: 14pt; line-height: 1.6; color: #0b2026; }
# MAGIC .ds-eyebrow { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #FF5F46; margin-bottom: 6px; }
# MAGIC .ds-callout ul { margin: 8px 0 0; padding-left: 20px; }
# MAGIC .ds-callout li { margin-bottom: 8px; }
# MAGIC </style>
# MAGIC <div class="ds-callout">
# MAGIC   <div class="ds-eyebrow">Stance</div>
# MAGIC   <strong style="font-size:16pt;">Table history and system tables are supporting evidence, not a complete privacy audit.</strong>
# MAGIC   <ul>
# MAGIC     <li>They show what was recorded on the platform. They don't cover copies that left the platform, exports, screenshots or a downstream system that's not on Databricks.</li>
# MAGIC     <li>They record that an operation ran. Whether it satisfied the request is a judgment you make by reading them together, not a value one table returns.</li>
# MAGIC     <li>Be precise about the difference between what the evidence shows and what it only lets you infer.</li>
# MAGIC   </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="background:#FFFFFF; border-radius:8px; padding:12px; box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <svg width="100%" viewBox="0 0 960 200" role="img" style="font-family: sans-serif;">
# MAGIC   <line x1="60" y1="110" x2="900" y2="110" stroke="#5E7077" stroke-width="2"/>
# MAGIC   <circle cx="150" cy="110" r="6" fill="#FF5F46"/>
# MAGIC   <text x="150" y="86" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026">operation runs</text>
# MAGIC   <text x="150" y="140" text-anchor="middle" font-size="12" fill="#5E7077">DELETE, tag, grant</text>
# MAGIC   <text x="150" y="157" text-anchor="middle" font-size="12" fill="#5E7077">the table changes now</text>
# MAGIC   <rect x="150" y="98" width="560" height="24" rx="6" fill="#FFAB00" fill-opacity="0.18" stroke="#FFAB00" stroke-width="1.4"/>
# MAGIC   <text x="430" y="114" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026">asynchronous ingestion: minutes to a couple of hours</text>
# MAGIC   <circle cx="710" cy="110" r="6" fill="#00A972"/>
# MAGIC   <text x="710" y="86" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026">row appears in system.*</text>
# MAGIC   <text x="710" y="140" text-anchor="middle" font-size="12" fill="#007a53">queryable now</text>
# MAGIC   <text x="835" y="102" text-anchor="middle" font-size="12" fill="#5E7077">time</text>
# MAGIC   <path d="M835 110 L892 110" fill="none" stroke="#5E7077" stroke-width="1.5"/>
# MAGIC   <text x="480" y="192" text-anchor="middle" font-size="13" fill="#0b2026">A live scan wouldn't have the rows yet, so this demo reads a point-in-time snapshot instead.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Follow a Column Through Lineage
# MAGIC
# MAGIC Start with the auditor's first question: is this sensitive column governed and where did it go? Take **national_id**. Classification detected it, a governed tag marks it restricted and column lineage shows the downstream tables it reached. That's the scope you would have to erase and the scope you must be able to point at.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="text-align:center; margin-bottom: 12px;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700;">Follow one column: national_id</div>
# MAGIC   <div style="font-size: 14pt; color: #5E7077; margin-top: 4px;">It reaches the silver copy and must be erased there too. The gold aggregate never carries it.</div>
# MAGIC </div>
# MAGIC <div style="background:#FFFFFF; border-radius:8px; padding:12px; box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <svg width="100%" viewBox="0 0 980 300" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="col-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="30" y="18" width="250" height="30" rx="15" fill="#CD7F32"/><text x="155" y="38" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF" letter-spacing="2">SOURCE</text>
# MAGIC   <rect x="375" y="18" width="250" height="30" rx="15" fill="#5A6F77"/><text x="500" y="38" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF" letter-spacing="2">SILVER</text>
# MAGIC   <rect x="720" y="18" width="230" height="30" rx="15" fill="#FFAB00"/><text x="835" y="38" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" letter-spacing="2">GOLD</text>
# MAGIC   <!-- source table -->
# MAGIC   <rect x="30" y="86" width="250" height="150" rx="8" fill="#CD7F32" fill-opacity="0.10" stroke="#CD7F32" stroke-width="1.6"/>
# MAGIC   <text x="155" y="116" text-anchor="middle" font-size="13.5" font-weight="700" fill="#0b2026" font-family="monospace">customer_profile</text>
# MAGIC   <rect x="70" y="132" width="170" height="34" rx="6" fill="#FFFFFF" stroke="#98102A" stroke-width="2"/><text x="155" y="154" text-anchor="middle" font-size="13" font-weight="700" fill="#98102A" font-family="monospace">national_id</text>
# MAGIC   <text x="155" y="188" text-anchor="middle" font-size="12" fill="#5E7077">class.us_ssn, HIGH</text>
# MAGIC   <text x="155" y="208" text-anchor="middle" font-size="12" fill="#5E7077">sensitivity_level = restricted</text>
# MAGIC   <!-- silver table -->
# MAGIC   <rect x="375" y="86" width="250" height="150" rx="8" fill="#5A6F77" fill-opacity="0.10" stroke="#5A6F77" stroke-width="1.6"/>
# MAGIC   <text x="500" y="116" text-anchor="middle" font-size="13.5" font-weight="700" fill="#0b2026" font-family="monospace">customer_enriched</text>
# MAGIC   <rect x="415" y="132" width="170" height="34" rx="6" fill="#FFFFFF" stroke="#98102A" stroke-width="2"/><text x="500" y="154" text-anchor="middle" font-size="13" font-weight="700" fill="#98102A" font-family="monospace">national_id</text>
# MAGIC   <text x="500" y="188" text-anchor="middle" font-size="12" fill="#98102A">identifier lands here</text>
# MAGIC   <text x="500" y="208" text-anchor="middle" font-size="12" fill="#5E7077">in erasure scope</text>
# MAGIC   <!-- gold table -->
# MAGIC   <rect x="720" y="86" width="230" height="150" rx="8" fill="#FFAB00" fill-opacity="0.12" stroke="#FFAB00" stroke-width="1.6"/>
# MAGIC   <text x="835" y="116" text-anchor="middle" font-size="12.5" font-weight="700" fill="#0b2026" font-family="monospace">customer_metrics_gold</text>
# MAGIC   <text x="835" y="150" text-anchor="middle" font-size="12" fill="#5E7077">country, revenue,</text>
# MAGIC   <text x="835" y="168" text-anchor="middle" font-size="12" fill="#5E7077">counts by group</text>
# MAGIC   <text x="835" y="200" text-anchor="middle" font-size="12" fill="#007a53" font-weight="700">no national_id</text>
# MAGIC   <!-- arrows -->
# MAGIC   <path d="M280 149 L369 149" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#col-arrow)"/>
# MAGIC   <text x="324" y="138" text-anchor="middle" font-size="11.5" fill="#5E7077" font-weight="700">flows</text>
# MAGIC   <path d="M625 149 L714 149" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#col-arrow)"/>
# MAGIC   <text x="669" y="132" text-anchor="middle" font-size="11.5" fill="#5E7077" font-weight="700">aggregated,</text>
# MAGIC   <text x="669" y="146" text-anchor="middle" font-size="11.5" fill="#5E7077" font-weight="700">dropped</text>
# MAGIC   <line x1="662" y1="158" x2="676" y2="172" stroke="#98102A" stroke-width="2"/><line x1="676" y1="158" x2="662" y2="172" stroke="#98102A" stroke-width="2"/>
# MAGIC   <text x="490" y="280" text-anchor="middle" font-size="13" fill="#0b2026">Column lineage proves national_id reaches customer_enriched, so erasure must cover it there and stops before the gold aggregate.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">What "governed" and "in scope" mean here</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Governed means detected and tagged.** Classification detected `national_id` as `class.us_ssn` and a governed `sensitivity_level = restricted` tag was applied, which is what an ABAC policy matches on. That's the "this column is governed" half of the auditor's question.
# MAGIC - **In scope means lineage reached it.** Column lineage shows `national_id` propagated to `customer_enriched`, so an erasure that only touched the source would have left a copy behind. Following the column is how you set the scope correctly.
# MAGIC - **The gold aggregate is out of scope for per-row erasure.** `customer_metrics_gold` groups by country and tier, so no row is about one person and `national_id` never appears there. You recompute it if the counts must change, you don't delete a person from it.
# MAGIC - **You're reading snapshots.** In production these are `system.data_classification.results`, `system.access.column_lineage` and `information_schema.column_tags` read live.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# The classification snapshot is the "what exists" evidence.
# national_id was detected as class.us_ssn at HIGH confidence.
# This is the sensitive column the auditor asked about.
display(spark.sql(f"""
SELECT table_name, column_name, class_tag, confidence, frequency
FROM {CLASS_SNAP}
WHERE table_name = 'customer_profile' AND column_name = 'national_id'
"""))

# COMMAND ----------

# information_schema holds the CURRENT tags, so reading it back proves the column is governed now, not just detected.
# Guarded: where an account-user can't read the tag, the detection above stands as the evidence.
try:
    tags = spark.sql(f"""
        SELECT column_name, tag_name, tag_value
        FROM `{CATALOG}`.information_schema.column_tags
        WHERE schema_name = '{DEMO_SCHEMA}'
          AND table_name = 'customer_profile'
          AND tag_name = 'sensitivity_level'
        ORDER BY column_name
    """)
    if tags.count() == 0:
        print("No readable governed 'sensitivity_level' tag here. The classification detection in C1 is the 'what exists' evidence for this demo.")
    else:
        display(tags)
except Exception as e:
    print("information_schema.column_tags is not readable in this workspace:")
    print(str(e).splitlines()[0])
    print("The classification detection in C1 is the 'what exists' evidence for this demo.")

# COMMAND ----------

# Column lineage answers where the identifier went.
# national_id reaches customer_enriched (the silver copy), which is why erasure had to cover it there.
# It doesn't appear in any edge into customer_metrics_gold, so the gold aggregate carries no identifier.
print("Column lineage for national_id (which downstream columns it becomes):")
display(spark.sql(f"""
SELECT source_table_name AS source_table, source_column_name AS source_column,
       target_table_name AS reaches_table, target_column_name AS as_column
FROM {COLUMN_LINEAGE}
WHERE source_table_name = 'customer_profile' AND source_column_name = 'national_id'
ORDER BY reaches_table
"""))

print("Table lineage for the estate (the overall chain):")
display(spark.sql(f"""
SELECT DISTINCT source_table_name AS source_table, target_table_name AS target_table
FROM {TABLE_LINEAGE}
ORDER BY source_table, target_table
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Join the Statement, the Actor and the Action
# MAGIC
# MAGIC Now the second question: did the erasure actually run and who ran it? Query history holds the statement and who ran it. The audit log holds the governed write that Unity Catalog authorized for the table. They share no statement id, so you line them up on the table, the actor and the time. Run the queries first, then read how they connect.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1040px; margin: 0 auto 10px; font-family: sans-serif;">
# MAGIC <div style="background:#FFFFFF; border-radius:8px; padding:12px; box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <svg width="100%" viewBox="0 0 980 320" role="img" style="font-family: sans-serif;">
# MAGIC   <text x="490" y="30" text-anchor="middle" font-size="18" font-weight="700" fill="#0b2026">How the sources line up for one request</text>
# MAGIC   <text x="490" y="52" text-anchor="middle" font-size="13.5" fill="#5E7077">The register records each statement id. The audit write has none, so it matches by table, actor and time.</text>
# MAGIC   <!-- Panel A: register + action log -->
# MAGIC   <rect x="20" y="78" width="240" height="132" rx="10" fill="#1B5162" fill-opacity="0.06" stroke="#1B5162" stroke-width="1.4"/>
# MAGIC   <text x="140" y="104" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026">Request register</text>
# MAGIC   <text x="140" y="125" text-anchor="middle" font-size="12.5" font-family="monospace" fill="#5E7077">erasure_requests</text>
# MAGIC   <line x1="36" y1="136" x2="244" y2="136" stroke="#EEEDE9" stroke-width="1"/>
# MAGIC   <text x="140" y="161" text-anchor="middle" font-size="13.5" font-weight="700" font-family="monospace" fill="#007a53">statement_id</text>
# MAGIC   <text x="140" y="182" text-anchor="middle" font-size="12" fill="#5E7077">recorded per action</text>
# MAGIC   <text x="140" y="200" text-anchor="middle" font-size="12" fill="#5E7077">with target_table</text>
# MAGIC   <!-- Panel B: query history -->
# MAGIC   <rect x="370" y="78" width="240" height="132" rx="10" fill="#FFAB00" fill-opacity="0.12" stroke="#B27A00" stroke-width="1.4"/>
# MAGIC   <text x="490" y="104" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026">Query history</text>
# MAGIC   <text x="490" y="125" text-anchor="middle" font-size="12.5" font-family="monospace" fill="#5E7077">system.query.history</text>
# MAGIC   <line x1="386" y1="136" x2="594" y2="136" stroke="#EEEDE9" stroke-width="1"/>
# MAGIC   <text x="490" y="159" text-anchor="middle" font-size="13" font-weight="700" font-family="monospace" fill="#007a53">statement_id</text>
# MAGIC   <text x="490" y="180" text-anchor="middle" font-size="12" fill="#0b2026">executed_by (actor)</text>
# MAGIC   <text x="490" y="198" text-anchor="middle" font-size="12" fill="#0b2026">statement_type &middot; start_time</text>
# MAGIC   <!-- Panel C: audit -->
# MAGIC   <rect x="720" y="78" width="240" height="132" rx="10" fill="#FF5F46" fill-opacity="0.08" stroke="#FF5F46" stroke-width="1.4"/>
# MAGIC   <text x="840" y="104" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026">Audit</text>
# MAGIC   <text x="840" y="125" text-anchor="middle" font-size="12.5" font-family="monospace" fill="#5E7077">system.access.audit</text>
# MAGIC   <line x1="736" y1="136" x2="944" y2="136" stroke="#EEEDE9" stroke-width="1"/>
# MAGIC   <text x="840" y="156" text-anchor="middle" font-size="11" font-family="monospace" fill="#007a53">generateTemporary</text>
# MAGIC   <text x="840" y="170" text-anchor="middle" font-size="11" font-family="monospace" fill="#007a53">TableCredential</text>
# MAGIC   <text x="840" y="189" text-anchor="middle" font-size="12" fill="#0b2026">table_full_name</text>
# MAGIC   <text x="840" y="205" text-anchor="middle" font-size="11.5" font-family="monospace" fill="#5E7077">operation READ_WRITE</text>
# MAGIC   <!-- key 1: statement_id links register and query history -->
# MAGIC   <line x1="260" y1="151" x2="370" y2="151" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <rect x="265" y="136" width="100" height="30" rx="15" fill="#00A972"/>
# MAGIC   <text x="315" y="156" text-anchor="middle" font-size="12" font-weight="700" font-family="monospace" fill="#FFFFFF">statement_id</text>
# MAGIC   <text x="315" y="185" text-anchor="middle" font-size="11" fill="#5E7077">register records it</text>
# MAGIC   <!-- key 2: table + actor + time links query history and audit -->
# MAGIC   <line x1="610" y1="151" x2="720" y2="151" stroke="#618794" stroke-width="1.6" stroke-dasharray="5 3"/>
# MAGIC   <rect x="611" y="136" width="108" height="30" rx="15" fill="#618794"/>
# MAGIC   <text x="665" y="156" text-anchor="middle" font-size="11" font-weight="700" fill="#FFFFFF">table+actor+time</text>
# MAGIC   <text x="665" y="185" text-anchor="middle" font-size="11" fill="#5E7077">no shared id</text>
# MAGIC   <!-- band -->
# MAGIC   <rect x="20" y="244" width="940" height="56" rx="8" fill="#F8F9FC" stroke="#EEEDE9" stroke-width="1"/>
# MAGIC   <text x="40" y="269" font-size="13" fill="#0b2026"><tspan font-weight="700" fill="#1B5162">Why two links: </tspan>the register stores each statement id, so it joins query history exactly.</text>
# MAGIC   <text x="40" y="289" font-size="13" fill="#0b2026">The credential vend has no statement id, so you match it by the same table, actor and time.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-callout { border-left: 6px solid #1B5162; border-radius: 8px; padding: 16px 22px; background: #F8F9FC; font-size: 14pt; line-height: 1.6; color: #0b2026; }
# MAGIC .ds-eyebrow { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #1B5162; margin-bottom: 6px; }
# MAGIC .ds-callout ul { margin: 8px 0 0; padding-left: 20px; }
# MAGIC .ds-callout li { margin-bottom: 8px; }
# MAGIC .ds-callout code { background:#EEEDE9; border-radius:4px; padding:1px 5px; font-size:14pt; }
# MAGIC </style>
# MAGIC <div class="ds-callout">
# MAGIC   <div class="ds-eyebrow">How they connect</div>
# MAGIC   <p style="margin:0 0 6px;">No single surrogate key stitches every source together. Two links carry the request across them:</p>
# MAGIC   <ul>
# MAGIC     <li><strong>Statement id, exact.</strong> The register records the <code>statement_id</code> of each erasure action, so the register and <code>query.history</code> join on it directly, with no need to read the statement text.</li>
# MAGIC     <li><strong>Table, actor and time, correlated.</strong> Unity Catalog audits a table write as <code>generateTemporaryTableCredential</code> with <code>operation</code> set to <code>READ_WRITE</code>, keyed by <code>table_full_name</code>. It carries no statement id, so you line it up with the statement by the same table, the same actor and an overlapping time window.</li>
# MAGIC     <li><strong>Request id, attribution.</strong> The engineer also tagged each statement with a <code>/* DSR-2026-0412 */</code> comment. It is a useful label, but <code>statement_text</code> is redacted for non-privileged readers, so the id the register recorded is the reliable tie.</li>
# MAGIC   </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 22px 26px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Ask Genie Code</div>
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">You're about to write the join by hand, which is the skill. Once you can read the sources, <strong>Genie Code</strong> can draft the same join. Click <strong>Copy</strong>, paste the prompt and then check its query against the columns you just saw, especially the map access on <code>request_params</code>.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">For request DSR-2026-0412, join erasure_request_statements to snap_query_history on statement_id, then match each write to a snap_audit generateTemporaryTableCredential event by table_full_name, actor and an overlapping time. Return the statement, the actor and the governed operation.</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1040px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .rd-eyebrow { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #1B5162; margin-bottom: 10px; }
# MAGIC .rd-row { display: flex; gap: 22px; align-items: stretch; flex-wrap: wrap; margin-bottom: 18px; }
# MAGIC .rd-card { flex: 1 1 380px; background: #F9F7F4; border: 1px solid #EEEDE9; border-top: 6px solid var(--rd); border-radius: 8px; padding: 16px 18px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .rd-who { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; font-size: 15pt; font-weight: 700; color: #0b2026; margin-bottom: 3px; }
# MAGIC .rd-tag { font-size: 11pt; font-weight: 700; color: #fff; background: var(--rd); border-radius: 5px; padding: 2px 9px; letter-spacing: 0.5px; }
# MAGIC .rd-sub { font-size: 13pt; color: #5E7077; margin-bottom: 12px; }
# MAGIC .rd-mock { background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 6px 12px; }
# MAGIC .rd-field { display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; padding: 7px 0; border-bottom: 1px solid #F1F0EC; font-family: monospace; font-size: 14pt; }
# MAGIC .rd-field:last-child { border-bottom: none; }
# MAGIC .rd-k { color: #5E7077; }
# MAGIC .rd-v { color: #0b2026; overflow-wrap: anywhere; text-align: right; }
# MAGIC .rd-redacted { color: #98102A; font-weight: 700; background: rgba(152,16,42,0.10); border-radius: 4px; padding: 0 7px; }
# MAGIC .rd-sql { color: #007a53; font-weight: 700; overflow-wrap: anywhere; text-align: right; }
# MAGIC .rd-note { border-left: 6px solid #1B5162; border-radius: 8px; padding: 16px 22px; background: #F8F9FC; font-size: 14pt; line-height: 1.6; }
# MAGIC .rd-note code { background:#EEEDE9; border-radius:4px; padding:1px 5px; font-size:13.5pt; }
# MAGIC .rd-note ul { margin: 10px 0 0; padding-left: 20px; }
# MAGIC .rd-note li { margin-bottom: 8px; }
# MAGIC </style>
# MAGIC <div class="rd-eyebrow">What you can read in statement_text</div>
# MAGIC <div class="rd-row">
# MAGIC   <div class="rd-card" style="--rd:#98102A;">
# MAGIC     <div class="rd-who"><span>Account user</span><span class="rd-tag">DEFAULT VIEW</span></div>
# MAGIC     <div class="rd-sub">No special privilege</div>
# MAGIC     <div class="rd-mock">
# MAGIC       <div class="rd-field"><span class="rd-k">statement_id</span><span class="rd-v">01ef&hellip;a1b2</span></div>
# MAGIC       <div class="rd-field"><span class="rd-k">statement_type</span><span class="rd-v">DELETE</span></div>
# MAGIC       <div class="rd-field"><span class="rd-k">statement_text</span><span class="rd-redacted">&lt;REDACTED&gt;</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="rd-card" style="--rd:#00A972;">
# MAGIC     <div class="rd-who"><span>Admin or databricks_pii_access</span><span class="rd-tag">PRIVILEGED</span></div>
# MAGIC     <div class="rd-sub">Reader with the PII privilege</div>
# MAGIC     <div class="rd-mock">
# MAGIC       <div class="rd-field"><span class="rd-k">statement_id</span><span class="rd-v">01ef&hellip;a1b2</span></div>
# MAGIC       <div class="rd-field"><span class="rd-k">statement_type</span><span class="rd-v">DELETE</span></div>
# MAGIC       <div class="rd-field"><span class="rd-k">statement_text</span><span class="rd-sql">DELETE &hellip; /* DSR-2026-0412 */</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div class="rd-note">
# MAGIC   <strong>Why it's readable in this demo.</strong> SQL text can carry personal data in <code>WHERE</code> clauses, so <code>statement_text</code> comes back <code>&lt;REDACTED&gt;</code> for everyone except account admins and members of the <code>databricks_pii_access</code> group, which an admin has to create before anyone can join it. This demo reads a snapshot that keeps the real text, so the query below can filter on the request id in the statement comment and show the SQL.
# MAGIC   <ul>
# MAGIC     <li><code>statement_id</code> is never redacted, so the join to the audit log on the statement id works with or without the privilege.</li>
# MAGIC     <li>In production, reading or filtering on <code>statement_text</code> needs that privilege or an admin-granted dynamic view. Without it you correlate on <code>statement_id</code>, actor and time instead.</li>
# MAGIC   </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# The register recorded the statement id of each erasure action, so the request ties to
# query.history by statement_id. That join is exact and does not depend on reading the
# statement text, which is redacted for non-privileged readers (see the panel above).
display(spark.sql(f"""
SELECT rs.statement_type, q.statement_id, q.executed_by, q.execution_status, q.start_time, q.statement_text
FROM {REGISTER_STMTS} rs
JOIN {QUERY_HIST} q ON q.statement_id = rs.statement_id
WHERE rs.request_id = '{REQUEST_ID}'
ORDER BY q.start_time
"""))

# COMMAND ----------

# The audit log's contribution is the governed write. Unity Catalog audits a table write as a
# generateTemporaryTableCredential with operation READ_WRITE, keyed by table_full_name. It carries
# no statement id, so you line it up with the statement by the same table, the same actor and an
# overlapping time window. The keys in request_params depend on the action, so a metadata read
# (getTable) carries full_name_arg instead.
display(spark.sql(f"""
SELECT rs.statement_type,
       rs.target_table,
       a.action_name,
       a.request_params['operation'] AS operation,
       a.user_identity.email AS actor,
       a.event_time,
       a.response.status_code AS status_code
FROM {REGISTER_STMTS} rs
JOIN {QUERY_HIST} q ON q.statement_id = rs.statement_id
JOIN {AUDIT} a
  ON a.request_params['table_full_name'] = rs.target_table
 AND a.user_identity.email = q.executed_by
 AND a.event_time BETWEEN q.start_time - INTERVAL 2 MINUTES AND q.end_time + INTERVAL 2 MINUTES
WHERE rs.request_id = '{REQUEST_ID}'
ORDER BY a.event_time
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Add the Erasure Validation
# MAGIC
# MAGIC The statement ran and an actor is on record. The last piece the auditor wants is the after state: is the subject actually gone? An empty result for the subject in every in-scope table is the validation. Then read the request register, which the earlier erasure work closed as verified.

# COMMAND ----------

# The validation is an empty result.
# Counting the subject's rows in the two erasure tables returns zero in both, because the erasure already removed them.
# An empty result for the subject is the current-state proof that the deletion took effect.
display(spark.sql(f"""
SELECT 'erasure_bronze' AS table_name, count(*) AS subject_rows
FROM {BRONZE} WHERE customer_id = {SUBJECT_ID}
UNION ALL
SELECT 'erasure_silver' AS table_name, count(*) AS subject_rows
FROM {SILVER} WHERE customer_id = {SUBJECT_ID}
"""))

# COMMAND ----------

# The register is the request control plane from the erasure demo.
# It holds one row per request keyed by request_id, with no raw PII.
# The earlier erasure work closed it as verified, with a completion time, the count of affected assets and an evidence reference.
display(spark.sql(f"""
SELECT request_id, subject_key, status, affected_asset_count, completed_at, evidence_ref
FROM {REGISTER}
WHERE request_id = '{REQUEST_ID}'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Assemble the Evidence Row
# MAGIC
# MAGIC Now pull it together. One row that answers the auditor in full: the request, the sensitive data in scope, the actor, the action, the completion and the verification. Each column comes from a different source, tied together by the statement id the register recorded and by the table, actor and time that line up the audit event.

# COMMAND ----------

# Tied together on the statement id (register action log to query.history) and on table, actor and
# time (query.history to the audit credential vend). The DELETE is the representative action.
# max(target_table_name) picks one representative downstream table when the column reaches several, keeping the row compact.
display(spark.sql(f"""
WITH scope AS (
  SELECT c.column_name, c.class_tag, max(l.target_table_name) AS reaches_table
  FROM {CLASS_SNAP} c
  LEFT JOIN {COLUMN_LINEAGE} l
    ON l.source_table_name = c.table_name AND l.source_column_name = c.column_name
  WHERE c.table_name = 'customer_profile' AND c.column_name = 'national_id'
  GROUP BY c.column_name, c.class_tag
),
verify AS (
  SELECT (SELECT count(*) FROM {BRONZE} WHERE customer_id = {SUBJECT_ID})
       + (SELECT count(*) FROM {SILVER} WHERE customer_id = {SUBJECT_ID}) AS subject_rows_remaining
)
SELECT
  r.request_id,
  r.subject_key,
  concat(scope.column_name, ' (', scope.class_tag, '), reaches ', scope.reaches_table) AS scope_sensitive_data,
  q.executed_by AS actor,
  concat(rs.statement_type, ' on ', rs.target_table, ', audited as ', a.action_name, ' (', a.request_params['operation'], ')') AS action,
  r.status AS completion,
  r.completed_at,
  verify.subject_rows_remaining AS subject_rows_after
FROM {REGISTER} r
JOIN {REGISTER_STMTS} rs
  ON rs.request_id = r.request_id AND rs.statement_type = 'DELETE'
JOIN {QUERY_HIST} q
  ON q.statement_id = rs.statement_id
JOIN {AUDIT} a
  ON a.request_params['table_full_name'] = rs.target_table
 AND a.user_identity.email = q.executed_by
 AND a.event_time BETWEEN q.start_time - INTERVAL 2 MINUTES AND q.end_time + INTERVAL 2 MINUTES
CROSS JOIN scope
CROSS JOIN verify
WHERE r.request_id = '{REQUEST_ID}'
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="rc-wrap">
# MAGIC <style>
# MAGIC .rc-wrap { width: 100%; display: flex; flex-direction: column; align-items: center; gap: 18px; font-family: sans-serif; }
# MAGIC .rc-wrap * { box-sizing: border-box; }
# MAGIC .rc-hint { width: 100%; max-width: 1100px; text-align: center; font-size: 14pt; color: #5E7077; font-style: italic; }
# MAGIC .rc-hint strong { color: #FF5F46; font-style: normal; font-weight: 700; }
# MAGIC .rc-row { display: block; white-space: nowrap; font-size: 0; width: 100%; max-width: 1100px; }
# MAGIC .rc-box { display: inline-block; width: 32%; margin-right: 2%; min-height: 96px; background: #F9F7F4; border-top: 8px solid transparent; border-left: 2px solid transparent; border-right: 2px solid transparent; border-bottom: 2px solid transparent; border-radius: 8px; padding: 16px 14px; text-align: center; cursor: pointer; user-select: none; vertical-align: top; transition: transform 0.12s; box-shadow: 0 2px 8px rgba(27,49,57,0.06); white-space: normal; }
# MAGIC .rc-box:last-child { margin-right: 0; }
# MAGIC .rc-box:hover { transform: translateY(-2px); }
# MAGIC .rc-box.active { background: #fff; border-left-color: var(--pc); border-right-color: var(--pc); border-bottom-color: var(--pc); }
# MAGIC .rc-label { display: block; font-weight: 700; font-size: 14pt; letter-spacing: 1px; text-transform: uppercase; line-height: 1.2; pointer-events: none; }
# MAGIC .rc-title { display: block; font-size: 14pt; color: #0b2026; line-height: 1.3; margin-top: 6px; pointer-events: none; }
# MAGIC .rc-detail-wrap { overflow: hidden; max-height: 0; opacity: 0; transition: max-height 0.35s ease, opacity 0.28s ease, margin-top 0.28s ease; margin-top: 0; width: 100%; max-width: 1100px; }
# MAGIC .rc-detail-wrap.open { max-height: 700px; opacity: 1; margin-top: 12px; }
# MAGIC .rc-detail-card { background: #F9F7F4; border-radius: 10px; padding: 22px 26px; border-top: 7px solid #ccc; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .rc-detail-title { font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 12px; line-height: 1.3; }
# MAGIC .rc-detail-body { font-size: 14pt; color: #0b2026; line-height: 1.55; }
# MAGIC .rc-detail-body ul { margin: 0; padding-left: 22px; }
# MAGIC .rc-detail-body li { margin-bottom: 8px; }
# MAGIC .rc-detail-body li:last-child { margin-bottom: 0; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="rc-hint">The row is strong evidence, but read it precisely. <strong>Click each card</strong> to separate what it proves from what it doesn't.</div>
# MAGIC
# MAGIC <div class="rc-row">
# MAGIC   <span class="rc-box" data-id="0" onclick="rcSelect(0)" style="--pc:#00A972; border-top-color:#00A972;">
# MAGIC     <span class="rc-label" style="color:#00A972;">Proven</span>
# MAGIC     <span class="rc-title">What the row establishes as fact</span>
# MAGIC   </span><span class="rc-box" data-id="1" onclick="rcSelect(1)" style="--pc:#4299E0; border-top-color:#4299E0;">
# MAGIC     <span class="rc-label" style="color:#4299E0;">Inferred</span>
# MAGIC     <span class="rc-title">What you reason, not read directly</span>
# MAGIC   </span><span class="rc-box" data-id="2" onclick="rcSelect(2)" style="--pc:#FFAB00; border-top-color:#FFAB00;">
# MAGIC     <span class="rc-label" style="color:#B27A00;">Still open</span>
# MAGIC     <span class="rc-title">What the row doesn't cover</span>
# MAGIC   </span>
# MAGIC </div>
# MAGIC
# MAGIC <div class="rc-detail-wrap" id="rc-detail-wrap">
# MAGIC   <div class="rc-detail-card" id="rc-detail-card"></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var REVEAL = [
# MAGIC   { title: "Proven as fact", color: "#00A972", text: "<ul><li>national_id is personal data and is currently tagged restricted: it's governed.</li><li>An erasure statement ran, by a named actor, against the in-scope table and finished.</li><li>The subject returns zero rows in every in-scope table right now.</li></ul>" },
# MAGIC   { title: "Inferred, not read directly", color: "#4299E0", text: "<ul><li>That these statements belong to this request: the register records which statement ids the request covers, which is a control you maintain, not something the statement itself proves.</li><li>That every downstream copy is handled: you infer it from lineage plus the deletes, so lineage has to be current.</li></ul>" },
# MAGIC   { title: "Still open", color: "#B27A00", text: "<ul><li>Physical purge finishes only after REORG and a retention-aware VACUUM, so old files may persist past this snapshot.</li><li>Copies that left the platform, exports and non-Databricks systems aren't in these tables.</li><li>Time travel and Change Data Feed hold the value until they age out.</li></ul>" }
# MAGIC ];
# MAGIC var rcCurrent = null;
# MAGIC function rcSelect(id) {
# MAGIC   var wrap = document.getElementById('rc-detail-wrap');
# MAGIC   var card = document.getElementById('rc-detail-card');
# MAGIC   var d = REVEAL[id];
# MAGIC   document.querySelectorAll('.rc-box').forEach(function(b) {
# MAGIC     b.classList.toggle('active', parseInt(b.dataset.id) === id && rcCurrent !== id);
# MAGIC   });
# MAGIC   if (rcCurrent === id) { wrap.classList.remove('open'); rcCurrent = null; return; }
# MAGIC   rcCurrent = id;
# MAGIC   card.style.borderTopColor = d.color;
# MAGIC   card.innerHTML = '<div class="rc-detail-title" style="color:' + d.color + ';">' + d.title + '</div><div class="rc-detail-body">' + d.text + '</div>';
# MAGIC   wrap.classList.add('open');
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ev-row { display: flex; gap: 20px; flex-wrap: wrap; }
# MAGIC .ev-card { flex: 1; min-width: 240px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 18px 20px; border-top: 8px solid var(--accent); }
# MAGIC .ev-badge { font-size: 20pt; font-weight: 800; color: var(--accent); line-height: 1; margin-bottom: 8px; }
# MAGIC .ev-title { font-size: 15pt; font-weight: 700; color: #0b2026; margin-bottom: 6px; }
# MAGIC .ev-text { font-size: 14pt; color: #5E7077; line-height: 1.5; }
# MAGIC .green { --accent:#00A972; } .amber { --accent:#FFAB00; }
# MAGIC </style>
# MAGIC <div class="ev-row">
# MAGIC   <div class="ev-card green">
# MAGIC     <div class="ev-badge">&check;</div>
# MAGIC     <div class="ev-title">Governed</div>
# MAGIC     <div class="ev-text">national_id is classified and tagged restricted, confirmed against the current tags.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card green">
# MAGIC     <div class="ev-badge">&check;</div>
# MAGIC     <div class="ev-title">Scoped and acted on</div>
# MAGIC     <div class="ev-text">Lineage set the scope and the statement and audit rows show the erasure ran, by whom.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card green">
# MAGIC     <div class="ev-badge">&check;</div>
# MAGIC     <div class="ev-title">Verified and closed</div>
# MAGIC     <div class="ev-text">The subject returns zero rows and the register reads verified with an evidence reference.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card amber">
# MAGIC     <div class="ev-badge">&#33;</div>
# MAGIC     <div class="ev-title">Still to confirm</div>
# MAGIC     <div class="ev-text">Physical purge after VACUUM, plus any copies outside these tables. The row is proof, not the whole audit.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Wrap-Up
# MAGIC
# MAGIC That evidence row is the capstone of the whole course. It proves the **verify** step and it only exists because each earlier step produced something you could point at. Here's the model one last time, with the evidence each step leaves behind.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1120px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .sf-row { display: flex; align-items: stretch; justify-content: center; gap: 0; flex-wrap: wrap; }
# MAGIC .sf-step { flex: 1 1 210px; max-width: 250px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 16px 16px 14px; border-top: 8px solid var(--accent); text-align: center; }
# MAGIC .sf-step.here { box-shadow: 0 4px 14px rgba(255,171,0,0.34); }
# MAGIC .sf-num { width: 34px; height: 34px; border-radius: 50%; background: var(--accent); color: #fff; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 15pt; margin: 2px auto 10px; }
# MAGIC .sf-title { font-size: 15pt; font-weight: 700; color: #0b2026; margin-bottom: 6px; }
# MAGIC .sf-tech { font-size: 14pt; color: #0b2026; line-height: 1.4; margin-bottom: 8px; }
# MAGIC .sf-ev { font-size: 14pt; color: #5E7077; line-height: 1.4; border-top: 1px dashed #EEEDE9; padding-top: 8px; }
# MAGIC .sf-ev strong { color: var(--accent); }
# MAGIC .sf-arrow { flex: 0 0 30px; display: flex; align-items: center; justify-content: center; font-size: 26pt; color: #618794; }
# MAGIC </style>
# MAGIC <div class="sf-row">
# MAGIC   <div class="sf-step" style="--accent:#4299E0;">
# MAGIC     <div class="sf-num">1</div>
# MAGIC     <div class="sf-title">Identify</div>
# MAGIC     <div class="sf-tech">Data Classification and governed tags find and label sensitive data.</div>
# MAGIC     <div class="sf-ev"><strong>Evidence:</strong> a class.* detection and a sensitivity_level tag.</div>
# MAGIC   </div>
# MAGIC   <div class="sf-arrow">&rarr;</div>
# MAGIC   <div class="sf-step" style="--accent:#00A972;">
# MAGIC     <div class="sf-num">2</div>
# MAGIC     <div class="sf-title">Protect</div>
# MAGIC     <div class="sf-tech">ABAC and masks hide values at query time; de-identification transforms the stored value.</div>
# MAGIC     <div class="sf-ev"><strong>Evidence:</strong> a masked result, an effective policy and a joinable pseudonym.</div>
# MAGIC   </div>
# MAGIC   <div class="sf-arrow">&rarr;</div>
# MAGIC   <div class="sf-step" style="--accent:#FF5F46;">
# MAGIC     <div class="sf-num">3</div>
# MAGIC     <div class="sf-title">Manage</div>
# MAGIC     <div class="sf-tech">Delete from the authoritative layer, propagate downstream and purge the files.</div>
# MAGIC     <div class="sf-ev"><strong>Evidence:</strong> an empty erasure query and a purged file.</div>
# MAGIC   </div>
# MAGIC   <div class="sf-arrow">&rarr;</div>
# MAGIC   <div class="sf-step here" style="--accent:#FFAB00;">
# MAGIC     <div class="sf-num">4</div>
# MAGIC     <div class="sf-title">Verify</div>
# MAGIC     <div class="sf-tech">System tables and lineage prove each control worked.</div>
# MAGIC     <div class="sf-ev"><strong>Evidence:</strong> the row you just built. You're here.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">The through-line, from Project Overview and Setup to this one</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Identify came first because you can't protect what you haven't found.** Classification and governed tags turned unknown columns into labeled, policy-ready attributes.
# MAGIC - **Protect used those tags as the security boundary.** ABAC matched the tag and applied the mask and de-identification transformed the stored value so an analyst could use the data without the original.
# MAGIC - **Manage honored erasure.** The erasure flow deleted the subject, propagated the delete downstream and purged the files, so the data was gone from current state and from storage.
# MAGIC - **Verify is where the course lands.** Every step left evidence on purpose and this notebook pulled that evidence into one row. That's the habit to carry out of the course: a control isn't done until you can show it worked.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### What comes next
# MAGIC
# MAGIC The **Summary and Next Steps** lecture that follows collects the documentation for every technique in the course, the related topics the course left out on purpose and the training and certification to take next. After it, the **Fulfil and Verify an Erasure Request** lab has you run the whole erasure and evidence flow yourself on an isolated estate.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, you:
# MAGIC
# MAGIC 1. **Mapped** each privacy question to its source and named what each source doesn't prove on its own.
# MAGIC 2. **Followed** the sensitive column **national_id** from its classification and governed tag through lineage to the downstream table it reaches.
# MAGIC 3. **Joined** the query-history statement to the audit event, correlating on the request id and the statement id.
# MAGIC 4. **Added** the erasure validation, an empty result for the subject and read the verified request register.
# MAGIC 5. **Assembled** one compact evidence row and read it precisely, separating what it proves from what it only lets you infer.
# MAGIC
# MAGIC That completes the course's demos. You framed privacy against security, governance and compliance, discovered and classified sensitive data, protected it with tag-driven ABAC and manual masks, de-identified it and managed keys, fulfilled and verified an erasure request and proved every control with evidence.
# MAGIC
# MAGIC Next, the **Summary and Next Steps** lecture collects the documentation and where to go next. Then the **Fulfil and Verify an Erasure Request** lab has you build the whole erasure and evidence flow yourself on the isolated lab estate; it runs offline, so you can finish it at your own pace. The **20-question assessment** comes after the lab and checks your understanding across the course. The habit to keep is the one this notebook made concrete: a privacy control isn't done until you can show it worked.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### References and documentation
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .cl-docs { background: #F8F9FC; border: 3px solid #FF5F46; border-radius: 10px; padding: 22px 26px; text-align: center; color: #0b2026; }
# MAGIC .cl-docs-title { font-size: 18pt; font-weight: 700; margin-bottom: 8px; }
# MAGIC .cl-docs-sub { font-size: 14pt; line-height: 1.55; margin-bottom: 16px; }
# MAGIC .cl-docs-links { display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }
# MAGIC .cl-doc-btn { display: inline-block; padding: 10px 18px; background: #FF5F46; color: #FFFFFF !important; text-decoration: none; border-radius: 6px; font-size: 14pt; font-weight: 600; }
# MAGIC .cl-doc-btn:hover { background: #FF6952; text-decoration: none; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="cl-docs">
# MAGIC   <div class="cl-docs-title">Evidence system tables</div>
# MAGIC   <div class="cl-docs-sub">These schemas and behaviors change over time, so confirm the current column names and the ingestion lag against the docs rather than a static copy. The ABAC, erasure and Trust Center links are in the Summary and Next Steps lecture.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/admin/system-tables/audit-logs" target="_blank">Audit system table &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/admin/system-tables/query-history" target="_blank">Query history &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/admin/system-tables/lineage" target="_blank">Lineage system tables &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/admin/system-tables/data-classification" target="_blank">Classification system table &rarr;</a>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC