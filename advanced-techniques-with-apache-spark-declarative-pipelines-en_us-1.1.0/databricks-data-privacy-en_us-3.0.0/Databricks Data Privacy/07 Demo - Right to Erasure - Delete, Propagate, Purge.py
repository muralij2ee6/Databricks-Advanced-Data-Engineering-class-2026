# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">07 Demo - Right to Erasure - Delete, Propagate, Purge</span>
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
# MAGIC     <span style="color:#0b2026; font-weight:700;"><span style="color:#FF5F46;">&#9679;</span> Manage</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Verify</span>
# MAGIC   </span>
# MAGIC   <span style="font-size:10.5pt; color:#618794; margin-left:auto;">step 3 of 4</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Right to Erasure: Delete, Propagate, Purge
# MAGIC #### Duration: 35 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC An erasure request arrives for one person and you have to remove them and be able to prove it. This notebook works that request end to end. It's the **manage** step of the course, where you act on personal data over its lifecycle: you delete the subject and purge the data from storage. An erasure has to be shown to have worked, so this notebook also produces the first evidence and the next demo turns that into a full audit.
# MAGIC
# MAGIC The visual below sets up the request and the path this demo takes through it.
# MAGIC
# MAGIC > The find step reads the shared **priv_demo** estate through the classification and lineage snapshots the course provisions. The destructive delete and purge run on a dedicated, isolated copy (**erasure_bronze** and **erasure_silver**) that carries the same subject and has deletion vectors enabled, so the shared estate the other demos read is never changed.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you'll be able to:
# MAGIC
# MAGIC 1. **Design** a request control plane that captures the subject key, request id, deadline, status, scope and evidence references and **explain** why it's not the Databricks platform control plane.
# MAGIC 2. **Apply** classification and lineage to find every table that holds a subject's data before you delete anything.
# MAGIC 3. **Delete** a subject from the authoritative layer with `DELETE` and `MERGE` and **propagate** the delete downstream with batch SQL, idempotently.
# MAGIC 4. **Diagnose** why a successful `DELETE` on a deletion-vector table is a logical delete, not a physical purge.
# MAGIC 5. **Configure** physical purge with `REORG TABLE ... APPLY (PURGE)` and retention-aware `VACUUM` and **evaluate** `DESCRIBE HISTORY` as one evidence source.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .er-eyebrow { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; margin: 8px 0 12px; }
# MAGIC .er-req { display: flex; align-items: center; gap: 18px; border-radius: 10px; padding: 18px 22px; background: #1B5162; box-shadow: 0 2px 10px rgba(27,49,57,0.08); flex-wrap: wrap; }
# MAGIC .er-req-badge { flex: 0 0 auto; background: rgba(255,255,255,0.14); color: #fff; border: 1px solid rgba(255,255,255,0.35); border-radius: 8px; font-family: monospace; font-size: 13.5pt; font-weight: 700; padding: 8px 14px; }
# MAGIC .er-req-text { flex: 1 1 320px; color: #F9F7F4; font-size: 15pt; line-height: 1.45; }
# MAGIC .er-catch { border-left: 6px solid #FF5F46; border-radius: 8px; padding: 14px 20px; background: rgba(255,95,70,0.10); font-size: 14pt; line-height: 1.55; margin: 18px 0 6px; }
# MAGIC .er-flow { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }
# MAGIC .er-step { flex: 1 1 155px; min-width: 155px; background: #fff; border: 1px solid #EEEDE9; border-top: 5px solid var(--st); border-radius: 8px; padding: 12px 14px; }
# MAGIC .er-step-head { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
# MAGIC .er-step-n { flex: 0 0 auto; display: inline-flex; align-items: center; justify-content: center; width: 22px; height: 22px; border-radius: 50%; background: var(--st); color: #fff; font-size: 11pt; font-weight: 700; }
# MAGIC .er-step-t { font-weight: 700; font-size: 13.5pt; color: #0b2026; }
# MAGIC .er-step-s { font-size: 11pt; font-weight: 700; color: var(--st); margin-left: auto; font-family: monospace; }
# MAGIC .er-step-p { font-size: 12.5pt; color: #4b5c63; line-height: 1.4; margin-top: 6px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="er-eyebrow">The request</div>
# MAGIC <div class="er-req">
# MAGIC   <div class="er-req-badge">DSR-2026-0412</div>
# MAGIC   <div class="er-req-text">A customer asks to be erased. Remove them from every table their identifiers reach and delete the values from storage rather than just hiding them. Then prove it.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="er-catch">
# MAGIC   <strong>The catch:</strong> these tables use <strong>deletion vectors</strong>, a small side file that marks which rows are deleted so they vanish from query results without the data file being rewritten. So a <code>DELETE</code> removes the subject from what queries return, but the raw value stays in the file until <code>REORG ... APPLY (PURGE)</code> rewrites it and a retention-aware <code>VACUUM</code> clears the old files. Erasure is delete, then purge, then prove.
# MAGIC </div>
# MAGIC
# MAGIC <div class="er-eyebrow">The path this demo takes</div>
# MAGIC <div class="er-flow">
# MAGIC   <div class="er-step" style="--st:#618794;"><div class="er-step-head"><span class="er-step-n">1</span><span class="er-step-t">Track</span><span class="er-step-s">B</span></div><div class="er-step-p">Record the request in a register and keep replays idempotent.</div></div>
# MAGIC   <div class="er-step" style="--st:#00A972;"><div class="er-step-head"><span class="er-step-n">2</span><span class="er-step-t">Find</span><span class="er-step-s">C</span></div><div class="er-step-p">Classification and lineage locate every table the subject reaches.</div></div>
# MAGIC   <div class="er-step" style="--st:#4299E0;"><div class="er-step-head"><span class="er-step-n">3</span><span class="er-step-t">Delete</span><span class="er-step-s">D</span></div><div class="er-step-p">MERGE-delete from the authoritative layer, then propagate downstream.</div></div>
# MAGIC   <div class="er-step" style="--st:#FFAB00;"><div class="er-step-head"><span class="er-step-n">4</span><span class="er-step-t">Purge</span><span class="er-step-s">E&#183;F</span></div><div class="er-step-p">REORG APPLY (PURGE) rewrites the files so the value is really gone.</div></div>
# MAGIC   <div class="er-step" style="--st:#98102A;"><div class="er-step-head"><span class="er-step-n">5</span><span class="er-step-t">Vacuum</span><span class="er-step-s">G</span></div><div class="er-step-p">A retention-aware VACUUM clears the old files after the window.</div></div>
# MAGIC   <div class="er-step" style="--st:#1B5162;"><div class="er-step-head"><span class="er-step-n">6</span><span class="er-step-t">Prove</span><span class="er-step-s">H</span></div><div class="er-step-p">DESCRIBE HISTORY as evidence, then close the request as verified.</div></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">Before you begin</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   <ul style="margin:0; padding-left:20px;">
# MAGIC     <li style="margin-bottom:8px;"><strong>The destructive steps run on a dedicated copy.</strong> The delete, <code>REORG</code> and <code>VACUUM</code> in Sections D through G only touch <strong>erasure_bronze</strong> and <strong>erasure_silver</strong>, dedicated copies with deletion vectors enabled and the subject seeded. The shared <strong>priv_demo</strong> estate is only read.</li>
# MAGIC     <li style="margin-bottom:8px;"><strong>Re-run the setup to reset the subject.</strong> To show the before and after again, re-run the setup in Section A first. It rebuilds the erasure tables with the subject present. Re-running only the notebook is safe, but the subject will already be gone.</li>
# MAGIC     <li style="margin-bottom:0;"><strong>The default retention window is 168 hours.</strong> In Section G, <code>VACUUM ... DRY RUN</code> will most likely list zero files right after the demo, because nothing is older than the 7 day window yet. That's the retention window working as designed, not an error.</li>
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
# MAGIC **NOTE:** This course uses **SQL and Python and was tested on Serverless V6**. `DELETE`, `MERGE`, `REORG TABLE ... APPLY (PURGE)` and `VACUUM` all run on serverless.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the setup below. It resolves your per-user catalog, ensures the shared **priv_demo** estate exists and builds the isolated **erasure_bronze** and **erasure_silver** tables with deletion vectors enabled, seeded with the subject to erase.
# MAGIC
# MAGIC The **classification** and **lineage** snapshots the find step reads (`snap_classification_results`, `snap_table_lineage`, `snap_column_lineage`) are deterministic, reproducible copies the setup builds to mirror the shape of the system tables, so the find step runs the same every time and doesn't depend on live system tables that lag and are access-restricted. The find step reads them and never changes them; the delete and purge run only on the isolated `erasure_bronze` and `erasure_silver`.
# MAGIC
# MAGIC If your workspace can't create catalogs, open `./Includes/Classroom-Setup-07` and set `FORCED_CATALOG` to an existing catalog first.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-07

# COMMAND ----------

from pyspark.sql import functions as F

# Re-bind the names this notebook uses so every cell is explicit about what it reads and mutates.
CATALOG = my_catalog
DEMO_SCHEMA = "priv_demo"
BRONZE_TABLE = "erasure_bronze"
SILVER_TABLE = "erasure_silver"
SUBJECT_ID = 4212  # resolved from the request's subject_key by an access-controlled lookup

BRONZE = f"`{CATALOG}`.`{DEMO_SCHEMA}`.{BRONZE_TABLE}"
SILVER = f"`{CATALOG}`.`{DEMO_SCHEMA}`.{SILVER_TABLE}"
REGISTER = f"`{CATALOG}`.`{DEMO_SCHEMA}`.erasure_requests"
CLASS_SNAP = f"`{CATALOG}`.`{DEMO_SCHEMA}`.snap_classification_results"
TABLE_LINEAGE = f"`{CATALOG}`.`{DEMO_SCHEMA}`.snap_table_lineage"
COLUMN_LINEAGE = f"`{CATALOG}`.`{DEMO_SCHEMA}`.snap_column_lineage"

# Request metadata. These are pseudonymous. The register never stores raw PII.
REQUEST_ID = "DSR-2026-0412"
SUBJECT_KEY = "SUBJ-7C4A9E"

print(f"Catalog: {CATALOG}")
print(f"Request {REQUEST_ID} concerns subject_key {SUBJECT_KEY}, resolved to customer_id {SUBJECT_ID}.")
print("Find reads the shared priv_demo estate. Delete and purge run on erasure_bronze / erasure_silver only.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. The Request Control Plane
# MAGIC
# MAGIC Before touching data, you need a place that records the request and drives it to completion. That management layer is the **request control plane**. It decides and tracks what should happen and to what, while the deletes, the propagation and the purge are the work it coordinates. It's a design pattern you build, for example a governed **erasure_requests** table, not a Databricks product feature.
# MAGIC
# MAGIC The name is reused, so name both meanings once. The **Databricks platform control plane** is the managed backend in your Databricks account, the web application and the job and query scheduling. The **request control plane** here is unrelated to it, despite the shared name.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-row { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 260px; }
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px; border-top: 8px solid var(--accent, #4299E0); }
# MAGIC .blue{--accent:#4299E0;} .coral{--accent:#FF5F46;}
# MAGIC .ds-card-label { font-size: 14pt; font-weight: 700; letter-spacing: 2px; text-transform: uppercase; color: var(--accent); margin-bottom: 4px; }
# MAGIC .ds-card-title { font-size: 17pt; font-weight: 700; line-height: 1.25; margin: 0 0 8px; }
# MAGIC .ds-card-para { font-size: 14pt; color: #0b2026; line-height: 1.55; margin: 0; }
# MAGIC .svg-wrap { margin-top: 20px; background: #FFFFFF; border-radius: 8px; padding: 12px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-card blue">
# MAGIC     <div class="ds-card-label">Not this one</div>
# MAGIC     <div class="ds-card-title">Databricks platform control plane</div>
# MAGIC     <div class="ds-card-para">The managed backend services in your Databricks account: the web application and job and query scheduling. Platform architecture, nothing to do with a privacy request.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-card coral">
# MAGIC     <div class="ds-card-label">This one</div>
# MAGIC     <div class="ds-card-title">Request control plane</div>
# MAGIC     <div class="ds-card-para">A tracking layer you build, for example an <code>erasure_requests</code> table. It records each data subject request and drives it to done while writing an evidence trail.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="svg-wrap">
# MAGIC <svg width="100%" viewBox="0 0 960 220" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="cp-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="30" y="55" width="300" height="110" rx="10" fill="#FF5F46" fill-opacity="0.10" stroke="#FF5F46" stroke-width="1.8"/>
# MAGIC   <text x="180" y="86" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026">Request control plane</text>
# MAGIC   <text x="180" y="110" text-anchor="middle" font-size="13" fill="#5E7077" font-family="monospace">erasure_requests</text>
# MAGIC   <text x="180" y="134" text-anchor="middle" font-size="13" fill="#5E7077">decides and tracks:</text>
# MAGIC   <text x="180" y="152" text-anchor="middle" font-size="13" fill="#5E7077">id, status, deadline, scope, evidence</text>
# MAGIC   <path d="M330 110 L420 110" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#cp-arrow)"/>
# MAGIC   <text x="375" y="98" text-anchor="middle" font-size="12" fill="#5E7077" font-weight="700">coordinates</text>
# MAGIC   <rect x="430" y="35" width="500" height="150" rx="10" fill="#F9F7F4" stroke="#1B5162" stroke-width="1.5" stroke-dasharray="5 3"/>
# MAGIC   <text x="450" y="28" font-size="12" font-weight="700" fill="#1B5162" letter-spacing="1.5">EXECUTION (does the work)</text>
# MAGIC   <rect x="450" y="60" width="140" height="52" rx="8" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="520" y="91" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">DELETE</text>
# MAGIC   <rect x="610" y="60" width="140" height="52" rx="8" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="680" y="91" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">propagate</text>
# MAGIC   <rect x="770" y="60" width="140" height="52" rx="8" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="840" y="84" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" font-family="monospace">REORG</text><text x="840" y="102" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" font-family="monospace">VACUUM</text>
# MAGIC   <text x="680" y="150" text-anchor="middle" font-size="13" fill="#5E7077">the SQL that changes and purges data</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why a control plane</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **The register is the source of truth for the request.** Erasure jobs get re-run, pipelines reprocess data and a request can be replayed. A single row per request, keyed by a request id, is what lets you know what is done and what is left.
# MAGIC - **The control plane decides and tracks and the execution does the work.** This borrows the control plane versus data plane split from systems thinking. The register doesn't delete anything itself, it coordinates the SQL that does.
# MAGIC - **The word is overloaded on purpose here.** In Databricks architecture the platform control plane is the managed backend. The request control plane is a privacy tracking layer you build. They share a name and nothing else.
# MAGIC - **This is a design pattern, not a product feature.** There's no built in erasure register. You build one and it hands off to the evidence demo later in the course.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .fc-card { background: #F9F7F4; border-radius: 10px; padding: 20px 24px; border-top: 8px solid #4299E0; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .fc-title { font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 6px; }
# MAGIC .fc-intro { font-size: 14pt; line-height: 1.55; color: #0b2026; margin-bottom: 14px; }
# MAGIC .fc-item { display: grid; grid-template-columns: 220px 1fr; gap: 20px; padding: 11px 0; border-bottom: 1px solid #EEEDE9; align-items: start; }
# MAGIC .fc-item:last-child { border-bottom: none; }
# MAGIC .fc-key { font-size: 14pt; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; line-height: 1.3; color: #2272B4; }
# MAGIC .fc-val { font-size: 14pt; color: #0b2026; line-height: 1.5; }
# MAGIC .ds-callout { border-left: 6px solid #FF5F46; border-radius: 8px; padding: 16px 22px; background: rgba(255,95,70,0.10); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 18px; }
# MAGIC .fc-ahead { border-left: 6px solid #1B5162; border-radius: 8px; padding: 14px 20px; background: #F8F9FC; font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 16px; }
# MAGIC .fc-ahead-eyebrow { font-size: 12pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #1B5162; margin-bottom: 4px; }
# MAGIC @media (max-width: 600px) { .fc-item { grid-template-columns: 1fr; gap: 4px; } }
# MAGIC </style>
# MAGIC
# MAGIC <div class="fc-card">
# MAGIC   <div class="fc-title">What each request row captures</div>
# MAGIC   <div class="fc-intro">One row per request. Each field earns its place by answering a question you'll be asked later.</div>
# MAGIC   <div class="fc-item"><div class="fc-key">Subject key</div><div class="fc-val">A stable, pseudonymous id for the person, used to locate their data across tables. Pseudonymous on purpose, so the register doesn't hoard raw identifiers.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">Request id</div><div class="fc-val">A unique id per request. It's the idempotency key and the join key that ties all the evidence back to one request.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">Deadline</div><div class="fc-val">The regulatory clock, roughly 30 days under GDPR and CCPA, so you can flag requests at risk of missing the deadline.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">Status</div><div class="fc-val">A small state machine, for example received, in progress, verified, closed, so a rerun knows what is already done.</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">Scope</div><div class="fc-val">What the request covers: which subject, which datasets are in play and the request type (erasure, access, correction, export).</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">Evidence references</div><div class="fc-val">Pointers to the proof: table history, the affected asset count and the completion timestamp. This is the hand-off to the evidence demo.</div></div>
# MAGIC   <div class="ds-callout"><strong>Keep raw PII out of the operational fields.</strong> Use the pseudonymous subject key and don't copy the person's email or name into a status or notes column. Leaking PII into the very table that manages its deletion defeats the purpose and it becomes one more place you then have to erase.</div>
# MAGIC </div>
# MAGIC <div class="fc-ahead">
# MAGIC   <div class="fc-ahead-eyebrow">Looking ahead</div>
# MAGIC   Alongside this one row per request, a production register also keeps a per-action log that records the statement id of each erasure statement, which the evidence demo joins back to query history to tie every action to the request.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">How the fields drive idempotent processing</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **The request id is the idempotency key.** You key every write on it with `MERGE ... WHEN MATCHED`, so reprocessing the same request updates the one row instead of inserting a duplicate.
# MAGIC - **Status guards the work.** A rerun reads the status and skips what is already done, so replaying a request doesn't repeat the deletes.
# MAGIC - **Set-based deletes are safe to re-run.** Deleting a subject who is already gone affects zero rows, which is a no-op, not an error. That's what makes the whole flow replayable.
# MAGIC - **The subject key stays pseudonymous.** Resolving it to a concrete internal id, like `customer_id` here, is itself an access-controlled step, so the register doesn't store the raw identifier.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# The register holds one row per request, keyed by request_id, with only the pseudonymous subject_key. No email or name goes in here.
# MERGE seeds the received request.
spark.sql(f"""
CREATE OR REPLACE TABLE {REGISTER} (
  request_id STRING,
  subject_key STRING,
  request_type STRING,
  status STRING,
  received_date DATE,
  deadline_date DATE,
  scope STRING,
  affected_asset_count INT,
  completed_at TIMESTAMP,
  evidence_ref STRING
)
""")

spark.sql(f"""
MERGE INTO {REGISTER} t
USING (
  SELECT '{REQUEST_ID}' AS request_id,
         '{SUBJECT_KEY}' AS subject_key,
         'erasure' AS request_type,
         'received' AS status,
         DATE'2026-08-20' AS received_date,
         DATE'2026-09-19' AS deadline_date,
         'priv_demo customer estate: erasure_bronze (authoritative), erasure_silver (downstream)' AS scope
) s
ON t.request_id = s.request_id
WHEN NOT MATCHED THEN INSERT (request_id, subject_key, request_type, status, received_date, deadline_date, scope)
  VALUES (s.request_id, s.subject_key, s.request_type, s.status, s.received_date, s.deadline_date, s.scope)
""")

print("Request register ready. Note: subject_key is pseudonymous and there is no email or name column.")
display(spark.sql(f"SELECT request_id, subject_key, request_type, status, received_date, deadline_date, scope FROM {REGISTER}"))

# COMMAND ----------

# Run the SAME seeding MERGE again. 
# Because it's keyed on request_id and matches the existing row, WHEN NOT MATCHED does nothing, so the register still holds exactly one request. 
# This is what lets an erasure job be safely re-run without double-processing.
spark.sql(f"""
MERGE INTO {REGISTER} t
USING (SELECT '{REQUEST_ID}' AS request_id, '{SUBJECT_KEY}' AS subject_key) s
ON t.request_id = s.request_id
WHEN NOT MATCHED THEN INSERT (request_id, subject_key, request_type, status)
  VALUES (s.request_id, s.subject_key, 'erasure', 'received')
""")

display(spark.sql(f"SELECT count(*) AS request_rows FROM {REGISTER}"))
print("Still one row. Keying the MERGE on request_id makes reprocessing idempotent.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Find the Subject's Data
# MAGIC
# MAGIC Before deleting, locate every table that holds the subject's data. Two sources answer that without a manual column hunt: **classification** tells you which columns are personal data and **lineage** tells you where a table's data propagated. Both are read from the snapshots the course provisions, because a live scan is asynchronous and lineage lags.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="text-align:center; margin-bottom: 12px;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700;">Classification says which columns, lineage says which tables</div>
# MAGIC   <div style="font-size: 14pt; color: #5E7077; margin-top: 4px;">The subject's identifiers reach the silver table, but not the gold aggregate.</div>
# MAGIC </div>
# MAGIC <div style="background:#FFFFFF; border-radius:8px; padding:12px; box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <svg width="100%" viewBox="0 0 960 300" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="ln-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="30" y="15" width="200" height="30" rx="15" fill="#CD7F32"/><text x="130" y="35" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF" letter-spacing="2">BRONZE</text>
# MAGIC   <rect x="375" y="15" width="200" height="30" rx="15" fill="#5A6F77"/><text x="475" y="35" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF" letter-spacing="2">SILVER</text>
# MAGIC   <rect x="730" y="15" width="200" height="30" rx="15" fill="#FFAB00"/><text x="830" y="35" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" letter-spacing="2">GOLD</text>
# MAGIC   <rect x="30" y="95" width="200" height="96" rx="8" fill="#CD7F32" fill-opacity="0.10" stroke="#CD7F32" stroke-width="1.6"/>
# MAGIC   <text x="130" y="130" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">customer_profile</text>
# MAGIC   <text x="130" y="152" text-anchor="middle" font-size="13" fill="#5E7077">authoritative source</text>
# MAGIC   <text x="130" y="172" text-anchor="middle" font-size="13" fill="#98102A">holds the identifiers</text>
# MAGIC   <rect x="30" y="222" width="170" height="54" rx="8" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.3"/>
# MAGIC   <text x="115" y="246" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" font-family="monospace">loyalty</text>
# MAGIC   <text x="115" y="264" text-anchor="middle" font-size="12" fill="#5E7077">joined in</text>
# MAGIC   <rect x="375" y="95" width="200" height="96" rx="8" fill="#5A6F77" fill-opacity="0.10" stroke="#5A6F77" stroke-width="1.6"/>
# MAGIC   <text x="475" y="130" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">customer_enriched</text>
# MAGIC   <text x="475" y="152" text-anchor="middle" font-size="13" fill="#5E7077">downstream copy</text>
# MAGIC   <text x="475" y="172" text-anchor="middle" font-size="13" fill="#98102A">identifiers land here too</text>
# MAGIC   <rect x="730" y="95" width="200" height="96" rx="8" fill="#FFAB00" fill-opacity="0.12" stroke="#FFAB00" stroke-width="1.6"/>
# MAGIC   <text x="830" y="130" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" font-family="monospace">customer_metrics_gold</text>
# MAGIC   <text x="830" y="152" text-anchor="middle" font-size="13" fill="#5E7077">aggregate by country</text>
# MAGIC   <text x="830" y="172" text-anchor="middle" font-size="13" fill="#007a53">no identifiers</text>
# MAGIC   <path d="M230 135 L369 135" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#ln-arrow)"/>
# MAGIC   <text x="300" y="122" text-anchor="middle" font-size="12" fill="#5E7077" font-weight="700">identifiers flow</text>
# MAGIC   <path d="M200 245 L360 175" fill="none" stroke="#5E7077" stroke-width="1.6" marker-end="url(#ln-arrow)"/>
# MAGIC   <path d="M575 135 L724 135" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#ln-arrow)"/>
# MAGIC   <text x="650" y="122" text-anchor="middle" font-size="12" fill="#5E7077" font-weight="700">aggregated</text>
# MAGIC   <text x="480" y="294" text-anchor="middle" font-size="13" fill="#0b2026">Erasure scope: the two identifiable tables. The gold aggregate needs no per-row deletion.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why classification and lineage, together</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Classification answers "which columns".** It flags the personal-data columns in a table, so you know what you're erasing and what to check afterward, without reading every column by hand.
# MAGIC - **Lineage answers "which tables".** A subject's data rarely sits in one place. Lineage shows the copies and derivations, so the erasure scope is the source plus everything downstream that still identifies the person.
# MAGIC - **A non-identifying aggregate can be out of scope.** `customer_metrics_gold` groups by country and loyalty tier, so no row is about one person. Column lineage confirms the identifiers stop at silver, so gold needs no per-row deletion, only recomputation if the counts must change.
# MAGIC - **These are snapshots on purpose.** Classification scans asynchronously and lineage lags by minutes to hours, so the course reads point-in-time snapshots for a reproducible demo. In production you would read the live `system.data_classification.results` and `system.access.*_lineage`.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# The classification snapshot flags the personal-data columns. 
# These are the columns you're erasing when you delete the subject's rows and the columns to check are gone afterward.
display(spark.sql(f"""
SELECT table_name, column_name, class_tag, confidence
FROM {CLASS_SNAP}
WHERE table_name = 'customer_profile' AND confidence = 'HIGH'
ORDER BY column_name
"""))

# COMMAND ----------

# Table lineage shows the chain: customer_profile feeds customer_enriched, which feeds the gold aggregate.
# Column lineage shows the identifiers reach customer_enriched.
# Identifiers don't appear in customer_metrics_gold though, so the aggregate carries no personal identifier.
print("Table lineage for the estate:")
display(spark.sql(f"""
SELECT DISTINCT source_table_name AS source_table, target_table_name AS target_table
FROM {TABLE_LINEAGE}
ORDER BY source_table, target_table
"""))

print("Column lineage for the subject's identifier columns (email, national_id, phone):")
display(spark.sql(f"""
SELECT DISTINCT source_column_name AS identifier, target_table_name AS reaches_table, target_column_name AS as_column
FROM {COLUMN_LINEAGE}
WHERE source_table_name = 'customer_profile'
  AND source_column_name IN ('email', 'national_id', 'phone')
ORDER BY identifier, reaches_table
"""))

# COMMAND ----------

# Show the subject's rows in the two identifiable tables, then a count per table. 
# This is a read only step on the shared estate: it locates the subject, it doesn't change anything.
# The gold aggregate has no customer_id to count, it doesn't identify the person.
# The subject's identifiers, in both tables: the same values propagated from profile to enriched.
display(spark.sql(f"""
SELECT 'customer_profile' AS table_name, customer_id, email, national_id, phone, country
FROM `{CATALOG}`.`{DEMO_SCHEMA}`.customer_profile WHERE customer_id = {SUBJECT_ID}
UNION ALL
SELECT 'customer_enriched' AS table_name, customer_id, email, national_id, phone, country
FROM `{CATALOG}`.`{DEMO_SCHEMA}`.customer_enriched WHERE customer_id = {SUBJECT_ID}
"""))

# The count per table confirms the subject is present in both.
display(spark.sql(f"""
SELECT 'customer_profile' AS table_name, count(*) AS subject_rows
FROM `{CATALOG}`.`{DEMO_SCHEMA}`.customer_profile WHERE customer_id = {SUBJECT_ID}
UNION ALL
SELECT 'customer_enriched' AS table_name, count(*) AS subject_rows
FROM `{CATALOG}`.`{DEMO_SCHEMA}`.customer_enriched WHERE customer_id = {SUBJECT_ID}
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-callout { border-left: 6px solid #1B5162; border-radius: 8px; padding: 16px 22px; background: #F8F9FC; font-size: 14pt; line-height: 1.6; color: #0b2026; }
# MAGIC .ds-eyebrow { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #1B5162; margin-bottom: 6px; }
# MAGIC .ds-callout ul { margin: 8px 0 0; padding-left: 20px; }
# MAGIC .ds-callout li { margin-bottom: 8px; }
# MAGIC </style>
# MAGIC <div class="ds-callout">
# MAGIC   <div class="ds-eyebrow">The erasure scope</div>
# MAGIC   <ul>
# MAGIC     <li>The subject's data lives in <strong>customer_profile</strong> (authoritative) and <strong>customer_enriched</strong> (downstream). Both must be erased.</li>
# MAGIC     <li><strong>customer_metrics_gold</strong> is a non-identifying aggregate. It needs no per-row deletion, only recomputation if the counts must change.</li>
# MAGIC     <li>From here on, the destructive steps run on the isolated <code>erasure_bronze</code> (a copy of <code>customer_profile</code>) and <code>erasure_silver</code> (a copy of <code>customer_enriched</code>), which carry the same subject and have deletion vectors enabled, so the shared estate is never changed.</li>
# MAGIC   </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 22px 26px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Ask Genie Code</div>
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">You found the downstream tables by reading lineage yourself, which is the skill. Once you can read it, <strong>Genie Code</strong> can help you ask the same question in the notebook or SQL editor. Click <strong>Copy</strong> and paste the prompt, then check its answer against the lineage you just read.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">Which tables are downstream of priv_demo.customer_profile and which of their columns come from its email or national_id column?</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Delete From the Authoritative Layer, Then Propagate
# MAGIC
# MAGIC Run first, then diagnose. Delete the subject from the earliest authoritative layer, **erasure_bronze**, with a `DELETE`, then propagate the delete downstream to **erasure_silver** with a `MERGE`. Delete from the source first so the authoritative record goes before its copies.
# MAGIC
# MAGIC > What you should see: the subject is present before, one row is deleted from each table and the subject is absent from the current state of both afterward.

# COMMAND ----------

# The subject is in both tables. DESCRIBE DETAIL shows the file count now, so you can compare it after the delete. 
# A small seed means one data file, which keeps the deletion-vector behavior clear.
print("Subject rows before the delete:")
display(spark.sql(f"""
SELECT 'erasure_bronze' AS table_name, count(*) AS subject_rows FROM {BRONZE} WHERE customer_id = {SUBJECT_ID}
UNION ALL
SELECT 'erasure_silver' AS table_name, count(*) AS subject_rows FROM {SILVER} WHERE customer_id = {SUBJECT_ID}
"""))

# Remember the file count and size now, so E1 can show them next to the after-delete numbers.
_before = spark.sql(f"DESCRIBE DETAIL {BRONZE}").select("numFiles", "sizeInBytes").collect()[0]
FILES_BEFORE_DELETE, SIZE_BEFORE_DELETE = _before["numFiles"], _before["sizeInBytes"]
print(f"erasure_bronze before the delete: {FILES_BEFORE_DELETE} data file(s), {SIZE_BEFORE_DELETE} bytes.")

# COMMAND ----------

# DELETE removes the subject from the current table state. 
# The returned num_affected_rows confirms one row was deleted. 
# Because deletion vectors are enabled, this is a soft delete, which Section E examines. 
# For now, note that the delete succeeded.
display(spark.sql(f"DELETE FROM {BRONZE} WHERE customer_id = {SUBJECT_ID}"))

# COMMAND ----------

# MERGE ... WHEN MATCHED THEN DELETE is the propagation form.
# It deletes the subject from the downstream copy and is safe to re-run: if the subject is already gone, nothing matches.
# Use plain batch SQL like this for downstream propagation.
display(spark.sql(f"""
MERGE INTO {SILVER} t
USING (SELECT {SUBJECT_ID} AS customer_id) s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN DELETE
"""))

# COMMAND ----------

# The subject is now absent from the current state of both tables. 
# Re-running the DELETE affects zero rows, which is a no-op, not an error. 
print("Subject rows after the delete (expect 0 in both):")
display(spark.sql(f"""
SELECT 'erasure_bronze' AS table_name, count(*) AS subject_rows FROM {BRONZE} WHERE customer_id = {SUBJECT_ID}
UNION ALL
SELECT 'erasure_silver' AS table_name, count(*) AS subject_rows FROM {SILVER} WHERE customer_id = {SUBJECT_ID}
"""))

print("Re-running the DELETE (expect num_affected_rows = 0):")
display(spark.sql(f"DELETE FROM {BRONZE} WHERE customer_id = {SUBJECT_ID}"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-callout { border-left: 6px solid #FFAB00; border-radius: 8px; padding: 16px 22px; background: rgba(255,171,0,0.12); font-size: 14pt; line-height: 1.6; color: #0b2026; }
# MAGIC .ds-eyebrow { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #B27A00; margin-bottom: 6px; }
# MAGIC .ds-callout ul { margin: 8px 0 0; padding-left: 20px; }
# MAGIC .ds-callout li { margin-bottom: 8px; }
# MAGIC </style>
# MAGIC <div class="ds-callout">
# MAGIC   <div class="ds-eyebrow">Streaming versus batch</div>
# MAGIC   <ul>
# MAGIC     <li>You may see guidance that wires erasure through <strong>Structured Streaming</strong>, with Change Data Feed internals and options like <code>ignoreDeletes</code> and <code>skipChangeCommits</code> to keep a stream from breaking on a delete.</li>
# MAGIC     <li>For a bounded set of subjects, that's more machinery than the job needs. A batch <code>DELETE</code> on the source and a <code>MERGE</code> to propagate downstream is simpler to reason about and to prove, which is what this course uses.</li>
# MAGIC     <li>A third option, batch <strong>Change Data Feed</strong>, fits when a delete must reach many downstream tables. It stays batch, not streaming. The next cell shows where it fits.</li>
# MAGIC   </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .cdf-title { font-size: 18pt; font-weight: 700; color: #0b2026; }
# MAGIC .cdf-sub { font-size: 13pt; color: #5E7077; margin: 2px 0 14px; }
# MAGIC .cdf-intro { font-size: 14pt; line-height: 1.55; margin-bottom: 8px; }
# MAGIC .cdf-wrap { background: #fff; border-radius: 8px; padding: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .cdf-note { border-left: 6px solid #4299E0; border-radius: 8px; padding: 14px 20px; background: #F8F9FC; font-size: 14pt; line-height: 1.55; margin-top: 16px; }
# MAGIC </style>
# MAGIC <div class="cdf-title">Change Data Feed: an option for propagating at scale</div>
# MAGIC <div class="cdf-sub">Optional. The demo propagates with a batch MERGE. This is where CDF fits instead.</div>
# MAGIC <div class="cdf-intro">Change Data Feed (CDF) records every row-level change a table commits, so you can read what changed between versions rather than only the current snapshot. After you delete the subject from the source, its change feed holds exactly the deleted rows. One batch read can then drive the same delete into many downstream tables at once.</div>
# MAGIC <div class="cdf-wrap">
# MAGIC <svg width="100%" viewBox="0 0 1000 300" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="cf-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="24" y="112" width="214" height="76" rx="8" fill="#CD7F32" fill-opacity="0.10" stroke="#CD7F32" stroke-width="1.6"/>
# MAGIC   <text x="131" y="144" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">erasure_bronze</text>
# MAGIC   <text x="131" y="168" text-anchor="middle" font-size="12.5" fill="#98102A">DELETE id 4212 (committed)</text>
# MAGIC   <path d="M238 150 L330 150" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#cf-arrow)"/>
# MAGIC   <text x="284" y="140" text-anchor="middle" font-size="11.5" font-weight="700" fill="#618794">commit</text>
# MAGIC   <rect x="334" y="104" width="300" height="96" rx="10" fill="#4299E0" fill-opacity="0.06" stroke="#4299E0" stroke-width="1.7"/>
# MAGIC   <text x="484" y="130" text-anchor="middle" font-size="14" font-weight="700" fill="#2272B4">Change Data Feed</text>
# MAGIC   <text x="484" y="149" text-anchor="middle" font-size="11.5" fill="#5E7077">the table's committed row changes</text>
# MAGIC   <rect x="352" y="159" width="264" height="30" rx="6" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/>
# MAGIC   <text x="484" y="179" text-anchor="middle" font-size="12" fill="#0b2026" font-family="monospace">_change_type=delete &#183; id 4212</text>
# MAGIC   <path d="M636 146 L740 70" fill="none" stroke="#5E7077" stroke-width="1.7" marker-end="url(#cf-arrow)"/>
# MAGIC   <path d="M636 150 L740 152" fill="none" stroke="#5E7077" stroke-width="1.7" marker-end="url(#cf-arrow)"/>
# MAGIC   <path d="M636 154 L740 234" fill="none" stroke="#5E7077" stroke-width="1.7" marker-end="url(#cf-arrow)"/>
# MAGIC   <text x="690" y="128" text-anchor="middle" font-size="11" font-weight="700" fill="#618794">apply to each</text>
# MAGIC   <rect x="742" y="44" width="234" height="52" rx="8" fill="#FFFFFF" stroke="#5A6F77" stroke-width="1.4"/>
# MAGIC   <text x="859" y="66" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" font-family="monospace">erasure_silver</text>
# MAGIC   <text x="859" y="85" text-anchor="middle" font-size="11" fill="#5E7077">downstream copy</text>
# MAGIC   <rect x="742" y="126" width="234" height="52" rx="8" fill="#FFFFFF" stroke="#5A6F77" stroke-width="1.4"/>
# MAGIC   <text x="859" y="148" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" font-family="monospace">customer_360</text>
# MAGIC   <text x="859" y="167" text-anchor="middle" font-size="11" fill="#5E7077">another copy</text>
# MAGIC   <rect x="742" y="208" width="234" height="52" rx="8" fill="#FFFFFF" stroke="#5A6F77" stroke-width="1.4"/>
# MAGIC   <text x="859" y="230" text-anchor="middle" font-size="13" font-weight="700" fill="#0b2026" font-family="monospace">ml_features</text>
# MAGIC   <text x="859" y="249" text-anchor="middle" font-size="11" fill="#5E7077">and any other</text>
# MAGIC   <text x="24" y="288" font-size="13" fill="#0b2026">One read of the committed deletes drives them into every downstream table. The taught MERGE handles one table at a time.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC <div class="cdf-note"><strong>When to reach for it:</strong> for this demo, with one subject and two tables, the batch <code>DELETE</code> and <code>MERGE</code> are simpler to prove. CDF earns its place when the fan-out is large or you want one change-driven job to cover every copy.</div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">How you read the change feed</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Enable it per table.** Set `delta.enableChangeDataFeed = true`. From then on Delta records inserts, updates and deletes as change rows, at the cost of a little extra write and storage.
# MAGIC - **Read it in batch.** `SELECT * FROM table_changes('schema.table', startVersion)`, or the DataFrame `readChangeFeed` option with a `startingVersion`. This is a batch read of committed history, not a running stream.
# MAGIC - **Each change row is labelled.** `_change_type` is `insert`, `update_preimage`, `update_postimage` or `delete`, alongside `_commit_version` and `_commit_timestamp`. For erasure you filter to the `delete` rows and apply them downstream.
# MAGIC - **The feed still holds the value.** Like time travel, the change records keep the old values until they age out with the retention window, so they are one more place a deleted value lingers until `VACUUM` and retention clear it (Sections F and G).
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Logical Delete Isn't Physical Purge
# MAGIC
# MAGIC The `DELETE` succeeded and the subject is gone from every query. That proves current-state absence. It doesn't prove the value is gone from storage. On a table with **deletion vectors** enabled, a `DELETE` marks the row deleted in metadata and doesn't rewrite the data file, so the value physically remains in the file until a later rewrite.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="background:#FFFFFF; border-radius:8px; padding:12px; box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <svg width="100%" viewBox="0 0 970 320" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="dv-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- data file -->
# MAGIC   <text x="45" y="40" font-size="12" font-weight="700" fill="#1B5162" letter-spacing="1.5">DATA FILE (PARQUET)</text>
# MAGIC   <rect x="30" y="50" width="250" height="215" rx="10" fill="#F9F7F4" stroke="#5E7077" stroke-width="1.5"/>
# MAGIC   <rect x="50" y="70" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="155" y="92" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 4210</text>
# MAGIC   <rect x="50" y="112" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#FF5F46" stroke-width="2"/><text x="155" y="134" text-anchor="middle" font-size="13" fill="#98102A" font-family="monospace">customer 4212  (subject)</text>
# MAGIC   <rect x="50" y="154" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="155" y="176" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 4213</text>
# MAGIC   <rect x="50" y="196" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="155" y="218" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 4300</text>
# MAGIC   <text x="155" y="252" text-anchor="middle" font-size="12" fill="#5E7077">all rows still physically present</text>
# MAGIC   <!-- gate: deletion vector applied at read -->
# MAGIC   <path d="M280 157 L360 157" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#dv-arrow)"/>
# MAGIC   <rect x="365" y="112" width="230" height="92" rx="10" fill="#FFAB00" fill-opacity="0.14" stroke="#FFAB00" stroke-width="1.8"/>
# MAGIC   <text x="480" y="140" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">deletion vector</text>
# MAGIC   <text x="480" y="162" text-anchor="middle" font-size="12" fill="#5E7077">marks row 4212 deleted,</text>
# MAGIC   <text x="480" y="180" text-anchor="middle" font-size="12" fill="#5E7077">in metadata, applied at read</text>
# MAGIC   <path d="M595 157 L675 157" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#dv-arrow)"/>
# MAGIC   <!-- current state -->
# MAGIC   <text x="695" y="40" font-size="12" font-weight="700" fill="#00A972" letter-spacing="1.5">CURRENT TABLE STATE</text>
# MAGIC   <rect x="680" y="50" width="250" height="215" rx="10" fill="#F9F7F4" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <rect x="700" y="82" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="805" y="104" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 4210</text>
# MAGIC   <rect x="700" y="124" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="805" y="146" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 4213</text>
# MAGIC   <rect x="700" y="166" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="805" y="188" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 4300</text>
# MAGIC   <text x="805" y="230" text-anchor="middle" font-size="12" fill="#007a53">4212 absent from every query</text>
# MAGIC   <text x="485" y="298" text-anchor="middle" font-size="13" fill="#0b2026">A DELETE on a deletion-vector table is a soft delete: the file isn't rewritten, so the value for 4212 stays in the file until REORG rewrites it.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why deletion vectors matter for erasure</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **They exist for speed.** Rewriting a whole Parquet file to remove a few rows is expensive. A deletion vector records which rows are removed, so `DELETE`, `UPDATE` and `MERGE` are much faster. The cost is deferred to read time and to a later file rewrite.
# MAGIC - **They're enabled with a table property.** `delta.enableDeletionVectors` isn't on by default for Delta tables. The erasure tables here set it, which is why the first delete is a soft delete. Some managed tables have it on, so check the table's properties rather than assume.
# MAGIC - **The privacy consequence is the whole point here.** Because the file isn't rewritten, the deleted value physically remains until a rewrite materializes the deletion. So a successful `DELETE` proves current-state absence, not physical removal.
# MAGIC - **How they get materialized.** `OPTIMIZE`, `REORG TABLE ... APPLY (PURGE)` or a write with auto-compaction rewrite the files and bake in the deletes. For erasure you use `REORG ... APPLY (PURGE)`, then `VACUUM`.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC The diagram shows the mechanism. Two checks on the real table confirm it. Both should agree.
# MAGIC
# MAGIC | Check | What you run | Soft-delete signal |
# MAGIC | --- | --- | --- |
# MAGIC | **E1. The data files** | `DESCRIBE DETAIL` before and after | The file count and size are the same. A rewrite would have changed them. |
# MAGIC | **E2. The operation log** | `DESCRIBE HISTORY` on the DELETE | `numDeletedRows = 1` but `numRemovedFiles = 0`. Zero files removed means no rewrite. |
# MAGIC
# MAGIC So the subject is gone from every query, yet their values are still in the file. Read `numRemovedFiles` as the signal: a soft delete can still add a tiny deletion-vector file, so `numAddedFiles` may be non-zero without a rewrite. Section F does the rewrite.

# COMMAND ----------

# Same data files before and after, so nothing was rewritten.
_after = spark.sql(f"DESCRIBE DETAIL {BRONZE}").select("numFiles", "sizeInBytes").collect()[0]
print("erasure_bronze data files, before versus after the delete:")
display(spark.createDataFrame(
    [("before the delete", FILES_BEFORE_DELETE, SIZE_BEFORE_DELETE),
     ("after the delete", _after["numFiles"], _after["sizeInBytes"])],
    ["stage", "num_files", "size_bytes"]
))

# COMMAND ----------

# numRemovedFiles = 0 on the DELETE is the soft-delete signal (the two checks above explain why).
# Oldest first, so the first row is the real delete; a later 0-row DELETE is the idempotent re-run.
hist = spark.sql(f"DESCRIBE HISTORY {BRONZE}")
display(
    hist.selectExpr(
        "version",
        "operation",
        "operationMetrics['numDeletedRows'] AS numDeletedRows",
        "operationMetrics['numRemovedFiles'] AS numRemovedFiles",
        "operationMetrics['numAddedFiles'] AS numAddedFiles",
    )
    .where("operation = 'DELETE'")
    .orderBy(F.col("version").asc())
    .limit(5)
)

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
# MAGIC <div class="rc-hint">A deleted value can persist in three places. <strong>Click each card</strong> to see what clears it.</div>
# MAGIC
# MAGIC <div class="rc-row">
# MAGIC   <span class="rc-box" data-id="0" onclick="rcSelect(0)" style="--pc:#FF5F46; border-top-color:#FF5F46;">
# MAGIC     <span class="rc-label" style="color:#FF5F46;">Data files</span>
# MAGIC     <span class="rc-title">Deletion vector, file not rewritten</span>
# MAGIC   </span><span class="rc-box" data-id="1" onclick="rcSelect(1)" style="--pc:#4299E0; border-top-color:#4299E0;">
# MAGIC     <span class="rc-label" style="color:#4299E0;">Older versions</span>
# MAGIC     <span class="rc-title">Time travel can still read the value</span>
# MAGIC   </span><span class="rc-box" data-id="2" onclick="rcSelect(2)" style="--pc:#618794; border-top-color:#618794;">
# MAGIC     <span class="rc-label" style="color:#618794;">Change Data Feed</span>
# MAGIC     <span class="rc-title">Change records hold the old value</span>
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
# MAGIC   { title: "In the data files", color: "#FF5F46", text: "<ul><li>With deletion vectors, the DELETE didn't rewrite the Parquet file, so the value is still in it.</li><li><strong>Cleared by</strong> REORG TABLE ... APPLY (PURGE), which rewrites the files without the deleted rows (Section F).</li></ul>" },
# MAGIC   { title: "In older table versions", color: "#4299E0", text: "<ul><li>Delta keeps older versions and time travel can read files that still contain the value.</li><li><strong>Cleared by</strong> VACUUM, once those files are unreferenced and older than the retention window (Section G).</li></ul>" },
# MAGIC   { title: "In the Change Data Feed", color: "#618794", text: "<ul><li>If Change Data Feed is enabled, its change records hold the old value under the same retention.</li><li><strong>Cleared by</strong> the same retention: the change data ages out with the file retention window, so plan for it.</li></ul>" }
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

# MAGIC %md
# MAGIC ## F. Physical Purge With `REORG TABLE ... APPLY (PURGE)`
# MAGIC
# MAGIC To actually remove the value from the current files, rewrite them. `REORG TABLE ... APPLY (PURGE)` rewrites the files that contain soft-deleted data, materializing the deletion vectors so the current version's files no longer hold the deleted rows. It creates a new table version, but it doesn't delete the old files. Those come out with `VACUUM` in Section G.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .sf-row { display: flex; align-items: stretch; justify-content: center; gap: 0; flex-wrap: wrap; }
# MAGIC .sf-step { flex: 1 1 250px; max-width: 300px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 18px 18px 16px; border-top: 8px solid var(--accent); text-align: center; }
# MAGIC .sf-step.dim { opacity: 0.55; }
# MAGIC .sf-num { width: 36px; height: 36px; border-radius: 50%; background: var(--accent); color: #fff; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 16pt; margin: 2px auto 12px; }
# MAGIC .sf-title { font-size: 15pt; font-weight: 700; color: #0b2026; margin-bottom: 8px; }
# MAGIC .sf-code { font-family: monospace; font-size: 14pt; color: #0b2026; background: #EEEDE9; border-radius: 4px; padding: 3px 6px; display: inline-block; margin-bottom: 8px; }
# MAGIC .sf-desc { font-size: 14pt; color: #5E7077; line-height: 1.5; }
# MAGIC .sf-arrow { flex: 0 0 44px; display: flex; align-items: center; justify-content: center; font-size: 30pt; color: #618794; }
# MAGIC </style>
# MAGIC <div class="sf-row">
# MAGIC   <div class="sf-step" style="--accent:#FF5F46;">
# MAGIC     <div class="sf-num">1</div>
# MAGIC     <div class="sf-title">Delete</div>
# MAGIC     <div class="sf-code">DELETE / MERGE</div>
# MAGIC     <div class="sf-desc">Soft delete: a deletion vector marks the row, the file isn't rewritten.</div>
# MAGIC   </div>
# MAGIC   <div class="sf-arrow">&rarr;</div>
# MAGIC   <div class="sf-step" style="--accent:#00A972;">
# MAGIC     <div class="sf-num">2</div>
# MAGIC     <div class="sf-title">Rewrite</div>
# MAGIC     <div class="sf-code">REORG ... APPLY (PURGE)</div>
# MAGIC     <div class="sf-desc">Rewrites the files without the deleted rows. Old files still exist.</div>
# MAGIC   </div>
# MAGIC   <div class="sf-arrow">&rarr;</div>
# MAGIC   <div class="sf-step dim" style="--accent:#1B5162;">
# MAGIC     <div class="sf-num">3</div>
# MAGIC     <div class="sf-title">Remove old files</div>
# MAGIC     <div class="sf-code">VACUUM</div>
# MAGIC     <div class="sf-desc">Removes unreferenced files, once they're older than the retention window.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">What REORG does and doesn't do</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **APPLY (PURGE) only rewrites files that contain soft-deleted data.** It materializes the deletion vectors, so the current version's files no longer hold the deleted rows.
# MAGIC - **It doesn't remove the old files.** The docs are explicit: after `APPLY (PURGE)` the soft-deleted data may still exist in the old files and you run `VACUUM` to physically delete them. That's step three.
# MAGIC - **It's idempotent.** Running `REORG ... APPLY (PURGE)` again on the same data has no effect, so it's safe to re-run.
# MAGIC - **You can scope it.** `REORG TABLE t WHERE predicate APPLY (PURGE)` limits the rewrite to matching files, which matters on a large table where you don't want to rewrite everything.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# REORG rewrites the file(s) that hold soft-deleted data, so the current version no longer contains the subject's value. 
# It returns a small metrics result. 
# It doesn't remove the old files, VACUUM does.
display(spark.sql(f"REORG TABLE {BRONZE} APPLY (PURGE)"))

# COMMAND ----------

# The most recent operation is the rewrite (recorded as an OPTIMIZE / REORG operation). 
# Unlike the soft delete, it shows files removed and files added, which is the file rewrite that materialized the deletion. 
# The old files are now unreferenced by the current version, ready for VACUUM.
hist = spark.sql(f"DESCRIBE HISTORY {BRONZE}")
display(
    hist.selectExpr(
        "version",
        "operation",
        "operationMetrics['numRemovedFiles'] AS numRemovedFiles",
        "operationMetrics['numAddedFiles'] AS numAddedFiles",
    )
    .orderBy(F.col("version").desc())
    .limit(4)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Retention-Aware `VACUUM` With `DRY RUN`
# MAGIC
# MAGIC `REORG` left the old files in place. 
# MAGIC <br /><br />
# MAGIC `VACUUM` removes data files that are no longer referenced by the current version and are older than the retention threshold. 
# MAGIC <br /><br />
# MAGIC The default threshold is 168 hours, 7 days, set by `delta.deletedFileRetentionDuration`. Use `DRY RUN` to preview which files it would remove before running it for real.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .vac-row { display: flex; gap: 14px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .vac-card { flex: 1 1 340px; min-width: 300px; background: #fff; border: 1px solid #EEEDE9; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid var(--c); overflow: hidden; }
# MAGIC .vac-hd { padding: 11px 16px; font-weight: 700; font-size: 15pt; color: #fff; background: var(--c); }
# MAGIC .vac-body { padding: 14px 16px; }
# MAGIC .vac-file { font-size: 12.5pt; color: #5E7077; margin-bottom: 10px; line-height: 1.45; }
# MAGIC .vac-file code { font-family: monospace; color: #0b2026; }
# MAGIC .vac-cond { display: flex; justify-content: space-between; align-items: center; gap: 10px; padding: 8px 0; border-bottom: 1px solid #EEEDE9; font-size: 13.5pt; }
# MAGIC .vac-badge { flex: 0 0 auto; font-size: 11.5pt; font-weight: 700; border-radius: 5px; padding: 2px 10px; }
# MAGIC .badge-yes { background: rgba(0,169,114,0.14); color: #007a53; }
# MAGIC .badge-no { background: rgba(255,171,0,0.20); color: #B27A00; }
# MAGIC .vac-out { margin-top: 12px; font-size: 14pt; font-weight: 700; font-family: monospace; }
# MAGIC .vac-mid { flex: 0 0 auto; display: flex; flex-direction: column; align-items: center; justify-content: center; min-width: 110px; text-align: center; color: #5E7077; }
# MAGIC .vac-mid-arrow { font-size: 24pt; color: #618794; line-height: 1; }
# MAGIC .vac-note { border-left: 6px solid #FFAB00; background: rgba(255,171,0,0.10); border-radius: 8px; padding: 14px 20px; font-size: 14pt; line-height: 1.55; margin-top: 16px; }
# MAGIC </style>
# MAGIC <div class="vac-row">
# MAGIC   <div class="vac-card" style="--c:#B27A00;">
# MAGIC     <div class="vac-hd">Right after REORG (now)</div>
# MAGIC     <div class="vac-body">
# MAGIC       <div class="vac-file">The old file is still on disk and holds <code>4212</code>'s value.</div>
# MAGIC       <div class="vac-cond"><span>Unreferenced by the current version</span><span class="vac-badge badge-yes">YES</span></div>
# MAGIC       <div class="vac-cond"><span>Older than 168h (7 days)</span><span class="vac-badge badge-no">NOT YET</span></div>
# MAGIC       <div class="vac-out" style="color:#B27A00;">VACUUM ... DRY RUN &rarr; 0 files</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="vac-mid">
# MAGIC     <div class="vac-mid-arrow">&rarr;</div>
# MAGIC     <div style="font-size:12.5pt; font-weight:700; margin-top:4px;">168h passes</div>
# MAGIC     <div style="font-size:11.5pt;">the retention window</div>
# MAGIC   </div>
# MAGIC   <div class="vac-card" style="--c:#00A972;">
# MAGIC     <div class="vac-hd">After the 168h window</div>
# MAGIC     <div class="vac-body">
# MAGIC       <div class="vac-file">The same unreferenced file is now old enough to remove.</div>
# MAGIC       <div class="vac-cond"><span>Unreferenced by the current version</span><span class="vac-badge badge-yes">YES</span></div>
# MAGIC       <div class="vac-cond"><span>Older than 168h (7 days)</span><span class="vac-badge badge-yes">YES</span></div>
# MAGIC       <div class="vac-out" style="color:#007a53;">VACUUM removes the file</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div class="vac-note"><strong>VACUUM removes a file only when both are true.</strong> Right after REORG the old file is unreferenced but only minutes old, so <code>DRY RUN</code> lists nothing yet. That is the 168-hour window working, not a failure. Plan the purge to finish after the window passes.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="cw" style="margin-top:16px;">
# MAGIC <div style="border-left:6px solid #98102A; background:rgba(152,16,42,0.08); border-radius:8px; padding:16px 22px; font-size:14pt; line-height:1.55; color:#0b2026;">
# MAGIC   <div style="font-size:15pt; font-weight:700; color:#98102A; margin-bottom:6px;">Zero-retention VACUUM is a demo shortcut, not a production practice</div>
# MAGIC   In production a <code>VACUUM</code> only removes files older than the retention window (default 168h). The window protects in-flight reads and writes, because a shorter window can delete files a running job still needs and corrupt the table.
# MAGIC   <br/><br/>
# MAGIC   To show the purged end state now, without waiting seven days, the next cell temporarily lowers this demo table's <code>delta.deletedFileRetentionDuration</code> to zero, runs <code>VACUUM</code>, then restores 168 hours. Treat it as a demo-only shortcut. For a real erasure, plan the purge to complete after the window elapses. The same window is what clears time travel and Change Data Feed copies.
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# DRY RUN lists files that are unreferenced AND older than the retention threshold. 
# Right after the REORG the unreferenced files are only minutes old, so it lists nothing yet. 
# That's the 168-hour window working. 
# A real VACUUM deletes the eligible files once the window passes.
print("Files VACUUM would remove right now (likely none, because nothing is older than 168 hours yet):")
display(spark.sql(f"VACUUM {BRONZE} DRY RUN"))

# COMMAND ----------

# Demo shortcut on the isolated erasure_bronze.
# Setting the table's retention to zero lets VACUUM remove all unreferenced files immediately; it's restored in the finally.
# In production you let the 168-hour window elapse instead of forcing it.
# See the warning above.
# (Serverless note: the session config that would disable the retention check is not available on Spark Connect, so this lowers the table property instead, which achieves the same thing.)
spark.sql(f"ALTER TABLE {BRONZE} SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours')")
try:
    print("Old, unreferenced files that still hold 4212's value (the default window was hiding these):")
    display(spark.sql(f"VACUUM {BRONZE} DRY RUN"))

    print("Purging them now (demo only)..")
    spark.sql(f"VACUUM {BRONZE}")

    print("Nothing left to remove: the old files are gone, so 4212's value is purged from storage.")
    display(spark.sql(f"VACUUM {BRONZE} DRY RUN"))
finally:
    spark.sql(f"ALTER TABLE {BRONZE} SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 168 hours')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. `DESCRIBE HISTORY` as Evidence
# MAGIC
# MAGIC The table history is one evidence source. It records that the operations ran, with metrics: the soft `DELETE`, the `REORG` rewrite and the `VACUUM`. The `VACUUM` is there because the demo forced it to zero retention to show the purged end state; in production it runs after the retention window. This is supporting evidence, not a complete privacy audit. That fuller picture comes from the system tables in the next demo. Here it's enough to close the request with a concrete reference.

# COMMAND ----------

# The history shows the erasure sequence, newest first: the VACUUM, the REORG rewrite, the DELETE soft delete and the initial write. 
# This is the evidence that the delete, the rewrite and the purge ran on this table.
hist = spark.sql(f"DESCRIBE HISTORY {BRONZE}")
display(
    hist.select("version", "timestamp", "operation", "operationMetrics")
    .where("operation <> 'SET TBLPROPERTIES'")
    .orderBy(F.col("version").desc())
    .limit(6)
)

# COMMAND ----------

# Update the register to verified. 
# Record the completion timestamp, the count of assets that held the subject (bronze and silver) and an evidence reference pointing at the table history. 
spark.sql(f"""
MERGE INTO {REGISTER} t
USING (SELECT '{REQUEST_ID}' AS request_id) s
ON t.request_id = s.request_id
WHEN MATCHED THEN UPDATE SET
  status = 'verified',
  affected_asset_count = 2,
  completed_at = current_timestamp(),
  evidence_ref = 'DESCRIBE HISTORY {DEMO_SCHEMA}.{BRONZE_TABLE} and {DEMO_SCHEMA}.{SILVER_TABLE}: DELETE on both, then REORG PURGE and a demo zero-retention VACUUM on {BRONZE_TABLE}; production purges each layer after the 168h window'
""")

display(spark.sql(f"""
SELECT request_id, subject_key, status, affected_asset_count, completed_at, evidence_ref
FROM {REGISTER}
"""))

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
# MAGIC     <div class="ev-title">Gone from current state</div>
# MAGIC     <div class="ev-text">The subject returns zero rows in erasure_bronze and erasure_silver.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card green">
# MAGIC     <div class="ev-badge">&check;</div>
# MAGIC     <div class="ev-title">Operations recorded</div>
# MAGIC     <div class="ev-text">History shows the DELETE, the REORG file rewrite and the VACUUM, with metrics.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card green">
# MAGIC     <div class="ev-badge">&check;</div>
# MAGIC     <div class="ev-title">Request closed</div>
# MAGIC     <div class="ev-text">The register is verified, with a completion time, an asset count and an evidence reference.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card amber">
# MAGIC     <div class="ev-badge">&#33;</div>
# MAGIC     <div class="ev-title">In production, not immediately</div>
# MAGIC     <div class="ev-text">The demo forced the purge with a zero-retention VACUUM to show the end state. In production the VACUUM runs after the 168-hour window, which is also when time travel and Change Data Feed copies clear.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, you:
# MAGIC
# MAGIC 1. **Designed** a request control plane that captures the subject key, request id, deadline, status, scope and evidence references, kept it free of raw PII and made processing idempotent with `MERGE`.
# MAGIC 2. **Found** the subject's data with classification and lineage and set the erasure scope to the identifiable tables while leaving the non-identifying aggregate out.
# MAGIC 3. **Deleted** the subject from the authoritative layer and **propagated** the delete downstream with batch SQL.
# MAGIC 4. **Diagnosed** why the `DELETE` was a logical delete on a deletion-vector table, then **purged** with `REORG TABLE ... APPLY (PURGE)` and a retention-aware `VACUUM`, forced here to zero retention to show the end state.
# MAGIC 5. Read `DESCRIBE HISTORY` as evidence and closed the request as verified.
# MAGIC
# MAGIC Next, in **Privacy Evidence with System Tables**, you'll assemble one compact evidence row for a request by following a sensitive column from classification through lineage to a downstream table, joining to query history and the audit event and adding the erasure validation you produced here.

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
# MAGIC .cl-doc-btn { display: inline-block; padding: 10px 22px; background: #FF5F46; color: #FFFFFF !important; text-decoration: none; border-radius: 6px; font-size: 14pt; font-weight: 600; min-width: 80px; }
# MAGIC .cl-doc-btn:hover { background: #FF6952; text-decoration: none; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="cl-docs">
# MAGIC   <div class="cl-docs-title">Deletion, purge and erasure guidance</div>
# MAGIC   <div class="cl-docs-sub">These behaviors and syntax change over time, so confirm the current deletion-vector, <code>REORG TABLE</code> and <code>VACUUM</code> references and the GDPR and CCPA erasure guidance rather than a static list.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/delta/deletion-vectors" target="_blank">Deletion vectors &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/sql/language-manual/delta-reorg-table" target="_blank">REORG TABLE &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum" target="_blank">VACUUM &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/ldp/gdpr" target="_blank">GDPR and CCPA erasure &rarr;</a>
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