# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">10 Lab - Fulfil and Verify an Erasure Request</span>
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
# MAGIC     <span style="color:#0b2026; font-weight:700;"><span style="color:#FFAB00;">&#9679;</span> Verify</span>
# MAGIC   </span>
# MAGIC   <span style="font-size:10.5pt; color:#618794; margin-left:auto;">steps 3-4 of 4</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab - Fulfil and Verify an Erasure Request
# MAGIC #### Duration: 35 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC An erasure request arrived for one customer of an orders estate and a prior job ran a `DELETE` on the bronze table only. It never propagated the delete downstream, never purged the files and left no evidence trail. This is a lab, so you finish the erasure yourself and prove it worked.
# MAGIC
# MAGIC You'll track the request in a control plane, use lineage to find every downstream table that still holds the subject, delete and propagate with `MERGE`, separate what a `DELETE` did from what it didn't, purge the files with `REORG TABLE ... APPLY (PURGE)` and a retention-aware `VACUUM`, verify the current state is empty while a non-identifying gold aggregate stays correct and assemble the completion evidence.
# MAGIC
# MAGIC This is the **manage and verify** step of the course and its final notebook. It puts the erasure mechanics from Demo 07 and the evidence idea from Demo 08 to work on a realistic, half-finished request. Each task has a **Reveal the solution** dropdown if you get stuck.
# MAGIC
# MAGIC > This lab runs entirely in your own isolated **priv_lab** schema, a separate orders estate from the demo retailer. It never touches the shared **priv_demo** data and its objects are named separately from the first lab's, so both labs can share the schema.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you'll be able to:
# MAGIC
# MAGIC 1. **Design** a request control plane keyed on the request id and free of raw PII and **track** the request in it.
# MAGIC 2. **Apply** lineage to find every downstream table that still holds the subject before you delete anything.
# MAGIC 3. **Delete** the subject with `MERGE` and **propagate** the delete downstream, idempotently.
# MAGIC 4. **Diagnose** logical delete versus physical purge and **configure** `REORG TABLE ... APPLY (PURGE)` with a retention-aware `VACUUM`.
# MAGIC 5. **Verify** the subject is absent from every in-scope table while the non-identifying gold aggregate stays correct and **assemble** the completion evidence.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">How to work this lab</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   Treat each lettered section as a task, not a walkthrough. Read the prompt, write your fix as SQL in the <strong>YOUR TASK</strong> cell, then run the check below it to see the result. Reveal the solution only to recover or to compare. Everything runs on your own isolated <code>priv_lab</code> orders estate, so you can re-run the setup at any time to reset the broken starting state and try again.
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
# MAGIC - A **Serverless SQL warehouse** (2X-Small is enough) for the SQL you run across the course.
# MAGIC
# MAGIC ![Serverless Select](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/common/select-serverless-v6.png)
# MAGIC <br></br>
# MAGIC   - How to select an environment version:
# MAGIC [AWS](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies#-select-an-environment-version) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/serverless/dependencies#-select-an-environment-version)
# MAGIC
# MAGIC **NOTE:** This lab uses **SQL and Python and was tested on Serverless V6**. `DELETE`, `MERGE`, `REORG TABLE ... APPLY (PURGE)` and `VACUUM` all run on serverless.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the setup below. It builds your isolated **priv_lab** orders estate and stages the half-finished erasure:
# MAGIC
# MAGIC - **orders_bronze** (authoritative) and **orders_silver** (downstream), both with deletion vectors enabled, plus the non-identifying **sales_by_country_gold** aggregate
# MAGIC - the subject already deleted from **orders_bronze** only (a soft delete, not purged), still present in **orders_silver** and still counted in **sales_by_country_gold**
# MAGIC - an empty **privacy_requests** register and the **lineage** and **classification** snapshots the find step reads
# MAGIC
# MAGIC If your workspace can't create catalogs, open `./Includes/Classroom-Setup-10` and set `FORCED_CATALOG` to an existing catalog first.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-10

# COMMAND ----------

from pyspark.sql import functions as F

# Set the session defaults so the SQL you write can use short names and bind the request metadata for this lab.
CATALOG = my_catalog
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{LAB_SCHEMA}`")

# Request metadata.
# The subject_key is pseudonymous.
# Resolving it to a concrete customer_id is an access-controlled lookup, so the register never stores the raw id.
REQUEST_ID = "DSR-2026-0731"
SUBJECT_KEY = "SUBJ-9F3C7A"
SUBJECT_ID = 1462  # resolved from the request's subject_key by an access-controlled lookup

print(f"Working in {CATALOG}.{LAB_SCHEMA}.")

# COMMAND ----------

# You write plain SQL in the YOUR TASK cells.
# Your catalog and schema are already selected, so use the bare table names below and type the request and subject values as SQL literals.
# To check your work, run the verification helpers (each is a provided function that just displays a result).
display(spark.createDataFrame([
    ("request_id", REQUEST_ID, "The erasure request id you log in the register (C, H)"),
    ("subject_key", SUBJECT_KEY, "The subject's pseudonymous key (log this, never the raw id)"),
    ("customer_id", str(SUBJECT_ID), "The resolved id you match on to find, delete and verify (D, E, G)"),
], ["value to type", "current_value", "use_for"]))

display(spark.createDataFrame([
    (BRONZE_TABLE, "table", "Authoritative order-level table with PII"),
    (SILVER_TABLE, "table", "Downstream enriched copy with PII"),
    (GOLD_TABLE, "table", "Non-identifying country aggregate you recompute in G"),
    (REGISTER_TABLE, "table", "The request register you write to in C and H"),
    (CLASS_SNAPSHOT, "snapshot", "Classification detections (the find step reads it)"),
    (COLUMN_LINEAGE_SNAP, "snapshot", "Column lineage (the find step reads it)"),
], ["name", "kind", "use_for"]))

display(spark.createDataFrame([
    ("count_subject_rows()", "Subject rows left in orders_bronze and orders_silver"),
    ("show_delete_history()", "The DELETE and MERGE operations that deleted rows (0 files removed and a deletion vector added means a soft delete)"),
    ("show_rewrite_history()", "The recent operations after REORG PURGE (files removed and added)"),
    ("show_classification()", "The HIGH-confidence personal-data columns"),
    ("show_register()", "The request register rows (no email or name column)"),
    ("show_gold()", "The gold aggregate (country totals, no identifiers)"),
], ["run this", "to see"]))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. The Scenario
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-scenario { background: #F9F7F4; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 8px solid #98102A; }
# MAGIC .ds-scenario-band { background: #98102A; color: #fff; padding: 14px 20px; text-align: center; font-size: 18pt; font-weight: 700; }
# MAGIC .ds-scenario-body { padding: 18px 22px; }
# MAGIC .ds-scenario-q { font-size: 16pt; font-style: italic; color: #0b2026; margin-bottom: 12px; text-align: center; }
# MAGIC .ds-scenario-text { font-size: 14pt; color: #0b2026; line-height: 1.6; overflow-wrap: anywhere; }
# MAGIC .ds-scenario-text ul { margin: 12px 0; padding-left: 20px; }
# MAGIC .ds-scenario-text li { margin-bottom: 8px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-scenario">
# MAGIC   <div class="ds-scenario-band">An erasure request, left half-finished</div>
# MAGIC   <div class="ds-scenario-body">
# MAGIC     <div class="ds-scenario-q">"We deleted that customer weeks ago. Why does the request still fail verification?"</div>
# MAGIC     <div class="ds-scenario-text">An erasure request came in for one customer of the orders estate. A prior job ran a <code>DELETE</code> on <b>orders_bronze</b> and stopped there.
# MAGIC       <ul>
# MAGIC         <li>The subject is gone from the <b>orders_bronze</b> current state, but the delete never reached <b>orders_silver</b>, which still identifies them.</li>
# MAGIC         <li>Their orders are still counted in <b>sales_by_country_gold</b>, the country aggregate built from silver.</li>
# MAGIC         <li>The tables use deletion vectors and nothing was purged, so the old values still sit in the data files.</li>
# MAGIC         <li>No request was ever tracked, so there's no evidence the erasure ran.</li>
# MAGIC       </ul>
# MAGIC       <strong>Your task: track the request, finish the deletion everywhere the subject lives, purge the files, verify the result and assemble the evidence.</strong>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Four things are wrong
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-row { display: flex; gap: 20px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 240px; }
# MAGIC .blue{--accent:#4299E0;} .amber{--accent:#B27A00;} .coral{--accent:#FF5F46;} .red{--accent:#98102A;}
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px; border-top: 8px solid var(--accent, #4299E0); display: flex; flex-direction: column; overflow-wrap: anywhere; }
# MAGIC .ds-card-label { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--accent); margin-bottom: 4px; }
# MAGIC .ds-card-title { font-size: 16pt; font-weight: 700; line-height: 1.25; margin: 0 0 8px; }
# MAGIC .ds-card p { font-size: 14pt; line-height: 1.5; margin: 0; color: #0b2026; }
# MAGIC .ds-viz { margin-top: auto; padding-top: 14px; border-top: 1px dashed #E0DED9; }
# MAGIC .ds-viz-row { display:flex; align-items:center; justify-content:center; gap:8px; flex-wrap:wrap; }
# MAGIC .ds-chip { font-size:14pt; color:#0b2026; background:#fff; border:1px solid var(--accent); border-radius:20px; padding:4px 10px; font-family: Menlo,Consolas,monospace; }
# MAGIC .ds-chip.bad { background: rgba(152,16,42,0.10); color:#98102A; border-color:#98102A; }
# MAGIC .ds-chip.ok { background: rgba(0,169,114,0.12); color:#007a53; border-color:#00A972; }
# MAGIC .ds-viz-cap { font-size: 14pt; color: #618794; text-align: center; margin-top: 10px; line-height: 1.4; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-card red">
# MAGIC     <div class="ds-card-label">Bronze</div>
# MAGIC     <div class="ds-card-title">Deleted, not purged</div>
# MAGIC     <p>The prior <code>DELETE</code> removed the subject from the current state. With deletion vectors on, the file wasn't rewritten, so the value is still in it.</p>
# MAGIC     <div class="ds-viz">
# MAGIC       <div class="ds-viz-row">
# MAGIC         <span class="ds-chip ok">current: gone</span>
# MAGIC         <span class="ds-chip bad">file: still there</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">A soft delete, not a purge</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-card amber">
# MAGIC     <div class="ds-card-label">Silver</div>
# MAGIC     <div class="ds-card-title">Still identifies the subject</div>
# MAGIC     <p>The delete never propagated downstream. <code>orders_silver</code> still holds the subject's rows, with their name and email.</p>
# MAGIC     <div class="ds-viz">
# MAGIC       <div class="ds-viz-row">
# MAGIC         <span class="ds-chip bad">orders_silver: present</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">The propagation was skipped</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-card coral">
# MAGIC     <div class="ds-card-label">Gold</div>
# MAGIC     <div class="ds-card-title">Still counts the subject</div>
# MAGIC     <p><code>sales_by_country_gold</code> was built while the subject was present, so their orders still inflate their country's totals.</p>
# MAGIC     <div class="ds-viz">
# MAGIC       <div class="ds-viz-row">
# MAGIC         <span class="ds-chip bad">counts the subject</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">An aggregate, but stale</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-card blue">
# MAGIC     <div class="ds-card-label">Register</div>
# MAGIC     <div class="ds-card-title">No evidence trail</div>
# MAGIC     <p>Nothing tracked the request, so there's no record of what was in scope, whether it completed or how to prove it.</p>
# MAGIC     <div class="ds-viz">
# MAGIC       <div class="ds-viz-row">
# MAGIC         <span class="ds-chip bad">privacy_requests: empty</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">Nothing to hand an auditor</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why a bronze-only delete leaves all four problems</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **A delete on the source is only the first step.** The subject's data was copied downstream into silver and summarized into gold. Deleting from bronze alone leaves every copy in place.
# MAGIC - **Current-state absence isn't physical removal.** With deletion vectors, the bronze delete marked the rows gone but didn't rewrite the file, so the values remain recoverable until a purge.
# MAGIC - **An aggregate built from stale data stays stale.** Gold doesn't identify anyone, but its country totals were computed with the subject's orders included, so they no longer match the data you intend to keep.
# MAGIC - **Without a register there's no proof.** Erasure is a control you have to be able to show worked. No tracked request means no scope, no completion time and no evidence reference to hand to an auditor.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# The analyst view of the broken state.
# Bronze is empty in the current state (the prior delete), but silver still holds the subject, the gold aggregate still counts their orders, and the request register is empty, so there is no evidence trail yet.
count_subject_rows()
show_gold()
show_register()

# COMMAND ----------

# The prior DELETE shows rows_deleted > 0, files_removed = 0 and a deletion vector added: the file wasn't rewritten, so it was a soft delete and the value is still in the file.
# silver shows no delete at all yet, the gap you fix in Section E.
show_delete_history()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Your tasks, end to end
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-steps { display: flex; flex-direction: column; gap: 12px; }
# MAGIC .ds-step { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 14px 18px; border-left: 6px solid var(--accent, #4299E0); display: flex; align-items: center; gap: 16px; }
# MAGIC .blue { --accent:#4299E0; } .amber { --accent:#FFAB00; } .coral { --accent:#FF5F46; } .green { --accent:#00A972; } .red { --accent:#98102A; } .teal { --accent:#1B5162; }
# MAGIC .ds-step-letter { width: 40px; height: 40px; border-radius: 50%; background: var(--accent); color: #fff; font-size: 16pt; font-weight: 800; line-height: 1; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
# MAGIC .ds-step-body { font-size: 14pt; line-height: 1.5; }
# MAGIC .ds-step-title { font-size: 16pt; font-weight: 700; color: #0b2026; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-steps">
# MAGIC   <div class="ds-step blue">
# MAGIC     <div class="ds-step-letter">C</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title">Track the request.</span> Open a register row for the request, keyed on the request id, with no raw PII.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-step teal">
# MAGIC     <div class="ds-step-letter">D</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title">Find the downstream assets.</span> Use classification and lineage to list every table that still holds the subject.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-step amber">
# MAGIC     <div class="ds-step-letter">E</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title">Delete and propagate.</span> <code>MERGE ... WHEN MATCHED THEN DELETE</code> across the authoritative table and downstream.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-step coral">
# MAGIC     <div class="ds-step-letter">F</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title">Logical delete versus physical purge.</span> <code>REORG ... APPLY (PURGE)</code>, then a retention-aware <code>VACUUM ... DRY RUN</code>.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-step green">
# MAGIC     <div class="ds-step-letter">G</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title">Verify.</span> Current state empty for the subject and the non-identifying gold aggregate stays correct.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-step red">
# MAGIC     <div class="ds-step-letter">H</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title">Assemble the evidence.</span> Close the request as verified, with an evidence reference and no unnecessary raw PII.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Track the Request
# MAGIC
# MAGIC Before touching data, open the control plane. The **request control plane** is a tracking layer you build, for example a **privacy_requests** table, that records the request and drives it to completion. It's a design pattern, not a Databricks product feature and it's unrelated to the Databricks platform control plane.
# MAGIC
# MAGIC **Your task:** in the **YOUR TASK** cell, register the request in **privacy_requests** with an idempotent `MERGE` keyed on `request_id`. Store the pseudonymous `subject_key`, the request type, a status, the received and deadline dates and the scope. Don't put the customer's email or name in it. Run the `MERGE` again to prove it doesn't duplicate, then run `show_register()` to see the tracked request.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Register the request with an idempotent MERGE INTO privacy_requests keyed ON request_id,
# MAGIC --    WHEN NOT MATCHED THEN INSERT: request_id 'DSR-2026-0731', subject_key 'SUBJ-9F3C7A',
# MAGIC --    request_type 'erasure', status 'in_progress', received_date, deadline_date, scope.
# MAGIC --    Store subject_key, never the customer_id or email.
# MAGIC -- 2. Run the same MERGE again to prove it's idempotent (still one row).
# MAGIC -- Reveal the solution below if you get stuck.
# MAGIC

# COMMAND ----------

show_register()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### REVEAL THE SOLUTION
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:700; color:#FF5F46; cursor:pointer;">Reveal the solution (C)</summary>
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:10px 0;">
# MAGIC The register stores the pseudonymous <code>subject_key</code>, never the raw identifier. Keying the <code>MERGE</code> on <code>request_id</code> is what makes re-running it safe.
# MAGIC <div style="margin-top:10px;border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">-- 1. Register the request. Idempotent: keyed on request_id, no raw PII.
# MAGIC MERGE INTO privacy_requests t
# MAGIC USING (
# MAGIC   SELECT 'DSR-2026-0731'  AS request_id,
# MAGIC          'SUBJ-9F3C7A'    AS subject_key,
# MAGIC          'erasure'        AS request_type,
# MAGIC          'in_progress'    AS status,
# MAGIC          DATE'2026-08-20' AS received_date,
# MAGIC          DATE'2026-09-19' AS deadline_date,
# MAGIC          'priv_lab orders estate: orders_bronze (authoritative), orders_silver (downstream)' AS scope
# MAGIC ) s
# MAGIC ON t.request_id = s.request_id
# MAGIC WHEN NOT MATCHED THEN INSERT (request_id, subject_key, request_type, status, received_date, deadline_date, scope)
# MAGIC   VALUES (s.request_id, s.subject_key, s.request_type, s.status, s.received_date, s.deadline_date, s.scope);
# MAGIC
# MAGIC -- 2. Run it again: WHEN NOT MATCHED does nothing, so there's still one row.
# MAGIC MERGE INTO privacy_requests t
# MAGIC USING (SELECT 'DSR-2026-0731' AS request_id, 'SUBJ-9F3C7A' AS subject_key) s
# MAGIC ON t.request_id = s.request_id
# MAGIC WHEN NOT MATCHED THEN INSERT (request_id, subject_key, request_type, status)
# MAGIC   VALUES (s.request_id, s.subject_key, 'erasure', 'in_progress');</pre></div>
# MAGIC <div style="margin-top:12px;">Notice there's no email or name column in the register. Resolving <code>subject_key</code> to a <code>customer_id</code> for the deletes is a separate, access-controlled step, so the tracking table doesn't become one more place holding raw PII.</div>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Find the Downstream Assets With Lineage
# MAGIC
# MAGIC You can't finish what you can't find. Two sources locate the subject's data without a manual column hunt: **classification** says which columns are personal data and **lineage** says which tables a column propagated to. Read them, then confirm where the subject still appears. Both are read from the snapshots the setup provisions, because a live scan is asynchronous and lineage lags.

# COMMAND ----------

# The classification snapshot flags the personal-data columns in the estate: the columns you erase when you delete the subject's rows.
# Both bronze and silver carry the customer's name and email.
show_classification()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read the COLUMN lineage snapshot to see which downstream tables the subject's identifier
# MAGIC -- columns (customer_email, customer_name) reach:
# MAGIC --   SELECT DISTINCT source_column_name, target_table_name
# MAGIC --   FROM orders_snap_column_lineage
# MAGIC --   WHERE source_table_name = 'orders_bronze'
# MAGIC --     AND source_column_name IN ('customer_email','customer_name');
# MAGIC -- Then run count_subject_rows() below to confirm where the subject still appears.
# MAGIC -- Reveal the solution below if you get stuck.
# MAGIC

# COMMAND ----------

count_subject_rows()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### REVEAL THE SOLUTION
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:700; color:#FF5F46; cursor:pointer;">Reveal the solution (D)</summary>
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:10px 0;">
# MAGIC Column lineage shows the identifiers reach <code>orders_silver</code> but not the gold aggregate. The row counts confirm the trap: bronze looks clean, but silver still holds the subject.
# MAGIC <div style="margin-top:10px;border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">-- Where do the subject's identifier columns propagate? (customer_email, customer_name)
# MAGIC SELECT DISTINCT source_column_name AS identifier, target_table_name AS reaches_table
# MAGIC FROM orders_snap_column_lineage
# MAGIC WHERE source_table_name = 'orders_bronze'
# MAGIC   AND source_column_name IN ('customer_email','customer_name')
# MAGIC ORDER BY identifier, reaches_table;</pre></div>
# MAGIC <div style="margin-top:12px;">The erasure scope is the two identifiable tables. <code>sales_by_country_gold</code> holds no identifier column, so it needs no per-row deletion, only a recomputation once the source data changes. You handle that in Section G.</div>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 22px 26px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Ask Genie Code</div>
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">You found the downstream table by reading lineage yourself, which is the skill. Once you can read it, <strong>Genie Code</strong> can help you ask the same question. Click <strong>Copy</strong>, paste the prompt and check its answer against the lineage you just read.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">Which tables are downstream of priv_lab.orders_bronze and which of their columns come from its customer_email or customer_name column?</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Delete and Propagate With `MERGE`
# MAGIC
# MAGIC Now finish the deletion. `MERGE ... WHEN MATCHED THEN DELETE` is the form to use: it deletes the subject and is safe to re-run, because if the subject is already gone nothing matches. Delete from the authoritative layer first, then propagate downstream.
# MAGIC
# MAGIC > The prior job already cleared the subject from **orders_bronze**, so your `MERGE` there affects zero rows. That's the idempotency point, not a mistake. The real gap is **orders_silver**, which the prior job never touched.

# COMMAND ----------

# Confirm the starting point: bronze's current state is already clean (the prior delete), but silver still holds the subject's rows.
# Your MERGE fixes silver.
count_subject_rows()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. MERGE ... WHEN MATCHED THEN DELETE the subject (customer_id = 1462) from orders_bronze
# MAGIC --    (authoritative). This affects 0 rows: the prior job already cleared bronze's current state.
# MAGIC -- 2. MERGE ... WHEN MATCHED THEN DELETE the subject from orders_silver (downstream), the
# MAGIC --    propagation the prior job skipped. Match ON customer_id in both.
# MAGIC -- Reveal the solution below if you get stuck.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### REVEAL THE SOLUTION
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:700; color:#FF5F46; cursor:pointer;">Reveal the solution (E)</summary>
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:10px 0;">
# MAGIC <div style="border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">-- 1. Delete from the authoritative layer. Affects 0 rows: the prior job already
# MAGIC --    cleared bronze's current state. A no-op MERGE is safe, which is the point.
# MAGIC MERGE INTO orders_bronze t
# MAGIC USING (SELECT 1462 AS customer_id) s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN DELETE;
# MAGIC
# MAGIC -- 2. Propagate the delete downstream. This removes the subject from silver, the
# MAGIC --    step the prior job skipped.
# MAGIC MERGE INTO orders_silver t
# MAGIC USING (SELECT 1462 AS customer_id) s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN DELETE;</pre></div>
# MAGIC <div style="margin-top:12px;">Delete from the authoritative layer first so the source record goes before its copies. <code>MERGE ... WHEN MATCHED THEN DELETE</code> is set based and idempotent, so re-running it changes nothing once the subject is gone.</div>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# Both tables now return zero subject rows in the current state.
# The MERGE deletes are idempotent: re-running your YOUR TASK cell affects zero rows, which is what makes the erasure flow safe to replay.
count_subject_rows()

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
# MAGIC     <li>For a bounded set of subjects, that's more machinery than the job needs. A batch <code>DELETE</code> on the source and a <code>MERGE</code> to propagate downstream is simpler to reason about and to prove, which is what this lab uses.</li>
# MAGIC     <li>Batch <strong>Change Data Feed</strong> has a place, reading a source table's committed changes in batch to drive deletes across many downstream tables at once. It's an option for propagation, not a requirement and it's still batch, not streaming.</li>
# MAGIC   </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Logical Delete Versus Physical Purge
# MAGIC
# MAGIC The subject is gone from every query now. That proves current-state absence, not that the value left storage. Both tables use **deletion vectors**, so the deletes were soft: the row is marked deleted in metadata and the data file isn't rewritten. The value stays in the file until you rewrite it with `REORG TABLE ... APPLY (PURGE)` and the old files remain until a retention-aware `VACUUM` removes them.

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
# MAGIC   <rect x="50" y="70" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="155" y="92" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 1108</text>
# MAGIC   <rect x="50" y="112" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#FF5F46" stroke-width="2"/><text x="155" y="134" text-anchor="middle" font-size="13" fill="#98102A" font-family="monospace">customer 1462  (subject)</text>
# MAGIC   <rect x="50" y="154" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="155" y="176" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 1339</text>
# MAGIC   <rect x="50" y="196" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="155" y="218" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 1520</text>
# MAGIC   <text x="155" y="252" text-anchor="middle" font-size="12" fill="#5E7077">all rows still physically present</text>
# MAGIC   <!-- gate: deletion vector applied at read -->
# MAGIC   <path d="M280 157 L360 157" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#dv-arrow)"/>
# MAGIC   <rect x="365" y="112" width="230" height="92" rx="10" fill="#FFAB00" fill-opacity="0.14" stroke="#FFAB00" stroke-width="1.8"/>
# MAGIC   <text x="480" y="140" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">deletion vector</text>
# MAGIC   <text x="480" y="162" text-anchor="middle" font-size="12" fill="#5E7077">marks customer 1462 deleted,</text>
# MAGIC   <text x="480" y="180" text-anchor="middle" font-size="12" fill="#5E7077">in metadata, applied at read</text>
# MAGIC   <path d="M595 157 L675 157" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#dv-arrow)"/>
# MAGIC   <!-- current state -->
# MAGIC   <text x="695" y="40" font-size="12" font-weight="700" fill="#00A972" letter-spacing="1.5">CURRENT TABLE STATE</text>
# MAGIC   <rect x="680" y="50" width="250" height="215" rx="10" fill="#F9F7F4" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <rect x="700" y="82" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="805" y="104" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 1108</text>
# MAGIC   <rect x="700" y="124" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="805" y="146" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 1339</text>
# MAGIC   <rect x="700" y="166" width="210" height="34" rx="5" fill="#FFFFFF" stroke="#EEEDE9" stroke-width="1.2"/><text x="805" y="188" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">customer 1520</text>
# MAGIC   <text x="805" y="230" text-anchor="middle" font-size="12" fill="#007a53">customer 1462 absent from every query</text>
# MAGIC   <text x="485" y="298" text-anchor="middle" font-size="13" fill="#0b2026">A DELETE on a deletion-vector table is a soft delete: the file isn't rewritten, so the value for 1462 stays in the file until REORG rewrites it.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why both tables need a purge, not just silver</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Bronze was soft-deleted by the prior job.** Its current state has been clean for weeks, but because deletion vectors were on, the file was never rewritten, so the subject's value is still recoverable in it. Bronze needs the purge just as much as silver.
# MAGIC - **Silver was soft-deleted just now.** Your `MERGE` in Section E marked the rows deleted and didn't rewrite the file either, so the value is in that file too.
# MAGIC - **The privacy consequence is the whole point.** A successful `DELETE` proves current-state absence, not physical removal. Only a rewrite materializes the deletion in the files.
# MAGIC - **How they get materialized.** `OPTIMIZE`, `REORG TABLE ... APPLY (PURGE)` or a write with auto-compaction rewrite the files and bake in the deletes. For erasure you use `REORG ... APPLY (PURGE)`, then `VACUUM`.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# bronze shows the prior job's DELETE and silver shows your MERGE. Both have files_removed = 0 and a deletion vector added: the files weren't rewritten, so a purge is still required on both.
show_delete_history()

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
# MAGIC     <div class="sf-title">Delete (done)</div>
# MAGIC     <div class="sf-code">DELETE / MERGE</div>
# MAGIC     <div class="sf-desc">Soft delete: a deletion vector marks the row, the file isn't rewritten.</div>
# MAGIC   </div>
# MAGIC   <div class="sf-arrow">&rarr;</div>
# MAGIC   <div class="sf-step" style="--accent:#00A972;">
# MAGIC     <div class="sf-num">2</div>
# MAGIC     <div class="sf-title">Rewrite (your task)</div>
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
# MAGIC - **It doesn't remove the old files.** After `APPLY (PURGE)` the soft-deleted data may still exist in the old files and you run `VACUUM` to physically delete them. That's step three.
# MAGIC - **It's idempotent.** Running `REORG ... APPLY (PURGE)` again on the same data has no effect, so it's safe to re-run.
# MAGIC - **You can scope it.** `REORG TABLE t WHERE predicate APPLY (PURGE)` limits the rewrite to matching files, which matters on a large table where you don't want to rewrite everything.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1-2. Rewrite the files that hold soft-deleted data, on both tables:
# MAGIC --        REORG TABLE orders_bronze APPLY (PURGE);
# MAGIC --        REORG TABLE orders_silver APPLY (PURGE);
# MAGIC -- 3-4. Preview what a VACUUM would remove, without deleting anything:
# MAGIC --        VACUUM orders_bronze DRY RUN;
# MAGIC --        VACUUM orders_silver DRY RUN;
# MAGIC -- Respect the 168-hour retention window: don't RETAIN 0 HOURS or disable the safety check.
# MAGIC -- Then run show_rewrite_history() below to confirm the rewrite.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### REVEAL THE SOLUTION
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:700; color:#FF5F46; cursor:pointer;">Reveal the solution (F)</summary>
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:10px 0;">
# MAGIC <code>REORG</code> rewrites the files that hold soft-deleted data. <code>VACUUM ... DRY RUN</code> then previews which old files are eligible for removal, without deleting anything.
# MAGIC <div style="margin-top:10px;border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">-- 1-2. Rewrite the files that hold soft-deleted data, on both tables.
# MAGIC REORG TABLE orders_bronze APPLY (PURGE);
# MAGIC REORG TABLE orders_silver APPLY (PURGE);
# MAGIC
# MAGIC -- 3-4. Preview what VACUUM would remove. Right after REORG the newly unreferenced files are
# MAGIC --      only minutes old, so a compliant VACUUM removes nothing yet. That is the 168-hour
# MAGIC --      retention window working, not a failure. Don't RETAIN 0 HOURS.
# MAGIC VACUUM orders_bronze DRY RUN;
# MAGIC VACUUM orders_silver DRY RUN;</pre></div>
# MAGIC <div style="margin-top:12px;">A real <code>VACUUM</code> (no <code>DRY RUN</code>) run after the retention window elapses physically deletes the eligible files. Plan the purge to complete after the window rather than shortening it.</div>
# MAGIC </div>
# MAGIC </details>

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
# MAGIC   <div class="ds-eyebrow">The retention window</div>
# MAGIC   <ul>
# MAGIC     <li>You may see guidance that disables the safety check (<code>spark.databricks.delta.retentionDurationCheck.enabled</code>) and runs <code>VACUUM ... RETAIN 0 HOURS</code> to purge immediately.</li>
# MAGIC     <li>The window protects running jobs. A retention window shorter than your longest running job can delete files an in-flight read or write still needs, which corrupts the table. Treat a zero-hour VACUUM as a rare special case with a clear reason, not the normal path.</li>
# MAGIC     <li>For erasure, plan the purge to complete after the retention window elapses. Time travel and any Change Data Feed records age out on the same window, so the value clears from those too once it passes.</li>
# MAGIC   </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# After REORG, the most recent operation on each table shows files removed and added, the file rewrite that materialized the deletion.
# The old files are now unreferenced, awaiting VACUUM after the window.
show_rewrite_history()

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Verify Empty Current State and a Correct Gold Aggregate
# MAGIC
# MAGIC Now prove it. The subject must be absent from every table that identified them. The gold aggregate never identified anyone, so it needs no per-row deletion, but its country totals were computed while the subject was present, so recompute it from the erased silver to keep the numbers accurate.
# MAGIC
# MAGIC **Your task:** confirm the subject is gone with `count_subject_rows()`, then in the **YOUR TASK** cell recompute **sales_by_country_gold** from the erased **orders_silver** with `CREATE OR REPLACE TABLE`. Run `show_gold()` to confirm the aggregate carries no identifier column and the subject's country still groups many customers.

# COMMAND ----------

count_subject_rows()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recompute sales_by_country_gold from the erased orders_silver so its totals are accurate:
# MAGIC --   CREATE OR REPLACE TABLE sales_by_country_gold AS
# MAGIC --   SELECT country, count(*) AS order_count, round(sum(amount), 2) AS total_amount,
# MAGIC --          count(DISTINCT customer_id) AS distinct_customers
# MAGIC --   FROM orders_silver GROUP BY country;
# MAGIC -- Then run show_gold() below: no identifier column, and the subject's country still groups many customers.
# MAGIC

# COMMAND ----------

show_gold()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### REVEAL THE SOLUTION
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:700; color:#FF5F46; cursor:pointer;">Reveal the solution (G)</summary>
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:10px 0;">
# MAGIC The subject is absent from the identifiable tables. Recomputing gold from the erased silver keeps the totals accurate and the country group still holds many customers, so it doesn't single anyone out.
# MAGIC <div style="margin-top:10px;border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">-- Recompute the aggregate from the erased silver so its totals are accurate.
# MAGIC CREATE OR REPLACE TABLE sales_by_country_gold AS
# MAGIC SELECT country,
# MAGIC        count(*)                    AS order_count,
# MAGIC        round(sum(amount), 2)       AS total_amount,
# MAGIC        count(DISTINCT customer_id) AS distinct_customers
# MAGIC FROM orders_silver
# MAGIC GROUP BY country;</pre></div>
# MAGIC <div style="margin-top:12px;">Recomputing gold is for accuracy, not an erasure requirement, because the aggregate never identified the subject. A country total over many customers can't single one out and the lab's generator keeps many customers in each country so the groups stay large. The rule to remember: an aggregate is only non-identifying while each group stays large enough that no row is really about one person.</div>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .cov-wrap { background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 8px solid #00A972; padding: 20px 24px; }
# MAGIC .cov-title { font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 14px; }
# MAGIC .cov-flow { display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
# MAGIC .cov-set { border: 1px solid #EEEDE9; border-radius: 8px; background: #fff; padding: 12px 16px; flex: 1; min-width: 240px; }
# MAGIC .cov-set-h { font-size: 14pt; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; margin-bottom: 8px; }
# MAGIC .cov-chip { display: inline-block; font-family: monospace; font-size: 14pt; background: rgba(66,153,224,0.10); color: #2272B4; border-radius: 6px; padding: 3px 9px; margin: 3px 4px 0 0; }
# MAGIC .cov-chip.gone { background: rgba(152,16,42,0.10); color: #98102A; text-decoration: line-through; }
# MAGIC .cov-chip.keep { background: rgba(0,169,114,0.14); color: #007a53; font-weight: 700; }
# MAGIC .cov-arrow { color: #00A972; font-size: 24pt; font-weight: 700; text-align: center; flex: 0 0 auto; }
# MAGIC .cov-note { font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 16px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="cov-wrap">
# MAGIC   <div class="cov-title">Erase the person, keep the analytics</div>
# MAGIC   <div class="cov-flow">
# MAGIC     <div class="cov-set">
# MAGIC       <div class="cov-set-h">Identifiable layers (erased)</div>
# MAGIC       <span class="cov-chip gone">orders_bronze</span><span class="cov-chip gone">orders_silver</span>
# MAGIC     </div>
# MAGIC     <div class="cov-arrow">&rarr;</div>
# MAGIC     <div class="cov-set">
# MAGIC       <div class="cov-set-h">Non-identifying aggregate (kept, recomputed)</div>
# MAGIC       <span class="cov-chip keep">sales_by_country_gold</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="cov-note">The subject is gone from every table that named them. The gold aggregate holds only country totals, so it never identified the subject and needs no per-row deletion. You recompute it for accuracy and because each country still groups many customers, no row is about one person. That's why a privacy-preserving aggregate can outlive an erasure.</div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Assemble the Evidence
# MAGIC
# MAGIC Close the request. Update the register to **verified**, keyed on `request_id`, with a completion timestamp, the count of assets that held the subject and an evidence reference pointing at the table history. The `DESCRIBE HISTORY` you read is supporting evidence that the operations ran, not a complete audit. That fuller picture comes from the system tables in Demo 08.
# MAGIC
# MAGIC **Your task:** in the **YOUR TASK** cell, close the request as verified with the completion fields, then run `show_register()` to read the closed request back and confirm it still carries no raw PII.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Close the request with an idempotent MERGE INTO privacy_requests keyed ON request_id
# MAGIC -- ('DSR-2026-0731'), WHEN MATCHED THEN UPDATE SET status = 'verified', affected_asset_count = 2
# MAGIC -- (orders_bronze + orders_silver), completed_at = current_timestamp(), evidence_ref = a pointer
# MAGIC -- to the DESCRIBE HISTORY of the two tables.
# MAGIC -- Then run show_register() below to read the closed request and confirm it has no email or name column.
# MAGIC

# COMMAND ----------

show_register()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### REVEAL THE SOLUTION
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:700; color:#FF5F46; cursor:pointer;">Reveal the solution (H)</summary>
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:10px 0;">
# MAGIC Closing the request is another idempotent <code>MERGE</code> keyed on <code>request_id</code>. The evidence reference points at the table history you produced and the register still stores only the pseudonymous <code>subject_key</code>.
# MAGIC <div style="margin-top:10px;border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">-- Close the request as verified, with the completion evidence.
# MAGIC MERGE INTO privacy_requests t
# MAGIC USING (SELECT 'DSR-2026-0731' AS request_id) s
# MAGIC ON t.request_id = s.request_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   status               = 'verified',
# MAGIC   affected_asset_count = 2,
# MAGIC   completed_at         = current_timestamp(),
# MAGIC   evidence_ref         = 'DESCRIBE HISTORY orders_bronze and orders_silver: DELETE and REORG PURGE done; VACUUM after the 168h retention window';</pre></div>
# MAGIC <div style="margin-top:12px;">The register now tells the whole story of the request: who (a pseudonymous key), what (erasure), the scope, the completion time, the two assets erased and where to find the proof. None of that's raw PII.</div>
# MAGIC </div>
# MAGIC </details>

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
# MAGIC     <div class="ev-text">The subject returns zero rows in orders_bronze and orders_silver.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card green">
# MAGIC     <div class="ev-badge">&check;</div>
# MAGIC     <div class="ev-title">Files rewritten</div>
# MAGIC     <div class="ev-text">REORG purged both tables. History shows the soft deletes and the file rewrites.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card green">
# MAGIC     <div class="ev-badge">&check;</div>
# MAGIC     <div class="ev-title">Request verified, no PII</div>
# MAGIC     <div class="ev-text">The register is verified, with a completion time and an evidence reference and no raw PII.</div>
# MAGIC   </div>
# MAGIC   <div class="ev-card amber">
# MAGIC     <div class="ev-badge">&#33;</div>
# MAGIC     <div class="ev-title">Still required</div>
# MAGIC     <div class="ev-text">A real VACUUM completes only after the 168-hour window. Time travel and Change Data Feed hold copies until they age out.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this lab, you completed and verified an erasure request that a prior job left half-done:
# MAGIC
# MAGIC 1. **Tracked** the request in a control plane, keyed on the request id and free of raw PII.
# MAGIC 2. **Found** the downstream table that still held the subject with classification and lineage.
# MAGIC 3. **Deleted** the subject with `MERGE` and **propagated** the delete downstream, idempotently.
# MAGIC 4. **Purged** the files with `REORG TABLE ... APPLY (PURGE)` and previewed a retention-aware `VACUUM`.
# MAGIC 5. **Verified** the subject is absent from every in-scope table, kept the non-identifying gold aggregate correct and **assembled** the completion evidence.
# MAGIC
# MAGIC That closes the course. You ran the privacy decision model on every technique and ended each one by showing the control worked with verifiable evidence: a tag, a masked result, an empty erasure query, an audit, lineage or a history row. You framed privacy against security, governance and compliance, discovered and classified sensitive data, protected it by purpose and risk with tag-driven ABAC and manual masks, de-identified it and managed the keys, fulfilled and verified an erasure request and proved every control with evidence. Next, work through the **assessment**: twenty scenario-based questions that ask you to apply these decisions rather than recall syntax.

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