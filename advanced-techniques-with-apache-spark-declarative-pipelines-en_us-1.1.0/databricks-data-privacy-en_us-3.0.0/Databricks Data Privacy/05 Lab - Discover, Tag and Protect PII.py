# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">05 Lab - Discover, Tag and Protect PII</span>
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
# MAGIC     <span style="color:#0b2026; font-weight:700;"><span style="color:#4299E0;">&#9679;</span> Identify</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span style="color:#0b2026; font-weight:700;"><span style="color:#00A972;">&#9679;</span> Protect</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Manage</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Verify</span>
# MAGIC   </span>
# MAGIC   <span style="font-size:10.5pt; color:#618794; margin-left:auto;">steps 1-2 of 4</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab - Discover, Tag and Protect PII
# MAGIC #### Duration: 25 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC You're the data engineer responsible for data privacy across a multinational retailer's estate. You've already classified that estate and turned its tags into ABAC policies. The retailer also owns a travel booking subsidiary whose **customer_profile** table holds the same kind of identifiers but hasn't had the same attention.
# MAGIC
# MAGIC A teammate there protected the table months ago, but the protection is brittle. They masked two columns by name, one classification tag is wrong and a column added later for international bookings is exposed. This is a lab, so you diagnose and fix it yourself, then prove the control works.
# MAGIC
# MAGIC You'll:
# MAGIC
# MAGIC - Verify the classifications against the tags and correct the one that's wrong.
# MAGIC - Replace the name-based mask with one schema-scoped, tag-driven **ABAC** policy.
# MAGIC - Validate the control against a restricted identity and a privileged exception.
# MAGIC - Tag the newly added column and watch the central policy cover it with no change to the table.
# MAGIC - Record a one-line justification for the control decision.
# MAGIC
# MAGIC Everything you need was covered in the two prior demos: Data Classification and governed tags (Demo 03) and tag-driven ABAC versus manual masks (Demo 04). Each task has a **Reveal the solution** dropdown if you get stuck.
# MAGIC
# MAGIC > This lab runs entirely in your own isolated **priv_lab** schema, a separate estate from the demo retailer. It never touches the shared **priv_demo** data.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you'll be able to:
# MAGIC
# MAGIC 1. **Diagnose** a wrong or missing governed tag by tracing a classification detection to the tag it should drive.
# MAGIC 2. **Replace** a brittle name-based column mask with one schema-scoped, tag-driven ABAC policy.
# MAGIC 3. **Validate** the control live for **account users** and against a privileged exception (**account privileged**).
# MAGIC 4. **Demonstrate** that tagging a newly added column brings it under the central policy with no change to the table or the policy.
# MAGIC 5. **Record** a one-line justification for the control decision.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">How to work this lab</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   Treat each lettered section as a task, not a walkthrough. Read the prompt, write your fix as SQL in the <strong>YOUR TASK</strong> cell, then run the check below it to see the result. Reveal the solution only to recover or to compare. Your catalog and schema are already selected, so you write to <code>customer_profile</code>, <code>priv_lab</code> and the other objects by their short names.
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
# MAGIC **NOTE:** This lab uses **SQL and Python and was tested on Serverless V6**. Column mask and row filter policies need Databricks Runtime 16.4 or above, which serverless satisfies.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the setup below. It builds your isolated **priv_lab** estate and stages the broken starting point:
# MAGIC
# MAGIC - a **customer_profile** table for the travel booking subsidiary, with `email`, `phone`, `national_id` and a newly added `passport_number`
# MAGIC - the governed `sensitivity_level` tags a teammate applied, with `national_id` tagged **wrong** and `passport_number` **untagged**
# MAGIC - the teammate's brittle name-based mask on `email` and `national_id` only
# MAGIC - a reviewable classification snapshot, `priv_lab.snap_classification_results`, you use to verify the tags
# MAGIC
# MAGIC If your workspace can't create catalogs, open `./Includes/Classroom-Setup-05` and set `FORCED_CATALOG` to an existing catalog first.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-05

# COMMAND ----------

# Set the session defaults so the SQL you write can stay short and confirm the table you'll repair.
spark.sql(f"USE CATALOG `{my_catalog}`")
spark.sql(f"USE SCHEMA `{LAB_SCHEMA}`")
print(f"Working in {my_catalog}.{LAB_SCHEMA}. The table to repair is {LAB_SCHEMA}.customer_profile.")

# COMMAND ----------

# You write plain SQL in the YOUR TASK cells.
# Your catalog and schema are already selected, so use these short names directly.
# To check your work, run the verification helpers (each is a provided function that just displays a result).
display(spark.createDataFrame([
    ("customer_profile", "table", "The table you repair (SELECT, ALTER TABLE)"),
    ("priv_lab", "schema", "Your lab schema (the ABAC policy goes ON SCHEMA priv_lab)"),
    ("mask_restricted", "function", "The mask UDF you create in Section E"),
    ("mask_lab_restricted", "policy", "The ABAC policy you create in Section E"),
    ("account users", "group", "ABAC TO set: the built-in group every user is in"),
    ("account privileged", "group", "ABAC EXCEPT set: the reviewers who see originals"),
], ["name", "kind", "use_for"]))

display(spark.createDataFrame([
    ("verify_tags()", "Compares each detection to the tag applied on the column (WRONG / MISSING / ok)"),
    ("validate_control()", "Shows who you are, the table masked to you and the effective policy"),
    ("show_effective_policies()", "Lists the column-mask policies in force on customer_profile"),
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
# MAGIC   <div class="ds-scenario-band">The travel subsidiary's customer table</div>
# MAGIC   <div class="ds-scenario-body">
# MAGIC     <div class="ds-scenario-q">"I protected the customer table months ago. Why is a passport column showing up in the clear?"</div>
# MAGIC     <div class="ds-scenario-text">A teammate protected <b>priv_lab.customer_profile</b> for the retailer's travel booking subsidiary. It looked fine at the time, but the business has moved on and the protection hasn't kept up.
# MAGIC       <ul>
# MAGIC         <li>An analyst needs the table to segment travelers, but must not read raw identifiers.</li>
# MAGIC         <li>The teammate masked <b>email</b> and <b>national_id</b> by attaching a mask to each column by name.</li>
# MAGIC         <li>Since then the team added <b>passport_number</b> for international bookings and one classification tag was set wrong.</li>
# MAGIC       </ul>
# MAGIC       <strong>Your task: verify the classifications, replace the brittle mask with a tag-driven ABAC policy, validate it and prove the new column is covered.</strong>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Three things are wrong
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-row { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 260px; }
# MAGIC .blue{--accent:#4299E0;} .amber{--accent:#FFAB00;} .red{--accent:#98102A;}
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px; border-top: 8px solid var(--accent, #4299E0); display: flex; flex-direction: column; overflow-wrap: anywhere; }
# MAGIC .ds-card-label { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--accent); margin-bottom: 4px; }
# MAGIC .ds-card-title { font-size: 17pt; font-weight: 700; line-height: 1.25; margin: 0 0 8px; }
# MAGIC .ds-card p { font-size: 14pt; line-height: 1.55; margin: 0; color: #0b2026; }
# MAGIC .ds-viz { margin-top: auto; padding-top: 14px; border-top: 1px dashed #E0DED9; }
# MAGIC .ds-viz-row { display:flex; align-items:center; justify-content:center; gap:8px; flex-wrap:wrap; }
# MAGIC .ds-chip { font-size:14px; color:#0b2026; background:#fff; border:1px solid var(--accent); border-radius:20px; padding:4px 10px; font-family: Menlo,Consolas,monospace; }
# MAGIC .ds-chip.bad { background: rgba(152,16,42,0.10); color:#98102A; border-color:#98102A; }
# MAGIC .ds-viz-cap { font-size: 14px; color: #618794; text-align: center; margin-top: 10px; line-height: 1.4; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-card blue">
# MAGIC     <div class="ds-card-label">Defect 1</div>
# MAGIC     <div class="ds-card-title">A classification tag is wrong</div>
# MAGIC     <p>Classification detects <code>national_id</code> as a national identifier, a <b>restricted</b> class. The teammate tagged it <b>internal</b>. A policy that trusts the tag would under-protect it.</p>
# MAGIC     <div class="ds-viz">
# MAGIC       <div class="ds-viz-row">
# MAGIC         <span class="ds-chip">detected: restricted</span>
# MAGIC         <span style="color:#98102A;font-weight:700;">&ne;</span>
# MAGIC         <span class="ds-chip bad">tagged: internal</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">Verify the tag against the detection</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-card amber">
# MAGIC     <div class="ds-card-label">Defect 2</div>
# MAGIC     <div class="ds-card-title">The mask is hardwired to names</div>
# MAGIC     <p>The mask was attached to <code>email</code> and <code>national_id</code> one column at a time, by name. It protects only the columns someone remembered to name and it must be maintained by hand.</p>
# MAGIC     <div class="ds-viz">
# MAGIC       <div class="ds-viz-row">
# MAGIC         <span class="ds-chip">mask &rarr; email</span>
# MAGIC         <span class="ds-chip">mask &rarr; national_id</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">One mask per named column</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-card red">
# MAGIC     <div class="ds-card-label">Defect 3</div>
# MAGIC     <div class="ds-card-title">A new column is exposed</div>
# MAGIC     <p><code>passport_number</code> was added later and carries the same restricted class as <code>national_id</code>. The name-based mask never covered it, so it's readable by everyone.</p>
# MAGIC     <div class="ds-viz">
# MAGIC       <div class="ds-viz-row">
# MAGIC         <span class="ds-chip bad">passport_number: exposed</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">No tag, no mask</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why the three defects share one root cause</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **All three come from protecting by column name instead of by classification.** When protection is a hand-maintained list of columns, the list drifts the moment the data changes.
# MAGIC - **The wrong tag matters more than it looks.** Once you move to a tag-driven policy, a wrong tag is a hole in the control. That's why you verify and correct the tag before you rely on it.
# MAGIC - **The exposed column is what the name-based approach can't catch.** Nobody updated the mask list when `passport_number` arrived. A schema-scoped policy that matches the tag would have covered it the moment it was tagged.
# MAGIC - **The fix ties them together:** classify correctly, tag from the classification and let one policy protect every column that carries the tag.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# Read a few rows as yourself. For a non-privileged reader, email and national_id come back masked (the teammate's name-based mask), but passport_number, added later, is exposed.
display(spark.sql(f"""
SELECT customer_id, email, national_id, passport_number, phone, membership_level
FROM {LAB_TABLE}
LIMIT 8
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. The chain you'll repair, then prove
# MAGIC
# MAGIC Every task in this lab is one step in the same chain: a classification **detection** tells you what a column holds, you set a governed **tag** from it and one **policy** matches the tag and decides who sees the value.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .tr-flow { display: flex; align-items: stretch; justify-content: center; gap: 10px; flex-wrap: wrap; }
# MAGIC .tr-stage { background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 7px solid var(--accent); padding: 16px 16px; width: 205px; text-align: center; }
# MAGIC .tr-eyebrow { font-size: 14px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--accent); margin-bottom: 6px; }
# MAGIC .tr-title { font-size: 16pt; font-weight: 700; color: #0b2026; margin-bottom: 8px; line-height: 1.2; }
# MAGIC .tr-code { font-family: Menlo,Consolas,monospace; font-size: 13px; color: #0b2026; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 6px 8px; word-break: normal; }
# MAGIC .tr-arrow { display: flex; flex-direction: column; align-items: center; justify-content: center; flex: 0 0 104px; padding: 0 4px; }
# MAGIC .tr-arrow-label { font-size: 14px; color: #618794; text-align: center; margin-bottom: 4px; line-height: 1.3; }
# MAGIC .blue{--accent:#4299E0;} .amber{--accent:#B27A00;} .coral{--accent:#FF5F46;}
# MAGIC .tr-out { display: flex; gap: 16px; justify-content: center; flex-wrap: wrap; margin-top: 18px; }
# MAGIC .tr-pill { border-radius: 22px; padding: 8px 16px; font-size: 14pt; font-family: Menlo,Consolas,monospace; border: 2px solid var(--pc); background: #fff; color: #0b2026; }
# MAGIC .tr-pill .who { font-family: sans-serif; font-weight: 700; color: var(--pc); margin-right: 8px; }
# MAGIC .restricted{--pc:#98102A;} .privileged{--pc:#00A972;}
# MAGIC .tr-outcap { text-align:center; font-size:14pt; color:#0b2026; margin-top: 14px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="tr-flow">
# MAGIC   <div class="tr-stage blue">
# MAGIC     <div class="tr-eyebrow">Detection</div>
# MAGIC     <div class="tr-title">What it holds</div>
# MAGIC     <div class="tr-code">class.passport</div>
# MAGIC   </div>
# MAGIC   <div class="tr-arrow">
# MAGIC     <svg width="80" height="22" viewBox="0 0 80 22"><defs><marker id="tra1" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#618794" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></marker></defs><path d="M4 11 L72 11" fill="none" stroke="#618794" stroke-width="1.8" marker-end="url(#tra1)"/></svg>
# MAGIC     <div class="tr-arrow-label">you set the tag</div>
# MAGIC   </div>
# MAGIC   <div class="tr-stage amber">
# MAGIC     <div class="tr-eyebrow">Governed tag</div>
# MAGIC     <div class="tr-title">You decide</div>
# MAGIC     <div class="tr-code">sensitivity_level = restricted</div>
# MAGIC   </div>
# MAGIC   <div class="tr-arrow">
# MAGIC     <svg width="80" height="22" viewBox="0 0 80 22"><path d="M4 11 L72 11" fill="none" stroke="#618794" stroke-width="1.8" marker-end="url(#tra1)"/></svg>
# MAGIC     <div class="tr-arrow-label">policy matches</div>
# MAGIC   </div>
# MAGIC   <div class="tr-stage coral">
# MAGIC     <div class="tr-eyebrow">ABAC policy</div>
# MAGIC     <div class="tr-title">One control</div>
# MAGIC     <div class="tr-code">has_tag_value(...)</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="tr-out">
# MAGIC   <div class="tr-pill restricted"><span class="who">account users</span>ja****</div>
# MAGIC   <div class="tr-pill privileged"><span class="who">account privileged</span>james.smith@example.com</div>
# MAGIC </div>
# MAGIC <div class="tr-outcap">The policy decides who sees the original and who sees a masked value.</div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Reading the chain</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Detection to tag (Task D).** Data Classification says what a column holds. You decide the sensitivity_level and set the governed tag. Task D corrects a tag that doesn't match the detection.
# MAGIC - **Tag to policy (Task E).** One schema-scoped policy matches the tag with `has_tag_value` and applies a mask. It replaces the per-column, name-based mask.
# MAGIC - **Policy to outcome (Task F).** The policy names the principals in `TO` and `EXCEPT`, so a restricted analyst sees a masked value and a privileged reviewer sees the original.
# MAGIC - **The chain keeps up (Task G).** Tag a new column and it enters the chain at the tag stage, so the same policy covers it with no edit.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Your Tasks
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-steps { display: flex; flex-direction: column; gap: 14px; }
# MAGIC .ds-step { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 14px 18px; border-left: 6px solid var(--accent, #4299E0); display: flex; align-items: center; gap: 16px; }
# MAGIC .blue { --accent:#4299E0; } .amber { --accent:#FFAB00; } .coral { --accent:#FF5F46; } .green { --accent:#00A972; } .red { --accent:#98102A; }
# MAGIC .ds-step-num { width: 34px; height: 34px; border-radius: 50%; background: var(--accent); color: #fff; font-size: 16pt; font-weight: 800; line-height: 1; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
# MAGIC .ds-step-body { font-size: 14pt; line-height: 1.5; }
# MAGIC .ds-step-title { font-size: 16pt; font-weight: 700; color: #0b2026; }
# MAGIC .ds-step-title .sec { color: var(--accent); }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-steps">
# MAGIC   <div class="ds-step blue">
# MAGIC     <div class="ds-step-num">1</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title"><span class="sec">D.</span> Verify and correct the classification.</span> Compare the detections to the applied tags and fix the one that's wrong.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-step amber">
# MAGIC     <div class="ds-step-num">2</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title"><span class="sec">E.</span> Replace the brittle mask with ABAC.</span> Drop the name-based masks and create one schema-scoped, tag-driven policy.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-step coral">
# MAGIC     <div class="ds-step-num">3</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title"><span class="sec">F.</span> Validate restricted versus privileged.</span> Confirm the policy masks for an analyst and shows the original for a privileged reviewer.</div>
# MAGIC   </div>
# MAGIC   <div class="ds-step green">
# MAGIC     <div class="ds-step-num">4</div>
# MAGIC     <div class="ds-step-body"><span class="ds-step-title"><span class="sec">G.</span> Prove the new column is covered.</span> Tag <code>passport_number</code>, watch the policy cover it with no change and record a one-line justification.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Verify and Correct the Classification
# MAGIC
# MAGIC A governed tag is only as good as the classification it reflects. Start by tracing each detection to the tag that's applied today, then correct the one that doesn't match.
# MAGIC
# MAGIC **Your task:**
# MAGIC 1. Run `verify_tags()` below. It compares each **HIGH**-confidence detection to the `sensitivity_level` tag applied on the column.
# MAGIC 2. Read the `finding` column. One row is **WRONG TAG** and one is **MISSING TAG**.
# MAGIC 3. In the **YOUR TASK** cell, correct the **WRONG TAG** with an `ALTER TABLE ... SET TAGS` statement, then run `verify_tags()` again to confirm. Leave the missing one for Section G.

# COMMAND ----------

verify_tags()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The verify above flagged one WRONG TAG. Correct it so the tag matches the detection.
# MAGIC -- Pattern: ALTER TABLE customer_profile ALTER COLUMN <col> SET TAGS ('sensitivity_level' = '<value>');
# MAGIC -- Leave the MISSING TAG column for Section G. Reveal the solution below if you get stuck.
# MAGIC

# COMMAND ----------

verify_tags()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### REVEAL THE SOLUTION
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:700; color:#FF5F46; cursor:pointer;">Reveal the solution (D)</summary>
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:10px 0;">
# MAGIC The detection reads <code>class.us_ssn</code> (a national identifier, a restricted class), but the tag says <code>internal</code>. Correct it to <code>restricted</code>.
# MAGIC <div style="margin-top:10px;border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">ALTER TABLE customer_profile
# MAGIC ALTER COLUMN national_id SET TAGS ('sensitivity_level' = 'restricted');
# MAGIC -- Run verify_tags() again: national_id now reads "ok".
# MAGIC -- passport_number still reads "MISSING TAG". You tag it in Section G.</pre></div>
# MAGIC <div style="margin-top:12px;">Why correct it now: in the next step you replace the name-based mask with a policy that matches <code>sensitivity_level = restricted</code>. If <code>national_id</code> were still tagged <code>internal</code>, dropping its old mask would leave it exposed. The tag is now part of the security boundary.</div>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Replace the Brittle Mask with ABAC
# MAGIC
# MAGIC The teammate attached a mask to two columns by name. Replace that with one schema-scoped, tag-driven policy: it matches the `sensitivity_level` tag, so it covers every restricted column in the schema and any restricted column added later.
# MAGIC <br/><br/>
# MAGIC <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap; justify-content: center; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <div style="border: 1.5px solid #618794; border-radius: 12px; padding: 18px 22px; width: 300px; background: #ffffff;">
# MAGIC     <div style="font-size: 16pt; font-weight: 700; color: #0b2026; text-align: center;">Name-based mask</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center; margin-top: 6px;">What the teammate did</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 14px; font-size: 14pt; color: #0b2026;">One mask per named column</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Hard to maintain</div>
# MAGIC     <div style="border: 1px dashed #98102A; border-radius: 6px; background: rgba(152,16,42,0.08); padding: 4px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">New columns aren't covered</div>
# MAGIC   </div>
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; gap: 8px; min-width: 120px;">
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center;">replace with</div>
# MAGIC     <svg width="110" height="20" viewBox="0 0 110 20"><defs><marker id="cmp-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#1B3139" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs><path d="M5 10 L100 10" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#cmp-arrow)"/></svg>
# MAGIC   </div>
# MAGIC   <div style="border: 2px solid #FF5F46; border-radius: 12px; padding: 18px 22px; width: 300px; background: #F9F7F4;">
# MAGIC     <div style="font-size: 16pt; font-weight: 700; color: #FF5F46; text-align: center;">Tag-driven ABAC</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center; margin-top: 6px;">What you build</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 14px; font-size: 14pt; color: #0b2026;">One policy on the schema</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Matches the sensitivity_level tag</div>
# MAGIC     <div style="border: 1px dashed #2272B4; border-radius: 6px; background: rgba(34,114,180,0.08); padding: 4px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">New tagged columns covered</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="max-width:1100px;margin:0 auto;font-family:sans-serif;font-size:14pt;line-height:1.6;color:#0b2026;">
# MAGIC <strong>Your task, in the YOUR TASK cell:</strong>
# MAGIC <ol>
# MAGIC   <li>Drop the name-based masks on <code>email</code> and <code>national_id</code> with <code>ALTER TABLE ... ALTER COLUMN ... DROP MASK</code>.</li>
# MAGIC   <li>Create an <strong>unconditional</strong> mask UDF <code>priv_lab.mask_restricted(val STRING)</code>. It only transforms the value, because the policy decides who.</li>
# MAGIC   <li>Create one policy <code>mask_lab_restricted</code> <code>ON SCHEMA priv_lab</code> that applies the mask <code>TO &#96;account users&#96; EXCEPT &#96;account privileged&#96;</code> and matches <code>has_tag_value('sensitivity_level','restricted')</code>. If you get an error that <code>account privileged</code> can't be found, that group isn't provisioned in your workspace: drop the <code>EXCEPT</code> clause and write <code>TO &#96;account users&#96;</code> so the mask still applies.</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why the mask UDF is unconditional</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **The ABAC mask function only transforms the value.** It doesn't check who's asking, because the policy's `TO` and `EXCEPT` decide that. The teammate's manual mask had to branch on `is_account_group_member` inside the function. ABAC moves the who out into the policy.
# MAGIC - **The policy goes on the schema, not the table.** A schema-scoped policy covers every table under it and every column that carries the matched tag, now and later. That is why the new column in Section G is covered with no extra work.
# MAGIC - **`MATCH COLUMNS` targets a tag, not a name.** `has_tag_value('sensitivity_level','restricted')` selects the columns to mask by their tag, so there's no brittle list of column names in the policy.
# MAGIC - **Drop the old masks first.** A column should be governed by one control. Remove the name-based masks so the tag-driven policy is the only control on the column.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Drop the two name-based masks (one statement each):
# MAGIC --      ALTER TABLE customer_profile ALTER COLUMN email DROP MASK;
# MAGIC --      ALTER TABLE customer_profile ALTER COLUMN national_id DROP MASK;
# MAGIC -- 2. Create an unconditional mask UDF mask_restricted(val STRING): it only transforms the value.
# MAGIC -- 3. Create one policy mask_lab_restricted ON SCHEMA priv_lab, COLUMN MASK mask_restricted,
# MAGIC --    TO `account users` EXCEPT `account privileged`, matching has_tag_value('sensitivity_level','restricted').
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
# MAGIC <div style="border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">-- 1. Drop the brittle name-based masks.
# MAGIC ALTER TABLE customer_profile ALTER COLUMN email DROP MASK;
# MAGIC ALTER TABLE customer_profile ALTER COLUMN national_id DROP MASK;
# MAGIC -- 2. An unconditional mask UDF: it only transforms the value.
# MAGIC CREATE OR REPLACE FUNCTION mask_restricted(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN val IS NULL THEN NULL ELSE concat(left(val, 2), '****') END;
# MAGIC -- 3. One schema-scoped, tag-driven ABAC policy.
# MAGIC CREATE OR REPLACE POLICY mask_lab_restricted
# MAGIC ON SCHEMA priv_lab
# MAGIC COLUMN MASK mask_restricted
# MAGIC TO `account users` EXCEPT `account privileged`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('sensitivity_level', 'restricted') AS c
# MAGIC ON COLUMN c;</pre></div>
# MAGIC <div style="margin-top:12px;">Creating a policy needs <code>MANAGE</code> on the schema, which you have on your own <code>priv_lab</code>, plus <code>EXECUTE</code> on the function. <code>account privileged</code> is a custom account group provisioned outside the notebook.</div>
# MAGIC <div style="border-left: 4px solid #FFAB00; background: rgba(255,171,0,0.10); padding: 12px 16px; border-radius: 6px; margin-top: 12px; font-size: 14pt; line-height: 1.55; color: #0b2026;">
# MAGIC   <strong style="color:#B27A00;">If the create fails with <code>PRINCIPAL_DOES_NOT_EXIST</code> ("Could not find principal with name account privileged"):</strong> that account group isn't provisioned in your workspace. Drop the <code>EXCEPT</code> clause and write <code>TO &#96;account users&#96;</code> so the mask still applies.
# MAGIC </div>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Try it: ask Genie Code
# MAGIC
# MAGIC You built the policy by hand, so you know what a correct one looks like. That's the right position to be in before you let the assistant write one. Ask **Genie Code** to draft the same policy, then check its answer against what you wrote.
# MAGIC <br/><br/>
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 22px 26px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="font-size: 16pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Draft it, then verify it</div>
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">Click <strong>Copy</strong> and paste the prompt into Genie Code in the notebook or SQL editor. Then compare its policy to yours: is the mask function unconditional, does it target the tag rather than column names and is the scope the schema.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">Write a Unity Catalog ABAC column-mask policy on schema priv_lab that masks every column tagged sensitivity_level=restricted for account users, except account privileged, using a mask that keeps the first two characters. Then tell me how to confirm the policy is effective on a table.</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Validate Restricted Versus Privileged
# MAGIC
# MAGIC The policy is in place. Run the check below to see it work. Because you are in the built-in **account users** group and not in **account privileged**, the table returns `email` and `national_id` masked to you, live, while `phone` stays visible because it's **confidential**, not **restricted**. `SHOW EFFECTIVE POLICIES` is the identity-independent evidence that the same policy returns the originals to an **account privileged** reviewer.

# COMMAND ----------

# validate_control() shows who you are, the table masked to you (email and national_id), phone left visible, and the effective policy on the table.
validate_control()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .out-grid { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .out-grid > * { flex: 1; min-width: 300px; }
# MAGIC .out-card { background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); overflow: hidden; border-top: 8px solid var(--accent); }
# MAGIC .out-band { color: #fff; padding: 12px 18px; font-size: 16pt; font-weight: 700; background: var(--accent); }
# MAGIC .out-body { padding: 16px 20px; }
# MAGIC .out-item { display: flex; justify-content: space-between; gap: 12px; padding: 8px 0; border-bottom: 1px solid #EEEDE9; font-size: 14pt; }
# MAGIC .out-item:last-child { border-bottom: none; }
# MAGIC .out-k { color: #5E7077; }
# MAGIC .out-v { font-weight: 700; font-family: monospace; color: #0b2026; overflow-wrap: anywhere; text-align: right; }
# MAGIC .restricted{--accent:#98102A;} .privileged{--accent:#00A972;}
# MAGIC .ds-callout { border-left: 6px solid #1B5162; border-radius: 8px; padding: 16px 22px; background: #F8F9FC; box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 20px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="out-grid">
# MAGIC   <div class="out-card restricted">
# MAGIC     <div class="out-band">account users (restricted)</div>
# MAGIC     <div class="out-body">
# MAGIC       <div class="out-item"><span class="out-k">email</span><span class="out-v">ja****</span></div>
# MAGIC       <div class="out-item"><span class="out-k">national_id</span><span class="out-v">12****</span></div>
# MAGIC       <div class="out-item"><span class="out-k">phone (confidential)</span><span class="out-v">+14155550142</span></div>
# MAGIC       <div class="out-item"><span class="out-k">In the policy</span><span class="out-v">TO account users</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="out-card privileged">
# MAGIC     <div class="out-band">account privileged (exception)</div>
# MAGIC     <div class="out-body">
# MAGIC       <div class="out-item"><span class="out-k">email</span><span class="out-v">james.smith@example.com</span></div>
# MAGIC       <div class="out-item"><span class="out-k">national_id</span><span class="out-v">123-45-6789</span></div>
# MAGIC       <div class="out-item"><span class="out-k">phone (confidential)</span><span class="out-v">+14155550142</span></div>
# MAGIC       <div class="out-item"><span class="out-k">In the policy</span><span class="out-v">EXCEPT account privileged</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-callout">
# MAGIC   <strong>The control worked.</strong> One policy replaced the name-based masks. As an account users member outside the EXCEPT set you read the masked values live. An account privileged reviewer reads the originals, neither of you changing the query. The <code>phone</code> column stays visible because it's tagged <code>confidential</code>, not <code>restricted</code>, so the policy leaves it alone.
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Prove the New Column Is Covered
# MAGIC
# MAGIC `passport_number` is still exposed and still untagged. Because your policy matches the tag rather than column names, tagging the column is all it takes to bring it under the mask: no `ALTER POLICY`, no change to the table's protection.
# MAGIC
# MAGIC **Your task:** in the **YOUR TASK** cell, tag `passport_number` as **restricted** and record a one-line justification with `SET TBLPROPERTIES`, then run `verify_tags()`: `passport_number` flips from **MISSING TAG** to **ok**. Because the policy matches the tag, it now covers the column.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Tag the newly added column so the policy covers it:
# MAGIC --      ALTER TABLE customer_profile ALTER COLUMN passport_number SET TAGS ('sensitivity_level' = 'restricted');
# MAGIC -- 2. Record a one-line control justification:
# MAGIC --      ALTER TABLE customer_profile SET TBLPROPERTIES ('privacy.control_justification' = '<one line>');
# MAGIC -- Reveal the solution below if you get stuck.
# MAGIC

# COMMAND ----------

verify_tags()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### REVEAL THE SOLUTION
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:700; color:#FF5F46; cursor:pointer;">Reveal the solution (G)</summary>
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:10px 0;">
# MAGIC <div style="border-radius:6px;overflow:hidden;border:1px solid #DCE0E2;"><div style="display:flex;justify-content:flex-end;background:#EEEDE9;padding:6px 12px;"><button onclick="var pre=this.parentElement.nextElementSibling;var ta=document.createElement('textarea');ta.value=pre.innerText;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);var b=this;b.innerText='Copied!';setTimeout(function(){b.innerText='Copy';},2000);" style="background:#FF5F46;color:#FFFFFF;border:none;border-radius:6px;padding:6px 14px;font-size:14pt;font-weight:700;cursor:pointer;white-space:nowrap;">Copy</button></div><pre style="margin:0;padding:14px;background:#F9F7F4;font-family:Menlo,Consolas,monospace;font-size:14pt;line-height:1.6;color:#0b2026;overflow-x:auto;white-space:pre-wrap;">-- 1. Tag the newly added column. The policy already matches this tag.
# MAGIC ALTER TABLE customer_profile
# MAGIC ALTER COLUMN passport_number SET TAGS ('sensitivity_level' = 'restricted');
# MAGIC
# MAGIC -- 2. Record a one-line justification for the control decision (verifiable evidence).
# MAGIC ALTER TABLE customer_profile SET TBLPROPERTIES (
# MAGIC   'privacy.control_justification' =
# MAGIC   'Least-privilege masking of restricted PII'
# MAGIC );
# MAGIC
# MAGIC -- Then run verify_tags(): passport_number flips from MISSING TAG to ok, so the policy now covers it.</pre></div>
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
# MAGIC .cov-set-h { font-size: 14px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; margin-bottom: 8px; }
# MAGIC .cov-chip { display: inline-block; font-family: monospace; font-size: 14pt; background: rgba(152,16,42,0.10); color: #98102A; border-radius: 6px; padding: 3px 9px; margin: 3px 4px 0 0; }
# MAGIC .cov-chip.new { background: rgba(0,169,114,0.14); color: #007a53; font-weight: 700; }
# MAGIC .cov-arrow { color: #00A972; font-size: 24pt; font-weight: 700; text-align: center; flex: 0 0 auto; }
# MAGIC .cov-note { font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 16px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="cov-wrap">
# MAGIC   <div class="cov-title">One policy and the tag decides the reach</div>
# MAGIC   <div class="cov-flow">
# MAGIC     <div class="cov-set">
# MAGIC       <div class="cov-set-h">Masked after Section E</div>
# MAGIC       <span class="cov-chip">email</span><span class="cov-chip">national_id</span>
# MAGIC     </div>
# MAGIC     <div class="cov-arrow">&rarr;</div>
# MAGIC     <div class="cov-set">
# MAGIC       <div class="cov-set-h">Tag passport_number = restricted</div>
# MAGIC       <span class="cov-chip">email</span><span class="cov-chip">national_id</span><span class="cov-chip new">passport_number</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="cov-note">No <code>ALTER POLICY</code>, no <code>ALTER TABLE</code> on the protection, no new mask. A tag brought the column under the control. This is the difference from the name-based mask, which never would have covered <code>passport_number</code> until someone remembered to add it.</div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this lab, you repaired an inherited, brittle control end to end:
# MAGIC
# MAGIC 1. **Verified the classification** by tracing each detection to its tag and corrected `national_id` from **internal** to **restricted**.
# MAGIC 2. **Replaced the name-based mask** with one schema-scoped, tag-driven ABAC policy that matches `sensitivity_level = restricted`.
# MAGIC 3. **Validated** the control live as an `account users` member outside the EXCEPT set, with the restricted columns masked in your own query and `SHOW EFFECTIVE POLICIES` as the identity-independent evidence.
# MAGIC 4. **Proved the new column is covered** by tagging `passport_number`, with no change to the table or the policy and recorded a one-line justification.
# MAGIC
# MAGIC You've now run the discover, tag and protect steps yourself. Next, in **De-identification and Key Management**, the course goes past masking to pseudonymization and encryption and to protecting the salt and key with Databricks Secrets.

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
# MAGIC   <div class="cl-docs-title">Classification, governed tags and ABAC</div>
# MAGIC   <div class="cl-docs-sub">The classification classes, the governed-tag limits and the <code>CREATE POLICY</code> grammar change over time, so confirm them in the current documentation rather than a static list.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification" target="_blank">Data Classification &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/admin/governed-tags" target="_blank">Governed tags &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/" target="_blank">Unity Catalog ABAC &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/tables/row-and-column-filters" target="_blank">Row filters and column masks &rarr;</a>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>