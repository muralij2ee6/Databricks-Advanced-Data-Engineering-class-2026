# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">04 Demo - ABAC and Manual Masks</span>
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
# MAGIC     <span style="color:#0b2026; font-weight:700;"><span style="color:#00A972;">&#9679;</span> Protect</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Manage</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Verify</span>
# MAGIC   </span>
# MAGIC   <span style="font-size:10.5pt; color:#618794; margin-left:auto;">step 2 of 4</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ABAC and Manual Masks
# MAGIC #### Duration: 30 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC You classified the estate and tagged the sensitive columns. This notebook is the **protect** step: turn those tags into controls that decide who sees the original value and who sees a masked one.
# MAGIC
# MAGIC An analyst needs **customer_profile** for analysis but must not see the raw identifiers. You'll run the query first to see the problem, choose a control with the protection decision model, then build a tag-driven **ABAC** column mask that a policy applies across a whole schema. You'll validate it against a restricted identity and a privileged exception, add a newly tagged column and watch the central policy cover it with no change to the table, then see the manual `SET MASK` and `SET ROW FILTER` alternative and when a large estate needs it.
# MAGIC
# MAGIC > This demo works in its own isolated **priv_secure** schema so the policies and masks it applies never change the shared **priv_demo** estate the later demos read. Because every user is in the built-in `account users` group and you are not in `account privileged`, the policy masks the restricted columns in your own query live, with no group provisioning. It also keeps a deterministic fallback with the policy's own mask function, so the contrast still shows if the policy could not be created.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you'll be able to:
# MAGIC
# MAGIC 1. **Choose** a protection technique by purpose and risk, from minimizing data through masking, filtering, pseudonymizing, encrypting and deleting.
# MAGIC 2. **Explain** the anatomy of a tag-driven ABAC policy: its scope, principal targeting with `TO` and `EXCEPT`, a governed-tag match and a Unity Catalog UDF.
# MAGIC 3. **Apply** a schema-scoped ABAC column mask driven by the `sensitivity_level` tag and **validate** a restricted identity against a privileged exception.
# MAGIC 4. **Demonstrate** that a newly tagged column is covered by the central policy with no change to the table.
# MAGIC 5. **Configure** the manual `SET MASK` and `SET ROW FILTER` alternative and **evaluate** when a governed-tag limit pushes an enterprise back to it.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">Before you begin</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   This demo changes the data it protects, so it works on an <strong>isolated</strong> copy of <code>customer_profile</code> in the <strong>priv_secure</strong> schema. The shared <strong>priv_demo</strong> estate the other demos read is never touched.
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
# MAGIC **NOTE:** This course uses **SQL and Python and was tested on Serverless V6**. Column mask and row filter policies need Databricks Runtime 16.4 or above, which serverless satisfies.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the setup below. It resolves your per-user catalog, ensures the shared **priv_demo** estate exists, builds the isolated **priv_secure** schema with a fresh copy of **customer_profile**, tags `email` and `national_id` as **restricted** and `phone` as **confidential** and prepares the `sensitivity_level` tag.
# MAGIC
# MAGIC If your workspace can't create catalogs, open `./Includes/Classroom-Setup-04` and set `FORCED_CATALOG` to an existing catalog first.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Protect by Purpose and Risk
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-banner { display: flex; align-items: center; gap: 20px; width: 100%; border-radius: 10px; padding: 24px 28px; background: var(--bnr-bg); border-left: 8px solid var(--bnr-accent); box-shadow: 0 2px 10px rgba(27,49,57,0.08); }
# MAGIC .ds-banner-icon { width: 60px; height: 60px; border-radius: 50%; flex-shrink: 0; background: var(--bnr-accent); color: var(--bnr-icon-fg); font-size: 30pt; font-weight: 700; line-height: 1; display: flex; align-items: center; justify-content: center; }
# MAGIC .ds-banner-title { font-size: 22pt; font-weight: 700; color: var(--bnr-title); line-height: 1.2; margin-bottom: 4px; }
# MAGIC .ds-banner-tagline { font-size: 15pt; color: var(--bnr-tagline); line-height: 1.4; }
# MAGIC .ds-banner.info { --bnr-bg: #1B5162; --bnr-accent: #4299E0; --bnr-icon-fg: #FFFFFF; --bnr-title: #FFFFFF; --bnr-tagline: #F9F7F4; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-banner info">
# MAGIC   <div class="ds-banner-icon">&#9873;</div>
# MAGIC   <div>
# MAGIC     <div class="ds-banner-title">An analyst needs the data</div>
# MAGIC     <div class="ds-banner-tagline">A retail analyst needs <strong>customer_profile</strong> to segment customers, but has no reason to read raw emails or national ids. Run the query first, then decide the control.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

spark.sql(f"USE CATALOG `{my_catalog}`")
spark.sql(f"USE SCHEMA `{SECURE_SCHEMA}`")

# No protection is applied yet, so every reader sees every raw identifier.
display(spark.sql(f"""
SELECT customer_id, email, national_id, phone, country
FROM `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
LIMIT 8
"""))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION `{my_catalog}`.`{SECURE_SCHEMA}`.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN email IS NULL THEN NULL
    WHEN INSTR(email, '@') <= 1 THEN email
    ELSE CONCAT(
      LEFT(email, 1),
      REPEAT('*', INSTR(email, '@') - 2),
      SUBSTRING(email, INSTR(email, '@'))
    )
  END
""")
print("[OK] mask_email UDF created.")

# COMMAND ----------

display(spark.sql(f"""
SELECT
  original,
  `{my_catalog}`.`{SECURE_SCHEMA}`.mask_email(original) AS masked
FROM VALUES
  ('alice.jones@example.com'),
  ('a@example.com'),
  (NULL)
AS t(original)
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FF5F46; background: rgba(255,95,70,0.10); padding: 14px 18px; border-radius: 4px; margin: 8px 0 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#98102A; margin-bottom:6px; font-size:15pt;">The problem</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">The analyst has <code>SELECT</code> on the table, so they read <code>email</code> and <code>national_id</code> in the clear. 
# MAGIC <br/>
# MAGIC A grant decides <strong>who can open the table</strong>, but it doesn't decide <strong>which values inside they may see</strong>. 
# MAGIC <br/>
# MAGIC That second decision is the protection control and you choose it by purpose and risk.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. The protection decision model
# MAGIC
# MAGIC The same question runs on every technique: what does the use case actually need and what's the risk if the original leaks. The techniques differ in what they do to the stored value, from keeping it in place and gating who reads it, through transforming it, to not keeping it at all. This demo builds two that leave the value in place, **mask** and **filter rows**.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .pm-card { background: #F9F7F4; border-radius: 10px; padding: 8px 22px; border-top: 8px solid #4299E0; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .pm-item { display: grid; grid-template-columns: 210px 1fr 1fr; gap: 18px; padding: 13px 0; border-bottom: 1px solid #EEEDE9; align-items: center; }
# MAGIC .pm-item:last-child { border-bottom: none; }
# MAGIC .pm-head { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; }
# MAGIC .pm-tech { font-size: 15pt; font-weight: 700; color: #0b2026; overflow-wrap: anywhere; }
# MAGIC .pm-tech .pm-mod { display: inline-block; font-size: 12px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; border-radius: 4px; padding: 1px 7px; margin-top: 5px; }
# MAGIC .pm-this { background: rgba(255,95,70,0.14); color: #98102A; }
# MAGIC .pm-later { background: rgba(66,153,224,0.14); color: #2272B4; }
# MAGIC .pm-what { font-size: 14pt; color: #0b2026; line-height: 1.5; }
# MAGIC .pm-when { font-size: 14pt; color: #0b2026; line-height: 1.5; }
# MAGIC .pm-row-hi { background: rgba(255,95,70,0.06); border-radius: 6px; }
# MAGIC @media (max-width: 640px) { .pm-item { grid-template-columns: 1fr; gap: 4px; } }
# MAGIC .pm-spec { margin: 4px 0 22px; }
# MAGIC .pm-spec-track { display: flex; gap: 12px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .pm-band { flex: 1; min-width: 220px; background: #F9F7F4; border-radius: 8px; padding: 12px 14px; border-top: 6px solid var(--bc, #618794); box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .b-keep { --bc: #FF5F46; } .b-xform { --bc: #4299E0; } .b-gone { --bc: #618794; }
# MAGIC .pm-band-h { font-size: 14pt; font-weight: 700; color: #0b2026; line-height: 1.3; margin-bottom: 8px; }
# MAGIC .pm-pill { display: inline-block; font-size: 14pt; border-radius: 6px; padding: 3px 10px; margin: 5px 6px 0 0; background: #EEEDE9; color: #0b2026; }
# MAGIC .pm-pill.this { background: rgba(255,95,70,0.14); color: #98102A; font-weight: 700; }
# MAGIC .pm-pill.later { background: rgba(66,153,224,0.14); color: #2272B4; font-weight: 600; }
# MAGIC .pm-spec-axis { text-align: center; font-size: 14pt; color: #618794; margin-top: 12px; }
# MAGIC .pm-spec-axis strong { color: #0b2026; font-weight: 700; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="pm-spec">
# MAGIC   <div class="pm-spec-track">
# MAGIC     <div class="pm-band b-keep">
# MAGIC       <div class="pm-band-h">Kept in place, gated at read</div>
# MAGIC       <div><span class="pm-pill this">Mask</span><span class="pm-pill this">Filter rows</span></div>
# MAGIC     </div>
# MAGIC     <div class="pm-band b-xform">
# MAGIC       <div class="pm-band-h">Transformed, recover with a key</div>
# MAGIC       <div><span class="pm-pill later">Pseudonymize</span><span class="pm-pill later">Encrypt</span></div>
# MAGIC     </div>
# MAGIC     <div class="pm-band b-gone">
# MAGIC       <div class="pm-band-h">Not kept, or the exact value removed</div>
# MAGIC       <div><span class="pm-pill">Suppress / generalize</span><span class="pm-pill">Minimize</span><span class="pm-pill later">Delete</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="pm-spec-axis">What happens to the original value: <strong>kept</strong> &rarr; <strong>transformed</strong> &rarr; <strong>removed</strong></div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="pm-card">
# MAGIC   <div class="pm-item">
# MAGIC     <div class="pm-head">Technique</div>
# MAGIC     <div class="pm-head">What it does to the value</div>
# MAGIC     <div class="pm-head">Choose it when</div>
# MAGIC   </div>
# MAGIC   <div class="pm-item">
# MAGIC     <div class="pm-tech">Minimize</div>
# MAGIC     <div class="pm-what">Don't collect or keep it at all.</div>
# MAGIC     <div class="pm-when">The use case never needs the value, so you don't hold it.</div>
# MAGIC   </div>
# MAGIC   <div class="pm-item">
# MAGIC     <div class="pm-tech">Suppress or generalize</div>
# MAGIC     <div class="pm-what">Drop the column, or coarsen it (bucket an age, truncate an IP).</div>
# MAGIC     <div class="pm-when">Analytics needs a shape or a range, not the exact value.</div>
# MAGIC   </div>
# MAGIC   <div class="pm-item pm-row-hi">
# MAGIC     <div class="pm-tech">Mask <span class="pm-mod pm-this">This demo</span></div>
# MAGIC     <div class="pm-what">Replace the value at query time for readers who aren't authorized.</div>
# MAGIC     <div class="pm-when">Some readers need the original and most don't. The row stays, the value is hidden.</div>
# MAGIC   </div>
# MAGIC   <div class="pm-item pm-row-hi">
# MAGIC     <div class="pm-tech">Filter rows <span class="pm-mod pm-this">This demo</span></div>
# MAGIC     <div class="pm-what">Hide whole rows from readers who aren't entitled to them.</div>
# MAGIC     <div class="pm-when">A reader should see only their slice, for example one region or one tenant.</div>
# MAGIC   </div>
# MAGIC   <div class="pm-item">
# MAGIC     <div class="pm-tech">Pseudonymize <span class="pm-mod pm-later">Demo 06</span></div>
# MAGIC     <div class="pm-what">Replace the identifier with a consistent token so rows still join.</div>
# MAGIC     <div class="pm-when">You need linkage across tables without holding the raw identifier.</div>
# MAGIC   </div>
# MAGIC   <div class="pm-item">
# MAGIC     <div class="pm-tech">Encrypt <span class="pm-mod pm-later">Demo 06</span></div>
# MAGIC     <div class="pm-what">Store ciphertext, reversible only with the key.</div>
# MAGIC     <div class="pm-when">An authorized process must recover the original value later.</div>
# MAGIC   </div>
# MAGIC   <div class="pm-item">
# MAGIC     <div class="pm-tech">Delete <span class="pm-mod pm-later">Demo 07</span></div>
# MAGIC     <div class="pm-what">Remove the value from current state, then purge it from storage.</div>
# MAGIC     <div class="pm-when">There is no lawful basis to keep it, as in an erasure request.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">How to choose</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **The decision is purpose first, then risk.** Start from what the use case genuinely needs. If it needs nothing, minimize. If it needs a shape, generalize. If it needs the value for some readers only, mask. The higher the risk of the raw value leaking, the stronger the technique you pick.
# MAGIC - **The techniques stack.** A single column often gets more than one. `national_id` can be masked for analysts and pseudonymized for the join key and later deleted on an erasure request. They aren't mutually exclusive.
# MAGIC - **Mask and filter rows keep the data in place.** They don't change the stored value, they change what a reader sees. That makes them the right first control when the table itself must stay usable for authorized work.
# MAGIC - **The rest of the course covers the others.** Pseudonymization and encryption are Demo 06, deletion is Demo 07. This demo builds the two controls that decide what a reader sees without transforming the stored data.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Tag-Driven ABAC
# MAGIC
# MAGIC Attribute-based access control (ABAC) is one policy that masks every column carrying a governed tag, applied at catalog or schema scope so newly tagged assets are covered without editing the policy.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .pa-code { background: #F9F7F4; border: 1px solid #EEEDE9; border-radius: 8px; padding: 16px 18px; overflow-x: auto; margin: 0 0 18px; font-family: Menlo,Consolas,monospace; font-size: 14pt; line-height: 1.6; color: #0b2026; }
# MAGIC .pa-code .k { color: #1B5162; font-weight: 700; }
# MAGIC .pa-code .b-scope { background: rgba(66,153,224,0.16); border-radius: 3px; padding: 0 3px; }
# MAGIC .pa-code .b-who { background: rgba(0,169,114,0.16); border-radius: 3px; padding: 0 3px; }
# MAGIC .pa-code .b-match { background: rgba(255,171,0,0.20); border-radius: 3px; padding: 0 3px; }
# MAGIC .pa-code .b-action { background: rgba(255,95,70,0.16); border-radius: 3px; padding: 0 3px; }
# MAGIC .pa-row { display: flex; gap: 20px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .pa-row > * { flex: 1; min-width: 230px; }
# MAGIC .pa-part { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 18px 20px; border-top: 8px solid var(--accent); overflow-wrap: anywhere; }
# MAGIC .pa-part .pa-eyebrow { font-size: 13px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--accent); margin-bottom: 4px; }
# MAGIC .pa-part .pa-title { font-size: 16pt; font-weight: 700; color: #0b2026; margin-bottom: 6px; font-family: Menlo,Consolas,monospace; }
# MAGIC .pa-part .pa-body { font-size: 14pt; line-height: 1.5; color: #0b2026; }
# MAGIC .scope{--accent:#4299E0;} .who{--accent:#00A972;} .match{--accent:#B27A00;} .action{--accent:#FF5F46;}
# MAGIC </style>
# MAGIC
# MAGIC <div class="pa-code"><span class="k">CREATE OR REPLACE POLICY</span> mask_secure_restricted<br/>
# MAGIC &nbsp;&nbsp;<span class="k">ON SCHEMA</span> <span class="b-scope">your_catalog.priv_secure</span><br/>
# MAGIC &nbsp;&nbsp;<span class="k">COLUMN MASK</span> <span class="b-action">priv_secure.mask_restricted</span><br/>
# MAGIC &nbsp;&nbsp;<span class="k">TO</span> <span class="b-who">`account users`</span> <span class="k">EXCEPT</span> <span class="b-who">`account privileged`</span><br/>
# MAGIC &nbsp;&nbsp;<span class="k">FOR TABLES</span><br/>
# MAGIC &nbsp;&nbsp;<span class="k">MATCH COLUMNS</span> <span class="b-match">has_tag_value('sensitivity_level', 'restricted')</span> <span class="k">AS</span> c<br/>
# MAGIC &nbsp;&nbsp;<span class="k">ON COLUMN</span> c</div>
# MAGIC
# MAGIC <div class="pa-row">
# MAGIC   <div class="pa-part scope">
# MAGIC     <div class="pa-eyebrow">Scope</div>
# MAGIC     <div class="pa-title">ON SCHEMA</div>
# MAGIC     <div class="pa-body">One policy governs the whole schema. Catalog scope is also allowed. Any table under the scope is covered.</div>
# MAGIC   </div>
# MAGIC   <div class="pa-part who">
# MAGIC     <div class="pa-eyebrow">Who</div>
# MAGIC     <div class="pa-title">TO ... EXCEPT</div>
# MAGIC     <div class="pa-body">The mask applies to every <code>account users</code> member except <code>account privileged</code>. The who lives in the policy.</div>
# MAGIC   </div>
# MAGIC   <div class="pa-part match">
# MAGIC     <div class="pa-eyebrow">Match</div>
# MAGIC     <div class="pa-title">MATCH COLUMNS</div>
# MAGIC     <div class="pa-body"><code>has_tag_value</code> selects every column whose <code>sensitivity_level</code> tag is <code>restricted</code>, so it targets tags, not column names.</div>
# MAGIC   </div>
# MAGIC   <div class="pa-part action">
# MAGIC     <div class="pa-eyebrow">Action</div>
# MAGIC     <div class="pa-title">COLUMN MASK</div>
# MAGIC     <div class="pa-body">A Unity Catalog UDF rewrites the matched column. <code>ON COLUMN c</code> passes the value in as the function's first argument.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Reading the clauses</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Scope decides reach.** A policy on the schema covers every current and future table in it. `MATCH COLUMNS` matches column-level governed tags; to gate on a table, schema or catalog tag you add a `WHEN` condition. A policy can carry up to three `MATCH COLUMNS` expressions and all must match.
# MAGIC - **The who moves into the policy.** `TO` names the principals the policy applies to and `EXCEPT` names the ones it skips. Use account groups, not individuals. Because targeting lives here, the mask function doesn't need to know who's asking.
# MAGIC - **The match targets a governed tag, not a name.** `has_tag_value('sensitivity_level','restricted')` and `has_tag('key')` are the current built-ins. The policy finds columns by their tag, so a brittle list of column names is never part of it.
# MAGIC - **Tag quality is now part of the security boundary.** If a sensitive column is tagged wrong or not at all, the policy doesn't cover it and if a harmless column is over-tagged it gets masked for no reason. This is why the classification and tagging work in Demo 03 matters here: the control is only as good as the tags it matches.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Why Tag-Driven ABAC Is the Primary Approach
# MAGIC
# MAGIC You can protect rows and columns with a **dynamic view** whose `CASE` calls `is_account_group_member()` and you may see that in other material. Tag-driven ABAC is the primary approach in this course, with manual masks as the bounded alternative. Here's how they compare.
# MAGIC <br/><br/>
# MAGIC <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap; justify-content: center; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <div style="border: 1.5px solid #618794; border-radius: 12px; padding: 20px 24px; width: 300px; background: #ffffff;">
# MAGIC     <div style="font-size: 16pt; font-weight: 700; color: #0b2026; text-align: center;">Dynamic view with IS_MEMBER</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center; margin-top: 6px;">A dynamic-view alternative</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 14px; font-size: 14pt; color: #0b2026;">Logic lives in each view</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Who is decided inside a CASE</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Only view readers protected</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Maintained view by view</div>
# MAGIC   </div>
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; gap: 8px; min-width: 130px;">
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center; line-height: 1.4;">this course uses</div>
# MAGIC     <svg width="120" height="20" viewBox="0 0 120 20" style="display: block;">
# MAGIC       <defs><marker id="cmp-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#1B3139" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC       <path d="M5 10 L110 10" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#cmp-arrow)"/>
# MAGIC     </svg>
# MAGIC   </div>
# MAGIC   <div style="border: 2px solid #FF5F46; border-radius: 12px; padding: 20px 24px; width: 300px; background: #F9F7F4;">
# MAGIC     <div style="font-size: 16pt; font-weight: 700; color: #FF5F46; text-align: center;">Tag-driven ABAC</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center; margin-top: 6px;">The primary approach</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 14px; font-size: 14pt; color: #0b2026;">One policy on the schema</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Who is decided in TO and EXCEPT</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">The base table is protected</div>
# MAGIC     <div style="border: 1px dashed #2272B4; border-radius: 6px; background: rgba(34,114,180,0.08); padding: 4px 10px; text-align: center; margin-top: 6px; font-size: 14pt; color: #0b2026;">New tagged columns covered</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why dynamic views don't scale</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **A dynamic view puts the logic and the who in one place.** The `CASE WHEN is_account_group_member(...)` sits inside every view, so the identity check is repeated and the base table is only protected for readers who go through the view. A reader with access to the base table sees everything.
# MAGIC - **ABAC separates the two.** The policy names the principals in `TO` and `EXCEPT` and the mask function only transforms a value. The base table itself is governed, no matter how it's queried, so there's no view to route people through.
# MAGIC - **ABAC covers change automatically.** A schema-scoped policy applies to columns that are tagged later and to tables added later. A dynamic view has to be edited every time the estate changes.
# MAGIC - **Dynamic views still have a place.** For a self-contained, read-only projection with no UDF lifecycle to manage, a dynamic view is simple to ship. For governing the base table across an estate, ABAC is the tool.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Apply a Schema-Scoped Mask Driven by the Tag
# MAGIC
# MAGIC Build the control in two steps: 
# MAGIC <br/>
# MAGIC - create the mask function
# MAGIC - create the policy that applies it to every **restricted** column in the schema. 
# MAGIC <br/><br/>
# MAGIC `email` and `national_id` are already tagged **restricted** on **priv_secure.customer_profile**.
# MAGIC
# MAGIC > What you should see: the function and policy are created. Section E confirms the policy is in force with `SHOW EFFECTIVE POLICIES` and validates the masked-versus-original behavior.

# COMMAND ----------

# The mask is unconditional: it only transforms the value and never checks the caller, because the policy's TO / EXCEPT decides who is masked. 
# Generic, so it covers any restricted STRING column.
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{my_catalog}`.`{SECURE_SCHEMA}`.mask_restricted(val STRING)
RETURNS STRING
RETURN CASE WHEN val IS NULL THEN NULL ELSE concat(left(val, 2), '****') END
""")
print(f"Created {my_catalog}.{SECURE_SCHEMA}.mask_restricted(val STRING).")

# COMMAND ----------

# Creating this needs MANAGE on the schema (you own priv_secure) plus EXECUTE on the function.
policy_sql = f"""
CREATE OR REPLACE POLICY mask_secure_restricted
ON SCHEMA `{my_catalog}`.`{SECURE_SCHEMA}`
COLUMN MASK `{my_catalog}`.`{SECURE_SCHEMA}`.mask_restricted
TO `account users` EXCEPT `account privileged`
FOR TABLES
MATCH COLUMNS has_tag_value('sensitivity_level', 'restricted') AS c
ON COLUMN c
"""
try:
    spark.sql(policy_sql)
    policy_created = True
    print("Created ABAC policy mask_secure_restricted on SCHEMA priv_secure.")
    print("It masks every column tagged sensitivity_level=restricted, for account users, except account privileged.")
except Exception as e:
    policy_created = False
    first = (str(e).splitlines() or [""])[0]
    print("""Couldn't create the policy here.
A tag-driven ABAC policy needs MANAGE on the schema (you have it on priv_secure), the sensitivity_level tag on the columns and the account privileged account group to exist for the EXCEPT clause.
Without it, E4 below shows the same contrast.""")
    print(f"({first})")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFAB00; background: rgba(255,171,0,0.10); padding: 14px 18px; border-radius: 6px; margin: 8px 0; font-family: sans-serif; color: #0b2026; font-size: 14pt; line-height: 1.6;">
# MAGIC   <strong style="display:block; color:#B27A00; font-size:15pt; margin-bottom:6px;">No <code>account privileged</code> group? Drop the EXCEPT clause.</strong>
# MAGIC   <code>EXCEPT account privileged</code> names a custom account group. If it is not provisioned in your account, the <code>CREATE OR REPLACE POLICY</code> above fails because the principal cannot be resolved. Change the policy's principal line to just the <code>TO</code> set:
# MAGIC   <div style="background:#F9F7F4; border:1px solid #EEEDE9; border-radius:6px; padding:10px 14px; margin:10px 0; font-family:Menlo,Consolas,monospace; font-size:14pt; color:#0b2026;">TO &#96;account users&#96;</div>
# MAGIC   The mask then applies to every account user, with no exception. You give up the privileged-reviewer side of the demo (no one reads the originals through the policy), but the mask itself works and the rest of the notebook runs.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Validate Restricted Versus Privileged
# MAGIC
# MAGIC <br/>
# MAGIC <div style="max-width: 960px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="font-size: 15pt; color: #0b2026; line-height: 1.5; margin-bottom: 14px;">The policy decides by identity. The same query returns a different result depending on who runs it, so you see your own result live and read the policy for everyone else's.</div>
# MAGIC <svg width="100%" viewBox="0 0 920 390" role="img" aria-label="You (in account users) and an account privileged reviewer run the same query through one ABAC policy. The policy returns the restricted columns masked to the analyst and in the clear to the reviewer. E1 reads your membership, E2 is your live masked result, E3 reads the policy with SHOW EFFECTIVE POLICIES and E4 is a fallback." style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="ev-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#1B3139" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="16" y="30" width="196" height="26" rx="13" fill="#1B5162" fill-opacity="0.10" stroke="#1B5162" stroke-width="1"/>
# MAGIC   <text x="114" y="47" text-anchor="middle" font-size="14" font-weight="700" fill="#1B5162">E1 &#183; Your membership</text>
# MAGIC   <rect x="16" y="64" width="196" height="78" rx="10" fill="#98102A" fill-opacity="0.06" stroke="#98102A" stroke-width="1.5"/>
# MAGIC   <text x="114" y="98" text-anchor="middle" font-size="17" font-weight="700" fill="#0b2026">You</text>
# MAGIC   <text x="114" y="122" text-anchor="middle" font-size="14" font-family="monospace" fill="#98102A">account users</text>
# MAGIC   <rect x="16" y="286" width="196" height="78" rx="10" fill="#00A972" fill-opacity="0.06" stroke="#00A972" stroke-width="1.5"/>
# MAGIC   <text x="114" y="320" text-anchor="middle" font-size="16" font-weight="700" fill="#0b2026">A reviewer</text>
# MAGIC   <text x="114" y="344" text-anchor="middle" font-size="14" font-family="monospace" fill="#00A972">account privileged</text>
# MAGIC   <rect x="258" y="178" width="160" height="72" rx="8" fill="#EEEDE9" stroke="#1B3139" stroke-width="1.5"/>
# MAGIC   <text x="338" y="209" text-anchor="middle" font-size="16" font-weight="600" fill="#0b2026">The same query</text>
# MAGIC   <text x="338" y="231" text-anchor="middle" font-size="14" fill="#618794">unchanged for both</text>
# MAGIC   <rect x="460" y="118" width="230" height="26" rx="13" fill="#1B5162" fill-opacity="0.10" stroke="#1B5162" stroke-width="1"/>
# MAGIC   <text x="575" y="135" text-anchor="middle" font-size="14" font-weight="700" fill="#1B5162">E3 &#183; SHOW EFFECTIVE POLICIES</text>
# MAGIC   <rect x="460" y="150" width="230" height="128" rx="12" fill="#ffffff" stroke="#1B5162" stroke-width="2"/>
# MAGIC   <text x="575" y="182" text-anchor="middle" font-size="17" font-weight="700" fill="#0b2026">ABAC policy</text>
# MAGIC   <text x="575" y="206" text-anchor="middle" font-size="14" fill="#618794">masks the restricted columns</text>
# MAGIC   <text x="575" y="232" text-anchor="middle" font-size="14" font-family="monospace" fill="#1B5162">TO account users</text>
# MAGIC   <text x="575" y="254" text-anchor="middle" font-size="14" font-family="monospace" fill="#1B5162">EXCEPT account privileged</text>
# MAGIC   <rect x="728" y="30" width="176" height="26" rx="13" fill="#1B5162" fill-opacity="0.10" stroke="#1B5162" stroke-width="1"/>
# MAGIC   <text x="816" y="47" text-anchor="middle" font-size="14" font-weight="700" fill="#1B5162">E2 &#183; Your live result</text>
# MAGIC   <rect x="728" y="64" width="176" height="78" rx="10" fill="#98102A" fill-opacity="0.06" stroke="#98102A" stroke-width="1.5"/>
# MAGIC   <text x="816" y="98" text-anchor="middle" font-size="16" font-weight="700" fill="#98102A">Masked</text>
# MAGIC   <text x="816" y="122" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">ja****  12****</text>
# MAGIC   <rect x="728" y="286" width="176" height="78" rx="10" fill="#00A972" fill-opacity="0.06" stroke="#00A972" stroke-width="1.5"/>
# MAGIC   <text x="816" y="320" text-anchor="middle" font-size="16" font-weight="700" fill="#00A972">Original</text>
# MAGIC   <text x="816" y="344" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">james.smith@&#8230;</text>
# MAGIC   <path d="M212 120 L256 194" fill="none" stroke="#1B3139" stroke-width="1.5" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M212 316 L256 234" fill="none" stroke="#1B3139" stroke-width="1.5" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M418 214 L456 214" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M690 190 L724 118" fill="none" stroke="#1B3139" stroke-width="1.5" marker-end="url(#ev-arrow)"/>
# MAGIC   <path d="M690 238 L724 310" fill="none" stroke="#1B3139" stroke-width="1.5" marker-end="url(#ev-arrow)"/>
# MAGIC </svg>
# MAGIC <div style="font-size: 14pt; color: #5E7077; line-height: 1.55; margin-top: 12px;"><strong>E4</strong> is a deterministic fallback, shown only if the policy could not be created.</div>
# MAGIC </div>

# COMMAND ----------

display(spark.sql("""
SELECT current_user() AS running_as,
       is_account_group_member('account users') AS in_account_users,
       is_account_group_member('account privileged') AS in_account_privileged
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT customer_id, email, national_id, phone
FROM `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
LIMIT 8
"""))

# COMMAND ----------

try:
    display(spark.sql(f"SHOW EFFECTIVE POLICIES ON TABLE `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile"))
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print("SHOW EFFECTIVE POLICIES didn't return here. Confirm the policy in Catalog Explorer instead.")
    print(f"({first})")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 6px; margin: 8px 0; font-family: sans-serif; color: #0b2026; font-size: 14pt; line-height: 1.6;">
# MAGIC   <strong style="display:block; color:#1B5162; font-size:15pt; margin-bottom:6px;">When the fallback runs</strong>
# MAGIC   E4 only adds something when the mask did not already apply to you in E2.
# MAGIC   <ul style="margin:8px 0 0; padding-left:20px;">
# MAGIC     <li style="margin-bottom:8px;"><strong>Policy in force and you are not in <code>account privileged</code>:</strong> E2 already returned the columns masked, so E4 just prints a note.</li>
# MAGIC     <li><strong>Policy could not be created</strong> (for example <code>account privileged</code> is not provisioned, so the <code>EXCEPT</code> clause fails): a bare query returns the originals, so E4 shows each original next to the policy's own mask function and the masked-versus-original contrast stays visible.</li>
# MAGIC   </ul>
# MAGIC </div>

# COMMAND ----------

if policy_created and not spark.sql("SELECT is_account_group_member('account privileged') AS m").first()["m"]:
    print("The policy is in force and you are in account users, not account privileged, so E2 above already showed the mask applied live. The deterministic fallback isn't needed here.")
else:
    display(spark.sql(f"""
    SELECT
      customer_id,
      email AS email_privileged_sees,
      `{my_catalog}`.`{SECURE_SCHEMA}`.mask_restricted(email) AS email_analyst_sees,
      national_id AS national_id_privileged_sees,
      `{my_catalog}`.`{SECURE_SCHEMA}`.mask_restricted(national_id) AS national_id_analyst_sees
    FROM `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
    LIMIT 8
    """))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 6px; margin: 8px 0; font-family: sans-serif; color: #0b2026; font-size: 14pt; line-height: 1.6;">
# MAGIC   <strong>Optional.</strong> The setup rebuilds <code>priv_secure</code> fresh on every run, so you don't need this to re-run the demo. Uncomment the line in the next cell to drop the policy yourself, for example to recreate it without the <code>EXCEPT</code> clause as the note above describes.
# MAGIC </div>

# COMMAND ----------

# spark.sql(f"DROP POLICY mask_secure_restricted ON SCHEMA `{my_catalog}`.`{SECURE_SCHEMA}`")

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
# MAGIC       <div class="out-item"><span class="out-k">In the policy</span><span class="out-v">TO account users</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="out-card privileged">
# MAGIC     <div class="out-band">account privileged (exception)</div>
# MAGIC     <div class="out-body">
# MAGIC       <div class="out-item"><span class="out-k">email</span><span class="out-v">james.smith@example.com</span></div>
# MAGIC       <div class="out-item"><span class="out-k">national_id</span><span class="out-v">123-45-6789</span></div>
# MAGIC       <div class="out-item"><span class="out-k">In the policy</span><span class="out-v">EXCEPT account privileged</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-callout">
# MAGIC   <strong>The control worked.</strong> You just read the masked values live as an account users member outside the EXCEPT set. An account privileged reviewer reads the originals, neither of you changing the query. The decision about who sees what lives in the policy, not in the data and not in the query.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">How to validate two identities from one session</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **E2 is the live proof.** You run as one identity at a time, which E1 prints. You are in `account users` and not in `account privileged`, so E2 masks the two restricted columns on a table you own, with no group provisioning. That is the policy enforcing on your own query.
# MAGIC - **E3 reads the policy for the privileged side.** A single session cannot be two identities, so instead of switching users, `SHOW EFFECTIVE POLICIES` in E3 confirms the same policy is attached and which columns it masks, independent of who queries. An `account privileged` member in the `EXCEPT` set reads the originals.
# MAGIC - **E4 is the deterministic fallback.** If the policy could not be created (for example `account privileged` is not provisioned), E2 shows originals and E4 then shows each original next to `priv_secure.mask_restricted(...)`, the exact function the policy applies, so the contrast is still visible. It is not a separate mask, it is the policy's mask.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. A New Tagged Column Is Covered Automatically
# MAGIC
# MAGIC A schema-scoped policy matches on the tag, not a column list, so any column tagged **restricted** later is covered with no change to the policy or the table. Here the team treats `last_name` (a name, **internal** by default) as **restricted** in this estate, a choice for this scenario rather than a reclassification of names in general. Tag it and watch it join the masked set:
# MAGIC
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1040px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .f-flow { display: flex; gap: 16px; align-items: center; flex-wrap: wrap; justify-content: center; }
# MAGIC .f-panel { flex: 1; min-width: 260px; background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 16px 18px; }
# MAGIC .f-head { font-size: 15pt; font-weight: 700; color: #1B5162; margin-bottom: 4px; }
# MAGIC .f-rule { font-size: 12.5px; font-family: monospace; color: #618794; margin-bottom: 12px; }
# MAGIC .f-label { font-size: 13px; font-weight: 700; letter-spacing: 0.5px; text-transform: uppercase; color: #618794; margin: 10px 0 6px; }
# MAGIC .f-chips { display: flex; gap: 6px; flex-wrap: wrap; }
# MAGIC .f-chip { font-size: 13px; font-weight: 600; border-radius: 999px; padding: 4px 12px; border: 1px solid #C7D0D4; color: #0b2026; background: #ffffff; }
# MAGIC .f-chip.masked { border-color: #98102A; color: #98102A; background: rgba(152,16,42,0.07); }
# MAGIC .f-chip.new { border-width: 2px; box-shadow: 0 0 0 3px rgba(152,16,42,0.12); }
# MAGIC .f-arrow { flex: 0 0 auto; text-align: center; color: #FF5F46; font-weight: 700; font-size: 20pt; min-width: 150px; }
# MAGIC .f-arrow .sub { display: block; font-size: 12px; font-weight: 600; color: #5E7077; font-family: monospace; margin-top: 2px; }
# MAGIC .f-cap { font-size: 14px; color: #618794; text-align: center; margin-top: 14px; line-height: 1.5; }
# MAGIC </style>
# MAGIC <div class="f-flow">
# MAGIC   <div class="f-panel">
# MAGIC     <div class="f-head">Before</div>
# MAGIC     <div class="f-rule">policy matches sensitivity_level = restricted</div>
# MAGIC     <div class="f-label">Masked</div>
# MAGIC     <div class="f-chips"><span class="f-chip masked">email</span><span class="f-chip masked">national_id</span></div>
# MAGIC     <div class="f-label">Visible</div>
# MAGIC     <div class="f-chips"><span class="f-chip">last_name &middot; internal</span><span class="f-chip">phone &middot; confidential</span></div>
# MAGIC   </div>
# MAGIC   <div class="f-arrow">&rarr;<span class="sub">SET TAGS<br/>last_name = restricted</span></div>
# MAGIC   <div class="f-panel">
# MAGIC     <div class="f-head">After</div>
# MAGIC     <div class="f-rule">same policy, unchanged</div>
# MAGIC     <div class="f-label">Masked</div>
# MAGIC     <div class="f-chips"><span class="f-chip masked">email</span><span class="f-chip masked">national_id</span><span class="f-chip masked new">last_name</span></div>
# MAGIC     <div class="f-label">Visible</div>
# MAGIC     <div class="f-chips"><span class="f-chip">phone &middot; confidential</span></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div class="f-cap">Only the tag changed. The policy and the table stayed exactly the same, so <code>last_name</code> joined the masked set on its own.</div>
# MAGIC </div>
# MAGIC
# MAGIC > What you should see: after tagging, `SHOW EFFECTIVE POLICIES` lists `last_name` alongside `email` and `national_id` and the mask preview covers it too. You didn't touch the policy.

# COMMAND ----------

# Applying a governed tag needs ASSIGN on the tag plus APPLY TAG on the table. 
# The try/except reports and continues if this environment does not allow it.
try:
    spark.sql(f"""
        ALTER TABLE `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
        ALTER COLUMN last_name SET TAGS ('sensitivity_level' = 'restricted')
    """)
    print("Tagged priv_secure.customer_profile.last_name sensitivity_level=restricted.")
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print("Couldn't apply the tag here. It needs ASSIGN on the sensitivity_level tag, granted for you at setup.")
    print(f"({first})")

# The SAME policy now covers the new column. Nothing about the policy or the table changed.
try:
    display(spark.sql(f"SHOW EFFECTIVE POLICIES ON TABLE `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile"))
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"SHOW EFFECTIVE POLICIES did not return here. ({first})")

# COMMAND ----------

# The last_name column is now restricted, so the policy applies mask_restricted to it for account users, exactly as it does for email and national_id.
# Preview raw and masked here.
display(spark.sql(f"""
SELECT
  customer_id,
  last_name,
  `{my_catalog}`.`{SECURE_SCHEMA}`.mask_restricted(last_name) AS last_name_analyst_sees
FROM `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
LIMIT 8
"""))

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
# MAGIC .cov-set-h { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; margin-bottom: 8px; }
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
# MAGIC       <div class="cov-set-h">Masked before</div>
# MAGIC       <span class="cov-chip">email</span><span class="cov-chip">national_id</span>
# MAGIC     </div>
# MAGIC     <div class="cov-arrow">&rarr;</div>
# MAGIC     <div class="cov-set">
# MAGIC       <div class="cov-set-h">Tag last_name = restricted</div>
# MAGIC       <span class="cov-chip">email</span><span class="cov-chip">national_id</span><span class="cov-chip new">last_name</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="cov-note">No <code>ALTER POLICY</code>, no <code>ALTER TABLE</code> on the protection, no new grant. A tag brought the column under the control. On a growing estate, that is how the policy keeps covering new columns without edits.</div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Properties for Privacy
# MAGIC
# MAGIC You applied the control and validated it. A Delta table carries key-value **table properties**, and they serve privacy in two ways.
# MAGIC
# MAGIC First, a **governance note you record**: attach a one-line control justification under a custom key so the decision stays accountable, saying what's masked, for whom, who is excepted and why. That is documentation that travels with the table (not a Databricks feature), and any reviewer reads it back with `SHOW TBLPROPERTIES`.
# MAGIC
# MAGIC Second, built-in **`delta.*` settings that govern behavior**. These change how deletes, purging, history and change records work, which is central once you reach erasure:
# MAGIC
# MAGIC | property | what it governs | default |
# MAGIC | --- | --- | --- |
# MAGIC | `privacy.control_justification` (custom) | your recorded reason for the control | none |
# MAGIC | `delta.deletedFileRetentionDuration` | how long deleted data files persist before `VACUUM` may purge them | 7 days |
# MAGIC | `delta.logRetentionDuration` | how long table history and time travel are kept | 30 days |
# MAGIC | `delta.enableDeletionVectors` | whether a delete marks rows (soft) or rewrites files | workspace default |
# MAGIC | `delta.enableChangeDataFeed` | whether row-level change records are kept | off |
# MAGIC
# MAGIC The erasure and de-identification demos configure the `delta.*` ones. Here you just record the justification, since that's the note the Discover, Tag and Protect PII lab asks you to write.

# COMMAND ----------

# Record a one-line justification for this control under a custom key, then read it back.
spark.sql(f"""
ALTER TABLE `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile SET TBLPROPERTIES (
  'privacy.control_justification' =
  'Least-privilege masking of restricted PII.'
)
""")
# SHOW TBLPROPERTIES lists every property set on the table: your justification plus any delta.* settings.
display(spark.sql(f"SHOW TBLPROPERTIES `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## G. When You Fall Back to Manual Row Filters and Column Masks
# MAGIC
# MAGIC ABAC is the primary control, but governed tags have an account limit, so a very large estate can't tag everything. For those columns you attach a control directly to one table with `ALTER TABLE ... SET MASK` and `ALTER TABLE ... SET ROW FILTER`. 
# MAGIC <br/>
# MAGIC Both are important because a real enterprise uses both.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-row { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 280px; }
# MAGIC .blue{--accent:#4299E0;} .amber{--accent:#FFAB00;}
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px; border-top: 8px solid var(--accent, #4299E0); display: flex; flex-direction: column; overflow-wrap: anywhere; }
# MAGIC .ds-card-title { font-size: 17pt; font-weight: 700; line-height: 1.25; margin: 0 0 4px; }
# MAGIC .ds-card-sub { font-size: 14pt; font-weight: 600; color: #1B5162; margin: 0 0 10px; line-height: 1.4; }
# MAGIC .ds-card ul { margin: 4px 0 0; padding-left: 20px; font-size: 14pt; line-height: 1.55; color: #0b2026; }
# MAGIC .ds-card li { margin-bottom: 8px; } .ds-card li:last-child { margin-bottom: 0; }
# MAGIC .ds-card code { font-family: monospace; background: #EEEDE9; padding: 1px 5px; border-radius: 3px; color: #1B5162; }
# MAGIC .ds-callout { border-left: 6px solid #FFAB00; border-radius: 8px; padding: 16px 22px; background: rgba(255,171,0,0.12); box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 20px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-card blue">
# MAGIC     <div class="ds-card-title">Tag-driven ABAC</div>
# MAGIC     <ul>
# MAGIC       <li>Central and new tagged assets are covered automatically.</li>
# MAGIC       <li>Needs a governed tag and an account allows <strong>1,000</strong> of them.</li>
# MAGIC       <li>The right default for a consistent, growing estate.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div class="ds-card amber">
# MAGIC     <div class="ds-card-title">Manual SET MASK / SET ROW FILTER</div>
# MAGIC     <ul>
# MAGIC       <li>Attach a UDF straight to a table, no governed tag required.</li>
# MAGIC       <li>The bounded alternative when the estate exceeds the tag limit.</li>
# MAGIC       <li>The who lives back inside the UDF, so it is more to maintain.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 20px auto 0;">
# MAGIC <svg width="100%" viewBox="0 0 860 300" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="gfo-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#1B3139" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <text x="205" y="26" text-anchor="middle" font-size="16" font-weight="700" fill="#2272B4">Tag-driven ABAC</text>
# MAGIC   <text x="645" y="26" text-anchor="middle" font-size="16" font-weight="700" fill="#B27A00">Manual controls</text>
# MAGIC   <line x1="430" y1="42" x2="430" y2="248" stroke="#EEEDE9" stroke-width="1.5"/>
# MAGIC   <rect x="20" y="112" width="130" height="60" rx="8" fill="#4299E0" fill-opacity="0.12" stroke="#4299E0" stroke-width="1.5"/>
# MAGIC   <text x="85" y="138" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026">ABAC policy</text>
# MAGIC   <text x="85" y="158" text-anchor="middle" font-size="14" fill="#618794">one governed tag</text>
# MAGIC   <rect x="250" y="62" width="158" height="44" rx="8" fill="#4299E0" fill-opacity="0.06" stroke="#4299E0" stroke-width="1.2"/>
# MAGIC   <text x="329" y="89" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">email</text>
# MAGIC   <rect x="250" y="119" width="158" height="44" rx="8" fill="#4299E0" fill-opacity="0.06" stroke="#4299E0" stroke-width="1.2"/>
# MAGIC   <text x="329" y="146" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">national_id</text>
# MAGIC   <rect x="250" y="176" width="158" height="44" rx="8" fill="#4299E0" fill-opacity="0.06" stroke="#4299E0" stroke-width="1.2"/>
# MAGIC   <text x="329" y="203" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">last_name</text>
# MAGIC   <path d="M150 132 L245 84" fill="none" stroke="#1B3139" stroke-width="1.4" marker-end="url(#gfo-arrow)"/>
# MAGIC   <path d="M150 142 L245 141" fill="none" stroke="#1B3139" stroke-width="1.4" marker-end="url(#gfo-arrow)"/>
# MAGIC   <path d="M150 152 L245 198" fill="none" stroke="#1B3139" stroke-width="1.4" marker-end="url(#gfo-arrow)"/>
# MAGIC   <text x="214" y="268" text-anchor="middle" font-size="14" fill="#0b2026">One policy covers every tagged column.</text>
# MAGIC   <text x="214" y="288" text-anchor="middle" font-size="14" fill="#618794">New tagged assets join automatically.</text>
# MAGIC   <rect x="452" y="62" width="150" height="44" rx="8" fill="#FFAB00" fill-opacity="0.12" stroke="#FFAB00" stroke-width="1.4"/>
# MAGIC   <text x="527" y="89" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">SET MASK</text>
# MAGIC   <rect x="674" y="62" width="150" height="44" rx="8" fill="#ffffff" stroke="#618794" stroke-width="1.4"/>
# MAGIC   <text x="749" y="89" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">table A</text>
# MAGIC   <path d="M602 84 L669 84" fill="none" stroke="#1B3139" stroke-width="1.4" marker-end="url(#gfo-arrow)"/>
# MAGIC   <rect x="452" y="119" width="150" height="44" rx="8" fill="#FFAB00" fill-opacity="0.12" stroke="#FFAB00" stroke-width="1.4"/>
# MAGIC   <text x="527" y="146" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">SET ROW FILTER</text>
# MAGIC   <rect x="674" y="119" width="150" height="44" rx="8" fill="#ffffff" stroke="#618794" stroke-width="1.4"/>
# MAGIC   <text x="749" y="146" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">table B</text>
# MAGIC   <path d="M602 141 L669 141" fill="none" stroke="#1B3139" stroke-width="1.4" marker-end="url(#gfo-arrow)"/>
# MAGIC   <rect x="452" y="176" width="150" height="44" rx="8" fill="#FFAB00" fill-opacity="0.12" stroke="#FFAB00" stroke-width="1.4"/>
# MAGIC   <text x="527" y="203" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">SET MASK</text>
# MAGIC   <rect x="674" y="176" width="150" height="44" rx="8" fill="#ffffff" stroke="#618794" stroke-width="1.4"/>
# MAGIC   <text x="749" y="203" text-anchor="middle" font-size="14" font-family="monospace" fill="#0b2026">table C</text>
# MAGIC   <path d="M602 198 L669 198" fill="none" stroke="#1B3139" stroke-width="1.4" marker-end="url(#gfo-arrow)"/>
# MAGIC   <text x="637" y="268" text-anchor="middle" font-size="14" fill="#0b2026">One control per table, written by hand.</text>
# MAGIC   <text x="637" y="288" text-anchor="middle" font-size="14" fill="#618794">Repeat it for every table.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-callout">
# MAGIC   <strong>Why both are important.</strong> The documented ceiling is 1,000 governed tags per account. That's small next to the tens of thousands of columns a large enterprise wants to govern, so those estates keep some data on manual row filters and column masks. Use ABAC first and manual controls for the columns beyond the tag budget.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">The tag limit</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **The account limit is the reason to understand both.** A governed tag drives ABAC and an account allows 1,000 of them. A taxonomy like `sensitivity_level` uses one tag with four values, so most estates fit. A very large estate that governs by many fine-grained tags can run out and those columns fall back to manual controls.
# MAGIC - **The manual UDF branches on the caller.** A manual `SET MASK` has no `TO` or `EXCEPT`, so the function itself has to decide who sees what with `is_account_group_member(...)`. This is the one place the identity check stays inside the function, which is exactly what ABAC moved out into the policy.
# MAGIC - **Manual controls protect the base table too.** Like ABAC, a row filter or column mask attached with `ALTER TABLE` governs the table for every reader, unlike a dynamic view. They are per table, so you repeat them table by table.
# MAGIC - **Row filters return a boolean.** A row filter UDF returns `TRUE` for rows the caller may see. A column mask UDF returns the value to show. Both bind to columns when you attach them.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G1. Manual Column Mask on `phone`
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1040px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .mm-why { border-left: 6px solid #FFAB00; background: rgba(255,171,0,0.10); border-radius: 8px; padding: 14px 18px; font-size: 14pt; line-height: 1.55; margin-bottom: 18px; }
# MAGIC .mm-why code, .mm-body code { font-family: monospace; background: #EEEDE9; padding: 1px 5px; border-radius: 3px; color: #1B5162; }
# MAGIC .mm-flow { display: flex; gap: 16px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .mm-step { flex: 1; min-width: 300px; background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 8px solid #FFAB00; padding: 16px 20px; display: flex; flex-direction: column; }
# MAGIC .mm-head { display: flex; align-items: center; gap: 10px; margin-bottom: 10px; }
# MAGIC .mm-n { display: inline-flex; align-items: center; justify-content: center; width: 28px; height: 28px; flex-shrink: 0; border-radius: 50%; background: #FFAB00; color: #0b2026; font-weight: 700; font-size: 14pt; }
# MAGIC .mm-title { font-size: 16pt; font-weight: 700; color: #0b2026; line-height: 1.2; }
# MAGIC .mm-code { font-family: monospace; font-size: 14px; background: #EEEDE9; color: #1B5162; padding: 6px 10px; border-radius: 4px; margin-bottom: 10px; overflow-wrap: anywhere; }
# MAGIC .mm-body { font-size: 14pt; line-height: 1.55; color: #0b2026; }
# MAGIC .mm-arrow { flex: 0 0 auto; align-self: center; color: #FFAB00; font-size: 26pt; font-weight: 700; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="mm-why">
# MAGIC   <code>phone</code> is tagged <strong>confidential</strong>. The ABAC policy masks only <strong>restricted</strong> columns, so it never covers <code>phone</code>. Protect it by hand with a mask bound straight to the column. A manual mask has no <code>TO</code> or <code>EXCEPT</code> clause, so the function itself decides who sees the original value.
# MAGIC </div>
# MAGIC
# MAGIC <div class="mm-flow">
# MAGIC   <div class="mm-step">
# MAGIC     <div class="mm-head"><span class="mm-n">1</span><span class="mm-title">Create the mask function</span></div>
# MAGIC     <div class="mm-code">CREATE FUNCTION mask_phone_manual(val)</div>
# MAGIC     <div class="mm-body">The UDF checks the caller with <code>is_account_group_member('account privileged')</code>. Privileged reviewers get the raw value and everyone else gets a masked one. That identity check sits inside the function, which is what the ABAC policy moved out into <code>TO</code>/<code>EXCEPT</code>.</div>
# MAGIC   </div>
# MAGIC   <div class="mm-arrow">&rarr;</div>
# MAGIC   <div class="mm-step">
# MAGIC     <div class="mm-head"><span class="mm-n">2</span><span class="mm-title">Attach it to the column</span></div>
# MAGIC     <div class="mm-code">ALTER COLUMN phone SET MASK ...</div>
# MAGIC     <div class="mm-body">Bind the function to <code>phone</code> on this one table. Drop any existing mask first so a re-run stays clean, because a column holds a single mask at a time.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION `{my_catalog}`.`{SECURE_SCHEMA}`.mask_phone_manual(val STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('account privileged') THEN val
  WHEN val IS NULL THEN NULL
  ELSE concat(left(val, 3), '******')
END
""")
print("Created the manual mask function priv_secure.mask_phone_manual.")

# COMMAND ----------

# Drop any existing mask first so a re-run is clean (a column keeps one mask).
try:
    spark.sql(f"ALTER TABLE `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile ALTER COLUMN phone DROP MASK")
except Exception:
    pass
spark.sql(f"""
ALTER TABLE `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
ALTER COLUMN phone SET MASK `{my_catalog}`.`{SECURE_SCHEMA}`.mask_phone_manual
""")
print("Attached a manual column mask to priv_secure.customer_profile.phone.")

# COMMAND ----------

# A row filter returns TRUE for rows the caller may see. Here account privileged sees every row and everyone else sees only United States rows. 
# This is the manual, per-table equivalent of an ABAC row filter policy.
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{my_catalog}`.`{SECURE_SCHEMA}`.region_row_filter(country STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('account privileged') OR country = 'United States'
""")

try:
    spark.sql(f"ALTER TABLE `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile DROP ROW FILTER")
except Exception:
    pass
spark.sql(f"""
ALTER TABLE `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
SET ROW FILTER `{my_catalog}`.`{SECURE_SCHEMA}`.region_row_filter ON (country)
""")
print("Attached a manual row filter on country to priv_secure.customer_profile.")

# COMMAND ----------

# DESCRIBE EXTENDED lists the attached column masks and the row filter, the evidence that the manual controls are in place on this one table.
display(spark.sql(f"DESCRIBE EXTENDED `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile"))

# COMMAND ----------

# Query the table as yourself: you are in account users, not account privileged. 
# phone comes back masked by the column mask from G1. 
# email stays masked by the ABAC policy from section D, so the manual mask and the policy both apply to the same table.
# <br/>
display(spark.sql(f"""
SELECT customer_id, first_name, phone, email, country
FROM `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
ORDER BY customer_id
LIMIT 10
"""))

# The row filter from G2 is clearest as a country breakdown: as a non-privileged caller you get only United States rows back, though the table holds customers from ten countries.
display(spark.sql(f"""
SELECT country, count(*) AS rows_visible_to_you
FROM `{my_catalog}`.`{SECURE_SCHEMA}`.customer_profile
GROUP BY country
ORDER BY country
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## H. Ask Genie Code to Draft the UDF, Then Verify by Hand
# MAGIC
# MAGIC You wrote the mask functions by hand, so you know what a correct one looks like. That's the position to be in before you let the assistant write one. Ask **Genie Code** to draft a masking UDF, then verify it on real inputs and edge cases before you trust it in a policy.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Try it: ask Genie Code</div>
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">Click <strong>Copy</strong> and paste the prompt into Genie Code in the notebook or SQL editor. Then read the function it returns and run the verification cell below before you attach it to anything.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">Write a Unity Catalog SQL UDF that masks an email address: keep the first character and the domain, replace the rest of the local part with asterisks and return NULL for NULL. Do not check group membership. Then give me three test inputs including an edge case.</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# A masking UDF of the kind Genie Code drafts for the prompt above.
# Before it goes near a policy, run it on representative inputs and edge cases: a normal address, a short one, a value that isn't an email and NULL.
# Reading the output is how you catch a wrong answer.
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{my_catalog}`.`{SECURE_SCHEMA}`.mask_email_draft(email STRING)
RETURNS STRING
RETURN CASE
  WHEN email IS NULL THEN NULL
  WHEN email NOT LIKE '%@%' THEN '****'
  ELSE concat(left(email, 1), '****@', substring_index(email, '@', -1))
END
""")

display(spark.sql(f"""
SELECT input, `{my_catalog}`.`{SECURE_SCHEMA}`.mask_email_draft(input) AS masked
FROM (VALUES
  ('james.smith@example.com'),
  ('a@b.co'),
  ('not-an-email'),
  (CAST(NULL AS STRING))
) AS t(input)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, you:
# MAGIC
# MAGIC 1. Ran the analyst's query first and saw that a grant opens the table but doesn't hide the columns, then chose a control with the **protection decision model**.
# MAGIC 2. Read the anatomy of a **tag-driven ABAC** policy: scope, `TO` and `EXCEPT`, a governed-tag match and a UDF and saw why it replaced dynamic views with `is_account_group_member()`.
# MAGIC 3. Built a schema-scoped **column mask** driven by the `sensitivity_level` tag and **validated** a restricted analyst against a privileged exception.
# MAGIC 4. Tagged a new column and confirmed the central policy **covered it automatically**, with no change to the table.
# MAGIC 5. Applied the manual `SET MASK` and `SET ROW FILTER` alternative and saw the governed-tag limit that makes an enterprise keep both.
# MAGIC
# MAGIC Next, in **De-identification and Key Management**, you'll go past masking to pseudonymization and encryption: replace an identifier with a consistent token so rows still join, protect the salt and key with Databricks Secrets instead of hardcoding them and see why a masked query result isn't anonymization.

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
# MAGIC   <div class="cl-docs-title">ABAC, governed tags and manual masks</div>
# MAGIC   <div class="cl-docs-sub">The ABAC syntax and the tag limits change over time, so confirm the <code>CREATE POLICY</code> grammar, the governed-tag ceilings and the row-filter and column-mask commands in the current documentation rather than a static list.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/" target="_blank">Unity Catalog ABAC &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/best-practices" target="_blank">ABAC best practices &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/tables/row-and-column-filters" target="_blank">Row filters and column masks &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/admin/governed-tags" target="_blank">Governed tags &rarr;</a>
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