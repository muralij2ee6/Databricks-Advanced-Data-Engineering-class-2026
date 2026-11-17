# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">06 Demo - De-identification and Key Management</span>
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
# MAGIC # De-identification and Key Management
# MAGIC #### Duration: 30 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Masking hides a value from a reader at query time. This notebook goes further: it changes the value that's stored, so an analyst can work with the data while the person behind it is protected. It's the second half of the **protect** step, one level deeper than the query-time masking of Demo 04.
# MAGIC
# MAGIC An analyst needs to join customer behavior across tables by customer, but must not be able to recover the person. The visual below sets up that use case and the path this demo takes through it. Throughout, the salt and the key come from **Databricks Secrets**, never from the notebook.
# MAGIC
# MAGIC > This demo reads the shared **priv_demo** estate and writes every de-identified output into its own **priv_deid** schema, with the reversal mapping kept in a separate **priv_vault**. It reads a salt and an AES key from the `dbacademy_privacy` secret scope.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you'll be able to:
# MAGIC
# MAGIC 1. **Apply** data minimization and **distinguish** anonymization from pseudonymization, explaining why a pseudonym is still personal data.
# MAGIC 2. **Construct** a salted `sha2` pseudonym that keeps rows joinable without being reversible, reading the salt from `secret()` instead of a hardcoded literal.
# MAGIC 3. **Design** a token vault so reversal is possible only through a separately governed table.
# MAGIC 4. **Configure** column-level encryption with `aes_encrypt` and `aes_decrypt` for authorized reversal in place, keyed from a secret.
# MAGIC 5. **Evaluate** suppression and generalization for analytics and choose the technique that fits by purpose and risk.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .uc-eyebrow { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; margin: 6px 0 14px; }
# MAGIC .uc-scene { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
# MAGIC .uc-tbl { flex: 1 1 240px; min-width: 240px; background: #F9F7F4; border: 1px solid #EEEDE9; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); overflow: hidden; }
# MAGIC .uc-tbl-h { background: #1B5162; color: #fff; font-family: monospace; font-size: 14pt; font-weight: 700; padding: 9px 14px; }
# MAGIC .uc-rows { padding: 10px 14px; }
# MAGIC .uc-r { font-family: monospace; font-size: 14pt; line-height: 1.95; color: #0b2026; white-space: nowrap; }
# MAGIC .uc-r.id { color: #98102A; }
# MAGIC .uc-tag { display: inline-block; font-family: sans-serif; font-size: 10.5pt; font-weight: 700; border-radius: 5px; padding: 1px 7px; margin-left: 8px; }
# MAGIC .uc-tag-key { background: rgba(66,153,224,0.14); color: #2272B4; }
# MAGIC .uc-tag-id { background: rgba(152,16,42,0.10); color: #98102A; }
# MAGIC .uc-join { flex: 0 0 auto; display: flex; flex-direction: column; align-items: center; justify-content: center; min-width: 120px; text-align: center; }
# MAGIC .uc-join-badge { background: #fff; border: 2px solid #4299E0; color: #2272B4; border-radius: 22px; font-weight: 700; font-size: 12.5pt; padding: 6px 16px; white-space: nowrap; }
# MAGIC .uc-join-cap { font-size: 12pt; color: #618794; margin-top: 8px; line-height: 1.4; }
# MAGIC .uc-callout { border-left: 6px solid #4299E0; border-radius: 8px; padding: 14px 20px; background: #F8F9FC; font-size: 14pt; line-height: 1.55; margin: 20px 0 6px; }
# MAGIC .uc-flow { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }
# MAGIC .uc-step { flex: 1 1 175px; min-width: 175px; background: #fff; border: 1px solid #EEEDE9; border-top: 5px solid var(--st); border-radius: 8px; padding: 12px 14px; }
# MAGIC .uc-step-head { display: flex; align-items: center; gap: 8px; margin-bottom: 3px; }
# MAGIC .uc-step-n { flex: 0 0 auto; display: inline-flex; align-items: center; justify-content: center; width: 22px; height: 22px; border-radius: 50%; background: var(--st); color: #fff; font-size: 11pt; font-weight: 700; }
# MAGIC .uc-step-t { font-weight: 700; font-size: 14pt; color: #0b2026; line-height: 1.15; }
# MAGIC .uc-step-p { font-size: 13pt; color: #4b5c63; line-height: 1.45; margin-top: 7px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="uc-eyebrow">The use case</div>
# MAGIC
# MAGIC <div class="uc-scene">
# MAGIC   <div class="uc-tbl">
# MAGIC     <div class="uc-tbl-h">customer_profile</div>
# MAGIC     <div class="uc-rows">
# MAGIC       <div class="uc-r">customer_id <span class="uc-tag uc-tag-key">join key</span></div>
# MAGIC       <div class="uc-r id">first_name <span class="uc-tag uc-tag-id">identity</span></div>
# MAGIC       <div class="uc-r id">email <span class="uc-tag uc-tag-id">identity</span></div>
# MAGIC       <div class="uc-r id">national_id <span class="uc-tag uc-tag-id">identity</span></div>
# MAGIC       <div class="uc-r">country</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="uc-join">
# MAGIC     <div class="uc-join-badge">&#8904; JOIN on customer_id</div>
# MAGIC     <div class="uc-join-cap">The analyst studies behavior across both tables.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="uc-tbl">
# MAGIC     <div class="uc-tbl-h">clickstream</div>
# MAGIC     <div class="uc-rows">
# MAGIC       <div class="uc-r">customer_id <span class="uc-tag uc-tag-key">join key</span></div>
# MAGIC       <div class="uc-r">event_time</div>
# MAGIC       <div class="uc-r">page</div>
# MAGIC       <div class="uc-r">action</div>
# MAGIC       <div class="uc-r">device</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="uc-callout">
# MAGIC   The join needs one <strong>stable key per customer</strong>. Today that key is <code>customer_id</code>, which points straight back to the person. The profile also carries names, emails and national ids the study never uses. The goal is a key the analyst can still join on that <strong>cannot be turned back into the person</strong>.
# MAGIC </div>
# MAGIC
# MAGIC <div class="uc-eyebrow">The path this demo takes</div>
# MAGIC <div class="uc-flow">
# MAGIC   <div class="uc-step" style="--st:#00A972;"><div class="uc-step-head"><span class="uc-step-n">1</span><span class="uc-step-t">Minimize</span></div><div class="uc-step-p">Keep only what the study needs. Drop the direct identifiers.</div></div>
# MAGIC   <div class="uc-step" style="--st:#4299E0;"><div class="uc-step-head"><span class="uc-step-n">2</span><span class="uc-step-t">Pseudonymize</span></div><div class="uc-step-p">A salted <code>sha2</code> key: still joinable, no way back to the person.</div></div>
# MAGIC   <div class="uc-step" style="--st:#2272B4;"><div class="uc-step-head"><span class="uc-step-n">3</span><span class="uc-step-t">Tokenize</span></div><div class="uc-step-p">A separate vault for when reversal must be possible, but only there.</div></div>
# MAGIC   <div class="uc-step" style="--st:#FFAB00;"><div class="uc-step-head"><span class="uc-step-n">4</span><span class="uc-step-t">Encrypt</span></div><div class="uc-step-p"><code>aes_encrypt</code> for authorized reversal in place, keyed from a secret.</div></div>
# MAGIC   <div class="uc-step" style="--st:#618794;"><div class="uc-step-head"><span class="uc-step-n">5</span><span class="uc-step-t">Generalize</span></div><div class="uc-step-p">Reduce precision or suppress what can still single someone out.</div></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">Before you begin</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   This demo reads a salt and an AES key from the <code>dbacademy_privacy</code> secret scope with the SQL <code>secret()</code> function. Where the secret is missing, the de-identification cells print guidance and continue rather than failing.
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
# MAGIC **NOTE:** This course uses **SQL and Python and was tested on Serverless V6**. The `sha2`, `aes_encrypt`, `aes_decrypt` and `secret` functions all run on serverless.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the setup below. It resolves your per-user catalog, ensures the shared **priv_demo** estate exists, creates the isolated **priv_deid** analytics schema this demo writes into, stages the separately governed **priv_vault** token vault and probes the `dbacademy_privacy` secret scope so you know whether the salt and key resolve here.
# MAGIC
# MAGIC If your workspace can't create catalogs, open `./Includes/Classroom-Setup-06` and set `FORCED_CATALOG` to an existing catalog first.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Minimize First
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
# MAGIC   <div class="ds-banner-icon">&#9635;</div>
# MAGIC   <div>
# MAGIC     <div class="ds-banner-title">Minimize before you transform</div>
# MAGIC     <div class="ds-banner-tagline">The safest sensitive value is the one the study never needs. Run the query first to see everything the source holds, then keep only what the analyst's join and metrics require.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

spark.sql(f"USE CATALOG `{my_catalog}`")
spark.sql(f"USE SCHEMA `{DEMO_SCHEMA}`")

# The shared source table carries far more than a behavioral study needs.
display(spark.sql(f"""
SELECT customer_id, first_name, last_name, email, national_id, country, loyalty_tier, date_of_birth, ip_address
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile
LIMIT 8
"""))

# COMMAND ----------

# As we don't need the direct identifiers, this drops name, email and national_id. 
# customer_id stays for now, but it still points straight back to the person, so it's the next thing to fix.
display(spark.sql(f"""
SELECT customer_id, country, loyalty_tier, date_of_birth, ip_address
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile
LIMIT 8
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .mz-card { background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 8px solid #00A972; padding: 20px 24px; }
# MAGIC .mz-title { font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 14px; }
# MAGIC .mz-cols { display: flex; gap: 16px; flex-wrap: wrap; }
# MAGIC .mz-col { flex: 1; min-width: 260px; border: 1px solid #EEEDE9; border-radius: 8px; background: #fff; padding: 14px 16px; }
# MAGIC .mz-col-h { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; margin-bottom: 10px; }
# MAGIC .mz-chip { display: inline-block; font-family: monospace; font-size: 14pt; border-radius: 6px; padding: 3px 9px; margin: 3px 4px 0 0; }
# MAGIC .mz-drop .mz-col-h { color: #98102A; }
# MAGIC .mz-drop .mz-chip { background: rgba(152,16,42,0.10); color: #98102A; text-decoration: line-through; }
# MAGIC .mz-keep .mz-col-h { color: #2272B4; }
# MAGIC .mz-keep .mz-chip { background: rgba(66,153,224,0.12); color: #2272B4; }
# MAGIC .mz-flag .mz-col-h { color: #B27A00; }
# MAGIC .mz-flag .mz-chip { background: rgba(255,171,0,0.16); color: #B27A00; }
# MAGIC .ds-callout { border-left: 6px solid #FF5F46; border-radius: 8px; padding: 16px 22px; background: rgba(255,95,70,0.10); box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 20px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="mz-card">
# MAGIC   <div class="mz-title">Minimization is the first control</div>
# MAGIC   <div class="mz-cols">
# MAGIC     <div class="mz-col mz-drop">
# MAGIC       <div class="mz-col-h">Drop, the study never needs it</div>
# MAGIC       <span class="mz-chip">first_name</span><span class="mz-chip">last_name</span><span class="mz-chip">email</span><span class="mz-chip">national_id</span>
# MAGIC     </div>
# MAGIC     <div class="mz-col mz-keep">
# MAGIC       <div class="mz-col-h">Keep, useful and low risk</div>
# MAGIC       <span class="mz-chip">country</span><span class="mz-chip">loyalty_tier</span>
# MAGIC     </div>
# MAGIC     <div class="mz-col mz-flag">
# MAGIC       <div class="mz-col-h">Still identifies, fix next</div>
# MAGIC       <span class="mz-chip">customer_id</span><span class="mz-chip">date_of_birth</span><span class="mz-chip">ip_address</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-callout"><strong>Keep only what the use case needs.</strong> Dropping the direct identifiers costs the study nothing. What's left still has a problem: <code>customer_id</code> links straight back to the person and <code>date_of_birth</code> and <code>ip_address</code> can single someone out. The rest of this notebook fixes those without breaking the analyst's join.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why minimize before you reach for a technique</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Data you don't hold can't leak.** Every column you drop from the analytics copy is one you never have to mask, pseudonymize, encrypt or erase later. Minimization removes work and risk at the same time.
# MAGIC - **Purpose decides what is needed.** A behavioral study needs to link a customer's rows and a few broad attributes. It doesn't need the name or the national id, so those go.
# MAGIC - **A linkage key isn't a free pass.** `customer_id` is what lets the study join behavior, but it's also the primary key of `customer_profile`, so it points straight back to the person. That's the exact case pseudonymization is for, which is Section D.
# MAGIC - **Quasi-identifiers still single people out.** A precise `date_of_birth` and a full `ip_address` can re-identify someone even with no name attached. Section H generalizes them.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Anonymization Versus Pseudonymization
# MAGIC
# MAGIC These two words are often used as if they mean the same thing. They don't and the difference decides whether privacy rules still apply. Pseudonymization keeps a way back to the person, anonymization doesn't.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ap-row { display: flex; align-items: center; gap: 18px; flex-wrap: wrap; justify-content: center; }
# MAGIC .ap-box { border-radius: 12px; padding: 20px 24px; width: 360px; background: #fff; }
# MAGIC .ap-pseudo { border: 2px solid #FFAB00; background: rgba(255,171,0,0.06); }
# MAGIC .ap-anon { border: 2px solid #00A972; background: rgba(0,169,114,0.06); }
# MAGIC .ap-h { font-size: 17pt; font-weight: 700; text-align: center; }
# MAGIC .ap-sub { font-size: 14pt; color: #5E7077; text-align: center; margin: 4px 0 12px; }
# MAGIC .ap-item { border: 1px solid #EEEDE9; border-radius: 6px; background: #fff; padding: 7px 12px; margin-top: 6px; font-size: 14pt; color: #0b2026; }
# MAGIC .ap-item.key { border-style: dashed; }
# MAGIC .ap-pseudo .ap-h { color: #B27A00; } .ap-pseudo .ap-item.key { border-color: #B27A00; background: rgba(255,171,0,0.10); font-weight: 700; }
# MAGIC .ap-anon .ap-h { color: #00A972; } .ap-anon .ap-item.gone { border-color: #00A972; background: rgba(0,169,114,0.10); font-weight: 700; }
# MAGIC .ap-mid { min-width: 120px; text-align: center; color: #618794; font-size: 14pt; line-height: 1.4; }
# MAGIC .ds-callout { border-left: 6px solid #98102A; border-radius: 8px; padding: 16px 22px; background: rgba(152,16,42,0.08); box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 22px; }
# MAGIC .ds-note { border-left: 4px solid #4299E0; background: rgba(66,153,224,0.10); padding: 12px 16px; border-radius: 4px; margin-top: 14px; font-size: 14pt; line-height: 1.55; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ap-row">
# MAGIC   <div class="ap-box ap-pseudo">
# MAGIC     <div class="ap-h">Pseudonymization</div>
# MAGIC     <div class="ap-sub">The link is protected, not destroyed</div>
# MAGIC     <div class="ap-item">Direct identifier replaced by a pseudonym</div>
# MAGIC     <div class="ap-item">Rows stay linkable across tables</div>
# MAGIC     <div class="ap-item key">A salt, key or vault still maps back</div>
# MAGIC   </div>
# MAGIC   <div class="ap-mid">still personal data &rarr;</div>
# MAGIC   <div class="ap-box ap-anon">
# MAGIC     <div class="ap-h">Anonymization</div>
# MAGIC     <div class="ap-sub">No reasonable route back to the person</div>
# MAGIC     <div class="ap-item">Direct identifiers removed or replaced</div>
# MAGIC     <div class="ap-item">Quasi-identifiers generalized</div>
# MAGIC     <div class="ap-item gone">No key exists to re-identify</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-callout"><strong>A pseudonym is still personal data.</strong> Because a salt, key or vault can map it back and because leftover quasi-identifiers can be linked to outside data, pseudonymized data can still be attributed to a person. It still needs access control, auditing and inclusion in erasure requests. A masked query result isn't anonymization for the same reason.</div>
# MAGIC
# MAGIC <div class="ds-note"><strong>This course is non-legal.</strong> This is the engineering reasoning, not legal advice. For a formal determination, point to your DPO or legal team and the Trust and Compliance Center.</div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Evidence versus inference</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **The test is whether re-identification is reasonably likely, by anyone.** The question is never whether one table names someone on its own. It's whether someone could get back to the person by means reasonably likely to be used, including joining to other data.
# MAGIC - **Pseudonymization fails that test by design.** Its whole point is to keep records linkable, so a consistent pseudonym plus the salt, key or vault gets you back to the person.
# MAGIC - **Anonymization has to defeat both routes.** It must remove the direct identifiers and generalize the quasi-identifiers enough that no reasonable linkage remains. That's a much higher bar and it's why truly anonymized data can fall outside privacy rules while pseudonymized data doesn't.
# MAGIC - **Be precise about what you can prove.** You can show that a hash isn't invertible, that's evidence. Whether a dataset is genuinely anonymous is a judgement about all the data someone could bring to bear, so treat it as an assessment.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Two routes to re-identification
# MAGIC
# MAGIC If a pseudonym can be reversed, it's through one of these two routes. Click each to see how it works and what closes it.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="rc-wrap">
# MAGIC <style>
# MAGIC .rc-wrap { width: 100%; display: flex; flex-direction: column; align-items: center; gap: 18px; font-family: sans-serif; }
# MAGIC .rc-wrap * { box-sizing: border-box; }
# MAGIC .rc-row { display: block; white-space: nowrap; font-size: 0; width: 100%; max-width: 1100px; }
# MAGIC .rc-box { display: inline-block; width: 49%; margin-right: 2%; min-height: 96px; background: #F9F7F4; border-top: 8px solid transparent; border-left: 2px solid transparent; border-right: 2px solid transparent; border-bottom: 2px solid transparent; border-radius: 8px; padding: 16px 14px; text-align: center; cursor: pointer; user-select: none; vertical-align: top; transition: transform 0.12s; box-shadow: 0 2px 8px rgba(27,49,57,0.06); white-space: normal; }
# MAGIC .rc-box:last-child { margin-right: 0; }
# MAGIC .rc-box:hover { transform: translateY(-2px); }
# MAGIC .rc-box.active { background: #fff; border-left-color: var(--pc); border-right-color: var(--pc); border-bottom-color: var(--pc); }
# MAGIC .rc-label { display: block; font-weight: 700; font-size: 14pt; letter-spacing: 1px; text-transform: uppercase; line-height: 1.2; pointer-events: none; }
# MAGIC .rc-title { display: block; font-size: 14pt; color: #0b2026; line-height: 1.3; margin-top: 6px; pointer-events: none; }
# MAGIC .rc-detail-wrap { overflow: hidden; max-height: 0; opacity: 0; transition: max-height 0.35s ease, opacity 0.28s ease, margin-top 0.28s ease; margin-top: 0; width: 100%; max-width: 1100px; }
# MAGIC .rc-detail-wrap.open { max-height: 900px; opacity: 1; margin-top: 12px; }
# MAGIC .rc-detail-card { background: #F9F7F4; border-radius: 10px; padding: 22px 26px; border-top: 7px solid #ccc; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .rc-detail-title { font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 12px; line-height: 1.3; }
# MAGIC .rc-detail-body { font-size: 14pt; color: #0b2026; line-height: 1.55; }
# MAGIC .rc-detail-body ul { margin: 0; padding-left: 22px; }
# MAGIC .rc-detail-body li { margin-bottom: 8px; }
# MAGIC .rc-detail-body li:last-child { margin-bottom: 0; }
# MAGIC .rc-detail-body code { font-family: monospace; background: #EEEDE9; padding: 1px 5px; border-radius: 3px; color: #1B5162; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="rc-row">
# MAGIC   <span class="rc-box" data-id="0" onclick="rcSelect(0)" style="--pc:#FF5F46; border-top-color:#FF5F46;">
# MAGIC     <span class="rc-label" style="color:#98102A;">Route 1: the key</span>
# MAGIC     <span class="rc-title">Reverse it with the salt, key or vault</span>
# MAGIC   </span><span class="rc-box" data-id="1" onclick="rcSelect(1)" style="--pc:#FFAB00; border-top-color:#B27A00;">
# MAGIC     <span class="rc-label" style="color:#B27A00;">Route 2: auxiliary data</span>
# MAGIC     <span class="rc-title">Link on the leftover quasi-identifiers</span>
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
# MAGIC   { title: "Route 1: linkage through the key", color: "#98102A",
# MAGIC     text: "<ul>"
# MAGIC       + "<li>Pseudonymization keeps records linkable on purpose, so a consistent pseudonym follows one person across rows and tables.</li>"
# MAGIC       + "<li>Anyone who holds or can obtain the salt, key or vault mapping can turn the pseudonym back into the person.</li>"
# MAGIC       + "<li><strong>What closes it:</strong> keep the salt and key in <code>secret()</code>, govern the vault tightly and audit every reversal. The value comes back only for authorized workloads.</li>"
# MAGIC       + "</ul>" },
# MAGIC   { title: "Route 2: linkage through auxiliary data", color: "#B27A00",
# MAGIC     text: "<ul>"
# MAGIC       + "<li>Even without the key, leftover quasi-identifiers such as date of birth, postal code and sex can single out an individual when joined to an outside dataset. This is the mosaic effect.</li>"
# MAGIC       + "<li>Roughly 87 percent of the US population is uniquely identifiable by ZIP code, date of birth and sex alone, so a pseudonym next to a precise birth date isn't safe.</li>"
# MAGIC       + "<li><strong>What closes it:</strong> generalize the quasi-identifiers (age bands, truncated IPs, rounded values), which is Section H. Pseudonymizing the id alone leaves this route open.</li>"
# MAGIC       + "</ul>" }
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
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why you usually combine techniques</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **The two routes need different fixes.** Route 1 is closed by protecting the key and vault. Route 2 is closed by generalizing the quasi-identifiers. Neither fix helps with the other route.
# MAGIC - **So real de-identification stacks techniques.** Pseudonymize the direct identifier for linkage and generalize the quasi-identifiers so the leftover columns can't single someone out. Doing only one leaves a way back.
# MAGIC - **A deterministic hash leaks frequencies.** Because the same value always hashes the same way, an attacker can count how often each pseudonym appears and match that against known value frequencies. Salting raises the cost, but this is why a hash is pseudonymization, not anonymization.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. The de-identification toolkit
# MAGIC
# MAGIC Five techniques, chosen by what the use case needs and the risk if the value leaks. This demo builds the four transforming ones. Read the table once, then the rest of the notebook builds each row.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .tk-card { background: #F9F7F4; border-radius: 10px; padding: 8px 22px; border-top: 8px solid #4299E0; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .tk-item { display: grid; grid-template-columns: 190px 1fr 118px 118px; gap: 16px; padding: 13px 0; border-bottom: 1px solid #EEEDE9; align-items: center; }
# MAGIC .tk-item:last-child { border-bottom: none; }
# MAGIC .tk-head { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; }
# MAGIC .tk-tech { font-size: 15pt; font-weight: 700; color: #0b2026; overflow-wrap: anywhere; }
# MAGIC .tk-tech .tk-sub { display: block; font-size: 12px; font-weight: 600; color: #5E7077; font-family: monospace; margin-top: 3px; }
# MAGIC .tk-what { font-size: 14pt; color: #0b2026; line-height: 1.5; }
# MAGIC .tk-cell { font-size: 14pt; font-weight: 700; text-align: center; }
# MAGIC .tk-yes { color: #007a53; } .tk-no { color: #98102A; } .tk-part { color: #B27A00; }
# MAGIC @media (max-width: 720px) { .tk-item { grid-template-columns: 1fr; gap: 4px; } .tk-cell { text-align: left; } }
# MAGIC </style>
# MAGIC
# MAGIC <div class="tk-card">
# MAGIC   <div class="tk-item">
# MAGIC     <div class="tk-head">Technique</div>
# MAGIC     <div class="tk-head">What it does to the value</div>
# MAGIC     <div class="tk-head">Reversible?</div>
# MAGIC     <div class="tk-head">Keeps linkage?</div>
# MAGIC   </div>
# MAGIC   <div class="tk-item">
# MAGIC     <div class="tk-tech">Suppress or generalize<span class="tk-sub">Section H</span></div>
# MAGIC     <div class="tk-what">Drop the value, or reduce it to a group (age band, /24, rounded).</div>
# MAGIC     <div class="tk-cell tk-no">No</div>
# MAGIC     <div class="tk-cell tk-no">No</div>
# MAGIC   </div>
# MAGIC   <div class="tk-item">
# MAGIC     <div class="tk-tech">Salted hash<span class="tk-sub">sha2, Section D</span></div>
# MAGIC     <div class="tk-what">A one-way, deterministic pseudonym over the identifier.</div>
# MAGIC     <div class="tk-cell tk-no">No</div>
# MAGIC     <div class="tk-cell tk-yes">Yes</div>
# MAGIC   </div>
# MAGIC   <div class="tk-item">
# MAGIC     <div class="tk-tech">Tokenize<span class="tk-sub">vault, Section F</span></div>
# MAGIC     <div class="tk-what">An opaque token, with the mapping kept in a separate vault.</div>
# MAGIC     <div class="tk-cell tk-part">Vault only</div>
# MAGIC     <div class="tk-cell tk-yes">Yes</div>
# MAGIC   </div>
# MAGIC   <div class="tk-item">
# MAGIC     <div class="tk-tech">Encrypt<span class="tk-sub">aes, Section G</span></div>
# MAGIC     <div class="tk-what">Ciphertext in place, recoverable only with the key.</div>
# MAGIC     <div class="tk-cell tk-part">Key only</div>
# MAGIC     <div class="tk-cell tk-no">No</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Which to use when</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Need trends, not individuals?** Suppress or generalize. Drop the column, or broaden it so it describes a group. This is the only row that reduces the auxiliary-data route.
# MAGIC - **Need to link or track but never reverse?** Salted `sha2`. The same input always produces the same pseudonym, so rows stay joinable, but the hash can't be turned back into the value.
# MAGIC - **Need authorized reversal, with the value out of the analytics store?** Tokenize. The analytics estate holds only opaque tokens and reversal happens through a separately governed vault that can be audited.
# MAGIC - **Need authorized reversal in place?** Encrypt with `aes_encrypt`. The ciphertext sits in the table and only the key recovers it. Because it isn't deterministic, it isn't a join key.
# MAGIC - **In every reversible or salted case, the salt or key lives in Databricks Secrets, not in code.** That's the thread through all four.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Salted Hashing With `sha2`
# MAGIC
# MAGIC A salted hash is the tool for linkage without reversal. `sha2` is a one-way function: it turns a value into a fixed-length hex string that can't be turned back. Because it's deterministic, the same input always hashes the same way, so rows stay joinable on the hash. The salt is a secret string added before hashing and it's what stops an attacker from rebuilding the mapping.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 980 250" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="sh-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- inputs -->
# MAGIC   <rect x="20" y="45" width="210" height="52" rx="8" fill="#4299E0" fill-opacity="0.10" stroke="#4299E0" stroke-width="1.6"/>
# MAGIC   <text x="125" y="70" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">customer_id</text>
# MAGIC   <text x="125" y="88" text-anchor="middle" font-size="12" fill="#5E7077">the identifier</text>
# MAGIC   <rect x="20" y="120" width="210" height="52" rx="8" fill="#FFAB00" fill-opacity="0.14" stroke="#FFAB00" stroke-width="1.6"/>
# MAGIC   <text x="125" y="145" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">secret('...','pii_salt')</text>
# MAGIC   <text x="125" y="163" text-anchor="middle" font-size="12" fill="#5E7077">salt, read at query time</text>
# MAGIC   <path d="M230 71 L286 100" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#sh-arrow)"/>
# MAGIC   <path d="M230 146 L286 117" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#sh-arrow)"/>
# MAGIC   <!-- function -->
# MAGIC   <rect x="290" y="82" width="210" height="54" rx="10" fill="#00A972" fill-opacity="0.12" stroke="#00A972" stroke-width="1.8"/>
# MAGIC   <text x="395" y="105" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">sha2( ... , 256)</text>
# MAGIC   <text x="395" y="124" text-anchor="middle" font-size="12" fill="#5E7077">one-way hash</text>
# MAGIC   <path d="M500 109 L556 109" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#sh-arrow)"/>
# MAGIC   <!-- output -->
# MAGIC   <rect x="560" y="82" width="400" height="54" rx="8" fill="#FFFFFF" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <text x="580" y="105" font-size="13" font-weight="700" fill="#007a53">pseudonym</text>
# MAGIC   <text x="580" y="125" font-size="13" fill="#5E7077" font-family="monospace">a1f9c3...  (64 hex chars)</text>
# MAGIC   <!-- one-way barrier -->
# MAGIC   <path d="M760 136 L760 178" fill="none" stroke="#98102A" stroke-width="1.6" stroke-dasharray="5 3"/>
# MAGIC   <line x1="742" y1="170" x2="778" y2="186" stroke="#98102A" stroke-width="2"/>
# MAGIC   <line x1="778" y1="170" x2="742" y2="186" stroke="#98102A" stroke-width="2"/>
# MAGIC   <text x="760" y="208" text-anchor="middle" font-size="13" font-weight="700" fill="#98102A">no route back to customer_id</text>
# MAGIC   <text x="395" y="232" text-anchor="middle" font-size="12" fill="#5E7077">deterministic: same id, same pseudonym, so rows still join</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why salt matters</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **The salt defeats precomputed and dictionary attacks.** Without a salt, low-entropy identifiers like SSNs, emails and phone numbers are trivially brute-forced, because the value space is small and the hash is deterministic. An attacker just hashes every candidate and matches. A secret salt they don't have breaks that.
# MAGIC - **`sha2(expr, bitLength)` takes a bit length of 224, 256, 384 or 512** and `0` also means 256. It returns a hex string, so 256 gives 64 hex characters. Longer lengths are longer strings, not more reversible; 256 is the common choice.
# MAGIC - **The deterministic hash is a trade-off.** Determinism is what keeps rows joinable. It also means identical values produce identical hashes, so the output preserves value frequencies, which leaks information. That's why a salted hash is pseudonymization, not anonymization.
# MAGIC - **Cross-dataset linkage needs the same salt.** Two tables only join on the pseudonym if they were hashed with the same salt, which is another reason the salt lives in one governed place (a secret) and its rotation is a decision.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# Start with the salt hardcoded in the notebook. It works, so run it and see the pseudonym.
# Then read the diagnosis below: the salt is now sitting in plain text where it should never be.
display(spark.sql(f"""
SELECT
  customer_id,
  sha2(concat(cast(customer_id AS STRING), 'salt_2024_prod'), 256) AS pseudonym
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile
LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="font-size:15pt; font-weight:700; color:#98102A; margin-bottom:8px;">The problem with the hardcoded salt</div>
# MAGIC <svg width="100%" viewBox="0 0 980 280" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="dg-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#98102A" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="25" y="46" width="300" height="62" rx="8" fill="#FFFFFF" stroke="#FF5F46" stroke-width="1.6"/>
# MAGIC   <text x="175" y="72" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">sha2(concat(id, salt), 256)</text>
# MAGIC   <text x="175" y="94" text-anchor="middle" font-size="13" font-weight="700" fill="#98102A" font-family="monospace">salt = 'salt_2024_prod'</text>
# MAGIC   <path d="M325 77 L406 77" fill="none" stroke="#98102A" stroke-width="1.8" marker-end="url(#dg-arrow)"/>
# MAGIC   <rect x="410" y="44" width="545" height="66" rx="8" fill="#98102A" fill-opacity="0.06" stroke="#98102A" stroke-width="1.5"/>
# MAGIC   <text x="432" y="70" font-size="13" font-weight="600" fill="#98102A">The salt literal is now captured in</text>
# MAGIC   <text x="432" y="93" font-size="13" fill="#0b2026">notebook source &#183; query history &#183; version control &#183; logs</text>
# MAGIC   <text x="25" y="152" font-size="14" font-weight="700" fill="#98102A">Dictionary attack</text>
# MAGIC   <rect x="25" y="160" width="225" height="58" rx="8" fill="#F9F7F4" stroke="#EEEDE9" stroke-width="1.4"/>
# MAGIC   <text x="137" y="184" text-anchor="middle" font-size="13" fill="#0b2026">candidate customer_id</text>
# MAGIC   <text x="137" y="203" text-anchor="middle" font-size="12" fill="#5E7077" font-family="monospace">1001, 1002, 1003, &#8230;</text>
# MAGIC   <path d="M250 189 L306 189" fill="none" stroke="#98102A" stroke-width="1.8" marker-end="url(#dg-arrow)"/>
# MAGIC   <rect x="310" y="160" width="265" height="58" rx="8" fill="#98102A" fill-opacity="0.06" stroke="#98102A" stroke-width="1.4"/>
# MAGIC   <text x="442" y="184" text-anchor="middle" font-size="13" fill="#0b2026" font-family="monospace">sha2(id, known salt)</text>
# MAGIC   <text x="442" y="203" text-anchor="middle" font-size="12" fill="#5E7077">the salt is no longer secret</text>
# MAGIC   <path d="M575 189 L631 189" fill="none" stroke="#98102A" stroke-width="1.8" marker-end="url(#dg-arrow)"/>
# MAGIC   <rect x="635" y="160" width="320" height="58" rx="8" fill="#98102A" fill-opacity="0.10" stroke="#98102A" stroke-width="1.6"/>
# MAGIC   <text x="795" y="184" text-anchor="middle" font-size="13" font-weight="600" fill="#0b2026">matches the pseudonyms</text>
# MAGIC   <text x="795" y="203" text-anchor="middle" font-size="12" fill="#98102A">the whole mapping is rebuilt</text>
# MAGIC   <text x="25" y="252" font-size="14" fill="#5E7077">The salt was meant to stop exactly this. A secret the query text never reveals is the fix.</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC The fix: the salt moves into a governed secret, read only at query time. The value is never written and query history and logs redact the reference.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 980 268" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="d2-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="25" y="54" width="230" height="62" rx="8" fill="#FFAB00" fill-opacity="0.14" stroke="#FFAB00" stroke-width="1.6"/>
# MAGIC   <text x="140" y="74" text-anchor="middle" font-size="13" fill="#5E7077">Secret scope</text>
# MAGIC   <text x="140" y="93" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">dbacademy_privacy</text>
# MAGIC   <text x="140" y="109" text-anchor="middle" font-size="12" fill="#5E7077" font-family="monospace">key: pii_salt</text>
# MAGIC   <text x="305" y="79" text-anchor="middle" font-size="12" fill="#0b2026" font-family="monospace">secret()</text>
# MAGIC   <path d="M255 85 L356 85" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#d2-arrow)"/>
# MAGIC   <rect x="360" y="54" width="250" height="62" rx="10" fill="#00A972" fill-opacity="0.12" stroke="#00A972" stroke-width="1.8"/>
# MAGIC   <text x="485" y="82" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">sha2(concat(id, salt), 256)</text>
# MAGIC   <text x="485" y="101" text-anchor="middle" font-size="12" fill="#5E7077">salt resolved at query time</text>
# MAGIC   <path d="M610 85 L711 85" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#d2-arrow)"/>
# MAGIC   <rect x="715" y="54" width="240" height="62" rx="8" fill="#FFFFFF" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <text x="835" y="82" text-anchor="middle" font-size="14" font-weight="700" fill="#007a53">pseudonym</text>
# MAGIC   <text x="835" y="101" text-anchor="middle" font-size="12" fill="#5E7077">deterministic, rows still join</text>
# MAGIC   <rect x="25" y="146" width="445" height="78" rx="8" fill="#00A972" fill-opacity="0.06" stroke="#00A972" stroke-width="1.4"/>
# MAGIC   <text x="41" y="168" font-size="13" font-weight="600" fill="#007a53">Never written</text>
# MAGIC   <text x="41" y="190" font-size="12" fill="#0b2026" font-family="monospace">secret('dbacademy_privacy','pii_salt')</text>
# MAGIC   <text x="41" y="210" font-size="12" fill="#5E7077">stays out of the notebook and version control</text>
# MAGIC   <rect x="510" y="146" width="445" height="78" rx="8" fill="#1B5162" fill-opacity="0.06" stroke="#1B5162" stroke-width="1.4"/>
# MAGIC   <text x="526" y="168" font-size="13" font-weight="600" fill="#1B5162">Redacted in query history and logs</text>
# MAGIC   <text x="526" y="190" font-size="12" fill="#0b2026">statement_text returns &lt;REDACTED&gt; by default</text>
# MAGIC   <text x="526" y="210" font-size="12" fill="#5E7077">secret() redacted in logs and profiles, best-effort</text>
# MAGIC   <text x="25" y="252" font-size="14" fill="#5E7077">Guarded: if the <tspan font-family="monospace" fill="#0b2026">dbacademy_privacy</tspan> scope is missing, the cell prints the one-time CLI setup and continues.</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

try:
    display(spark.sql(f"""
    SELECT
      customer_id,
      sha2(concat(cast(customer_id AS STRING), secret('{SECRET_SCOPE}', '{SALT_KEY}')), 256) AS pseudonym
    FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile
    LIMIT 5
    """))
    print("Salt read from secret(). The value is never written and query history and logs redact it.")
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"Couldn't read the salt from the '{SECRET_SCOPE}' scope here.")
    print("To set it up once yourself:")
    print(f"databricks secrets create-scope {SECRET_SCOPE}")
    print(f"databricks secrets put-secret {SECRET_SCOPE} {SALT_KEY} --string-value \"<a long random salt string>\"")
    print(f"({first})")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Rows Stay Joinable, the Original Is Not Recoverable
# MAGIC
# MAGIC The same salted pseudonym replaces `customer_id` in both analytics tables, so they still join, while the pseudonym gives no way back to the person.
# MAGIC
# MAGIC > What you should see: the analytics tables build with a pseudonym in place of the identity, the join returns event counts per pseudonymized customer and the tables contain no direct identifiers.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 980 300" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="e-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <text x="20" y="34" font-size="13" font-weight="700" fill="#1B5162">E1 &#183; build the de-identified tables</text>
# MAGIC   <rect x="20" y="46" width="205" height="72" rx="8" fill="#4299E0" fill-opacity="0.08" stroke="#4299E0" stroke-width="1.5"/>
# MAGIC   <text x="122" y="72" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">customer_profile</text>
# MAGIC   <text x="122" y="92" text-anchor="middle" font-size="12" fill="#0b2026">customer_id, name, email,</text>
# MAGIC   <text x="122" y="108" text-anchor="middle" font-size="12" fill="#5E7077">national_id, country, dob, ip</text>
# MAGIC   <path d="M225 82 L418 82" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#e-arrow)"/>
# MAGIC   <text x="322" y="73" text-anchor="middle" font-size="12" fill="#5E7077" font-family="monospace">sha2(customer_id + salt)</text>
# MAGIC   <rect x="422" y="46" width="258" height="72" rx="8" fill="#00A972" fill-opacity="0.06" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <text x="551" y="72" text-anchor="middle" font-size="14" font-weight="700" fill="#007a53" font-family="monospace">customer_analytics</text>
# MAGIC   <text x="551" y="92" text-anchor="middle" font-size="12" font-weight="700" fill="#007a53" font-family="monospace">customer_pseudonym</text>
# MAGIC   <text x="551" y="108" text-anchor="middle" font-size="12" fill="#5E7077">country, loyalty_tier, dob, ip</text>
# MAGIC   <text x="322" y="154" text-anchor="middle" font-size="13" font-weight="700" fill="#1B5162">same salt, so the same person gets the same pseudonym</text>
# MAGIC   <rect x="20" y="192" width="205" height="72" rx="8" fill="#4299E0" fill-opacity="0.08" stroke="#4299E0" stroke-width="1.5"/>
# MAGIC   <text x="122" y="218" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">clickstream</text>
# MAGIC   <text x="122" y="238" text-anchor="middle" font-size="12" fill="#0b2026">customer_id, event_id,</text>
# MAGIC   <text x="122" y="254" text-anchor="middle" font-size="12" fill="#5E7077">event_ts, page_url, device</text>
# MAGIC   <path d="M225 228 L418 228" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#e-arrow)"/>
# MAGIC   <text x="322" y="219" text-anchor="middle" font-size="12" fill="#5E7077" font-family="monospace">sha2(customer_id + salt)</text>
# MAGIC   <rect x="422" y="192" width="258" height="72" rx="8" fill="#00A972" fill-opacity="0.06" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <text x="551" y="218" text-anchor="middle" font-size="14" font-weight="700" fill="#007a53" font-family="monospace">clickstream_analytics</text>
# MAGIC   <text x="551" y="238" text-anchor="middle" font-size="12" font-weight="700" fill="#007a53" font-family="monospace">customer_pseudonym</text>
# MAGIC   <text x="551" y="254" text-anchor="middle" font-size="12" fill="#5E7077">event_id, event_ts, page_url</text>
# MAGIC   <text x="847" y="110" text-anchor="middle" font-size="13" font-weight="700" fill="#1B5162">E2 &#183; join on customer_pseudonym</text>
# MAGIC   <path d="M680 82 L731 148" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#e-arrow)"/>
# MAGIC   <path d="M680 228 L731 162" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#e-arrow)"/>
# MAGIC   <rect x="735" y="118" width="225" height="74" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.6"/>
# MAGIC   <text x="847" y="148" text-anchor="middle" font-size="14" font-weight="700" fill="#1B5162">events per pseudonym</text>
# MAGIC   <text x="847" y="172" text-anchor="middle" font-size="12" fill="#5E7077">behavior linked, person not</text>
# MAGIC   <text x="20" y="290" font-size="14" fill="#5E7077">E3 &#183; the analytics tables hold no customer_id, name, email or national_id and the pseudonym has no way back.</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# Guarded for a missing secret. Writes go only to priv_deid; priv_demo is never changed.
try:
    spark.sql(f"""
    CREATE OR REPLACE TABLE `{my_catalog}`.`{DEID_SCHEMA}`.customer_analytics AS
    SELECT
      sha2(concat(cast(customer_id AS STRING), secret('{SECRET_SCOPE}', '{SALT_KEY}')), 256) AS customer_pseudonym,
      country,
      loyalty_tier,
      date_of_birth,
      ip_address
    FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile
    """)
    spark.sql(f"""
    CREATE OR REPLACE TABLE `{my_catalog}`.`{DEID_SCHEMA}`.clickstream_analytics AS
    SELECT
      sha2(concat(cast(customer_id AS STRING), secret('{SECRET_SCOPE}', '{SALT_KEY}')), 256) AS customer_pseudonym,
      event_id,
      event_ts,
      page_url,
      device_type,
      country
    FROM `{my_catalog}`.`{DEMO_SCHEMA}`.clickstream
    """)
    print(f"Built {DEID_SCHEMA}.customer_analytics and {DEID_SCHEMA}.clickstream_analytics, keyed on the pseudonym.")
    # customer_analytics: the pseudonym replaces the identity and no direct identifiers remain.
    display(spark.sql(f"""
    SELECT * FROM `{my_catalog}`.`{DEID_SCHEMA}`.customer_analytics LIMIT 8
    """))
    # clickstream_analytics: the same pseudonym replaces customer_id, so the two tables still line up.
    display(spark.sql(f"""
    SELECT * FROM `{my_catalog}`.`{DEID_SCHEMA}`.clickstream_analytics LIMIT 8
    """))
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"Couldn't build the de-identified tables (needs the salt from the '{SECRET_SCOPE}' scope).")
    print("See Section D2 for the one-time CLI step.")
    print(f"({first})")

# COMMAND ----------

# The analyst joins on the pseudonym, never on customer_id.
try:
    display(spark.sql(f"""
    SELECT
      c.customer_pseudonym,
      c.country,
      c.loyalty_tier,
      count(k.event_id) AS events
    FROM `{my_catalog}`.`{DEID_SCHEMA}`.customer_analytics c
    JOIN `{my_catalog}`.`{DEID_SCHEMA}`.clickstream_analytics k USING (customer_pseudonym)
    GROUP BY c.customer_pseudonym, c.country, c.loyalty_tier
    ORDER BY events DESC
    LIMIT 8
    """))
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"Join not available (the de-identified tables were not built). ({first})")

# COMMAND ----------

# The evidence: DESCRIBE lists the columns present and no direct identifier is among them.
try:
    print("Columns in the de-identified analytics table (no customer_id, name, email or national_id):")
    display(spark.sql(f"DESCRIBE `{my_catalog}`.`{DEID_SCHEMA}`.customer_analytics"))
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"Table not available. ({first})")

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
# MAGIC .worked{--accent:#00A972;} .blocked{--accent:#98102A;}
# MAGIC .ds-callout { border-left: 6px solid #1B5162; border-radius: 8px; padding: 16px 22px; background: #F8F9FC; box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 20px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="out-grid">
# MAGIC   <div class="out-card worked">
# MAGIC     <div class="out-band">Linkage kept</div>
# MAGIC     <div class="out-body">
# MAGIC       <div class="out-item"><span class="out-k">Join key</span><span class="out-v">customer_pseudonym</span></div>
# MAGIC       <div class="out-item"><span class="out-k">Same person, both tables</span><span class="out-v">same pseudonym</span></div>
# MAGIC       <div class="out-item"><span class="out-k">Behavioral study</span><span class="out-v">still works</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="out-card blocked">
# MAGIC     <div class="out-band">Person protected</div>
# MAGIC     <div class="out-body">
# MAGIC       <div class="out-item"><span class="out-k">customer_id</span><span class="out-v">not in the table</span></div>
# MAGIC       <div class="out-item"><span class="out-k">email, national_id</span><span class="out-v">dropped</span></div>
# MAGIC       <div class="out-item"><span class="out-k">Recover id from pseudonym</span><span class="out-v">not possible</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-callout"><strong>The control worked.</strong> Rows stay joinable on the pseudonym, so the analyst keeps the study and the direct identifiers are gone, so the analytics estate can't leak them. This is pseudonymization, not anonymization: with the salt, the mapping could be rebuilt and the quasi-identifiers still in the table are handled in Section H.</div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">How to prove non-recoverability</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **No direct identifiers remain.** `DESCRIBE` in E3 lists the columns present and the direct identifiers are absent. There's no `customer_id`, `email` or `national_id` in the analytics table to leak and no lookup back to the person inside the analytics estate.
# MAGIC - **The join is the proof of linkage.** E2 returns event counts per pseudonymized customer, which only works because the same person hashes to the same pseudonym in both tables. That is the deterministic property doing its job.
# MAGIC - **Be precise about the claim.** You can prove the hash isn't invertible and that the id column is gone. You can't claim the dataset is anonymous, because the salt still maps the pseudonym back and the quasi-identifiers can be linked. That's why this is labeled pseudonymization.
# MAGIC - **Re-running is safe.** The tables use `CREATE OR REPLACE` and the salt comes from the same secret, so a second run produces the same pseudonyms and the join still lines up. Change the salt and the pseudonyms change, which is the rotation decision from Section D.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. Tokenization With a Vault
# MAGIC
# MAGIC A salted hash can't be reversed, which is right when you never need the value back. Sometimes an authorized workflow does need it back, for example a support agent who must contact a customer. Tokenization replaces the value with an opaque token and keeps the token-to-value mapping in a separate, governed vault, so reversal is possible only through the vault.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 1000 380" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="tk-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- analytics zone -->
# MAGIC   <rect x="40" y="34" width="400" height="150" rx="12" fill="#4299E0" fill-opacity="0.05" stroke="#4299E0" stroke-width="1.5"/>
# MAGIC   <text x="62" y="60" font-size="13" font-weight="700" fill="#2272B4" letter-spacing="1">ANALYTICS ESTATE &#183; priv_deid</text>
# MAGIC   <rect x="62" y="74" width="356" height="70" rx="8" fill="#FFFFFF" stroke="#4299E0" stroke-width="1.4"/>
# MAGIC   <text x="240" y="100" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">customer_contactable</text>
# MAGIC   <text x="240" y="126" text-anchor="middle" font-size="14" fill="#0b2026" font-family="monospace">email_token = <tspan fill="#2272B4" font-weight="700">TKN-9F3A2C</tspan></text>
# MAGIC   <text x="240" y="169" text-anchor="middle" font-size="13" fill="#5E7077">Holds the token, never the email.</text>
# MAGIC   <!-- vault zone -->
# MAGIC   <rect x="560" y="34" width="400" height="150" rx="12" fill="#98102A" fill-opacity="0.05" stroke="#98102A" stroke-width="1.5"/>
# MAGIC   <g transform="translate(582,46)"><rect x="0" y="9" width="20" height="14" rx="3" fill="#98102A"/><path d="M4 9 V6 a6 6 0 0 1 12 0 V9" fill="none" stroke="#98102A" stroke-width="2.4"/></g>
# MAGIC   <text x="612" y="60" font-size="13" font-weight="700" fill="#98102A" letter-spacing="1">GOVERNED VAULT &#183; priv_vault</text>
# MAGIC   <rect x="582" y="74" width="356" height="70" rx="8" fill="#FFFFFF" stroke="#98102A" stroke-width="1.4"/>
# MAGIC   <text x="760" y="100" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">email_token_vault</text>
# MAGIC   <text x="760" y="126" text-anchor="middle" font-size="14" fill="#0b2026" font-family="monospace"><tspan fill="#98102A" font-weight="700">TKN-9F3A2C</tspan> &#8594; maria@example.com</text>
# MAGIC   <text x="760" y="169" text-anchor="middle" font-size="13" fill="#5E7077">The only token &#8594; email map. Tight grants, audited.</text>
# MAGIC   <!-- same-token connector -->
# MAGIC   <path d="M440 112 L560 112" fill="none" stroke="#618794" stroke-width="1.4" stroke-dasharray="5 4"/>
# MAGIC   <text x="500" y="103" text-anchor="middle" font-size="12" fill="#618794" font-weight="700">same token</text>
# MAGIC   <!-- reversal depends on vault access -->
# MAGIC   <text x="40" y="234" font-size="14" font-weight="700" fill="#1B5162">Turning a token back into an email needs vault access</text>
# MAGIC   <!-- analyst row -->
# MAGIC   <rect x="40" y="252" width="150" height="34" rx="17" fill="#FFFFFF" stroke="#618794" stroke-width="1.5"/>
# MAGIC   <text x="115" y="274" text-anchor="middle" font-size="13" fill="#0b2026">Analyst</text>
# MAGIC   <path d="M195 269 L243 269" fill="none" stroke="#5E7077" stroke-width="1.6" marker-end="url(#tk-arrow)"/>
# MAGIC   <g transform="translate(250,258)"><rect x="0" y="9" width="18" height="12" rx="2.5" fill="#98102A"/><path d="M3.5 9 V6.5 a5.5 5.5 0 0 1 11 0 V9" fill="none" stroke="#98102A" stroke-width="2.2"/></g>
# MAGIC   <text x="276" y="274" font-size="12.5" font-weight="700" fill="#98102A">no priv_vault access</text>
# MAGIC   <path d="M450 269 L500 269" fill="none" stroke="#5E7077" stroke-width="1.6" marker-end="url(#tk-arrow)"/>
# MAGIC   <rect x="506" y="252" width="454" height="34" rx="8" fill="#98102A" fill-opacity="0.08" stroke="#98102A" stroke-width="1.3"/>
# MAGIC   <text x="733" y="274" text-anchor="middle" font-size="13" fill="#98102A">The email stays hidden; only TKN-9F3A2C is visible.</text>
# MAGIC   <!-- support agent row -->
# MAGIC   <rect x="40" y="304" width="150" height="34" rx="17" fill="#FFFFFF" stroke="#00A972" stroke-width="1.5"/>
# MAGIC   <text x="115" y="326" text-anchor="middle" font-size="13" fill="#0b2026">Support agent</text>
# MAGIC   <path d="M195 321 L243 321" fill="none" stroke="#5E7077" stroke-width="1.6" marker-end="url(#tk-arrow)"/>
# MAGIC   <g transform="translate(250,311)"><circle cx="6" cy="7" r="5.5" fill="none" stroke="#00A972" stroke-width="2.2"/><path d="M11 7 H25 M21 7 V12 M25 7 V12" fill="none" stroke="#00A972" stroke-width="2.2"/></g>
# MAGIC   <text x="284" y="326" font-size="12.5" font-weight="700" fill="#007a53">has priv_vault access</text>
# MAGIC   <path d="M450 321 L500 321" fill="none" stroke="#5E7077" stroke-width="1.6" marker-end="url(#tk-arrow)"/>
# MAGIC   <rect x="506" y="304" width="454" height="34" rx="8" fill="#00A972" fill-opacity="0.08" stroke="#00A972" stroke-width="1.3"/>
# MAGIC   <text x="733" y="326" text-anchor="middle" font-size="13" fill="#007a53">Joins the vault and recovers maria@example.com.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Tokenization versus hashing</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **A token has no mathematical relationship to the value.** Unlike a hash, you can't compute the value from the token. The only link is the row in the vault, which is why reversal is a controlled, auditable lookup rather than a computation anyone can run.
# MAGIC - **Tokens leak no distribution.** Because they are assigned rather than derived, tokens don't preserve value frequencies the way a deterministic hash does, so the frequency-analysis weakness of hashing doesn't apply.
# MAGIC - **The vault becomes the crown jewels.** Reversibility is concentrated in one place, so the vault must be governed tightly, kept out of the analytics estate and audited on every read. Compromise of the vault means full exposure.
# MAGIC - **On Databricks it's a pattern, not one function.** A separately governed lookup table in its own schema or catalog, with tight grants and audited access. Here the vault lives in `priv_vault`, ungranted to analysts.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# Join to the vault to carry the token instead of the email. No secret needed; the mapping lives in the vault.
spark.sql(f"USE CATALOG `{my_catalog}`")

spark.sql(f"""
CREATE OR REPLACE TABLE `{my_catalog}`.`{DEID_SCHEMA}`.customer_contactable AS
SELECT
  v.email_token,
  p.country,
  p.loyalty_tier
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile p
JOIN `{my_catalog}`.`{VAULT_SCHEMA}`.{VAULT_TABLE} v USING (email)
""")

# The analytics table carries only the opaque token. There is no email here.
display(spark.sql(f"""
SELECT email_token, country, loyalty_tier
FROM `{my_catalog}`.`{DEID_SCHEMA}`.customer_contactable
LIMIT 5
"""))

# COMMAND ----------

# Reversal needs priv_vault access. An analyst with priv_deid but not priv_vault sees only the token.
display(spark.sql(f"""
SELECT
  c.country,
  c.loyalty_tier,
  c.email_token,
  v.email AS email_recovered
FROM `{my_catalog}`.`{DEID_SCHEMA}`.customer_contactable c
JOIN `{my_catalog}`.`{VAULT_SCHEMA}`.{VAULT_TABLE} v USING (email_token)
LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## G. Column Encryption With `aes_encrypt` and `aes_decrypt`
# MAGIC
# MAGIC When an authorized process must recover the value in place, without a separate vault, encrypt the column. `aes_encrypt` turns the value into ciphertext stored in the table and `aes_decrypt` reverses it with the same key, both using GCM (Galois/Counter Mode), which is `aes_encrypt`'s default mode and also authenticates the data. GCM draws a fresh random initialization vector on each call, so the same input encrypts to different ciphertext every time. The key comes from `secret()` and here it's stored base64-encoded, so you decode it with `unbase64()` first.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 1000 280" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="en-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC     <marker id="en-arrow-grn" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#00A972" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- value and ciphertext -->
# MAGIC   <rect x="90" y="57" width="210" height="76" rx="8" fill="#FF5F46" fill-opacity="0.10" stroke="#FF5F46" stroke-width="1.6"/>
# MAGIC   <text x="195" y="88" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">national_id</text>
# MAGIC   <text x="195" y="112" text-anchor="middle" font-size="13.5" fill="#5E7077" font-family="monospace">123-45-6789</text>
# MAGIC   <rect x="560" y="57" width="350" height="76" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.6"/>
# MAGIC   <text x="735" y="88" text-anchor="middle" font-size="15" font-weight="700" fill="#1B5162" font-family="monospace">national_id_ct</text>
# MAGIC   <text x="735" y="112" text-anchor="middle" font-size="13" fill="#5E7077" font-family="monospace">base64: 3sJd+aQ2c...</text>
# MAGIC   <text x="735" y="152" text-anchor="middle" font-size="12.5" fill="#5E7077">Stored in the table, in place of the value.</text>
# MAGIC   <!-- encrypt (top) and decrypt (bottom), both need the key -->
# MAGIC   <path d="M300 84 L558 84" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#en-arrow)"/>
# MAGIC   <text x="430" y="74" text-anchor="middle" font-size="12" font-weight="700" fill="#618794">aes_encrypt &#183; key from secret()</text>
# MAGIC   <path d="M558 116 L302 116" fill="none" stroke="#00A972" stroke-width="1.8" marker-end="url(#en-arrow-grn)"/>
# MAGIC   <text x="430" y="132" text-anchor="middle" font-size="12" font-weight="700" fill="#007a53">aes_decrypt &#183; same key</text>
# MAGIC   <g transform="translate(414,93)"><circle cx="6" cy="7" r="5.5" fill="none" stroke="#1B5162" stroke-width="2.2"/><path d="M11 7 H25 M21 7 V12 M25 7 V12" fill="none" stroke="#1B5162" stroke-width="2.2"/></g>
# MAGIC   <!-- authorized only: the key is the gate -->
# MAGIC   <text x="90" y="190" font-size="14" font-weight="700" fill="#1B5162">Only the key reverses it, so recovery is authorized only</text>
# MAGIC   <g transform="translate(96,203)"><circle cx="6" cy="7" r="5.5" fill="none" stroke="#00A972" stroke-width="2.2"/><path d="M11 7 H25 M21 7 V12 M25 7 V12" fill="none" stroke="#00A972" stroke-width="2.2"/></g>
# MAGIC   <text x="128" y="220" font-size="13.5" fill="#007a53">With the key from secret(): returns <tspan font-family="monospace" font-weight="700">123-45-6789</tspan></text>
# MAGIC   <g transform="translate(96,238)"><rect x="0" y="9" width="18" height="12" rx="2.5" fill="#98102A"/><path d="M3.5 9 V6.5 a5.5 5.5 0 0 1 11 0 V9" fill="none" stroke="#98102A" stroke-width="2.2"/></g>
# MAGIC   <text x="128" y="255" font-size="13.5" fill="#98102A">Without the key: only <tspan font-family="monospace" font-weight="700">3sJd+aQ2c...</tspan>, no way back</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">The signatures, the base64 handling and why it isn't a join key</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **`aes_encrypt(expr, key [, mode ...])` returns `BINARY`.** The key must be 16, 24 or 32 bytes. You usually store the ciphertext base64-encoded with `base64(...)` so it fits a string column.
# MAGIC - **The key is stored base64-encoded in the secret.** `secret()` returns that base64 string, so you wrap it in `unbase64()` to get the raw key bytes before passing it to `aes_encrypt`. A 32-byte key gives AES-256.
# MAGIC - **`aes_decrypt(expr, key [, mode ...])` reverses it.** Decode the stored ciphertext with `unbase64(...)`, decrypt with the same key and mode and `cast(... AS STRING)` to read the value back.
# MAGIC - **Encryption is not a join key.** Because GCM's random initialization vector makes the same value encrypt to different ciphertext each time (good for security), you can't join on ciphertext. Use a salted hash when you need a stable join key and encryption when you need to recover the value.
# MAGIC - **The key controls access.** Anyone who can call `secret()` for the key and run `aes_decrypt` can recover the value, so scope the secret and control who can execute the decrypt path.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# base64(aes_encrypt(value, key, 'GCM')) stores the ciphertext as a string. 
# The key is unbase64(secret(...)): secret() returns the base64-encoded key and unbase64 decodes it to the raw bytes aes_encrypt needs. 
# Output lands in priv_deid only.
try:
    spark.sql(f"""
    CREATE OR REPLACE TABLE `{my_catalog}`.`{DEID_SCHEMA}`.customer_secured AS
    SELECT
      sha2(concat(cast(customer_id AS STRING), secret('{SECRET_SCOPE}', '{SALT_KEY}')), 256) AS customer_pseudonym,
      base64(aes_encrypt(national_id, unbase64(secret('{SECRET_SCOPE}', '{AES_KEY_NAME}')), 'GCM')) AS national_id_ct
    FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile
    """)
    print(f"Built {DEID_SCHEMA}.customer_secured with the national_id stored as ciphertext.")
    display(spark.sql(f"""
    SELECT customer_pseudonym, national_id_ct
    FROM `{my_catalog}`.`{DEID_SCHEMA}`.customer_secured
    LIMIT 5
    """))
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"Couldn't encrypt here (needs the AES key from the '{SECRET_SCOPE}' scope).")
    print("To set it up once yourself:")
    print(f"databricks secrets put-secret {SECRET_SCOPE} {AES_KEY_NAME} --string-value \"$(openssl rand -base64 32)\"")
    print(f"({first})")

# COMMAND ----------

# aes_decrypt reverses the ciphertext with the same key and mode. 
# Decode the stored base64 with unbase64, decrypt and cast the BINARY result back to a string. 
# Only a caller who can read the key and run this path recovers the value.
try:
    display(spark.sql(f"""
    SELECT
      customer_pseudonym,
      national_id_ct,
      cast(aes_decrypt(unbase64(national_id_ct), unbase64(secret('{SECRET_SCOPE}', '{AES_KEY_NAME}')), 'GCM') AS STRING) AS national_id_recovered
    FROM `{my_catalog}`.`{DEID_SCHEMA}`.customer_secured
    LIMIT 5
    """))
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"Couldn't decrypt here (needs the AES key and the customer_secured table). ({first})")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Encryption is deliberately non-deterministic, so the same value encrypts to different ciphertext each time.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 1000 250" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="g3-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <text x="130" y="78" text-anchor="middle" font-size="12.5" fill="#5E7077">the same value</text>
# MAGIC   <rect x="40" y="88" width="180" height="58" rx="8" fill="#FFAB00" fill-opacity="0.12" stroke="#FFAB00" stroke-width="1.6"/>
# MAGIC   <text x="130" y="123" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">123-45-6789</text>
# MAGIC   <path d="M220 104 L360 74" fill="none" stroke="#5E7077" stroke-width="1.7" marker-end="url(#g3-arrow)"/>
# MAGIC   <text x="290" y="78" text-anchor="middle" font-size="11.5" font-weight="700" fill="#618794">aes_encrypt, run 1</text>
# MAGIC   <path d="M220 130 L360 160" fill="none" stroke="#5E7077" stroke-width="1.7" marker-end="url(#g3-arrow)"/>
# MAGIC   <text x="290" y="170" text-anchor="middle" font-size="11.5" font-weight="700" fill="#618794">aes_encrypt, run 2</text>
# MAGIC   <rect x="364" y="44" width="300" height="54" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/>
# MAGIC   <text x="378" y="66" font-size="12.5" font-weight="700" fill="#1B5162" font-family="monospace">ciphertext_run_1</text>
# MAGIC   <text x="378" y="86" font-size="13" fill="#5E7077" font-family="monospace">3sJd+aQ2c...</text>
# MAGIC   <rect x="364" y="136" width="300" height="54" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/>
# MAGIC   <text x="378" y="158" font-size="12.5" font-weight="700" fill="#1B5162" font-family="monospace">ciphertext_run_2</text>
# MAGIC   <text x="378" y="178" font-size="13" fill="#5E7077" font-family="monospace">7bQ2x9Lm...</text>
# MAGIC   <text x="700" y="128" text-anchor="middle" font-size="34" font-weight="700" fill="#98102A">&#8800;</text>
# MAGIC   <text x="740" y="104" font-size="14" font-weight="700" fill="#0b2026">Different every run</text>
# MAGIC   <text x="740" y="126" font-size="12.5" fill="#5E7077">GCM draws a fresh random IV</text>
# MAGIC   <text x="740" y="152" font-size="13.5" font-weight="700" fill="#98102A">Can't join on ciphertext</text>
# MAGIC   <rect x="40" y="206" width="920" height="34" rx="8" fill="#F8F9FC" stroke="#C7D0D4" stroke-width="1"/>
# MAGIC   <text x="60" y="228" font-size="12.5" fill="#0b2026">Contrast: the salted hash in Section D is deterministic, so the same input maps to the same pseudonym and joins. Encryption never repeats.</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# Encrypting the SAME value twice gives DIFFERENT ciphertext, because GCM uses a fresh random IV each time. 
# That's why you can't join on ciphertext. 
# This is the key contrast with the salted hash in Section D, which is deterministic on purpose so rows stay joinable.
try:
    display(spark.sql(f"""
    SELECT
      base64(aes_encrypt('123-45-6789', unbase64(secret('{SECRET_SCOPE}', '{AES_KEY_NAME}')), 'GCM')) AS ciphertext_run_1,
      base64(aes_encrypt('123-45-6789', unbase64(secret('{SECRET_SCOPE}', '{AES_KEY_NAME}')), 'GCM')) AS ciphertext_run_2
    """))
    print("Same input, two different ciphertexts: encryption isn't deterministic, so it isn't a join key.")
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"Couldn't run the comparison here (needs the AES key). ({first})")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## H. Suppression and Generalization for Analytics
# MAGIC
# MAGIC Pseudonymizing the direct identifier keeps the key route controlled, as long as the salt lives in a secret. The quasi-identifiers left in the analytics table, `date_of_birth` and `ip_address`, still leave the auxiliary-data route open. Suppression drops what you don't need and generalization reduces precision so a value describes a group instead of a person.
# MAGIC <br/><br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .gen-card { background: #F9F7F4; border-radius: 10px; padding: 8px 22px; border-top: 8px solid #FFAB00; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .gen-item { display: grid; grid-template-columns: 210px 1fr 40px 1fr; gap: 16px; padding: 14px 0; border-bottom: 1px solid #EEEDE9; align-items: center; }
# MAGIC .gen-item:last-child { border-bottom: none; }
# MAGIC .gen-head { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; }
# MAGIC .gen-tech { font-size: 15pt; font-weight: 700; color: #0b2026; }
# MAGIC .gen-tech .gen-sub { display:block; font-size: 12px; font-weight: 600; color: #5E7077; margin-top: 3px; }
# MAGIC .gen-before { font-family: monospace; font-size: 14pt; color: #98102A; }
# MAGIC .gen-after { font-family: monospace; font-size: 14pt; color: #007a53; font-weight: 700; }
# MAGIC .gen-arrow { text-align: center; color: #618794; font-size: 18pt; }
# MAGIC @media (max-width: 680px) { .gen-item { grid-template-columns: 1fr; gap: 4px; } .gen-arrow { text-align: left; } }
# MAGIC .ds-callout { border-left: 6px solid #FF5F46; border-radius: 8px; padding: 16px 22px; background: rgba(255,95,70,0.10); box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 20px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="gen-card">
# MAGIC   <div class="gen-item">
# MAGIC     <div class="gen-head">Technique</div>
# MAGIC     <div class="gen-head">Before (identifying)</div>
# MAGIC     <div class="gen-head"></div>
# MAGIC     <div class="gen-head">After (a group)</div>
# MAGIC   </div>
# MAGIC   <div class="gen-item">
# MAGIC     <div class="gen-tech">Bin a date<span class="gen-sub">date_of_birth &rarr; age band</span></div>
# MAGIC     <div class="gen-before">1974-03-12</div>
# MAGIC     <div class="gen-arrow">&rarr;</div>
# MAGIC     <div class="gen-after">50s</div>
# MAGIC   </div>
# MAGIC   <div class="gen-item">
# MAGIC     <div class="gen-tech">Truncate an IP<span class="gen-sub">ip_address &rarr; /24</span></div>
# MAGIC     <div class="gen-before">203.0.113.42</div>
# MAGIC     <div class="gen-arrow">&rarr;</div>
# MAGIC     <div class="gen-after">203.0.113.0/24</div>
# MAGIC   </div>
# MAGIC   <div class="gen-item">
# MAGIC     <div class="gen-tech">Round a number<span class="gen-sub">exact &rarr; bucket</span></div>
# MAGIC     <div class="gen-before">points: 4,182</div>
# MAGIC     <div class="gen-arrow">&rarr;</div>
# MAGIC     <div class="gen-after">points: 4,000</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-callout"><strong>Use both techniques together.</strong> 
# MAGIC <br/><br/>
# MAGIC Pseudonymize the direct identifier and generalize the quasi-identifiers. 
# MAGIC <br/><br/>
# MAGIC Either one alone leaves a route back: the pseudonym without generalization still has a precise birth date to link on and generalization without a pseudonym still has a linkable key.</div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Suppression, generalization and keeping enough signal</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Suppression means omitting data.** Drop a column the analysis doesn't need, drop rows for groups too small to be safe, or withhold outliers. It's the strongest option and the first to consider.
# MAGIC - **Generalization reduces precision.** Bin an age into a range, truncate an IP to a /24 so it identifies a network rather than a host, round a number to a bucket or roll a specific category up to a broader one. The value now describes a group.
# MAGIC - **The goal is to keep signal while removing the ability to single someone out.** An age band and a network prefix still support trend analysis, but they no longer pick out one person the way a birth date and a full IP do.
# MAGIC - **These target the auxiliary-data route specifically.** They are how you move from pseudonymized toward genuinely anonymized, by closing the route that pseudonymizing the identifier leaves open.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# date_of_birth becomes a 10-year age band
# ip_address is truncated to a /24 (last octet zeroed)
display(spark.sql(f"""
SELECT
  date_of_birth,
  concat(cast(floor(datediff(current_date(), date_of_birth) / 365.25 / 10) * 10 AS INT), 's') AS age_band,
  ip_address,
  concat(substring_index(ip_address, '.', 3), '.0/24') AS ip_prefix
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile
LIMIT 8
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 980 290" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="h-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="45" y="58" width="350" height="152" rx="8" fill="#FFFFFF" stroke="#4299E0" stroke-width="1.6"/>
# MAGIC   <text x="220" y="86" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" font-family="monospace">customer_analytics</text>
# MAGIC   <text x="220" y="112" text-anchor="middle" font-size="13" fill="#007a53" font-family="monospace">customer_pseudonym</text>
# MAGIC   <text x="220" y="138" text-anchor="middle" font-size="13" fill="#98102A" font-family="monospace">date_of_birth: 1974-03-12</text>
# MAGIC   <text x="220" y="164" text-anchor="middle" font-size="13" fill="#98102A" font-family="monospace">ip_address: 203.0.113.42</text>
# MAGIC   <text x="220" y="194" text-anchor="middle" font-size="12" font-weight="700" fill="#98102A">auxiliary-data route: open</text>
# MAGIC   <path d="M395 134 L581 134" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#h-arrow)"/>
# MAGIC   <text x="488" y="124" text-anchor="middle" font-size="12" fill="#5E7077">generalize the</text>
# MAGIC   <text x="488" y="154" text-anchor="middle" font-size="12" fill="#5E7077">quasi-identifiers (H)</text>
# MAGIC   <rect x="585" y="58" width="350" height="152" rx="8" fill="#00A972" fill-opacity="0.06" stroke="#00A972" stroke-width="1.6"/>
# MAGIC   <text x="760" y="86" text-anchor="middle" font-size="14" font-weight="700" fill="#007a53" font-family="monospace">customer_analytics_safe</text>
# MAGIC   <text x="760" y="112" text-anchor="middle" font-size="13" fill="#007a53" font-family="monospace">customer_pseudonym</text>
# MAGIC   <text x="760" y="138" text-anchor="middle" font-size="13" fill="#007a53" font-family="monospace">age_band: 50s</text>
# MAGIC   <text x="760" y="164" text-anchor="middle" font-size="13" fill="#007a53" font-family="monospace">ip_prefix: 203.0.113.0/24</text>
# MAGIC   <text x="760" y="194" text-anchor="middle" font-size="12" font-weight="700" fill="#007a53">auxiliary-data route: closed</text>
# MAGIC   <text x="45" y="242" font-size="14" fill="#5E7077">Section E handled the direct identifier. Generalizing the quasi-identifiers here closes the auxiliary-data route from C1.</text>
# MAGIC   <text x="45" y="266" font-size="14" fill="#5E7077">Either technique alone leaves a way back; together they move the table from pseudonymized toward anonymized.</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# Guarded, because the pseudonym needs the salt from the secret scope.
try:
    spark.sql(f"""
    CREATE OR REPLACE TABLE `{my_catalog}`.`{DEID_SCHEMA}`.customer_analytics_safe AS
    SELECT
      customer_pseudonym,
      country,
      loyalty_tier,
      concat(cast(floor(datediff(current_date(), date_of_birth) / 365.25 / 10) * 10 AS INT), 's') AS age_band,
      concat(substring_index(ip_address, '.', 3), '.0/24') AS ip_prefix
    FROM `{my_catalog}`.`{DEID_SCHEMA}`.customer_analytics
    """)
    print(f"Built {DEID_SCHEMA}.customer_analytics_safe: pseudonym plus generalized quasi-identifiers.")
    display(spark.sql(f"""
    SELECT customer_pseudonym, country, loyalty_tier, age_band, ip_prefix
    FROM `{my_catalog}`.`{DEID_SCHEMA}`.customer_analytics_safe
    LIMIT 8
    """))
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print(f"Couldn't build the safe table (needs customer_analytics from Section E, which needs the salt).")
    print(f"({first})")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Try it: ask Genie Code</div>
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">You wrote the generalization by hand, so you can judge whether the assistant's answer is right. Ask <strong>Genie Code</strong> to draft a generalizer, then run the verification cell on real inputs and edge cases before you trust it.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">Write a Unity Catalog SQL UDF that takes a date of birth and returns a 10-year age band like '40s'. Return NULL for a NULL input and give me three test inputs including an edge case.</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# A generalizer of the kind Genie Code drafts for the prompt above. 
# Before it goes near a table, run it on representative inputs and edge cases: a normal date, a very recent one (a child) and NULL. 
# Reading the output is how you catch a wrong answer, for example an off-by-one age band.
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{my_catalog}`.`{DEID_SCHEMA}`.to_age_band(dob DATE)
RETURNS STRING
RETURN CASE
  WHEN dob IS NULL THEN NULL
  ELSE concat(cast(floor(datediff(current_date(), dob) / 365.25 / 10) * 10 AS INT), 's')
END
""")

display(spark.sql(f"""
SELECT input_dob, `{my_catalog}`.`{DEID_SCHEMA}`.to_age_band(input_dob) AS age_band
FROM (VALUES
  (DATE'1974-03-12'),
  (DATE'2007-09-01'),
  (CAST(NULL AS DATE))
) AS t(input_dob)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, you:
# MAGIC
# MAGIC 1. **Minimized** the analytics copy to what the use case needs, then separated **anonymization** from **pseudonymization** and saw why a pseudonym is still personal data.
# MAGIC 2. Built a salted `sha2` **pseudonym** that keeps rows joinable without being reversible and replaced a hardcoded salt with `secret()`.
# MAGIC 3. **Tokenized** a value so reversal is possible only through a separately governed vault and **encrypted** a column with `aes_encrypt` and `aes_decrypt` for authorized reversal in place.
# MAGIC 4. **Generalized** the quasi-identifiers to close the auxiliary-data route and combined generalization with the pseudonym in one analytics-safe table.
# MAGIC 5. Kept the salt and key in **Databricks Secrets** throughout, never in the notebook.
# MAGIC
# MAGIC Next, in **Right to Erasure: Delete, Propagate, Purge**, you'll act on a deletion request: find the subject's data, delete it from the earliest authoritative layer, propagate the delete downstream and see why a successful `DELETE` changes current state but doesn't prove physical purge.

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
# MAGIC   <div class="cl-docs-title">Secrets, hashing and encryption functions</div>
# MAGIC   <div class="cl-docs-sub">These function signatures and secret behaviors change over time, so confirm the current <code>sha2</code>, <code>aes_encrypt</code>, <code>aes_decrypt</code> and <code>secret</code> reference and the secret management guidance rather than a static list.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/security/secrets/" target="_blank">Secret management &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/sql/language-manual/functions/secret" target="_blank">secret() &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/sql/language-manual/functions/aes_encrypt" target="_blank">aes_encrypt &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/sql/language-manual/functions/sha2" target="_blank">sha2 &rarr;</a>
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