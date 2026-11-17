# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">03 Demo - Discover and Classify Sensitive Data</span>
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
# MAGIC     <span>&#9675; Protect</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Manage</span>
# MAGIC     <span style="color:#C7D0D4;">&rarr;</span>
# MAGIC     <span>&#9675; Verify</span>
# MAGIC   </span>
# MAGIC   <span style="font-size:10.5pt; color:#618794; margin-left:auto;">step 1 of 4</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Discover and Classify Sensitive Data
# MAGIC #### Duration: 25 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC You inherited the retailer estate and access controls are already in place, but nobody has recorded what is sensitive. This notebook is the **identify** step of the course: find the sensitive data before you try to protect or erase it.
# MAGIC
# MAGIC You'll use **Data Classification** to detect sensitive columns, review the detections the course prepared as a deterministic snapshot, handle a false positive and turn a detection into a governed **sensitivity_level** tag that a policy can act on. The stance for the whole demo is that a tag is only useful if it drives a control, so you classify with the control in mind.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you'll be able to:
# MAGIC
# MAGIC 1. **Explain** why unclassified data is a privacy risk even when access controls already exist.
# MAGIC 2. **Describe** how Data Classification detects sensitive data, applies `class.*` tags and records confidence and frequency.
# MAGIC 3. **Review** the prepared detections through the restricted snapshot and **evaluate** a false positive using confidence and samples.
# MAGIC 4. **Map** `class.*` detections to a governed **sensitivity_level** taxonomy, then inspect the governed tag.
# MAGIC 5. **Determine** what control each tag should drive, which sets up tag-driven protection in Demo 4.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">Before you begin</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   This demo reviews a deterministic snapshot of the detections for the demo estate in <code>snap_classification_results</code>. 
# MAGIC <br>
# MAGIC   The governed <strong>sensitivity_level</strong> tag comes from the separate Workspace Setup and this notebook's setup applies it to your estate columns. Section E inspects what is in place.
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
# MAGIC **NOTE:** This course uses **SQL and Python and was tested on Serverless V6**. All-purpose or classic compute may work but isn't guaranteed to behave the same or support all features demonstrated.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the setup below. It resolves your per-user catalog, ensures the shared **priv_demo** retailer estate exists (idempotent), stages the reviewable classification snapshot this demo reads and applies the governed **sensitivity_level** values to your estate columns.
# MAGIC
# MAGIC If your workspace can't create catalogs, open `./Includes/Classroom-Setup-03` and set `FORCED_CATALOG` to an existing catalog first.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-03

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Unknown Data Is a Privacy Risk
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
# MAGIC .ds-row { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; margin-top: 20px; }
# MAGIC .ds-row > * { flex: 1; min-width: 280px; }
# MAGIC .blue{--accent:#4299E0;} .green{--accent:#00A972;} .coral{--accent:#FF5F46;} .amber{--accent:#FFAB00;} .red{--accent:#98102A;}
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px; border-top: 8px solid var(--accent, #4299E0); display: flex; flex-direction: column; overflow-wrap: anywhere; }
# MAGIC .ds-card-title { font-size: 18pt; font-weight: 700; line-height: 1.25; margin: 0 0 8px; }
# MAGIC .ds-card ul { margin: 8px 0 0; padding-left: 20px; font-size: 14pt; line-height: 1.55; color: #0b2026; }
# MAGIC .ds-card li { margin-bottom: 10px; } .ds-card li:last-child { margin-bottom: 0; }
# MAGIC .ds-viz { margin-top: auto; padding-top: 14px; border-top: 1px dashed #E0DED9; }
# MAGIC .ds-viz-cap { font-size: 14px; color: #618794; text-align: center; margin-top: 10px; line-height: 1.4; }
# MAGIC .ds-chip { font-size: 13px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 3px 8px; color: #5E7077; }
# MAGIC .ds-chip.pii { border-color: #FF5F46; color: #98102A; font-weight: 700; }
# MAGIC .ds-callout { border-left: 6px solid #FF5F46; border-radius: 8px; padding: 18px 24px; background: rgba(255,95,70,0.10); box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 20px; }
# MAGIC .ds-eyebrow { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #98102A; margin-bottom: 6px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-banner info">
# MAGIC   <div class="ds-banner-icon">?</div>
# MAGIC   <div>
# MAGIC     <div class="ds-banner-title">You can't protect what you haven't found</div>
# MAGIC     <div class="ds-banner-tagline">Access controls decide who can open a table. They don't tell you which columns hold personal data.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-card blue">
# MAGIC     <div class="ds-card-title">Access control answers "who"</div>
# MAGIC     <ul>
# MAGIC       <li>Grants and policies decide who may read a table.</li>
# MAGIC       <li>They apply whether or not you know what is inside.</li>
# MAGIC       <li>A reader with access sees every column, sensitive or not.</li>
# MAGIC     </ul>
# MAGIC     <div class="ds-viz">
# MAGIC       <div style="display:flex; align-items:center; justify-content:center; gap:8px;">
# MAGIC         <span class="ds-chip">GRANT SELECT</span>
# MAGIC         <span style="color:#618794;">&rarr;</span>
# MAGIC         <span style="font-size:15px;">&#128274;</span>
# MAGIC         <span class="ds-chip">customer_profile</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">Controls the <span style="color:#2272B4;font-weight:700;">door</span>, not the contents</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-card coral">
# MAGIC     <div class="ds-card-title">Classification answers "what"</div>
# MAGIC     <ul>
# MAGIC       <li>It finds which columns hold emails, phones and ids.</li>
# MAGIC       <li>It works at the column level, across the estate.</li>
# MAGIC       <li>Now you know what to protect, mask and later erase.</li>
# MAGIC     </ul>
# MAGIC     <div class="ds-viz">
# MAGIC       <div style="display:flex; gap:6px; justify-content:center; flex-wrap:wrap;">
# MAGIC         <span class="ds-chip">country</span>
# MAGIC         <span class="ds-chip pii">email</span>
# MAGIC         <span class="ds-chip">tier</span>
# MAGIC         <span class="ds-chip pii">national_id</span>
# MAGIC       </div>
# MAGIC       <div class="ds-viz-cap">Which columns are <span style="color:#98102A;font-weight:700;">sensitive</span>?</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-callout">
# MAGIC   <div class="ds-eyebrow">The risk</div>
# MAGIC   An unknown sensitive column can't be masked, filtered, pseudonymized or erased, because no control targets a column nobody has identified. Manual, column by column PII hunting and hand maintained data dictionaries don't keep up with a changing estate. Discovery has to come first and it has to be repeatable.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why access control isn't enough</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Access control and classification answer different questions.** A grant decides who can open **customer_profile**. It says nothing about which columns inside are personal data. Both are needed and this demo supplies the missing half.
# MAGIC - **You can't target what you haven't found.** A column mask, a row filter, a pseudonymization step and an erasure all need to name the sensitive columns. If a column isn't identified, no control covers it.
# MAGIC - **The old habit was manual.** People used to hunt for PII column by column and keep a spreadsheet data dictionary by hand. On a real estate that drifts out of date the moment a new column lands, which is exactly when a leak appears.
# MAGIC - **The replacement is Data Classification plus governed tags.** The platform scans for sensitive data, tags it and keeps scanning as the estate changes.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. PII hiding in plain sight
# MAGIC
# MAGIC Before we classify anything, look at **support_records**. These read like ordinary support tickets, but the free-text `body_text` sometimes contains an email or a phone number that nobody catalogued as personal data.
# MAGIC
# MAGIC > What you should see: ticket rows whose `body_text` embeds a contact detail. This is synthetic data, but it's exactly the kind of hidden PII classification is built to find.

# COMMAND ----------

spark.sql(f"USE CATALOG `{my_catalog}`")
spark.sql(f"USE SCHEMA `{DEMO_SCHEMA}`")

display(spark.sql(f"""
SELECT ticket_id, channel, subject, body_text
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.support_records
WHERE body_text LIKE '%@%' OR body_text LIKE '%+1%'
LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. How Data Classification Works
# MAGIC
# MAGIC Data Classification is a Unity Catalog feature that scans your tables, detects sensitive data at the column level and applies system-governed `class.*` tags. It's GA and runs on serverless. You enable it on a catalog and an initial scan starts automatically.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .flow-grid { display: grid; grid-template-columns: 1fr auto 1fr auto 1fr auto 1fr; gap: 0; align-items: stretch; }
# MAGIC .flow-step { background: #F9F7F4; border: 3px solid var(--accent, #4299E0); border-radius: 10px; padding: 16px 12px; box-shadow: 0 2px 8px rgba(27,49,57,0.08); text-align: center; min-width: 0; }
# MAGIC .flow-num { font-size: 26pt; font-weight: 800; color: var(--accent, #4299E0); line-height: 1; margin-bottom: 8px; }
# MAGIC .flow-label { font-size: 14pt; font-weight: 600; color: #0b2026; line-height: 1.35; }
# MAGIC .flow-sub { font-size: 13px; color: #5E7077; line-height: 1.35; margin-top: 6px; }
# MAGIC .flow-arrow { display: flex; align-items: center; justify-content: center; color: #618794; font-size: 22pt; line-height: 1; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="flow-grid">
# MAGIC   <div class="flow-step" style="--accent:#4299E0;"><div class="flow-num">1</div><div class="flow-label">Enable on the catalog</div><div class="flow-sub">Needs MANAGE on the catalog</div></div>
# MAGIC   <div class="flow-arrow">&rarr;</div>
# MAGIC   <div class="flow-step" style="--accent:#00A972;"><div class="flow-num">2</div><div class="flow-label">Scan, asynchronously</div><div class="flow-sub">Starts on enable, about 15 min</div></div>
# MAGIC   <div class="flow-arrow">&rarr;</div>
# MAGIC   <div class="flow-step" style="--accent:#FFAB00;"><div class="flow-num">3</div><div class="flow-label">Detect and tag</div><div class="flow-sub">Applies class.* tags per column</div></div>
# MAGIC   <div class="flow-arrow">&rarr;</div>
# MAGIC   <div class="flow-step" style="--accent:#FF5F46;"><div class="flow-num">4</div><div class="flow-label">Record results</div><div class="flow-sub">One row per detection</div></div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #FFAB00; background: rgba(255,171,0,0.12); padding: 14px 18px; border-radius: 4px; margin: 20px 0 0;">
# MAGIC   <strong style="display:block; color:#0b2026; margin-bottom:6px; font-size:15pt;">Asynchronous and UI triggered</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">A scan is triggered from Catalog Explorer, not from SQL or an API. Enabling starts an initial scan that takes about 15 minutes for a small estate and new tables are then picked up within about 24 hours.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">What the four steps mean in practice</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **1. Enable on the catalog.** An admin turns Data Classification on for a catalog. This needs the `MANAGE` privilege on the catalog. It's optional for the course, because the demo reviews the prepared snapshot.
# MAGIC - **2. Scan, asynchronously.** Enabling Data Classification on a catalog starts an initial scan automatically; for a small estate it finishes in about 15 minutes. After that, the system decides when to rescan from Unity Catalog metadata and the docs note new or changed tables and columns are typically picked up within about 24 hours. You can trigger a full rescan from the Catalog UI, but there's no SQL or API to start one today.
# MAGIC - **3. Detect and tag.** Classification uses a model to detect sensitive data at the column level and applies system-governed `class.*` tags, for example `class.email_address` on an email column.
# MAGIC - **4. Record results.** Every detection becomes a row in `system.data_classification.results`, with the column, the class, a discrete confidence, a frequency and up to five sample values.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Data Classification in Catalog Explorer, Step by Step
# MAGIC
# MAGIC The demo reviews a prepared snapshot, because a scan is asynchronous and UI triggered. This is the live workflow that snapshot stands in for, from turning the feature on to turning a detection into a control. Use the numbered steps or the arrows to move through it.
# MAGIC <br/><br/>
# MAGIC <div class="dcc" id="dcc">
# MAGIC <style>
# MAGIC .dcc { max-width: 1000px; margin: 8px auto 0; font-family: sans-serif; color: #0b2026; }
# MAGIC .dcc * { box-sizing: border-box; }
# MAGIC .dcc-flow { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 14px; }
# MAGIC .dcc-chip { border: 1px solid #EEEDE9; background: #F9F7F4; color: #5E7077; border-radius: 6px; padding: 5px 11px; font-size: 14pt; line-height: 1.25; cursor: pointer; font-family: inherit; }
# MAGIC .dcc-chip.on { background: #FF5F46; border-color: #FF5F46; color: #fff; font-weight: 700; }
# MAGIC .dcc-stage { position: relative; }
# MAGIC .dcc-slides img { width: 100%; height: auto; display: block; border: 1px solid #EEEDE9; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .dcc-slides p { margin: 0 0 12px; }
# MAGIC .dcc.on .dcc-slides p { margin: 0; }
# MAGIC .dcc-nav { position: absolute; top: 50%; transform: translateY(-50%); width: 42px; height: 42px; border-radius: 50%; border: none; background: rgba(27,81,98,0.82); color: #fff; font-size: 24px; line-height: 42px; text-align: center; padding: 0; cursor: pointer; display: none; }
# MAGIC .dcc.on .dcc-nav { display: block; }
# MAGIC .dcc-prev { left: 12px; } .dcc-next { right: 12px; }
# MAGIC .dcc-cap { margin-top: 14px; min-height: 74px; }
# MAGIC .dcc-count { font-size: 14pt; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #FF5F46; }
# MAGIC .dcc-title { font-size: 16pt; font-weight: 700; color: #0b2026; margin-top: 2px; }
# MAGIC .dcc-body { font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 4px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="dcc-flow" id="dcc-flow"></div>
# MAGIC
# MAGIC <div class="dcc-stage">
# MAGIC <div class="dcc-slides" id="dcc-slides">
# MAGIC
# MAGIC ![Enable it on the catalog](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-01.png)
# MAGIC
# MAGIC ![Choose the schemas to scan](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-02.png)
# MAGIC
# MAGIC ![Confirm it is on, then open the results](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-03.png)
# MAGIC
# MAGIC ![The scan starts automatically](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-04.png)
# MAGIC
# MAGIC ![Watch the scan progress](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-05.png)
# MAGIC
# MAGIC ![Scan complete](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-06.png)
# MAGIC
# MAGIC ![Review the classified tags](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-07.png)
# MAGIC
# MAGIC ![Inspect a tag and its columns](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-08.png)
# MAGIC
# MAGIC ![Sample values are permissioned](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-09.png)
# MAGIC
# MAGIC ![See access, then turn it into a control](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/data-classification/dcc-step-10.png)
# MAGIC
# MAGIC </div>
# MAGIC <button class="dcc-nav dcc-prev" id="dcc-prev" aria-label="Previous step">&#8249;</button>
# MAGIC <button class="dcc-nav dcc-next" id="dcc-next" aria-label="Next step">&#8250;</button>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dcc-cap">
# MAGIC   <div class="dcc-count" id="dcc-count"></div>
# MAGIC   <div class="dcc-title" id="dcc-title"></div>
# MAGIC   <div class="dcc-body" id="dcc-body"></div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC (function(){
# MAGIC   var CAP=[
# MAGIC     {t:"Enable it on the catalog", d:"In the catalog Advanced settings, find Data Classification and click <strong>Enable</strong>. Enabling needs the <code>MANAGE</code> privilege on the catalog.", s:"Enable"},
# MAGIC     {t:"Choose the schemas to scan", d:"Turn the feature on and pick the schemas. <strong>All selected and future schemas</strong> keeps new schemas covered as they appear; here the <code>priv_demo</code> schema is selected. Click <strong>Save</strong>.", s:"Pick schemas"},
# MAGIC     {t:"Confirm it is on, then open the results", d:"The Advanced row now reads <strong>Enabled</strong>. Click <strong>View results</strong> to open the Data Classification page in Catalog Explorer.", s:"Confirm"},
# MAGIC     {t:"The scan starts automatically", d:"Enabling kicks off an initial scan with no extra step; for a small estate it finishes in about 15 minutes. The docs note new tables are then picked up within about 24 hours. You can also press <strong>Trigger Full Scan</strong>.", s:"Scan starts"},
# MAGIC     {t:"Watch the scan progress", d:"Early on, no tables are scanned yet and there are no classified tags. The page updates as it works through the catalog.", s:"Progress"},
# MAGIC     {t:"Scan complete", d:"All seven tables are scanned. The detected tags populate a moment later.", s:"Complete"},
# MAGIC     {t:"Review the classified tags", d:"The scan found twelve tags, eight system and four custom, each showing the number of detected columns, its auto-tagging status and any compliance-framework mapping.", s:"Review tags"},
# MAGIC     {t:"Inspect a tag and its columns", d:"Open a tag such as <code>class.name</code> to see the exact columns it matched. You can exclude a false positive (the system learns from it) and turn on auto-tagging so the tag is applied automatically. Here it shows as active.", s:"Inspect tag"},
# MAGIC     {t:"Sample values are permissioned", d:"Viewing sample values requires <code>SELECT</code> on <code>system.data_classification.results</code>, granted by an account admin. Without it the values stay hidden.", s:"Samples"},
# MAGIC     {t:"See access, then turn it into a control", d:"The <strong>User Access</strong> tab shows who read each column and whether they saw it unmasked. From here you assign an ABAC masking policy with <strong>New policy</strong>, which is where Demo 04 begins.", s:"Access → control"}
# MAGIC   ];
# MAGIC   var root=document.getElementById("dcc");
# MAGIC   var slides=document.getElementById("dcc-slides");
# MAGIC   var imgs=slides?slides.querySelectorAll("img"):[];
# MAGIC   var flow=document.getElementById("dcc-flow");
# MAGIC   var countEl=document.getElementById("dcc-count");
# MAGIC   var titleEl=document.getElementById("dcc-title");
# MAGIC   var bodyEl=document.getElementById("dcc-body");
# MAGIC   var n=imgs.length;
# MAGIC   if(!root||!flow||n===0){ return; }
# MAGIC   root.className="dcc on";
# MAGIC   var i=0;
# MAGIC   function box(el){ return (el.parentElement&&el.parentElement.tagName==="P")?el.parentElement:el; }
# MAGIC   function show(j){
# MAGIC     i=((j%n)+n)%n;
# MAGIC     for(var x=0;x<n;x++){ box(imgs[x]).style.display=(x===i)?"":"none"; }
# MAGIC     var ch=flow.children;
# MAGIC     for(var c=0;c<ch.length;c++){ ch[c].className=(c===i)?"dcc-chip on":"dcc-chip"; }
# MAGIC     countEl.textContent="Step "+(i+1)+" of "+n;
# MAGIC     if(CAP[i]){ titleEl.textContent=CAP[i].t; bodyEl.innerHTML=CAP[i].d; }
# MAGIC   }
# MAGIC   for(var k=0;k<CAP.length;k++){ (function(k){
# MAGIC     var b=document.createElement("button");
# MAGIC     b.type="button"; b.className="dcc-chip";
# MAGIC     b.textContent=(k+1)+". "+CAP[k].s; b.title=CAP[k].t;
# MAGIC     b.onclick=function(){ show(k); };
# MAGIC     flow.appendChild(b);
# MAGIC   })(k); }
# MAGIC   var p=document.getElementById("dcc-prev"), nx=document.getElementById("dcc-next");
# MAGIC   if(p) p.onclick=function(){ show(i-1); };
# MAGIC   if(nx) nx.onclick=function(){ show(i+1); };
# MAGIC   show(0);
# MAGIC })();
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. What the scan can detect and how sure it is
# MAGIC
# MAGIC Click each card to see what it covers.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="rc-wrap">
# MAGIC <style>
# MAGIC .rc-wrap { width: 100%; display: flex; flex-direction: column; align-items: center; gap: 18px; font-family: sans-serif; }
# MAGIC .rc-wrap * { box-sizing: border-box; }
# MAGIC .rc-row { display: block; white-space: nowrap; font-size: 0; width: 100%; max-width: 1100px; }
# MAGIC .rc-box { display: inline-block; width: 32%; margin-right: 2%; min-height: 96px; background: #F9F7F4; border-top: 8px solid transparent; border-left: 2px solid transparent; border-right: 2px solid transparent; border-bottom: 2px solid transparent; border-radius: 8px; padding: 16px 14px; text-align: center; cursor: pointer; user-select: none; vertical-align: top; transition: transform 0.12s; box-shadow: 0 2px 8px rgba(27,49,57,0.06); white-space: normal; }
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
# MAGIC   <span class="rc-box" data-id="0" onclick="rcSelect(0)" style="--pc:#4299E0; border-top-color:#4299E0;">
# MAGIC     <span class="rc-label" style="color:#4299E0;">Built-in classes</span>
# MAGIC     <span class="rc-title">Detected in every region</span>
# MAGIC   </span><span class="rc-box" data-id="1" onclick="rcSelect(1)" style="--pc:#00A972; border-top-color:#00A972;">
# MAGIC     <span class="rc-label" style="color:#00A972;">Regional classes</span>
# MAGIC     <span class="rc-title">Gated by workspace region</span>
# MAGIC   </span><span class="rc-box" data-id="2" onclick="rcSelect(2)" style="--pc:#FFAB00; border-top-color:#B27A00;">
# MAGIC     <span class="rc-label" style="color:#B27A00;">Confidence and frequency</span>
# MAGIC     <span class="rc-title">How sure and how often</span>
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
# MAGIC   { title: "Built-in classes, detected in every region", color: "#4299E0",
# MAGIC     text: "<ul>"
# MAGIC       + "<li>Identifiers such as <code>class.name</code>, <code>class.email_address</code>, <code>class.phone_number</code>, <code>class.ip_address</code>, <code>class.location</code>, <code>class.credit_card</code>, <code>class.bank_number</code>, <code>class.iban_code</code>, <code>class.passport</code> and <code>class.date_of_birth</code>.</li>"
# MAGIC       + "<li>Special-category classes such as <code>class.ethnicity</code>, <code>class.religious_belief</code>, <code>class.political_opinion</code>, <code>class.trade_union_membership</code>, <code>class.genetic_data</code>, <code>class.biometric_data</code> and <code>class.sexual_orientation</code>.</li>"
# MAGIC       + "<li>Health classes such as <code>class.medical_record_number</code>, <code>class.health_data</code>, <code>class.health_plan_beneficiary_number</code>, <code>class.medical_device_id</code>, <code>class.medical_license</code> and <code>class.medical_date</code>.</li>"
# MAGIC       + "<li>These are the same everywhere, so an email column is detected the same way in any region.</li>"
# MAGIC       + "</ul>" },
# MAGIC   { title: "Regional classes, gated by workspace region", color: "#00A972",
# MAGIC     text: "<ul>"
# MAGIC       + "<li>US tags are the exception: <code>class.us_ssn</code>, <code>class.us_itin</code> and <code>class.us_driver_license</code> are detected in every region.</li>"
# MAGIC       + "<li>Every other country's identifiers only fire when the workspace is deployed in that region, for example <code>class.uk_nino</code> and <code>class.uk_nhs</code> (UK), <code>class.de_id_card</code> and <code>class.de_tax_id</code> (Germany), <code>class.in_aadhaar</code> and <code>class.in_pan</code> (India), <code>class.br_cpf</code> and <code>class.br_cns</code> (Brazil), <code>class.ca_social_insurance_number</code> and <code>class.ca_health_number</code> (Canada), <code>class.au_tfn</code> and <code>class.au_medicare</code> (Australia), <code>class.jp_my_number</code> (Japan) or <code>class.it_codice_fiscale</code> (Italy).</li>"
# MAGIC       + "<li>On a multinational estate this is why a national id column can match in one region and not another.</li>"
# MAGIC       + "</ul>" },
# MAGIC   { title: "Confidence and frequency", color: "#B27A00",
# MAGIC     text: "<ul>"
# MAGIC       + "<li><strong>Confidence</strong> is discrete, either <code>HIGH</code> or <code>LOW</code>, not a numeric score. It is your first triage signal.</li>"
# MAGIC       + "<li><strong>Frequency</strong> runs from 0 to 1 and gives the share of rows that matched the class.</li>"
# MAGIC       + "<li>A low frequency can be normal. A national id column that only holds US-shaped values in a US region matches on a fraction of rows.</li>"
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
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why the class list is grouped this way</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Built-in classes are global.** They cover common identifiers, special-category data and health data and they're detected in every region. The supported-tags documentation is the source of truth for the full list and it also groups the classes by compliance framework.
# MAGIC - **Regional classes are gated, with one exception.** Country-specific identifiers only fire when the workspace is in the matching region. US tags are the exception and are available in every region. On a multinational estate this means a `national_id` column with values from ten countries may only match the classes available in your region.
# MAGIC - **Confidence is a coarse signal, on purpose.** It's `HIGH` or `LOW`, not a percentage. Treat `LOW` as a prompt to look at the samples before you act, not as a number to threshold.
# MAGIC - **Frequency explains partial matches.** A column can be genuinely sensitive and still show a low frequency, because only some rows carry a matching value. You'll see exactly that on `national_id` in Section D.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Review the Detections
# MAGIC
# MAGIC Now read the classification detections for your estate. You're reading the staged snapshot, `priv_demo.snap_classification_results`, which mirrors the shape of `system.data_classification.results`, including a `samples` column.
# MAGIC
# MAGIC > What you should see: one row per detected column, with its `class_tag`, `confidence`, `frequency` and up to five `samples`. The samples here are synthetic.

# COMMAND ----------

detections = spark.sql(f"""
SELECT table_name, column_name, class_tag, confidence, frequency, samples
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.snap_classification_results
ORDER BY table_name, column_name, class_tag
""")
print(f"{detections.count()} detection(s) for {my_catalog}.{DEMO_SCHEMA}")
display(detections)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 960px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #0b2026; margin: 4px 0 14px;">The live results table holds real values</div>
# MAGIC <svg width="100%" viewBox="0 0 900 400" role="img" aria-label="Data Classification owns the writes to the restricted system.data_classification.results table, so do not put a row filter or column mask on it; read the sample-free snap_classification_results snapshot instead." style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="dc-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#1B3139" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="18" y="46" width="180" height="76" rx="8" fill="#EEEDE9" stroke="#1B3139" stroke-width="1.5"/>
# MAGIC   <text x="108" y="82" text-anchor="middle" font-size="16" font-weight="600" fill="#0b2026">Data Classification</text>
# MAGIC   <text x="108" y="104" text-anchor="middle" font-size="14" fill="#618794">owns the writes</text>
# MAGIC   <path d="M198 84 L250 84" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#dc-arrow)"/>
# MAGIC   <rect x="256" y="40" width="446" height="96" rx="10" fill="#ffffff" stroke="#1B5162" stroke-width="2"/>
# MAGIC   <text x="479" y="72" text-anchor="middle" font-size="16" font-weight="700" fill="#0b2026" font-family="monospace">system.data_classification.results</text>
# MAGIC   <text x="479" y="95" text-anchor="middle" font-size="14" fill="#618794">Up to five real sample values per detection</text>
# MAGIC   <rect x="384" y="106" width="190" height="23" rx="11" fill="#98102A" fill-opacity="0.10" stroke="#98102A" stroke-width="1"/>
# MAGIC   <text x="479" y="122" text-anchor="middle" font-size="14" font-weight="700" fill="#98102A">RESTRICTED BY DEFAULT</text>
# MAGIC   <path d="M420 136 L300 196" fill="none" stroke="#1B3139" stroke-width="1.5" marker-end="url(#dc-arrow)"/>
# MAGIC   <path d="M540 136 L636 196" fill="none" stroke="#1B3139" stroke-width="1.5" marker-end="url(#dc-arrow)"/>
# MAGIC   <rect x="30" y="200" width="400" height="172" rx="10" fill="#98102A" fill-opacity="0.05" stroke="#98102A" stroke-width="1.5"/>
# MAGIC   <circle cx="66" cy="232" r="15" fill="#98102A"/>
# MAGIC   <path d="M60 226 L72 238 M72 226 L60 238" stroke="#ffffff" stroke-width="2.6" stroke-linecap="round"/>
# MAGIC   <text x="92" y="238" font-size="16" font-weight="700" fill="#0b2026">Don't filter or mask this table</text>
# MAGIC   <text x="52" y="284" font-size="14" fill="#0b2026">Data Classification owns the writes to</text>
# MAGIC   <text x="52" y="308" font-size="14" fill="#0b2026">this table, so a control placed here can</text>
# MAGIC   <text x="52" y="332" font-size="14" fill="#0b2026">interfere with how it records results.</text>
# MAGIC   <rect x="470" y="200" width="400" height="172" rx="10" fill="#00A972" fill-opacity="0.06" stroke="#00A972" stroke-width="1.5"/>
# MAGIC   <circle cx="506" cy="232" r="15" fill="#00A972"/>
# MAGIC   <path d="M499 232 L504 238 L513 226" fill="none" stroke="#ffffff" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/>
# MAGIC   <text x="530" y="238" font-size="16" font-weight="700" fill="#0b2026">Do read the snapshot instead</text>
# MAGIC   <text x="492" y="282" font-size="14" fill="#1B5162" font-family="monospace">snap_classification_results</text>
# MAGIC   <text x="492" y="312" font-size="14" fill="#0b2026">Deterministic and scoped to the estate,</text>
# MAGIC   <text x="492" y="336" font-size="14" fill="#0b2026">sample-free. This is what the demo reads.</text>
# MAGIC </svg>
# MAGIC <div style="font-size: 14pt; color: #5E7077; line-height: 1.55; margin-top: 14px;">The next cell shows that sample-free projection, the shape an analyst works with in production.</div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why a control on the live table breaks it</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **A control on the table breaks the scan.** Data Classification manages the writes to `system.data_classification.results`, so a table-level row filter or column mask set with `ALTER TABLE` interferes with the system and fails the scan. ABAC policies do not affect the scan and are allowed.
# MAGIC - **Read the snapshot instead.** A deterministic, sample-free `snap_classification_results` scoped to the demo estate is what this demo reads, so the review runs the same every time.
# MAGIC - **Seeing the live sample values needs a grant.** The samples are sensitive, so reading them from the system table requires `SELECT` on `system.data_classification.results`, granted by an account admin.
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# Sample-free projection from the snapshot (the shape analysts read in production).
display(spark.sql(f"""
SELECT table_name, column_name, class_tag, confidence, frequency
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.snap_classification_results
ORDER BY confidence DESC, table_name, column_name
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. One detection is wrong
# MAGIC
# MAGIC Scan the results and one row doesn't belong. The `created_at` timestamp was detected as `class.date_of_birth`. Any date-shaped column can trip the birth-date classifier, which is why it landed at `LOW` confidence.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .fp-grid { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .fp-grid > * { flex: 1; min-width: 300px; }
# MAGIC .fp-card { background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); overflow: hidden; border-top: 8px solid #98102A; }
# MAGIC .fp-band { background: #98102A; color: #fff; padding: 12px 18px; font-size: 16pt; font-weight: 700; }
# MAGIC .fp-body { padding: 16px 20px; }
# MAGIC .fp-item { display: flex; justify-content: space-between; gap: 12px; padding: 8px 0; border-bottom: 1px solid #EEEDE9; font-size: 14pt; }
# MAGIC .fp-item:last-child { border-bottom: none; }
# MAGIC .fp-k { color: #5E7077; }
# MAGIC .fp-v { font-weight: 700; font-family: monospace; color: #0b2026; overflow-wrap: anywhere; text-align: right; }
# MAGIC .fp-badge { display: inline-block; background: rgba(152,16,42,0.12); color: #98102A; font-weight: 700; border-radius: 4px; padding: 2px 8px; font-size: 13px; }
# MAGIC .ex-card { background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); overflow: hidden; border-top: 8px solid #00A972; }
# MAGIC .ex-band { background: #00A972; color: #fff; padding: 12px 18px; font-size: 16pt; font-weight: 700; }
# MAGIC .ex-body { padding: 16px 20px; font-size: 14pt; line-height: 1.55; }
# MAGIC .ex-body ul { margin: 8px 0 0; padding-left: 20px; }
# MAGIC .ex-body li { margin-bottom: 10px; } .ex-body li:last-child { margin-bottom: 0; }
# MAGIC .ds-note { border-left: 4px solid #4299E0; background: rgba(66,153,224,0.10); padding: 12px 16px; border-radius: 4px; margin-top: 20px; font-size: 14pt; line-height: 1.55; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="fp-grid">
# MAGIC   <div class="fp-card">
# MAGIC     <div class="fp-band">Flagged detection</div>
# MAGIC     <div class="fp-body">
# MAGIC       <div class="fp-item"><span class="fp-k">Column</span><span class="fp-v">customer_profile.created_at</span></div>
# MAGIC       <div class="fp-item"><span class="fp-k">Detected class</span><span class="fp-v">class.date_of_birth</span></div>
# MAGIC       <div class="fp-item"><span class="fp-k">Confidence</span><span class="fp-v"><span class="fp-badge">LOW</span></span></div>
# MAGIC       <div class="fp-item"><span class="fp-k">Samples</span><span class="fp-v">2021-06-02, 2022-01-17</span></div>
# MAGIC       <div class="fp-item"><span class="fp-k">Verdict</span><span class="fp-v" style="color:#98102A;">account dates, not births</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ex-card">
# MAGIC     <div class="ex-band">Exclude it (Beta)</div>
# MAGIC     <div class="ex-body">
# MAGIC       In the review panel you exclude the detection. Excluding it:
# MAGIC       <ul>
# MAGIC         <li>Removes the classification tag from that column.</li>
# MAGIC         <li>Prevents future scans from reapplying the tag.</li>
# MAGIC         <li>Feeds back to improve later classification accuracy.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-note">
# MAGIC   <strong>Read the samples before you act.</strong> The samples are account-creation timestamps, not birth dates, so this is a false positive. Confidence and samples together are how you triage: confirm the true detections and exclude the ones that don't hold up.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why date columns trip the birth-date class</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Date columns are a classic false positive.** A birth-date detector keys on date-shaped values, so any date or timestamp column can match. Here `created_at` is an account-creation time, but its shape looks like a date of birth.
# MAGIC - **Confidence flagged it for you.** The detection came in at `LOW`, which is the signal to look before acting. The genuine identifiers on this table came in at `HIGH`.
# MAGIC - **Exclusion is the correction, still in Beta.** You exclude a detection in the review panel. That removes the applied tag, stops future scans from reapplying it and feeds back to improve accuracy. It's the supported way to handle a false positive.
# MAGIC - **Don't just ignore it.** An un-excluded false positive keeps its tag and if that tag drives a control you end up masking or filtering a harmless column. Correct the classification so the downstream controls stay correct.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Triage by confidence
# MAGIC
# MAGIC Inspect the `LOW`-confidence detections first. `created_at` is the false positive to exclude and the `body_text` phone match is a real but partial detection in free text.
# MAGIC
# MAGIC > What you should see: only the `LOW`-confidence detections, with their samples. Every other detection came in at `HIGH`. Those are the shortlist a governance team would act on.

# COMMAND ----------

display(spark.sql(f"""
SELECT table_name, column_name, class_tag, confidence, frequency, samples
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.snap_classification_results
WHERE confidence = 'LOW'
ORDER BY table_name, column_name
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Try it: ask Genie Code</div>
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">You just triaged the detections by hand, so you can judge whether the assistant's answer is right. Ask <strong>Genie Code</strong> to summarize the same results. Click <strong>Copy</strong> and paste the prompt into Genie Code in the notebook or SQL editor.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">Using priv_demo.snap_classification_results, list each column and its class_tag, group by confidence and suggest a sensitivity level of public, internal, confidential or restricted for each detection.</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. From `class.*` Tags to a Governed Sensitivity Taxonomy
# MAGIC
# MAGIC A `class.*` tag tells you what a column is. It doesn't decide how to treat it and it doesn't enforce anything. You add a governed **sensitivity_level** tag that says how to treat the column and an ABAC policy in Demo 4 matches on that tag. The governed tag is the bridge from a detection to a control.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 980 300" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="br-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- Stage 1: detection -->
# MAGIC   <rect x="25" y="80" width="250" height="130" rx="10" fill="#4299E0" fill-opacity="0.10" stroke="#4299E0" stroke-width="1.8"/>
# MAGIC   <text x="150" y="112" text-anchor="middle" font-size="13" font-weight="700" fill="#2272B4" letter-spacing="1.5">WHAT IT IS</text>
# MAGIC   <text x="150" y="142" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">class.email_address</text>
# MAGIC   <text x="150" y="168" text-anchor="middle" font-size="13" fill="#5E7077">Detection from</text>
# MAGIC   <text x="150" y="186" text-anchor="middle" font-size="13" fill="#5E7077">Data Classification</text>
# MAGIC   <!-- arrow 1 -->
# MAGIC   <path d="M275 145 L363 145" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#br-arrow)"/>
# MAGIC   <text x="319" y="135" text-anchor="middle" font-size="12" fill="#618794" font-weight="700">you decide</text>
# MAGIC   <!-- Stage 2: governed tag -->
# MAGIC   <rect x="365" y="80" width="250" height="130" rx="10" fill="#FFAB00" fill-opacity="0.14" stroke="#FFAB00" stroke-width="1.8"/>
# MAGIC   <text x="490" y="112" text-anchor="middle" font-size="13" font-weight="700" fill="#B27A00" letter-spacing="1.5">HOW TO TREAT IT</text>
# MAGIC   <text x="490" y="142" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">sensitivity_level = restricted</text>
# MAGIC   <text x="490" y="168" text-anchor="middle" font-size="13" fill="#5E7077">Governed tag from a</text>
# MAGIC   <text x="490" y="186" text-anchor="middle" font-size="13" fill="#5E7077">controlled vocabulary</text>
# MAGIC   <!-- arrow 2 -->
# MAGIC   <path d="M615 145 L703 145" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#br-arrow)"/>
# MAGIC   <text x="659" y="135" text-anchor="middle" font-size="12" fill="#618794" font-weight="700">policy matches</text>
# MAGIC   <!-- Stage 3: control -->
# MAGIC   <rect x="705" y="80" width="250" height="130" rx="10" fill="#FF5F46" fill-opacity="0.10" stroke="#FF5F46" stroke-width="1.8"/>
# MAGIC   <text x="830" y="112" text-anchor="middle" font-size="13" font-weight="700" fill="#98102A" letter-spacing="1.5">THE CONTROL</text>
# MAGIC   <text x="830" y="142" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026">ABAC column mask</text>
# MAGIC   <text x="830" y="168" text-anchor="middle" font-size="13" fill="#5E7077">Restricted users see</text>
# MAGIC   <text x="830" y="186" text-anchor="middle" font-size="13" fill="#5E7077">a masked value (Demo 4)</text>
# MAGIC   <!-- footer band -->
# MAGIC   <text x="490" y="255" text-anchor="middle" font-size="14" font-weight="700" fill="#98102A">A class tag describes data. Only the governed tag, matched by a policy, changes what a user sees.</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why the class tag isn't the control</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **A class tag is a fact, not a decision.** `class.email_address` says a column holds email addresses. It doesn't say whether analysts may see it and by itself it doesn't hide anything.
# MAGIC - **The governed tag carries the decision.** You map the detection to a `sensitivity_level` value from a small controlled vocabulary. That value is the attribute an ABAC policy matches on, so it's where the treatment decision lives.
# MAGIC - **Why not act on the class tag directly?** Different columns with the same class can need different treatment and a controlled vocabulary keeps the decision consistent and reviewable across the estate. It also means one policy can cover many classes that share a sensitivity_level.
# MAGIC - **This is why classification pays off.** The detection is only useful once it drives a governed tag and the governed tag is only useful once a policy acts on it. Demo 4 builds that policy.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. A small, controlled sensitivity_level taxonomy
# MAGIC
# MAGIC Keep the vocabulary small so it stays consistent. Click a level to see what it means and the control it usually drives.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <style>
# MAGIC .rsd-row { display: block; white-space: nowrap; font-size: 0; width: 100%; box-sizing: border-box; }
# MAGIC .rsd-box { display: inline-block; width: 23.5%; margin-right: 2%; min-height: 92px; background: #F9F7F4; border-top: 8px solid transparent; border-left: 2px solid transparent; border-right: 2px solid transparent; border-bottom: 2px solid transparent; border-radius: 8px; padding: 14px 10px; text-align: center; box-sizing: border-box; cursor: pointer; user-select: none; vertical-align: top; transition: transform 0.12s; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .rsd-box:last-child { margin-right: 0; }
# MAGIC .rsd-box:hover { transform: translateY(-2px); }
# MAGIC .rsd-box.active { background: #fff; border-left-color: var(--pc); border-right-color: var(--pc); border-bottom-color: var(--pc); }
# MAGIC .rsd-title { display: block; font-size: 14pt; font-weight: 700; color: #0b2026; line-height: 1.3; white-space: normal; pointer-events: none; }
# MAGIC .rsd-sub { display: block; font-size: 13px; color: #5E7077; margin-top: 6px; white-space: normal; pointer-events: none; }
# MAGIC .rsd-detail-wrap { overflow: hidden; max-height: 0; opacity: 0; transition: max-height 0.35s ease, opacity 0.28s ease, margin-top 0.28s ease; margin-top: 0; }
# MAGIC .rsd-detail-wrap.open { max-height: 800px; opacity: 1; margin-top: 12px; }
# MAGIC .rsd-detail-card { background: #F9F7F4; border-radius: 10px; padding: 20px; box-sizing: border-box; border-top: 7px solid #ccc; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .rsd-block { border-left: 4px solid; border-radius: 0 6px 6px 0; padding: 10px 12px; margin-bottom: 12px; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="background: #F8F9FC; border: 3px solid #1B5162; border-radius: 10px; padding: 24px; box-sizing: border-box;">
# MAGIC   <div class="rsd-row">
# MAGIC     <span class="rsd-box active" data-id="0" onclick="rsdSelect(0)" style="--pc:#00A972; border-top-color:#00A972;"><span class="rsd-title">public</span><span class="rsd-sub">safe to share</span></span><span class="rsd-box" data-id="1" onclick="rsdSelect(1)" style="--pc:#4299E0; border-top-color:#4299E0;"><span class="rsd-title">internal</span><span class="rsd-sub">staff only</span></span><span class="rsd-box" data-id="2" onclick="rsdSelect(2)" style="--pc:#FFAB00; border-top-color:#B27A00;"><span class="rsd-title">confidential</span><span class="rsd-sub">limited access</span></span><span class="rsd-box" data-id="3" onclick="rsdSelect(3)" style="--pc:#98102A; border-top-color:#98102A;"><span class="rsd-title">restricted</span><span class="rsd-sub">tightest control</span></span>
# MAGIC   </div>
# MAGIC   <div class="rsd-detail-wrap" id="rsd-detail-wrap">
# MAGIC     <div class="rsd-detail-card" id="rsd-detail-card"></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var RSD = [
# MAGIC   { title: "public", color: "#00A972", roleBg: "rgba(0,169,114,0.10)",
# MAGIC     can: ["Non-personal, safe to share broadly", "Example: country, loyalty_tier"], cannot: [],
# MAGIC     role: "Usual control: none needed." },
# MAGIC   { title: "internal", color: "#4299E0", roleBg: "rgba(66,153,224,0.10)",
# MAGIC     can: ["Business data for staff, not the public", "Example: points_balance, order counts"], cannot: [],
# MAGIC     role: "Usual control: standard grants." },
# MAGIC   { title: "confidential", color: "#B27A00", roleBg: "rgba(255,171,0,0.12)",
# MAGIC     can: ["Personal data most analysts do not need in the clear", "Example: phone, street_address"], cannot: [],
# MAGIC     role: "Usual control: mask for most, allow an exception group." },
# MAGIC   { title: "restricted", color: "#98102A", roleBg: "rgba(152,16,42,0.10)",
# MAGIC     can: ["Direct identifiers with the tightest handling", "Example: email, national_id"], cannot: [],
# MAGIC     role: "Usual control: mask by default, few see the original." }
# MAGIC ];
# MAGIC var rsdCurrent = null;
# MAGIC function rsdSelect(id) {
# MAGIC   var wrap = document.getElementById('rsd-detail-wrap');
# MAGIC   var card = document.getElementById('rsd-detail-card');
# MAGIC   var p = RSD[id];
# MAGIC   document.querySelectorAll('.rsd-box').forEach(function(b) {
# MAGIC     b.classList.toggle('active', parseInt(b.dataset.id) === id && rsdCurrent !== id);
# MAGIC   });
# MAGIC   if (rsdCurrent === id) { wrap.classList.remove('open'); rsdCurrent = null; return; }
# MAGIC   rsdCurrent = id;
# MAGIC   var canHtml = p.can.map(function(c){ return '<li style="margin-bottom:4px;">' + c + '</li>'; }).join('');
# MAGIC   card.style.borderTopColor = p.color;
# MAGIC   card.innerHTML =
# MAGIC     '<div style="font-size:20pt;font-weight:700;margin-bottom:12px;color:#0b2026;">' + p.title + '</div>'
# MAGIC     + '<div class="rsd-block" style="background:#F8F9FC;border-color:' + p.color + ';">'
# MAGIC     + '<div style="font-weight:700;font-size:15pt;margin-bottom:4px;">What it covers</div>'
# MAGIC     + '<ul style="font-size:15pt;margin:0;padding-left:16px;">' + canHtml + '</ul></div>'
# MAGIC     + '<div class="rsd-block" style="background:' + p.roleBg + ';border-color:' + p.color + ';margin-bottom:0;">'
# MAGIC     + '<div style="font-weight:700;font-size:15pt;margin-bottom:4px;">Control it drives</div>'
# MAGIC     + '<div style="font-size:15pt;color:#0b2026;">' + p.role + '</div></div>';
# MAGIC   wrap.classList.add('open');
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why keep the vocabulary small</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Four levels are enough to start.** `public`, `internal`, `confidential` and `restricted` cover most decisions without turning tagging into a project. A governed tag can hold up to 500 allowed values, but a small set is easier to apply consistently.
# MAGIC - **The level is a treatment decision.** Two columns with different classes can share a level and the level is what a policy acts on. `email` and `national_id` are both `restricted` even though their classes differ.
# MAGIC - **Each level implies a control.** `restricted` and `confidential` usually drive a mask, with an exception group that sees the original. `internal` relies on ordinary grants and `public` needs no control. Demo 04 builds the policy that enforces this.
# MAGIC - **Consistency is the point.** A controlled vocabulary keeps everyone using the same words, so a policy written once covers every column tagged the same way, including columns added later.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Map each detection to a sensitivity_level
# MAGIC
# MAGIC Turn each detection into a recommended `sensitivity_level`. This is a deterministic mapping over the snapshot, so it runs the same for everyone and gives you the plan for what to tag.
# MAGIC
# MAGIC > What you should see: each detection with a proposed sensitivity_level. Direct identifiers map to **restricted**, contact details to **confidential** and the low-confidence `created_at` match is left to review. The setup you ran in Section A has already applied these values across your estate, except that `created_at` false positive, which you inspect in E4.

# COMMAND ----------

display(spark.sql(f"""
SELECT
  table_name,
  column_name,
  class_tag,
  confidence,
  CASE
    WHEN class_tag = 'class.date_of_birth' AND confidence = 'LOW' THEN 'review (likely false positive)'
    WHEN class_tag IN ('class.email_address', 'class.us_ssn', 'class.passport',
                       'class.credit_card', 'class.iban_code', 'class.bank_number') THEN 'restricted'
    WHEN class_tag IN ('class.phone_number', 'class.location', 'class.ip_address',
                       'class.date_of_birth') THEN 'confidential'
    WHEN class_tag = 'class.name' THEN 'internal'
    ELSE 'internal'
  END AS recommended_sensitivity
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.snap_classification_results
ORDER BY recommended_sensitivity, table_name, column_name
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E3. Apply the governed tag
# MAGIC <br/>
# MAGIC <div class="gt">
# MAGIC <style>
# MAGIC .gt { font-family: sans-serif; max-width: 1040px; margin: 0 auto; color: #0b2026; }
# MAGIC .gt * { box-sizing: border-box; }
# MAGIC .gt-lead { font-size: 15pt; line-height: 1.6; margin-bottom: 16px; }
# MAGIC .gt-viz { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; background: #F9F7F4; border: 1px solid #EEEDE9; border-radius: 10px; padding: 16px 20px; }
# MAGIC .gt-key { font-family: monospace; font-size: 14pt; font-weight: 700; color: #2272B4; background: #FFFFFF; border: 1.5px solid #2272B4; border-radius: 6px; padding: 8px 14px; white-space: nowrap; }
# MAGIC .gt-only { font-size: 13px; font-weight: 700; letter-spacing: 0.5px; text-transform: uppercase; color: #618794; }
# MAGIC .gt-vals { display: flex; gap: 8px; flex-wrap: wrap; }
# MAGIC .gt-val { font-size: 14px; font-weight: 600; background: #FFFFFF; border: 1px solid #C7D0D4; border-radius: 999px; padding: 6px 14px; color: #0b2026; }
# MAGIC .gt-val.hi { border-color: #FF5F46; color: #98102A; background: rgba(255,95,70,0.08); }
# MAGIC .gt-cap { font-size: 14px; color: #618794; line-height: 1.5; margin-top: 10px; }
# MAGIC .gt-note { font-size: 14pt; line-height: 1.6; color: #0b2026; border-left: 4px solid #618794; padding: 4px 0 4px 16px; margin-top: 20px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="gt-lead">A <strong>governed tag</strong> is a tag whose key and allowed values are set centrally, so a column can only carry a sanctioned value. That is what lets the ABAC policy in Demo 4 match on <code>sensitivity_level = restricted</code> and trust the value instead of a free-form label someone typed.</div>
# MAGIC
# MAGIC <div class="gt-viz">
# MAGIC   <span class="gt-key">sensitivity_level</span>
# MAGIC   <span class="gt-only">takes one of</span>
# MAGIC   <span class="gt-vals">
# MAGIC     <span class="gt-val">public</span>
# MAGIC     <span class="gt-val">internal</span>
# MAGIC     <span class="gt-val">confidential</span>
# MAGIC     <span class="gt-val hi">restricted</span>
# MAGIC   </span>
# MAGIC </div>
# MAGIC <div class="gt-cap">These four values are the whole allowed set, so any tagged column carries exactly one of them.</div>
# MAGIC
# MAGIC <div class="gt-note"><strong>Created by the Workspace Setup, applied by this notebook's setup.</strong> Creating a governed tag and granting <code>ASSIGN</code> on it are account-level admin actions a lesson notebook cannot do, so the Workspace Setup does both once. With <code>ASSIGN</code> granted, this notebook's setup applies the values to the tables you own.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Inspect the governed tag
# MAGIC
# MAGIC Read the governed tags back from `information_schema.column_tags`. Your setup applied the `sensitivity_level` value to every column you mapped in E2, except the `created_at` false positive, so the read-back is the evidence the plan is in place. If the setup couldn't apply the tag (it reports `[SKIPPED]`), no `sensitivity_level` rows come back and E2 stays the plan for what to tag. Demo 4's ABAC policy then matches on this tag.
# MAGIC
# MAGIC > What you should see: two kinds of row. The `sensitivity_level` rows carry a value (`restricted`, `confidential`, `internal`): the governed tags you mapped in E2, whose value the ABAC policy matches on. Where Data Classification has run on the catalog you also see `class.*` rows (like `class.name`, `class.location`) with a blank `tag_value`. Those are system tags from Data Classification. They are presence-based: the key itself signals that the column holds that kind of data, so there is no value to carry.

# COMMAND ----------

# Read the current column tags back from information_schema (the tag evidence).
try:
    tags = spark.sql(f"""
        SELECT table_name, column_name, tag_name, tag_value
        FROM `{my_catalog}`.information_schema.column_tags
        WHERE schema_name = '{DEMO_SCHEMA}'
          AND table_name IN ('customer_profile', 'support_records')
        ORDER BY table_name, column_name, tag_name
    """)
    n = tags.count()
    if n == 0:
        print("No column tags found yet for priv_demo. The recommended mapping in E2 is the plan for what to tag.")
    else:
        print(f"{n} column tag(s) currently on priv_demo tables:")
        display(tags)
except Exception as e:
    first = (str(e).splitlines() or [""])[0]
    print("Couldn't read information_schema.column_tags here. This is expected where you lack access to it.")
    print(f"({first})")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E5. Three kinds of tag and which ones a policy can use
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-row { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 260px; }
# MAGIC .blue{--accent:#4299E0;} .green{--accent:#00A972;} .coral{--accent:#FF5F46;} .amber{--accent:#FFAB00;} .red{--accent:#98102A;}
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px; border-top: 8px solid var(--accent, #4299E0); display: flex; flex-direction: column; overflow-wrap: anywhere; }
# MAGIC .ds-card-title { font-size: 17pt; font-weight: 700; line-height: 1.25; margin: 0 0 4px; }
# MAGIC .ds-card-sub { font-size: 14pt; font-weight: 600; color: #1B5162; margin: 0 0 10px; line-height: 1.4; }
# MAGIC .ds-card ul { margin: 4px 0 0; padding-left: 20px; font-size: 14pt; line-height: 1.55; color: #0b2026; }
# MAGIC .ds-card li { margin-bottom: 8px; } .ds-card li:last-child { margin-bottom: 0; }
# MAGIC .ds-card code { font-family: monospace; background: #EEEDE9; padding: 1px 5px; border-radius: 3px; color: #1B5162; }
# MAGIC .ds-callout { border-left: 6px solid #1B5162; border-radius: 8px; padding: 16px 22px; background: #F8F9FC; box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 20px; }
# MAGIC .tc { text-align: center; }
# MAGIC .ds-stat { font-size: 26pt; font-weight: 800; color: var(--accent); line-height: 1.1; }
# MAGIC .ds-stat-label { font-size: 14pt; color: #5E7077; margin-top: 6px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-card blue">
# MAGIC     <div class="ds-card-title">System <code>class.*</code> tags</div>
# MAGIC     <div class="ds-card-sub">Governed &middot; applied by the system</div>
# MAGIC     <ul>
# MAGIC       <li>Data Classification applies them, presence-based: the key is the signal.</li>
# MAGIC       <li>You don't define or assign them.</li>
# MAGIC       <li>ABAC can match on them directly, for example <code>has_tag('class.email_address')</code>.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div class="ds-card amber">
# MAGIC     <div class="ds-card-title">Your <code>sensitivity_level</code> tag</div>
# MAGIC     <div class="ds-card-sub">Governed &middot; you define and assign</div>
# MAGIC     <ul>
# MAGIC       <li>You define the key and its allowed values (public, internal, confidential, restricted).</li>
# MAGIC       <li>You map <code>class.*</code> detections to it after reviewing them.</li>
# MAGIC       <li>The course's ABAC policy matches on this.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div class="ds-card coral">
# MAGIC     <div class="ds-card-title">Free-form tags</div>
# MAGIC     <div class="ds-card-sub">Not governed</div>
# MAGIC     <ul>
# MAGIC       <li>Any key and value, no controlled vocabulary.</li>
# MAGIC       <li>ABAC cannot match on them.</li>
# MAGIC       <li>Useful for search and organization only.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="text-align:center; font-size:15pt; font-weight:700; color:#0b2026; margin: 24px 0 12px;">From detection to enforcement</div>
# MAGIC <svg width="100%" viewBox="0 0 900 300" role="img" style="font-family: sans-serif;" aria-label="Data Classification produces governed class.* detections. You review them, dropping false positives like the created_at date, and map the rest to the governed sensitivity_level tag, which the ABAC column mask matches on. A dashed path shows a policy could match class.* directly, which the course does not use.">
# MAGIC   <defs>
# MAGIC     <marker id="e5-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC     <marker id="e5-arrow-gray" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#90A5B1" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <rect x="16" y="46" width="250" height="150" rx="8" fill="#4299E0" fill-opacity="0.08" stroke="#4299E0" stroke-width="1.5"/>
# MAGIC   <text x="141" y="72" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026">class.* detections</text>
# MAGIC   <text x="141" y="91" text-anchor="middle" font-size="12" fill="#618794">governed &#183; from Data Classification</text>
# MAGIC   <text x="141" y="120" text-anchor="middle" font-size="12.5" font-family="monospace" fill="#2272B4">class.email_address</text>
# MAGIC   <text x="141" y="140" text-anchor="middle" font-size="12.5" font-family="monospace" fill="#2272B4">class.us_ssn</text>
# MAGIC   <text x="141" y="164" text-anchor="middle" font-size="12.5" font-family="monospace" fill="#98102A">class.date_of_birth &#10007;</text>
# MAGIC   <text x="141" y="182" text-anchor="middle" font-size="11.5" fill="#98102A">false positive on created_at</text>
# MAGIC   <path d="M266 121 L346 121" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#e5-arrow)"/>
# MAGIC   <text x="306" y="112" text-anchor="middle" font-size="12" fill="#5E7077">review + map</text>
# MAGIC   <rect x="350" y="46" width="210" height="150" rx="8" fill="#FFAB00" fill-opacity="0.10" stroke="#FFAB00" stroke-width="1.5"/>
# MAGIC   <text x="455" y="72" text-anchor="middle" font-size="15" font-weight="700" font-family="monospace" fill="#0b2026">sensitivity_level</text>
# MAGIC   <text x="455" y="91" text-anchor="middle" font-size="12" fill="#618794">governed &#183; your decision</text>
# MAGIC   <rect x="378" y="112" width="154" height="28" rx="14" fill="#98102A" fill-opacity="0.08" stroke="#98102A" stroke-width="1"/>
# MAGIC   <text x="455" y="131" text-anchor="middle" font-size="13" font-weight="600" fill="#98102A">restricted</text>
# MAGIC   <rect x="378" y="150" width="154" height="28" rx="14" fill="#FFAB00" fill-opacity="0.16" stroke="#B27A00" stroke-width="1"/>
# MAGIC   <text x="455" y="169" text-anchor="middle" font-size="13" font-weight="600" fill="#B27A00">confidential</text>
# MAGIC   <path d="M560 121 L646 121" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#e5-arrow)"/>
# MAGIC   <text x="603" y="112" text-anchor="middle" font-size="12" fill="#5E7077">matches</text>
# MAGIC   <rect x="650" y="86" width="234" height="72" rx="8" fill="#FF5F46" fill-opacity="0.08" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="767" y="116" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026">ABAC column mask</text>
# MAGIC   <text x="767" y="138" text-anchor="middle" font-size="12" fill="#618794">matches sensitivity_level</text>
# MAGIC   <path d="M141 196 C 141 268, 767 268, 767 158" fill="none" stroke="#90A5B1" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#e5-arrow-gray)"/>
# MAGIC   <text x="454" y="257" text-anchor="middle" font-size="12" fill="#90A5B1">a policy could match class.* directly (not used here)</text>
# MAGIC </svg>
# MAGIC
# MAGIC <div class="ds-callout" style="border-left-color:#FF5F46; background:#FFF6F4;">
# MAGIC   <strong>Both layers are governed, so ABAC could match <code>class.*</code> directly</strong> (the dashed path). The course maps to <code>sensitivity_level</code> first, for two reasons. A human review sits between automated detection and enforcement, so a false positive like <code>created_at</code> never reaches a policy. A single <code>sensitivity_level</code> value also stands in for many <code>class.*</code> classes, so one mask covers them all.
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-callout">
# MAGIC   <strong>Governed tags are GA, account-level and defined once in a tag policy</strong> that fixes the allowed values, so the vocabulary stays consistent. Keep PII and secrets out of tag keys and values, because they are broadly visible in metadata and system tables. ABAC requires a governed tag, not a free-form one, so tag quality is part of the security boundary.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">What a policy can match on</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **ABAC matches only on governed tags.** Both `class.*` and `sensitivity_level` are governed, so a policy can match either with `has_tag()` or `has_tag_value()`. A free-form tag has no controlled key or values, so ABAC cannot use it.
# MAGIC - **Separation of duties.** One group defines the vocabulary (the tag policy) and another applies it within the allowed values. Creating governed tags needs an account-level privilege and assigning one needs `ASSIGN` on the tag plus `APPLY TAG` on the object.
# MAGIC - **Matching on `sensitivity_level` is a design choice, not a platform limit.** A policy could match `class.*` directly. The course adds `sensitivity_level` as a reviewed, consolidated layer over those detections, for the two reasons in the note above.
# MAGIC - **Keep tags clean.** Tag keys and values are visible across metadata and system tables, so a tag is the wrong place for a raw value. Tag with a category like `restricted`, never with the sensitive value itself.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. Custom Classifiers, a Beta Preview
# MAGIC
# MAGIC The built-in classes detect widely used identifiers such as email addresses, national IDs and card numbers. They have no way to recognize an identifier that exists only inside your organization, like the retailer's internal agent code `AGT-0000` or a loyalty membership number. A custom classifier extends Data Classification to catch those. It is a Beta feature, so this section explains how it works rather than running it as a live step.
# MAGIC
# MAGIC You do not write a regular expression or an example format string. You describe the identifier in plain language and point the classifier at up to ten existing columns that already contain it and Databricks builds the detector from those inputs. When you create the classifier you also choose the governed tag it should apply. A built-in detection applies a `class.*` tag, which you then map to your `sensitivity_level` tag, the step you did in Section E. A custom classifier skips that mapping and applies the governed tag you chose, such as `sensitivity_level`, as soon as it finds a match.
# MAGIC
# MAGIC <div style="max-width: 1040px; margin: 8px auto 0; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 980 300" role="img" style="font-family: sans-serif;">
# MAGIC   <defs><marker id="cc-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC   <text x="490" y="26" text-anchor="middle" font-size="17" font-weight="700" fill="#0b2026">Two routes to a governed tag</text>
# MAGIC   <text x="490" y="48" text-anchor="middle" font-size="13" fill="#5E7077">A built-in class is mapped to a governed tag; a custom classifier applies one directly.</text>
# MAGIC   <text x="24" y="76" font-size="12.5" font-weight="700" fill="#618794">BUILT-IN CLASS</text>
# MAGIC   <rect x="24" y="84" width="188" height="56" rx="8" fill="#4299E0" fill-opacity="0.10" stroke="#4299E0" stroke-width="1.3"/>
# MAGIC   <text x="118" y="108" text-anchor="middle" font-size="12.5" fill="#0b2026">standard identifier</text>
# MAGIC   <text x="118" y="126" text-anchor="middle" font-size="11.5" fill="#5E7077">email, national ID, card</text>
# MAGIC   <line x1="212" y1="112" x2="246" y2="112" stroke="#5E7077" stroke-width="1.4" marker-end="url(#cc-ar)"/>
# MAGIC   <rect x="248" y="84" width="188" height="56" rx="8" fill="#4299E0" fill-opacity="0.10" stroke="#4299E0" stroke-width="1.3"/>
# MAGIC   <text x="342" y="106" text-anchor="middle" font-size="12.5" fill="#0b2026">class tag applied</text>
# MAGIC   <text x="342" y="126" text-anchor="middle" font-size="11.5" font-family="monospace" fill="#2272B4">class.email_address</text>
# MAGIC   <line x1="436" y1="112" x2="470" y2="112" stroke="#5E7077" stroke-width="1.4" marker-end="url(#cc-ar)"/>
# MAGIC   <rect x="472" y="84" width="188" height="56" rx="8" fill="#4299E0" fill-opacity="0.10" stroke="#4299E0" stroke-width="1.3"/>
# MAGIC   <text x="566" y="106" text-anchor="middle" font-size="12.5" fill="#0b2026">you map it to a</text>
# MAGIC   <text x="566" y="126" text-anchor="middle" font-size="12.5" fill="#0b2026">governed tag (Section E)</text>
# MAGIC   <line x1="660" y1="114" x2="724" y2="130" stroke="#5E7077" stroke-width="1.4" marker-end="url(#cc-ar)"/>
# MAGIC   <text x="24" y="194" font-size="12.5" font-weight="700" fill="#618794">CUSTOM CLASSIFIER</text>
# MAGIC   <rect x="24" y="202" width="188" height="56" rx="8" fill="#00A972" fill-opacity="0.10" stroke="#00A972" stroke-width="1.3"/>
# MAGIC   <text x="118" y="226" text-anchor="middle" font-size="12.5" fill="#0b2026">org-specific identifier</text>
# MAGIC   <text x="118" y="244" text-anchor="middle" font-size="11.5" font-family="monospace" fill="#007a53">AGT-0000</text>
# MAGIC   <line x1="212" y1="230" x2="246" y2="230" stroke="#5E7077" stroke-width="1.4" marker-end="url(#cc-ar)"/>
# MAGIC   <rect x="248" y="202" width="188" height="56" rx="8" fill="#00A972" fill-opacity="0.10" stroke="#00A972" stroke-width="1.3"/>
# MAGIC   <text x="342" y="226" text-anchor="middle" font-size="12.5" fill="#0b2026">custom classifier applies</text>
# MAGIC   <text x="342" y="244" text-anchor="middle" font-size="12.5" fill="#0b2026">its governed tag directly</text>
# MAGIC   <line x1="436" y1="230" x2="724" y2="178" stroke="#5E7077" stroke-width="1.4" marker-end="url(#cc-ar)"/>
# MAGIC   <rect x="726" y="96" width="230" height="120" rx="10" fill="#1B5162" stroke="#0b2026" stroke-width="1.4"/>
# MAGIC   <text x="841" y="140" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Governed tag</text>
# MAGIC   <text x="841" y="164" text-anchor="middle" font-size="13" fill="#F9F7F4">drives the control</text>
# MAGIC   <text x="841" y="188" text-anchor="middle" font-size="12" fill="#FFAB00">ABAC mask &middot; row filter</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC A custom classifier is defined once for the whole metastore, so it runs against every catalog that has Data Classification enabled rather than a single catalog. A new or changed classifier takes effect on the next scan and its detections then appear on the Data Classification results page within a few hours, where you review or exclude them exactly as you did for the built-in classes.
# MAGIC
# MAGIC <div style="max-width: 1040px; margin: 8px auto 0; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 980 250" role="img" style="font-family: sans-serif;">
# MAGIC   <defs><marker id="cc-ar2" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC   <text x="490" y="26" text-anchor="middle" font-size="17" font-weight="700" fill="#0b2026">Defining a custom classifier</text>
# MAGIC   <rect x="24" y="52" width="252" height="150" rx="8" fill="#FFAB00" fill-opacity="0.10" stroke="#B27A00" stroke-width="1.3"/>
# MAGIC   <text x="150" y="76" text-anchor="middle" font-size="12.5" font-weight="700" fill="#1B5162">YOU PROVIDE</text>
# MAGIC   <text x="44" y="106" font-size="12.5" fill="#0b2026">&bull; a plain-language description</text>
# MAGIC   <text x="44" y="134" font-size="12.5" fill="#0b2026">&bull; up to 10 sample columns</text>
# MAGIC   <text x="44" y="162" font-size="12.5" fill="#0b2026">&bull; a governed tag to apply</text>
# MAGIC   <line x1="276" y1="127" x2="308" y2="127" stroke="#5E7077" stroke-width="1.4" marker-end="url(#cc-ar2)"/>
# MAGIC   <rect x="310" y="90" width="170" height="74" rx="8" fill="#F9F7F4" stroke="#EEEDE9" stroke-width="1.3"/>
# MAGIC   <text x="395" y="122" text-anchor="middle" font-size="12.5" fill="#0b2026">Databricks builds</text>
# MAGIC   <text x="395" y="141" text-anchor="middle" font-size="12.5" fill="#0b2026">the detector</text>
# MAGIC   <line x1="480" y1="127" x2="512" y2="127" stroke="#5E7077" stroke-width="1.4" marker-end="url(#cc-ar2)"/>
# MAGIC   <rect x="514" y="90" width="170" height="74" rx="8" fill="#F9F7F4" stroke="#EEEDE9" stroke-width="1.3"/>
# MAGIC   <text x="599" y="114" text-anchor="middle" font-size="12.5" fill="#0b2026">runs on the next</text>
# MAGIC   <text x="599" y="133" text-anchor="middle" font-size="12.5" fill="#0b2026">scan, across the</text>
# MAGIC   <text x="599" y="152" text-anchor="middle" font-size="12.5" fill="#0b2026">whole metastore</text>
# MAGIC   <line x1="684" y1="127" x2="716" y2="127" stroke="#5E7077" stroke-width="1.4" marker-end="url(#cc-ar2)"/>
# MAGIC   <rect x="718" y="90" width="238" height="74" rx="8" fill="#00A972" fill-opacity="0.10" stroke="#00A972" stroke-width="1.3"/>
# MAGIC   <text x="837" y="114" text-anchor="middle" font-size="12.5" fill="#0b2026">detections on the results</text>
# MAGIC   <text x="837" y="133" text-anchor="middle" font-size="12.5" fill="#0b2026">page in a few hours,</text>
# MAGIC   <text x="837" y="152" text-anchor="middle" font-size="12.5" fill="#0b2026">reviewed like built-ins</text>
# MAGIC   <text x="490" y="228" text-anchor="middle" font-size="12.5" fill="#5E7077">Beta preview &middot; up to 50 classifiers per metastore &middot; only affects later scans</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC As with a built-in detection, a custom classifier is only worth adding once the governed tag it applies goes on to drive a control.
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Who can create one and the limits</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Permissions.** Creating, editing or deleting a custom classifier requires metastore admin. You also need `ASSIGN` on the governed tag it applies and `SELECT` on the tables that hold the sample columns.
# MAGIC - **Limits.** A metastore can hold up to 50 custom classifiers and each uses up to 10 sample columns. The governed tag is fixed once the classifier is created, so changing it means deleting and recreating the classifier.
# MAGIC - **Applies going forward.** A new or edited classifier only affects later scans. Existing results are not reclassified until then.
# MAGIC - **Still a preview.** The feature is in Beta, so confirm the current steps and limits in the custom classifiers documentation before relying on it: [AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-custom-classifiers) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification-custom-classifiers) | [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/data-classification-custom-classifiers).
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## G. What Control Should This Tag Drive?
# MAGIC
# MAGIC This is the question that makes classification worthwhile. A tag that doesn't drive a control is just a label. For each detection, decide the control before you move on.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .map-card { background: #F9F7F4; border-radius: 10px; padding: 8px 22px; border-top: 8px solid #FF5F46; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .map-item { display: grid; grid-template-columns: 250px 190px 1fr; gap: 18px; padding: 14px 0; border-bottom: 1px solid #EEEDE9; align-items: center; }
# MAGIC .map-item:last-child { border-bottom: none; }
# MAGIC .map-head { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #618794; }
# MAGIC .map-col { font-size: 14pt; font-family: monospace; font-weight: 700; color: #0b2026; overflow-wrap: anywhere; }
# MAGIC .map-tag { font-size: 14pt; font-weight: 700; }
# MAGIC .map-ctrl { font-size: 14pt; color: #0b2026; line-height: 1.5; }
# MAGIC .t-restricted { color: #98102A; } .t-confidential { color: #B27A00; } .t-internal { color: #2272B4; }
# MAGIC @media (max-width: 620px) { .map-item { grid-template-columns: 1fr; gap: 4px; } }
# MAGIC .ds-band-solid { background: #1B5162; color: #fff; border-radius: 10px; padding: 20px 28px; text-align: center; border-top: 6px solid #FF5F46; box-shadow: 0 2px 10px rgba(27,49,57,0.12); margin-top: 20px; }
# MAGIC .ds-band-lead { font-size: 17pt; font-weight: 700; color: #fff; line-height: 1.3; margin-bottom: 8px; }
# MAGIC .ds-band-sub { font-size: 14pt; color: #F9F7F4; font-weight: 600; line-height: 1.4; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="map-card">
# MAGIC   <div class="map-item">
# MAGIC     <div class="map-head">Column and class</div>
# MAGIC     <div class="map-head">Sensitivity</div>
# MAGIC     <div class="map-head">Control it should drive</div>
# MAGIC   </div>
# MAGIC   <div class="map-item">
# MAGIC     <div class="map-col">email<br><span style="font-size:12px;color:#5E7077;">class.email_address</span></div>
# MAGIC     <div class="map-tag t-restricted">restricted</div>
# MAGIC     <div class="map-ctrl">Column mask by default, an exception group sees the original (Demo 4).</div>
# MAGIC   </div>
# MAGIC   <div class="map-item">
# MAGIC     <div class="map-col">national_id<br><span style="font-size:12px;color:#5E7077;">class.us_ssn</span></div>
# MAGIC     <div class="map-tag t-restricted">restricted</div>
# MAGIC     <div class="map-ctrl">Mask and pseudonymize if a join key is needed (Demo 6).</div>
# MAGIC   </div>
# MAGIC   <div class="map-item">
# MAGIC     <div class="map-col">phone<br><span style="font-size:12px;color:#5E7077;">class.phone_number</span></div>
# MAGIC     <div class="map-tag t-confidential">confidential</div>
# MAGIC     <div class="map-ctrl">Mask for most analysts, allow the support role to see it.</div>
# MAGIC   </div>
# MAGIC   <div class="map-item">
# MAGIC     <div class="map-col">body_text<br><span style="font-size:12px;color:#5E7077;">class.email_address in free text</span></div>
# MAGIC     <div class="map-tag t-confidential">confidential</div>
# MAGIC     <div class="map-ctrl">Redact contact details in text, or restrict the column.</div>
# MAGIC   </div>
# MAGIC   <div class="map-item">
# MAGIC     <div class="map-col">created_at<br><span style="font-size:12px;color:#5E7077;">class.date_of_birth (LOW)</span></div>
# MAGIC     <div class="map-tag t-internal">exclude</div>
# MAGIC     <div class="map-ctrl">False positive. Exclude the detection so no control targets it.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-band-solid">
# MAGIC   <div class="ds-band-lead">A tag is only useful if it drives a control.</div>
# MAGIC   <div class="ds-band-sub">You classified with a control in mind and Demo 4 builds the tag-driven ABAC policy that enforces it.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">How this mapping sets up the rest of the course</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Classify with the control in mind.** If no control would ever act on a tag, you probably don't need the tag.
# MAGIC - **restricted and confidential drive masks.** Demo 4 builds a tag-driven ABAC column mask that hides these columns from most analysts and allows an exception group to see the original.
# MAGIC - **A join key needs more than a mask.** `national_id` is a direct identifier, so if downstream work needs to join on it, Demo 6 pseudonymizes it rather than only masking it.
# MAGIC - **Free text needs its own handling.** PII embedded in `body_text` isn't a whole-column identifier, so it calls for redaction in text or a restriction on the column.
# MAGIC - **A false positive drives nothing.** Excluding `created_at` keeps a control from ever targeting a harmless column, which is why correcting the classification matters.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, you:
# MAGIC
# MAGIC 1. Saw why **unclassified data is a risk** and found PII hiding in the free text of **support_records**.
# MAGIC 2. Learned how **Data Classification** detects sensitive data, applies `class.*` tags and records confidence and frequency.
# MAGIC 3. **Reviewed** the prepared detections through the restricted snapshot and used confidence and samples to **exclude** a false positive.
# MAGIC 4. **Mapped** detections to a governed **sensitivity_level** taxonomy, then inspected the tag through `information_schema`.
# MAGIC 5. Previewed **custom classifiers** and ended on the question that drives the next demo: what control should this tag drive?
# MAGIC
# MAGIC Next, in **Protect by Purpose and Risk: ABAC and Manual Masks**, you'll turn the `sensitivity_level` tag into a tag-driven ABAC column mask, validate a restricted identity against a privileged exception and confirm the policy covers a newly tagged column with no change to the table.

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
# MAGIC   <div class="cl-docs-title">Data Classification and governed tags</div>
# MAGIC   <div class="cl-docs-sub">The moving parts of this notebook change over time, so confirm the class list, the scan behavior and the governed-tag syntax in the current documentation rather than a static list.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification" target="_blank">Data Classification &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-tags" target="_blank">Supported tags &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-custom-classifiers" target="_blank">Custom classifiers &rarr;</a>
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