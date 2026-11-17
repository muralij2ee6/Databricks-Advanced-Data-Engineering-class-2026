# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">01 Demo - REQUIRED - Project Overview and Setup</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/common/databricks_academy_logo.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # REQUIRED - Project Overview and Setup
# MAGIC #### Duration: 15 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook sets up the environment the rest of the course reuses, so run it before any demo or lab. The lecture that follows introduces the running scenario and the privacy decision model.
# MAGIC
# MAGIC This course treats privacy as a set of decisions and controls you implement and verify on Databricks, not a compliance certificate. 
# MAGIC
# MAGIC You'll discover sensitive data, protect it by purpose and risk, de-identify it, fulfil an erasure request and prove every control worked with verifiable evidence.
# MAGIC
# MAGIC Here you'll build the shared demo estate and read the sensitive-column detections you'll work with. Data Classification scans on its own schedule, so you work from a deterministic snapshot rather than a live scan.
# MAGIC
# MAGIC > Serverless-first: this course runs entirely on serverless. You'll use a serverless notebook plus a serverless SQL warehouse.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you'll be able to:
# MAGIC
# MAGIC 1. **Explain** the two-layer setup: what Workspace-Setup provisions once (the governed tag, account groups and secret scope) versus what each notebook's own setup rebuilds.
# MAGIC 2. **Configure** your workspace and catalog for the course, including the catalog override for restricted workspaces.
# MAGIC 3. **Run** the setup and **verify** that the shared **priv_demo** estate exists.
# MAGIC 4. **Explain** why Data Classification is read from a deterministic snapshot rather than a live scan and **read** the detections it provides.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">Before you begin </strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   <strong>This notebook works from a prepared classification snapshot.</strong> It reads a deterministic snapshot of the detections rather than a live scan, so it never waits on one and reads the same every time. Section D explains why classification is scanned rather than queried live.
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

# MAGIC %md-sandbox
# MAGIC ## What the Workspace Setup Provisioned
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; display: flex; flex-direction: column; gap: 22px; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .intro { font-size: 15pt; line-height: 1.65; color: #0b2026; }
# MAGIC .ds-row { display: flex; gap: 20px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 290px; }
# MAGIC .blue{--accent:#4299E0;} .green{--accent:#00A972;} .coral{--accent:#FF5F46;} .amber{--accent:#FFAB00;} .red{--accent:#98102A;}
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 18px 20px; border-top: 8px solid var(--accent, #4299E0); display: flex; flex-direction: column; overflow-wrap: anywhere; }
# MAGIC .ds-card-title { font-size: 16pt; font-weight: 700; line-height: 1.25; margin: 0 0 4px; }
# MAGIC .ds-card-obj { font-family: monospace; font-size: 13px; color: #2272B4; margin-bottom: 8px; word-break: break-all; }
# MAGIC .ds-card-text { font-size: 14pt; color: #5E7077; line-height: 1.5; margin: 0; }
# MAGIC .ds-card-foot { margin-top: auto; padding-top: 14px; }
# MAGIC .ds-chip { display: inline-block; font-size: 13px; font-weight: 700; background: #EEEDE9; color: #1B5162; border-radius: 12px; padding: 4px 12px; }
# MAGIC .ds-diagram-title { font-size: 16pt; font-weight: 700; color: #0b2026; text-align: center; }
# MAGIC .ds-cap { font-size: 14px; color: #618794; text-align: center; margin-top: 10px; line-height: 1.45; }
# MAGIC .reasons { display: flex; flex-direction: column; gap: 12px; }
# MAGIC .reasons-title { font-size: 16pt; font-weight: 700; color: #0b2026; }
# MAGIC .reason { font-size: 14pt; line-height: 1.6; color: #0b2026; background: #F9F7F4; border-left: 6px solid var(--accent, #1B5162); border-radius: 6px; padding: 14px 18px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="intro">When this workspace was created, an admin ran a one-time workspace setup.
# MAGIC <br/>
# MAGIC It handles two account-level resources, a governed tag and a secret scope, and verifies a third, the account group, which is provisioned separately at the account level. 
# MAGIC <br/>
# MAGIC The data is separate as each notebook's own setup, rebuilds the estate and the snapshots it reads. The cards below explain all three.</div>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-card green">
# MAGIC     <div class="ds-card-title">Governed tag</div>
# MAGIC     <div class="ds-card-obj">sensitivity_level</div>
# MAGIC     <div class="ds-card-text">Values: public, internal, confidential and restricted. ASSIGN has been granted so you can apply it.</div>
# MAGIC     <div class="ds-card-foot"><span class="ds-chip">Demo 4 &middot; Lab 5</span></div>
# MAGIC   </div>
# MAGIC   <div class="ds-card amber">
# MAGIC     <div class="ds-card-title">Access groups</div>
# MAGIC     <div class="ds-card-obj">account users &middot; account privileged</div>
# MAGIC     <div class="ds-card-text">The built-in account users is the policy's TO set; account privileged is a custom account group provisioned at the account level. The workspace setup verifies it but does not create it.</div>
# MAGIC     <div class="ds-card-foot"><span class="ds-chip">Demo 4 &middot; Lab 5</span></div>
# MAGIC   </div>
# MAGIC   <div class="ds-card coral">
# MAGIC     <div class="ds-card-title">Secret scope</div>
# MAGIC     <div class="ds-card-obj">dbacademy_privacy</div>
# MAGIC     <div class="ds-card-text">A demo salt and AES key, read with <code>secret()</code> so keys never sit in a notebook.</div>
# MAGIC     <div class="ds-card-foot"><span class="ds-chip">Demo 6</span></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-diagram-title">How the two setups feed the demos</div>
# MAGIC <svg width="100%" viewBox="0 0 860 210" role="img" style="font-family: sans-serif;" aria-label="Two independent setups feed the demos. The workspace setup runs once, by an admin, and provides the governed tag, account groups and secret scope. Each notebook's own setup runs as you and rebuilds the estate and snapshots. The demos use both.">
# MAGIC   <defs><marker id="wp-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC   <rect x="12" y="24" width="300" height="80" rx="8" fill="#4299E0" fill-opacity="0.10" stroke="#4299E0" stroke-width="1.5"/>
# MAGIC   <text x="162" y="52" text-anchor="middle" font-size="16" font-weight="700" fill="#0b2026">Workspace setup</text>
# MAGIC   <text x="162" y="74" text-anchor="middle" font-size="13" fill="#5E7077">Once, by an admin.</text>
# MAGIC   <text x="162" y="93" text-anchor="middle" font-size="13" fill="#5E7077">Tag, secret scope. Verifies group.</text>
# MAGIC   <rect x="12" y="116" width="300" height="80" rx="8" fill="#00A972" fill-opacity="0.10" stroke="#00A972" stroke-width="1.5"/>
# MAGIC   <text x="162" y="144" text-anchor="middle" font-size="16" font-weight="700" fill="#0b2026">Your notebook setup</text>
# MAGIC   <text x="162" y="166" text-anchor="middle" font-size="13" fill="#5E7077">Section A, and every demo.</text>
# MAGIC   <text x="162" y="185" text-anchor="middle" font-size="13" fill="#5E7077">Estate and snapshots.</text>
# MAGIC   <path d="M312 64 L544 98" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#wp-arrow)"/>
# MAGIC   <path d="M312 156 L544 122" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#wp-arrow)"/>
# MAGIC   <rect x="548" y="70" width="300" height="80" rx="8" fill="#FF5F46" fill-opacity="0.10" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="698" y="104" text-anchor="middle" font-size="16" font-weight="700" fill="#0b2026">This notebook and the demos</text>
# MAGIC   <text x="698" y="126" text-anchor="middle" font-size="13" fill="#5E7077">Use both layers.</text>
# MAGIC </svg>
# MAGIC <div class="ds-cap">The workspace setup ran already when the workspace was created. Each individual notebook setup runs every time, so the estate and snapshots are always present and identical.</div>
# MAGIC
# MAGIC <div class="reasons">
# MAGIC   <div class="reasons-title">Why an admin does these three</div>
# MAGIC   <div class="reason blue">
# MAGIC     <strong>They need account-level rights.</strong> Creating a governed tag and a secret scope needs privileges a course notebook, running as you, doesn't have. The workspace setup provisions these two. The account privileged group is provisioned separately at the account level and the workspace setup only verifies it exists.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">The two setup layers, the names they create and why the groups come from outside</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Two setup layers.** Workspace Setup runs once for everyone and does the privileged work. Each notebook's `Classroom-Setup` runs as you and re-creates or reads your own copy, so you are never blocked if that step was skipped or you work in an isolated catalog.
# MAGIC - **The names are fixed.** Later demos expect these account-level names:
# MAGIC   - `sensitivity_level` governed tag with values public, internal, confidential, restricted
# MAGIC   - `account privileged` account group (the policy EXCEPT set; the TO set is the built-in `account users`)
# MAGIC   - `dbacademy_privacy` secret scope with keys `pii_salt` and `pii_aes_key`
# MAGIC - **The privileged group is not created by the workspace setup.** A workspace or job identity cannot create account groups. The workspace setup only verifies whether the principal is in `account privileged` and reports. `account users` (the TO set) is built-in, so every learner is in it and the mask applies to their own query with no provisioning. `account privileged` (the EXCEPT set, who see originals) is a custom account group, provisioned at the account level. The ABAC demo keeps a deterministic fallback if it is not in place.
# MAGIC - **The data is synthetic.** The estate values, the snapshot samples and the secret values are all synthetic, so nothing here is a real person or a real key.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the setup below. It resolves your per-user catalog, builds the shared **priv_demo** retailer estate (idempotent, so it's safe to re-run) and builds the deterministic classification snapshot the course reads.
# MAGIC
# MAGIC If your workspace can't create catalogs, open `./Includes/Classroom-Setup-01` and set `FORCED_CATALOG` to an existing catalog first.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Where This Notebook Fits
# MAGIC
# MAGIC The setup you just ran built the retailer estate the demos work on and staged the classification detections you read in Section D. The **Privacy, Security, Governance and Compliance** lecture comes next. It introduces the running scenario, the three people whose problems the course works through and the privacy decision model you reuse on every technique. The demos then work through those problems on the tables you explore here.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. What the Setup Created
# MAGIC <br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="text-align: center; margin-bottom: 16px;">
# MAGIC   <div style="font-size: 22pt; font-weight: 700; color: #0b2026;">The shared retailer estate in <span style="font-family:monospace;">priv_demo</span></div>
# MAGIC   <div style="font-size: 14pt; color: #618794; margin-top: 4px;">Raw PII tables refine into a silver table, then a non-identifying gold aggregate</div>
# MAGIC </div>
# MAGIC <svg width="100%" viewBox="0 0 980 380" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="est-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- layer header pills -->
# MAGIC   <rect x="30" y="12" width="230" height="32" rx="16" fill="#CD7F32"/><text x="145" y="34" text-anchor="middle" font-size="14" font-weight="700" fill="#FFFFFF" letter-spacing="2">RAW / BRONZE</text>
# MAGIC   <rect x="400" y="12" width="220" height="32" rx="16" fill="#5A6F77"/><text x="510" y="34" text-anchor="middle" font-size="14" font-weight="700" fill="#FFFFFF" letter-spacing="2">SILVER</text>
# MAGIC   <rect x="740" y="12" width="220" height="32" rx="16" fill="#FFAB00"/><text x="850" y="34" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026" letter-spacing="2">GOLD</text>
# MAGIC   <!-- arrows: customer_profile + loyalty -> customer_enriched -> customer_metrics_gold -->
# MAGIC   <path d="M260 92 L398 128" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#est-arrow)"/>
# MAGIC   <path d="M260 162 L398 150" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#est-arrow)"/>
# MAGIC   <path d="M620 140 L738 140" fill="none" stroke="#5E7077" stroke-width="1.8" marker-end="url(#est-arrow)"/>
# MAGIC   <!-- RAW nodes -->
# MAGIC   <rect x="30" y="66" width="230" height="52" rx="8" fill="#CD7F32" fill-opacity="0.10" stroke="#CD7F32" stroke-width="1.5"/>
# MAGIC   <text x="145" y="88" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">customer_profile</text>
# MAGIC   <text x="145" y="107" text-anchor="middle" font-size="12" fill="#5E7077">PII table</text>
# MAGIC   <rect x="30" y="136" width="230" height="52" rx="8" fill="#CD7F32" fill-opacity="0.10" stroke="#CD7F32" stroke-width="1.5"/>
# MAGIC   <text x="145" y="158" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">loyalty</text>
# MAGIC   <text x="145" y="177" text-anchor="middle" font-size="12" fill="#5E7077">points and tier</text>
# MAGIC   <rect x="30" y="206" width="230" height="52" rx="8" fill="#CD7F32" fill-opacity="0.10" stroke="#CD7F32" stroke-width="1.5"/>
# MAGIC   <text x="145" y="228" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">clickstream</text>
# MAGIC   <text x="145" y="247" text-anchor="middle" font-size="12" fill="#5E7077">events, IPs</text>
# MAGIC   <rect x="30" y="276" width="230" height="52" rx="8" fill="#CD7F32" fill-opacity="0.10" stroke="#CD7F32" stroke-width="1.5"/>
# MAGIC   <text x="145" y="298" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">support_records</text>
# MAGIC   <text x="145" y="317" text-anchor="middle" font-size="12" fill="#5E7077">free text with PII</text>
# MAGIC   <!-- SILVER node -->
# MAGIC   <rect x="400" y="100" width="220" height="80" rx="8" fill="#5A6F77" fill-opacity="0.10" stroke="#5A6F77" stroke-width="1.5"/>
# MAGIC   <text x="510" y="135" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">customer_enriched</text>
# MAGIC   <text x="510" y="156" text-anchor="middle" font-size="12" fill="#5E7077">profile joined to loyalty</text>
# MAGIC   <!-- GOLD node -->
# MAGIC   <rect x="740" y="100" width="220" height="80" rx="8" fill="#FFAB00" fill-opacity="0.14" stroke="#FFAB00" stroke-width="1.5"/>
# MAGIC   <text x="850" y="135" text-anchor="middle" font-size="15" font-weight="700" fill="#0b2026" font-family="monospace">customer_metrics_gold</text>
# MAGIC   <text x="850" y="156" text-anchor="middle" font-size="12" fill="#5E7077">aggregate, no identifiers</text>
# MAGIC </svg>
# MAGIC <div style="margin-top: 18px; padding: 16px 20px; background: #F9F7F4; border: 2px solid #1B5162; border-radius: 8px; font-size: 14pt; line-height: 1.55; text-align: left;">
# MAGIC   <strong>Lineage:</strong> <strong>customer_profile</strong> and <strong>loyalty</strong> feed <strong>customer_enriched</strong>, which rolls up into <strong>customer_metrics_gold</strong>. 
# MAGIC   <br /><br />
# MAGIC   This chain is what the erasure and evidence demos will trace later. 
# MAGIC   <br /><br />
# MAGIC   <strong>clickstream</strong> and <strong>support_records</strong> are additional raw PII sources you'll classify and protect.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">The tables and their sensitive columns</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **customer_profile:** the main PII table. Names, **email**, **phone**, street address, city, country, postal code, date of birth, **national_id** and IP address.
# MAGIC - **loyalty:** points balance, tier and enrollment date, one row per customer.
# MAGIC - **clickstream:** web events with IP addresses and user agents.
# MAGIC - **support_records:** tickets whose free-text body sometimes embeds an email or phone number, so classification and masking have a text target.
# MAGIC - **customer_enriched (silver):** a join of profile and loyalty. Built with a CTAS so column lineage is captured for the evidence demo.
# MAGIC - **customer_metrics_gold (gold):** counts and revenue by country and tier. It carries no identifiers, so it should stay correct even after an erasure.
# MAGIC - **All synthetic.** Values are generated to look like real PII (valid-looking emails, phones, per-country national ids) so Data Classification detects them.
# MAGIC - **Idempotent by skip-if-exists.** Re-running setup doesn't drop and rebuild, which would wipe the classification and governed tags and force a fresh scan.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. List the estate
# MAGIC
# MAGIC The setup left your session pointed at your catalog and the **priv_demo** schema. List the tables it built.
# MAGIC
# MAGIC > What you should see: the six tables named in the diagram above.

# COMMAND ----------

spark.sql(f"USE CATALOG `{my_catalog}`")
spark.sql(f"USE SCHEMA `{DEMO_SCHEMA}`")
print(f"Using {my_catalog}.{DEMO_SCHEMA}")
display(spark.sql(f"SHOW TABLES IN `{my_catalog}`.`{DEMO_SCHEMA}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Confirm the estate has data
# MAGIC
# MAGIC Count the rows in each table. Exact counts depend on the setup scale, so don't memorize them.
# MAGIC
# MAGIC > What you should see: non-zero rows in every table, with **customer_profile** around 50,000 at the default scale.

# COMMAND ----------

row_counts_sql = f"""
SELECT 'customer_profile' AS table_name, count(*) AS row_count FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile
UNION ALL SELECT 'loyalty', count(*) FROM `{my_catalog}`.`{DEMO_SCHEMA}`.loyalty
UNION ALL SELECT 'clickstream', count(*) FROM `{my_catalog}`.`{DEMO_SCHEMA}`.clickstream
UNION ALL SELECT 'support_records', count(*) FROM `{my_catalog}`.`{DEMO_SCHEMA}`.support_records
UNION ALL SELECT 'customer_enriched', count(*) FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_enriched
UNION ALL SELECT 'customer_metrics_gold', count(*) FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_metrics_gold
ORDER BY table_name
"""
display(spark.sql(row_counts_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Preview the unknown sensitive columns
# MAGIC
# MAGIC Look at **customer_profile**: several columns clearly hold personal data and it's your job across the course to find, protect and later erase it.
# MAGIC
# MAGIC > What you should see: synthetic rows with columns like `email`, `phone`, `national_id` and `ip_address`. This data is generated, not real.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. The joins that build the chain
# MAGIC
# MAGIC The diagram above is really two queries. **customer_enriched** joins **customer_profile** to **loyalty** on `customer_id`. **customer_metrics_gold** then rolls that up by country and tier. These are the joins the erasure and evidence demos trace. The setup already built the tables, so the cell below runs the same logic as read-only `SELECT`s.
# MAGIC
# MAGIC > What you should see: the profile identifiers sitting next to the loyalty columns, then a small per-country aggregate that carries no identifiers.

# COMMAND ----------

# These are read-only SELECTs that mirror how the setup joined the chain. They don't rebuild the tables.

# Silver: customer_enriched = customer_profile joined to loyalty on customer_id. 
# The profile brings the identifiers, loyalty brings points and tier and est_revenue is a synthetic per-customer measure the gold aggregate rolls up.
display(spark.sql(f"""
SELECT
  p.customer_id,
  p.email,
  p.national_id,
  p.country,
  l.points_balance,
  l.tier AS loyalty_program_tier,
  round(l.points_balance * 0.012 + pmod(hash(p.customer_id + 600), 500), 2) AS est_revenue
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_profile p
LEFT JOIN `{my_catalog}`.`{DEMO_SCHEMA}`.loyalty l USING (customer_id)
LIMIT 8
"""))

# COMMAND ----------

# Gold: customer_metrics_gold = customer_enriched aggregated by country and tier. The GROUP BY drops every identifier, so the aggregate stays valid after an erasure.
display(spark.sql(f"""
SELECT
  country,
  loyalty_tier,
  count(*)                   AS customer_count,
  round(sum(est_revenue), 2) AS total_est_revenue,
  round(avg(est_revenue), 2) AS avg_est_revenue
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.customer_enriched
GROUP BY country, loyalty_tier
ORDER BY customer_count DESC
LIMIT 8
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Why the Course Reads a Prepared Classification Snapshot
# MAGIC
# MAGIC Data Classification doesn't return results the instant a table is created. 
# MAGIC Enabling it on a catalog starts an initial scan that finishes in about 15 minutes for a small estate and the docs note new tables are then picked up within about 24 hours. 
# MAGIC It's triggered from the Catalog UI, the exact timing varies and the results table that holds real sample values is permission-restricted. 
# MAGIC So you work from a deterministic snapshot rather than the live table, which is reproducible and ready immediately.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 1040px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 980 250" role="img" style="font-family: sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="tl-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#5E7077" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- asynchronous scan window (the delay is the whole point) -->
# MAGIC   <rect x="150" y="96" width="470" height="48" rx="8" fill="#FFAB00" fill-opacity="0.14" stroke="#FFAB00" stroke-width="1.5" stroke-dasharray="6 4"/>
# MAGIC   <text x="385" y="125" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">asynchronous scan (about 15 min to 24 hours)</text>
# MAGIC   <!-- baseline -->
# MAGIC   <line x1="150" y1="170" x2="840" y2="170" stroke="#5E7077" stroke-width="1.8" marker-end="url(#tl-arrow)"/>
# MAGIC   <!-- stop 1: tables created -->
# MAGIC   <circle cx="150" cy="170" r="9" fill="#4299E0"/>
# MAGIC   <text x="150" y="200" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">Tables created</text>
# MAGIC   <text x="150" y="219" text-anchor="middle" font-size="12" fill="#5E7077">setup builds priv_demo</text>
# MAGIC   <!-- stop 2: detections land -->
# MAGIC   <circle cx="620" cy="170" r="9" fill="#FFAB00"/>
# MAGIC   <text x="620" y="200" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">Detections land</text>
# MAGIC   <text x="620" y="219" text-anchor="middle" font-size="12" fill="#5E7077">class.* tags applied</text>
# MAGIC   <!-- stop 3: timing -->
# MAGIC   <circle cx="710" cy="170" r="9" fill="#00A972"/>
# MAGIC   <text x="710" y="151" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">Live results queryable</text>
# MAGIC   <!-- top labels for stop 1 and window -->
# MAGIC   <text x="150" y="70" text-anchor="middle" font-size="13" fill="#4299E0" font-weight="700">day 0</text>
# MAGIC   <text x="620" y="70" text-anchor="middle" font-size="13" fill="#B27A00" font-weight="700">~15 min to 24h</text>
# MAGIC </svg>
# MAGIC <div style="margin-top: 8px; padding: 16px 20px; background: #F9F7F4; border: 2px solid #1B5162; border-radius: 8px; font-size: 14pt; line-height: 1.55; text-align: left;">
# MAGIC   A deterministic snapshot mirrors the results and is ready immediately and that snapshot is what the classify and evidence demos read.
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### D1. What Data Classification detects
# MAGIC
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-row { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 240px; }
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px; border-top: 8px solid var(--accent, #4299E0); }
# MAGIC .blue{--accent:#4299E0;} .green{--accent:#00A972;} .coral{--accent:#FF5F46;} .amber{--accent:#FFAB00;} .red{--accent:#98102A;}
# MAGIC .ds-card-title { font-size: 18pt; font-weight: 700; line-height: 1.25; margin: 0 0 8px; }
# MAGIC .ds-card-text { font-size: 14pt; color: #5E7077; line-height: 1.55; margin: 0; }
# MAGIC .ds-card ul, .ds-card ol { margin: 8px 0 0; padding-left: 20px; font-size: 14pt; line-height: 1.55; color: #0b2026; }
# MAGIC .ds-card li { margin-bottom: 10px; }
# MAGIC .ds-card li:last-child { margin-bottom: 0; }
# MAGIC .ds-callout { border-left: 6px solid var(--accent, #1B5162); border-radius: 8px; padding: 18px 24px; background: var(--tint, #F8F9FC); box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; }
# MAGIC .info { --accent:#1B5162; --tint:#F8F9FC; }
# MAGIC .cl-docs { background: #F8F9FC; border: 3px solid #FF5F46; border-radius: 10px; padding: 20px 24px; text-align: center; color: #0b2026; }
# MAGIC .cl-docs-title { font-size: 18pt; font-weight: 700; margin-bottom: 8px; }
# MAGIC .cl-docs-sub { font-size: 14pt; line-height: 1.5; margin-bottom: 16px; }
# MAGIC .cl-docs-links { display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }
# MAGIC .cl-doc-btn { display: inline-block; padding: 10px 24px; background: #FF5F46; color: #FFFFFF !important; text-decoration: none; border-radius: 6px; font-size: 14pt; font-weight: 600; min-width: 80px; }
# MAGIC .cl-doc-btn:hover { background: #FF6952; text-decoration: none; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="font-size: 15pt; line-height: 1.55; margin-bottom: 16px;">
# MAGIC   Data Classification scans columns automatically and applies <strong>system-governed <code style='font-size:15pt;'>class.*</code> tags</strong>. These tags are not user-created. The platform assigns them based on pattern-matching against known sensitive data types.
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-card blue">
# MAGIC     <div class="ds-card-title">Name and identity</div>
# MAGIC     <ul>
# MAGIC       <li><code>class.name</code></li>
# MAGIC       <li><code>class.date_of_birth</code></li>
# MAGIC       <li><code>class.age</code></li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div class="ds-card green">
# MAGIC     <div class="ds-card-title">Contact</div>
# MAGIC     <ul>
# MAGIC       <li><code>class.email_address</code></li>
# MAGIC       <li><code>class.phone_number</code></li>
# MAGIC       <li><code>class.location</code></li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div class="ds-card coral">
# MAGIC     <div class="ds-card-title">Financial</div>
# MAGIC     <ul>
# MAGIC       <li><code>class.credit_card</code></li>
# MAGIC       <li><code>class.us_ssn</code></li>
# MAGIC       <li><code>class.iban_code</code></li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div class="ds-card amber">
# MAGIC     <div class="ds-card-title">Network and device</div>
# MAGIC     <ul>
# MAGIC       <li><code>class.ip_address</code></li>
# MAGIC       <li><code>class.mac_address</code></li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 20px;">
# MAGIC <div class="ds-callout info">
# MAGIC   <strong>Regional identifiers.</strong> Some tags, such as the UK National Insurance Number or the German tax ID, only appear when the workspace is deployed in the corresponding region. US tags are the exception and are detected everywhere. The tag set depends on your workspace region.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 20px;">
# MAGIC <div class="cl-docs">
# MAGIC   <div class="cl-docs-title">Databricks Documentation</div>
# MAGIC   <div class="cl-docs-sub">Supported classification tags and compliance framework mappings.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-tags/" target="_blank">AWS</a>
# MAGIC     <a class="cl-doc-btn" href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification-tags/" target="_blank">Azure</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/gcp/en/data-governance/unity-catalog/data-classification-tags/" target="_blank">GCP</a>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">How system-governed tags work</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **System-governed** means you do not create these tags. Data Classification applies them automatically when it detects a pattern match in column data. They live in the `class.*` namespace in Unity Catalog.
# MAGIC - **Tag families.** The four families shown cover the most common detection categories. Within each, the tag name describes exactly what was detected: `class.email_address` means the scanner found email-shaped values, `class.us_ssn` means it found Social Security Number patterns.
# MAGIC - **One tag per detection.** Each row in the classification results maps one column to one `class.*` tag. A column can have multiple detections (for example, a free-text field that contains both emails and phone numbers) and each appears as a separate row.
# MAGIC - **Regional gating.** Tags such as `class.uk_nino` or `class.de_tax_id` are only applied when the workspace runs in the matching region. US tags are the exception and are detected in every region, so a US-region workspace will still not apply UK- or German-specific tags.
# MAGIC - **Why you read a snapshot, not the system table.** The live results table (`system.data_classification.results`) can hold up to five real sample values per detection, so direct access is restricted. `snap_classification_results` is built from it, dropping `samples` and scoping to `priv_demo`, so you read detections safely and immediately.
# MAGIC - **If you want live results.** An admin enables Data Classification (needs `MANAGE` on the catalog); enabling starts an initial scan automatically, which finishes in about 15 minutes for a small estate. The docs note new tables are then picked up within about 24 hours. This is optional; you read from the snapshot regardless.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### D2. How confidence and frequency describe a detection
# MAGIC
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-row { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 240px; }
# MAGIC .ds-card { background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px; border-top: 8px solid var(--accent, #4299E0); }
# MAGIC .blue{--accent:#4299E0;} .green{--accent:#00A972;} .coral{--accent:#FF5F46;} .amber{--accent:#FFAB00;} .red{--accent:#98102A;}
# MAGIC .ds-card-title { font-size: 18pt; font-weight: 700; line-height: 1.25; margin: 0 0 8px; }
# MAGIC .ds-card-text { font-size: 14pt; color: #5E7077; line-height: 1.55; margin: 0; }
# MAGIC .ds-card ul, .ds-card ol { margin: 8px 0 0; padding-left: 20px; font-size: 14pt; line-height: 1.55; color: #0b2026; }
# MAGIC .ds-card li { margin-bottom: 10px; }
# MAGIC .ds-card li:last-child { margin-bottom: 0; }
# MAGIC .ds-callout { border-left: 6px solid var(--accent, #1B5162); border-radius: 8px; padding: 18px 24px; background: var(--tint, #F8F9FC); box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; }
# MAGIC .caution { --accent:#FFAB00; --tint:rgba(255,171,0,0.12); }
# MAGIC .cl-docs { background: #F8F9FC; border: 3px solid #FF5F46; border-radius: 10px; padding: 20px 24px; text-align: center; color: #0b2026; }
# MAGIC .cl-docs-title { font-size: 18pt; font-weight: 700; margin-bottom: 8px; }
# MAGIC .cl-docs-sub { font-size: 14pt; line-height: 1.5; margin-bottom: 16px; }
# MAGIC .cl-docs-links { display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }
# MAGIC .cl-doc-btn { display: inline-block; padding: 10px 24px; background: #FF5F46; color: #FFFFFF !important; text-decoration: none; border-radius: 6px; font-size: 14pt; font-weight: 600; min-width: 80px; }
# MAGIC .cl-doc-btn:hover { background: #FF6952; text-decoration: none; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="font-size: 15pt; line-height: 1.55; margin-bottom: 16px;">
# MAGIC   Every detection in <code style='font-size:15pt;'>system.data_classification.results</code> carries two quality signals: a discrete <strong>confidence</strong> level and a numeric <strong>frequency</strong>.
# MAGIC </div>
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 8px auto 0;">
# MAGIC <svg width="100%" viewBox="0 0 900 340" role="img" style="font-family: sans-serif;">
# MAGIC   <defs><marker id="fq-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#618794" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC   <text x="508" y="26" text-anchor="middle" font-size="13" font-weight="700" fill="#618794" letter-spacing="1">FREQUENCY: how much of the column matched</text>
# MAGIC   <line x1="210" y1="40" x2="830" y2="40" stroke="#618794" stroke-width="1.3" marker-end="url(#fq-arrow)"/>
# MAGIC   <text x="335" y="62" text-anchor="middle" font-size="13" fill="#5E7077">some rows</text>
# MAGIC   <text x="681" y="62" text-anchor="middle" font-size="13" fill="#5E7077">most or all rows</text>
# MAGIC   <text x="26" y="200" text-anchor="middle" font-size="13" font-weight="700" fill="#618794" letter-spacing="1" transform="rotate(-90 26 200)">CONFIDENCE</text>
# MAGIC   <text x="150" y="128" text-anchor="end" font-size="16" font-weight="700" fill="#007a53">HIGH</text>
# MAGIC   <text x="150" y="148" text-anchor="end" font-size="12.5" fill="#5E7077">trust it</text>
# MAGIC   <text x="150" y="250" text-anchor="end" font-size="16" font-weight="700" fill="#B27A00">LOW</text>
# MAGIC   <text x="150" y="270" text-anchor="end" font-size="12.5" fill="#5E7077">review it first</text>
# MAGIC   <rect x="170" y="78" width="330" height="110" rx="8" fill="#00A972" fill-opacity="0.07" stroke="#00A972" stroke-opacity="0.45" stroke-width="1.2"/>
# MAGIC   <text x="188" y="110" font-size="13.5" fill="#0b2026">Real, but only in part of</text>
# MAGIC   <text x="188" y="130" font-size="13.5" fill="#0b2026">the column (often free text).</text>
# MAGIC   <text x="188" y="160" font-size="12.5" font-family="monospace" fill="#2272B4">support_records.body_text</text>
# MAGIC   <text x="188" y="178" font-size="12.5" font-weight="700" fill="#007a53">HIGH &#183; 0.50</text>
# MAGIC   <rect x="516" y="78" width="330" height="110" rx="8" fill="#00A972" fill-opacity="0.07" stroke="#00A972" stroke-opacity="0.45" stroke-width="1.2"/>
# MAGIC   <text x="534" y="110" font-size="13.5" fill="#0b2026">Real and in nearly every</text>
# MAGIC   <text x="534" y="130" font-size="13.5" fill="#0b2026">row.</text>
# MAGIC   <text x="534" y="160" font-size="12.5" font-family="monospace" fill="#2272B4">customer_profile.email</text>
# MAGIC   <text x="534" y="178" font-size="12.5" font-weight="700" fill="#007a53">HIGH &#183; 1.0</text>
# MAGIC   <rect x="170" y="200" width="330" height="110" rx="8" fill="#FFAB00" fill-opacity="0.10" stroke="#B27A00" stroke-opacity="0.45" stroke-width="1.2"/>
# MAGIC   <text x="188" y="232" font-size="13.5" fill="#0b2026">Scattered, weak match.</text>
# MAGIC   <text x="188" y="252" font-size="13.5" fill="#0b2026">Usually a false positive.</text>
# MAGIC   <text x="188" y="282" font-size="12.5" font-family="monospace" fill="#0b2026">created_at</text>
# MAGIC   <text x="188" y="300" font-size="12.5" font-weight="700" fill="#98102A">LOW &#183; low frequency</text>
# MAGIC   <rect x="516" y="200" width="330" height="110" rx="8" fill="#FFAB00" fill-opacity="0.10" stroke="#B27A00" stroke-opacity="0.45" stroke-width="1.2"/>
# MAGIC   <text x="534" y="232" font-size="13.5" fill="#0b2026">Matched widely, but the</text>
# MAGIC   <text x="534" y="252" font-size="13.5" fill="#0b2026">model is unsure. Confirm first.</text>
# MAGIC   <text x="534" y="282" font-size="12.5" fill="#5E7077">no course example here</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC <div style="font-size: 14pt; color: #5E7077; line-height: 1.5; margin-top: 12px; text-align: center;">Confidence (the row) decides whether to trust the detection. Frequency (the column) says how much of the column has it.</div>
# MAGIC
# MAGIC <div style="margin-top: 20px;">
# MAGIC <div class="ds-callout caution">
# MAGIC   <strong>Course data example.</strong> In the snapshot, <code>customer_profile.email</code> shows <strong>HIGH confidence, frequency 1.0</strong>, because every row has a valid email. The <code>created_at</code> column shows <strong>LOW confidence</strong>, a false positive worth cross-referencing. Demo 3 walks through exactly this kind of triage. The next cell shows both detections in the snapshot.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 20px;">
# MAGIC <div class="cl-docs">
# MAGIC   <div class="cl-docs-title">Databricks Documentation</div>
# MAGIC   <div class="cl-docs-sub">System table reference for <code>system.data_classification.results</code>.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/aws/en/admin/system-tables/data-classification/" target="_blank">AWS</a>
# MAGIC     <a class="cl-doc-btn" href="https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/data-classification/" target="_blank">Azure</a>
# MAGIC     <a class="cl-doc-btn" href="https://docs.databricks.com/gcp/en/admin/system-tables/data-classification/" target="_blank">GCP</a>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Reading confidence and frequency together</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **HIGH + frequency near 1.0** is the strongest signal: the column almost certainly holds this data type and nearly every row has it. Example: `customer_profile.email` tagged `class.email_address`.
# MAGIC - **HIGH + partial frequency** means the column holds this data type, but only some rows contain it. Example: `support_records.body_text` tagged `class.email_address`, where about half the tickets include an email address inline.
# MAGIC - **LOW + any frequency** is a candidate detection that needs human judgment. The scanner saw something that might be sensitive, but the pattern is ambiguous. Example: a `created_at` timestamp column tentatively tagged because some date formats overlap with date-of-birth patterns.
# MAGIC - **The triage workflow.** Demo 3 teaches you to filter by confidence and frequency to separate real detections from noise, then decide which tags warrant a protection control.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# The two detections the example above describes, read from the deterministic snapshot the setup built.
# email is the clean detection to trust. 
# created_at is the LOW-confidence false positive to review, a timestamp column the scanner tentatively tagged as a birth date. 
# The sample values are synthetic.
display(spark.sql(f"""
SELECT table_name, column_name, data_type, class_tag, confidence, frequency, samples
FROM `{my_catalog}`.`{DEMO_SCHEMA}`.snap_classification_results
WHERE table_name = 'customer_profile' AND column_name IN ('email', 'created_at')
ORDER BY frequency DESC
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### D3. Custom classifiers and the through-line to protection
# MAGIC
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .cw p { font-size: 14pt; line-height: 1.6; }
# MAGIC .ds-callout { border-left: 6px solid #1B5162; border-radius: 8px; padding: 18px 24px; background: #F8F9FC; box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 14pt; line-height: 1.55; color: #0b2026; margin-top: 16px; }
# MAGIC .ds-callout-title { font-size: 16pt; font-weight: 700; color: #0b2026; margin-bottom: 10px; line-height: 1.3; }
# MAGIC .ds-eyebrow { font-size: 14pt; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: #1B5162; margin-bottom: 6px; }
# MAGIC </style>
# MAGIC
# MAGIC <p>Some sensitive identifiers are specific to your organization, like an internal employee ID or an account code that no built-in <code>class.*</code> tag covers. A <strong>custom classifier</strong> (Beta) can detect those, previewed in Demo 3.</p>
# MAGIC
# MAGIC <div class="ds-callout">
# MAGIC   <div class="ds-callout-title">A tag is only useful if it drives a control.</div>
# MAGIC   Demo 3 classifies with the control in mind and Demo 4 turns those tags into tag-driven ABAC policies.
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Read the classification detections
# MAGIC
# MAGIC Read the classification detections for your estate from the deterministic snapshot the setup built.
# MAGIC
# MAGIC > What you should see: one row per detected sensitive column (for example `customer_profile.email` tagged `class.email_address`). The snapshot is always present, so this returns rows immediately.

# COMMAND ----------

# Read from the deterministic snapshot the setup built. It's always present and needs no live scan.
snap_name = f"`{my_catalog}`.`{DEMO_SCHEMA}`.snap_classification_results"
detections = spark.sql(
    f"SELECT catalog_name, schema_name, table_name, column_name, data_type, class_tag, "
    f"confidence, frequency, first_detected_time, latest_detected_time "
    f"FROM {snap_name} ORDER BY table_name, column_name"
)
print(f"Classification snapshot present: {detections.count()} detection(s) for {my_catalog}.{DEMO_SCHEMA}.")
display(detections)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Try it: ask Genie Code</div>
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">Want a quick read on what looks sensitive before the classification demo? Ask <strong>Genie Code</strong> in the notebook or SQL editor. You learn the fundamentals first so you can judge whether its answer is correct. Click <strong>Copy</strong> and paste the prompt into Genie Code.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">Which columns in the priv_demo.customer_profile table look like personal data and why?</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, you:
# MAGIC
# MAGIC 1. Saw the **two-layer setup**: what Workspace-Setup provisions once (the governed tag, account groups and secret scope) versus what each notebook's own setup rebuilds.
# MAGIC 2. Confirmed the course runs on **Serverless V6** compute and a **2X-Small serverless SQL warehouse**.
# MAGIC 3. Ran the **setup** and verified the shared **priv_demo** estate exists with data. Labs use an isolated **priv_lab** estate.
# MAGIC 4. Learned why Data Classification is read from a **prepared, deterministic snapshot** (the live scan is UI-triggered and its real sample values are permission-restricted) and read the detections it provides.
# MAGIC 5. Previewed the three pillars of a detection: **system-governed `class.*` tags**, **confidence and frequency** and **custom classifiers (Beta)**.
# MAGIC
# MAGIC This course treats privacy as decisions and controls you implement and verify on Databricks, not a compliance certificate.
# MAGIC
# MAGIC Next, the **Privacy, Security, Governance and Compliance** lecture introduces the identify, protect, manage and verify lifecycle, the running scenario and the privacy decision model that every demo and lab reuses.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### The Trust and Compliance Center
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .cl-docs { background: #F8F9FC; border: 3px solid #FF5F46; border-radius: 10px; padding: 22px 26px; text-align: center; color: #0b2026; }
# MAGIC .cl-docs-title { font-size: 18pt; font-weight: 700; margin-bottom: 8px; }
# MAGIC .cl-docs-sub { font-size: 14pt; line-height: 1.55; margin-bottom: 16px; }
# MAGIC .cl-docs-links { display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }
# MAGIC .cl-doc-btn { display: inline-block; padding: 10px 24px; background: #FF5F46; color: #FFFFFF !important; text-decoration: none; border-radius: 6px; font-size: 14pt; font-weight: 600; min-width: 80px; }
# MAGIC .cl-doc-btn:hover { background: #FF6952; text-decoration: none; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="cl-docs">
# MAGIC   <div class="cl-docs-title">Databricks Trust and Compliance Center</div>
# MAGIC   <div class="cl-docs-sub">The <strong>Privacy, Security, Governance and Compliance</strong> lecture covers the compliance picture: the per-industry standards and the difference between certifications Databricks holds and regulations it helps you meet. Those attestations and their scope change over time, so the live center is the source of truth rather than a static list printed in a notebook. Bookmark it now and take formal determinations to your DPO or legal team.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://www.databricks.com/trust" target="_blank">Security and Trust Center &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://www.databricks.com/trust/compliance" target="_blank">Compliance &rarr;</a>
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