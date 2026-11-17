# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo - Multi Flow SDP with Liquid Clustering and Data Quality
# MAGIC
# MAGIC #### Duration: ~35-40 Minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC This demonstration showcases how to build a robust incremental data pipeline using Apache Spark™ Declarative Pipelines (SDP) to consolidate data from multiple subsidiaries (data sources) into a single target streaming table. 
# MAGIC
# MAGIC You'll work with three fictional company subsidiaries: Bright Home, Lumina Sports, and Northstar Outfitters, each producing transaction data in different formats (`CSV` and `JSON`). The demo illustrates how to overcome common pipeline challenges including multiple flows into a single table, schema mismatches, data quality issues, and performance optimization requirements.
# MAGIC
# MAGIC Through hands-on implementation, you'll create a complete medallion architecture pipeline that incrementally ingests multiple data sources into a single bronze table using flows, applies data quality constraints and transformations in the silver layer with liquid clustering optimization, and creates business intelligence materialized views in the gold layer. 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demonstration, you will be able to:
# MAGIC - **Ingest multiple data sources into one bronze table** using Spark Declarative Pipelines having different file formats like CSV and JSON.
# MAGIC
# MAGIC - **Standardize schemas in the bronze layer** and map correct data types in silver layer to resolve differences across source systems.
# MAGIC
# MAGIC - **Add data quality checks and enable liquid clustering** in the silver tables to enforce basic rules and improve query performance.
# MAGIC
# MAGIC - **Build incremental materialized views** in the gold layer that refresh automatically and provide ready-to-use analytics.
# MAGIC
# MAGIC - **Run and monitor the full pipeline run** across bronze, silver, and gold, including incremental loads and data lineage tracking.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Multi Flow Pipeline Demonstration Overview
# MAGIC In this demonstration, you'll build a Apache Spark™ Declarative Pipeline that performs the full medallion flow from raw ingestion of multiple raw data sources to curated analytics
# MAGIC
# MAGIC 1. **Use multiple flows (3)** to incrementally ingest files from three cloud storage locations and write into a single bronze table. 
# MAGIC     - Each volume is a daily orders drop for a specific subsidiary that the company owns. We want all this data ingested into a single table for overall analysis.
# MAGIC 2. **Define and build the silver table** with a clean schema, apply basic data quality constraints, and **enable liquid clustering** for query performance as the data continues to grow.
# MAGIC 3. **Create gold materialized views** that automatically refresh and provide ready-to-use analytics.
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 520" width="100%" role="img" aria-label="Multi-flow pipeline: three volumes feed one bronze streaming table, then a liquid-clustered silver table, then two gold materialized views" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif;max-width:1200px;display:block;margin:12px auto;"><defs><marker id="ov-ah-g" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#7C8B94"/></marker><marker id="ov-ah-n" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B3139"/></marker><marker id="ov-ah-t" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B5062"/></marker><filter id="ov-sh" x="-8%" y="-8%" width="116%" height="130%"><feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#0B2026" flood-opacity="0.13"/></filter></defs><text x="14" y="30" font-size="20" font-weight="400" fill="#1B3139" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Cloud Storage</text><rect x="10" y="42" width="176" height="392" rx="18" fill="#fff" stroke="#9AA5AB" stroke-width="1.6" /><rect x="22" y="58" width="152" height="108" rx="8" fill="#fff" stroke="#8C98A0" stroke-width="1.5" stroke-dasharray="5 4" /><text x="98" y="82" font-size="13" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Bright Home Orders</text><text x="98" y="100" font-size="13" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Volume</text><g transform="translate(36,110) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><g transform="translate(80,110) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><g transform="translate(124,110) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><rect x="22" y="180" width="152" height="108" rx="8" fill="#fff" stroke="#8C98A0" stroke-width="1.5" stroke-dasharray="5 4" /><text x="98" y="204" font-size="13" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Lumina Sports</text><text x="98" y="222" font-size="13" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Orders Volume</text><g transform="translate(36,232) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><g transform="translate(80,232) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><g transform="translate(124,232) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><rect x="22" y="302" width="152" height="108" rx="8" fill="#fff" stroke="#8C98A0" stroke-width="1.5" stroke-dasharray="5 4" /><text x="98" y="326" font-size="13" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Northstar Outfitters</text><text x="98" y="344" font-size="13" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Orders Volume</text><g transform="translate(36,354) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >JSON</text></g><g transform="translate(80,354) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >JSON</text></g><g transform="translate(124,354) scale(0.95)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >JSON</text></g><text x="98" y="458" font-size="15" font-weight="400" fill="#1B3139" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Files are landed into</text><text x="98" y="478" font-size="15" font-weight="700" fill="#1B3139" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >each volume daily</text><rect x="230" y="200" width="250" height="190" rx="2" fill="none" stroke="#FFAB00" stroke-width="1.6" stroke-dasharray="6 5" /><svg x="238" y="206" width="44" height="40.1" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="286" y="232" font-size="15" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="242" y="248" width="226" height="48" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#ov-sh)"/><text x="355.0" y="277.0" font-size="14" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >orders_bronze_flows_demo</text><rect x="238" y="308" width="234" height="76" rx="0" fill="#F6F4F1" stroke="none" stroke-width="1" /><text x="355" y="338" font-size="16" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" fill="#1B3139">Using <tspan font-weight="700">multiple flows</tspan> to</text><text x="355" y="362" font-size="16" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" fill="#1B3139">write to a <tspan font-weight="700">single target</tspan></text><rect x="520" y="200" width="250" height="190" rx="2" fill="none" stroke="#8C98A0" stroke-width="1.6" stroke-dasharray="6 5" /><svg x="528" y="206" width="44" height="40.1" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="576" y="232" font-size="15" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="532" y="248" width="226" height="48" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#ov-sh)"/><text x="645.0" y="277.0" font-size="14" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >orders_silver_flows_demo</text><rect x="528" y="308" width="234" height="76" rx="0" fill="#F6F4F1" stroke="none" stroke-width="1" /><text x="645" y="352" font-size="17" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" fill="#1B3139">Enable <tspan font-weight="700">liquid clustering</tspan></text><path d="M174,112 C196,112 190,272 208,272" fill="none" stroke="#1B5062" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/><path d="M174,234 C196,234 190,272 208,272" fill="none" stroke="#1B5062" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/><path d="M174,356 C196,356 190,272 208,272" fill="none" stroke="#1B5062" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/><path d="M208,272 H229" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#ov-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><path d="M482,272 H518" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#ov-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><rect x="830" y="70" width="360" height="330" rx="2" fill="none" stroke="#FFAB00" stroke-width="1.8" stroke-dasharray="6 5" /><svg x="840" y="88" width="44" height="44" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><rect x="32.015" y="26.436" width="10.5" height="3.5" transform="rotate(131 37.265 28.186)"/><rect x="32.5" y="31" width="15" height="3"/><rect x="37.485" y="35.064" width="10.5" height="3.5" transform="rotate(131 42.735 36.814)"/></svg><text x="890" y="114" font-size="15" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Materialized View</text><rect x="842" y="130" width="336" height="54" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#ov-sh)"/><text x="1010" y="162" font-size="13.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >mv_daily_subsidiary_scorecard_demo</text><path d="M796,272 C808,272 808,157 820,157 H841" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#ov-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="840" y="246" width="44" height="44" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><rect x="32.015" y="26.436" width="10.5" height="3.5" transform="rotate(131 37.265 28.186)"/><rect x="32.5" y="31" width="15" height="3"/><rect x="37.485" y="35.064" width="10.5" height="3.5" transform="rotate(131 42.735 36.814)"/></svg><text x="890" y="272" font-size="15" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Materialized View</text><rect x="842" y="288" width="336" height="54" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#ov-sh)"/><text x="1010" y="320" font-size="13.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >mv_product_performance_by_subsidiary_demo</text><path d="M796,272 C808,272 808,315 820,315 H841" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#ov-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><path d="M770,272 H796" fill="none" stroke="#1B5062" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/><path d="M230,420 H742 V400 L826,444 L742,488 V468 H230 z" fill="#1B5062"/><text x="486" y="451" font-size="16.5" text-anchor="middle" fill="#fff" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif"><tspan font-weight="700">Incrementally</tspan> process <tspan font-weight="700">new files</tspan> in the streaming tables (ST)</text><rect x="830" y="420" width="360" height="66" rx="0" fill="#1B5062" stroke="none" stroke-width="1" /><text x="844" y="446" font-size="15.5" fill="#fff" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">Create <tspan font-weight="700">gold level materialized views</tspan></text><text x="844" y="468" font-size="15.5" font-weight="400" fill="#fff" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >for consumable metrics and analysis</text></svg>

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
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC Before starting this notebook, select the required compute environment listed below.
# MAGIC
# MAGIC - **Serverless Compute, Version 6**  
# MAGIC   - [How to select an environment version](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version)
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > **Workspace setup:** Using a Databricks Academy (Vocareum) workspace? Your catalog is created for you, so you can start. Using your own workspace or Databricks Free Edition? Complete **Course Overview → B. Workspace Setup** first (catalog creation and permissions, Marketplace data). Run this notebook only in a development or sandbox workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Access Marketplace Data
# MAGIC
# MAGIC The demo reads its source files from the Databricks Marketplace share **Simulated Retail Customer Data**.
# MAGIC
# MAGIC - **Vocareum workspace:** the share is already installed as `dbacademy_retail`. Use that as the value of `your_marketplace_share_catalog_name` below.
# MAGIC - **Your own workspace or Free Edition:** get your copy of the share by following **Course Overview → B2. Access Marketplace Data**, then set the variable below to your catalog name.

# COMMAND ----------

## Update the variable below to reference your marketplace catalog name

## NOTE: If you are using Vocareum, use the value 'dbacademy_retail' catalog below
your_marketplace_share_catalog_name = 'dbacademy_retail'

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Configure Your Catalog and Schema
# MAGIC
# MAGIC 1. Run the cell below to initialize your environment. 
# MAGIC
# MAGIC     This setup step does the following:
# MAGIC
# MAGIC     - **Assumes you have permission to create a catalog** when running outside of a Databricks provided Vocareum workspace
# MAGIC     - Create three schemas in your specified catalog:  
# MAGIC         - **multi_flow_1_bronze**
# MAGIC         - **multi_flow_2_silver**
# MAGIC         - **multi_flow_3_gold**  
# MAGIC     - Creates a three volumes in your **YOUR_LABUSER_CATALOG.multi_flow_1_bronze** schema and adds a single JSON file in each volume.
# MAGIC     - Checks your specified Serverless compute version
# MAGIC
# MAGIC This ensures that all schemas, tables and objects are created in your catalog.
# MAGIC
# MAGIC > **Important:** You must have permission to create catalogs in your own non Vocareum workspace. If you do not have the required permissions, this step will fail. If you cannot create catalogs, see **Course Overview → B2. Troubleshooting: Missing Create Catalog Permissions** before continuing.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-multiple-flows

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to view the value of the `my_vol_path` variable.
# MAGIC
# MAGIC    Confirm that the value references your **your-catalog.multi_flow_1_bronze** path. This will be used to dynamically reference your source volumes throughout this demonstration.
# MAGIC

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Source Volumes for Ingesting Multiple Flows into a Single Target
# MAGIC
# MAGIC Before building multiple flows that write to a single target streaming table, start by exploring the raw source data stored in the three volumes.  
# MAGIC
# MAGIC Each **volume** represents a **separate sales system** for a different subsidiary within our fictional company:
# MAGIC
# MAGIC - Bright Home Orders volume (`CSV` files)  
# MAGIC - Lumina Sports Orders volume (`CSV` files)  
# MAGIC - Northstar Outfitters Orders volume (`JSON` files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Explore the Source Volumes
# MAGIC
# MAGIC 1. View the files in the three source volumes (**multi_flow_1_bronze.bright_home_orders**, **lumina_sports_orders** and **northstar_outfitters_orders**).
# MAGIC
# MAGIC    Notice that each volume currently contains only one file, for the sales on **2025-11-01**: `CSV` for Bright Home and Lumina Sports, `JSON` for Northstar Outfitters.

# COMMAND ----------

for volume in ['bright_home_orders', 'lumina_sports_orders', 'northstar_outfitters_orders']:
    print(f'--- {volume}')
    spark.sql(f"LIST '{my_vol_path}/{volume}'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Count the rows in each file. Notice that **157**, **110** and **182** rows are present. This is simple sales order data from each subsidiary.

# COMMAND ----------

spark.sql(f"""
    SELECT 'bright_home_orders'          AS Volume, 'CSV'  AS FileFormat, count(*) AS TotalRows FROM read_files('{my_vol_path}/bright_home_orders')
    UNION ALL
    SELECT 'lumina_sports_orders'        AS Volume, 'CSV'  AS FileFormat, count(*) AS TotalRows FROM read_files('{my_vol_path}/lumina_sports_orders')
    UNION ALL
    SELECT 'northstar_outfitters_orders' AS Volume, 'JSON' AS FileFormat, count(*) AS TotalRows FROM read_files('{my_vol_path}/northstar_outfitters_orders')
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Compare the schema that is inferred for a `CSV` source (Bright Home Orders) with the `JSON` source (Northstar Outfitters), and preview the data. Lumina Sports Orders is also `CSV` and has the same structure as Bright Home Orders.
# MAGIC
# MAGIC    Notice that the inferred data types differ between the two formats (for example **order_timestamp**, **qty**, **discount_pct** and **order_date**).

# COMMAND ----------

for volume in ['bright_home_orders', 'northstar_outfitters_orders']:
    print(f'--- {volume}: inferred schema')
    spark.sql(f"DESCRIBE SELECT * FROM read_files('{my_vol_path}/{volume}')").display()
    print(f'--- {volume}: preview')
    spark.sql(f"SELECT * FROM read_files('{my_vol_path}/{volume}') LIMIT 5").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Raw Data Exploration Summary
# MAGIC
# MAGIC Below is a quick overview of what we discovered after examining the raw source volumes. 
# MAGIC
# MAGIC #### Schema Differences Comparison and Issues
# MAGIC
# MAGIC - Each raw data source uses its own structure (`CSV` or `JSON`).  
# MAGIC - When ingesting `CSV`, the inferred column types often differ from `JSON`.  
# MAGIC - Since each format infers schemas independently, combining these three flows into a single target table will introduce **schema mismatches that lead to ingestion conflicts**.
# MAGIC
# MAGIC | **Column Name**     | **bright_home_orders (CSV)** | **lumina_sports_orders (CSV)** | **northstar_outfitters_orders (JSON)** |
# MAGIC |---------------------|------------------------|---------------------------|---------------------------------|
# MAGIC | subsidiary_id       | string                 | string                    | string                          |
# MAGIC | order_id            | string                 | string                    | string                          |
# MAGIC | **order_timestamp** | **timestamp**          | **timestamp**             | **string**                      |
# MAGIC | customer_id         | string                 | string                    | string                          |
# MAGIC | region              | string                 | string                    | string                          |
# MAGIC | country             | string                 | string                    | string                          |
# MAGIC | city                | string                 | string                    | string                          |
# MAGIC | channel             | string                 | string                    | string                          |
# MAGIC | sku                 | string                 | string                    | string                          |
# MAGIC | category            | string                 | string                    | string                          |
# MAGIC | **qty**             | **int**                | **int**                   | **bigint**                      |
# MAGIC | unit_price          | double                 | double                    | double                          |
# MAGIC | **discount_pct**    | **int**                | **int**                   | **bigint**                      |
# MAGIC | coupon_code         | string                 | string                    | string                          |
# MAGIC | total_amount        | double                 | double                    | double                          |
# MAGIC | **order_date**      | **date**               | **date**                  | **string**                      |
# MAGIC | _rescued_data       | string                 | string                    | string                          |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC #### Goal: Ingest All Raw Source Files Into One Bronze Streaming Table
# MAGIC
# MAGIC To successfully combine all three sources into a single bronze streaming table, we will need to standardize the schema. 
# MAGIC
# MAGIC To do this, we will **ingest every column as a `STRING`** into the bronze table.
# MAGIC
# MAGIC This avoids data type conflicts between `CSV` and `JSON` files, since each format infers types differently. Normalizing everything to `STRING` keeps the bronze layer predictable, prevents ingestion failures, and lets us apply the correct data types later in the silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Create the Spark Declarative Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Create a Apache Spark™ Declarative Pipeline using the Lakeflow Pipelines Editor
# MAGIC Complete the following steps to create your Spark Declarative Pipeline:
# MAGIC
# MAGIC 1. In the main navigation pane, right-click **Jobs & Pipelines** and select **Open link in New Tab**.  
# MAGIC
# MAGIC 2. In the new tab, select **Create → ETL Pipeline**.  
# MAGIC
# MAGIC 3. At the top, complete the following:
# MAGIC    - Name your pipeline `demo_multi_flow_yourname`
# MAGIC    - Select your default **catalog** and **schema**:  
# MAGIC         - **Catalog:** The catalog you specified for this notebook  
# MAGIC         - **Schema:** **multi_flow_1_bronze**  
# MAGIC       **NOTE:** Clear the selected schema using the cross icon to view all schemas.
# MAGIC
# MAGIC 4. Rename the **transformations** folder to `ingest_multiple_flows`.
# MAGIC
# MAGIC 5. Rename the **my_transformation.py** file to `flow_ingestion.sql`.
# MAGIC
# MAGIC 6. Leave the **Lakeflow Pipelines Editor** page open.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ![Create SDP Checkpoint](https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multi_flow/pipeline_creation.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Using Multiple Flows to Write to a Single Target
# MAGIC
# MAGIC In many enterprise environments, data arrives from several systems that must be consolidated into a single table for downstream processing.
# MAGIC
# MAGIC In this example, the company has **three subsidiaries, each producing transaction data in slightly different raw file formats**. 
# MAGIC
# MAGIC With Spark Declarative Pipelines, you can define multiple flows that write to the same target streaming table, allowing all raw files in cloud storage to be **incrementally ingested into one unified destination**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Create the Bronze Target Table

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Start by creating the **bronze streaming table** that will serve as the landing zone for all incoming transaction data. 
# MAGIC
# MAGIC     This table ingests every column as `STRING` to ensure compatibility across the different source systems.
# MAGIC
# MAGIC
# MAGIC     Copy the SQL code below and paste it into your `flow_ingestion.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------
# MAGIC -- CREATE THE BRONZE TABLE STRUCTURE
# MAGIC ------------------------------------------
# MAGIC CREATE OR REPLACE STREAMING TABLE multi_flow_1_bronze.orders_bronze_flows_demo
# MAGIC (
# MAGIC   subsidiary_id   STRING,
# MAGIC   order_id        STRING,
# MAGIC   order_timestamp STRING,
# MAGIC   customer_id     STRING,
# MAGIC   region          STRING,
# MAGIC   country         STRING,
# MAGIC   city            STRING,
# MAGIC   channel         STRING,
# MAGIC   sku             STRING,
# MAGIC   category        STRING,
# MAGIC   qty             STRING,
# MAGIC   unit_price      STRING,
# MAGIC   discount_pct    STRING,
# MAGIC   coupon_code     STRING,
# MAGIC   total_amount    STRING,
# MAGIC   order_date      STRING,
# MAGIC   source_file     STRING,   -- Added by the _metadata column to return the source file name
# MAGIC   file_mod_time   TIMESTAMP -- Added by the _metadata column to return file modification time of the file. Returns a consistent value
# MAGIC )
# MAGIC COMMENT "Creates a single bronze streaming table with orders from all subsidiaries using multiple flows."
# MAGIC TBLPROPERTIES (
# MAGIC   'pipelines.reset.allowed' = false    -- prevent full table refreshes on the bronze table
# MAGIC );
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the code**
# MAGIC - The `COMMENT` clause adds descriptive metadata to the table for documentation purposes.
# MAGIC
# MAGIC - The `TBLPROPERTIES` statement configures the following setting:
# MAGIC   - **Reset Protection**: The `'pipelines.reset.allowed' = false` property prevents full refreshes on the streaming table, which helps avoid accidentally removing checkpoints and truncating the streaming table data.
# MAGIC
# MAGIC #### IMPORTANT: Understanding Full Table Refresh Protection
# MAGIC
# MAGIC This protection is particularly important when your raw data source automatically removes files after a certain timeframe. Without this setting, data that is no longer present in the source directory would not be reingested into the target table during a **Run pipeline with full table refresh** operation.
# MAGIC
# MAGIC **NOTE:** For guidance on when to use full refreshes, see the [Should I use a full refresh?](https://docs.databricks.com/aws/en/ldp/updates#should-i-use-a-full-refresh) documentation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Configure the Pipeline Parameters
# MAGIC
# MAGIC 1. Run the cell below to retrieve the key value pairs needed to set your pipeline configuration parameters for each **raw data source volume**.

# COMMAND ----------

config_parameters = [
    ('bright_home_orders_source',        f'{my_vol_path}/bright_home_orders'),
    ('lumina_sports_orders_source',      f'{my_vol_path}/lumina_sports_orders'),
    ('northstar_outfitters_orders_source', f'{my_vol_path}/northstar_outfitters_orders')
]

for key, value in config_parameters:
    print(f"Key: {key}\nValue: {value}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Copy the paths above and add each one by one as a configuration parameter in your **Spark Declarative Pipeline**.
# MAGIC
# MAGIC    This will allow your pipeline to reference each volume through parameters.
# MAGIC
# MAGIC    a. Select **Settings** in your pipeline tab.  
# MAGIC
# MAGIC    b. Under **Configuration**, select **Add configuration**. 
# MAGIC
# MAGIC    c. For each **Key**, enter the key name shown above.  
# MAGIC
# MAGIC    d. For each **Value**, enter the corresponding volume path.  
# MAGIC
# MAGIC    e. Select **Save**.
# MAGIC
# MAGIC    **NOTE:** For more details on configuration parameters, see the Databricks documentation: [Use parameters with Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/parameters)
# MAGIC
# MAGIC #### Checkpoint (your path will vary)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multi_flow/checkpoint_config_params.png" alt="Config Parameter Checkpoint" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Configure the Flows for the Three Volumes
# MAGIC
# MAGIC 1. Copy the code below and paste it into your `flow_ingestion.sql` file, below the bronze table definition.
# MAGIC
# MAGIC    It defines one flow for each subsidiary volume. All three flows write to the same bronze table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ------------------------------------------
# MAGIC -- BRONZE FLOW - BRIGHT HOME
# MAGIC ------------------------------------------
# MAGIC -- Read CSV files from the bright_home_orders volume
# MAGIC CREATE FLOW bright_home_orders_flow
# MAGIC AS INSERT INTO multi_flow_1_bronze.orders_bronze_flows_demo BY NAME
# MAGIC SELECT
# MAGIC   CAST(subsidiary_id AS STRING) AS subsidiary_id,
# MAGIC   CAST(order_id AS STRING) AS order_id,
# MAGIC   CAST(order_timestamp AS STRING) AS order_timestamp,
# MAGIC   CAST(customer_id AS STRING) AS customer_id,
# MAGIC   CAST(region AS STRING) AS region,
# MAGIC   CAST(country AS STRING) AS country,
# MAGIC   CAST(city AS STRING) AS city,
# MAGIC   CAST(channel AS STRING) AS channel,
# MAGIC   CAST(sku AS STRING) AS sku,
# MAGIC   CAST(category AS STRING) AS category,
# MAGIC   CAST(qty AS STRING) AS qty,
# MAGIC   CAST(unit_price AS STRING) AS unit_price,
# MAGIC   CAST(discount_pct AS STRING) AS discount_pct,
# MAGIC   CAST(coupon_code AS STRING) AS coupon_code,
# MAGIC   CAST(total_amount AS STRING) AS total_amount,
# MAGIC   CAST(order_date AS STRING) AS order_date,
# MAGIC   _metadata.file_name AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${bright_home_orders_source}',   -- Uses the configuration parameter to point to the bright_home_orders volume
# MAGIC   format => 'csv',
# MAGIC   header => true
# MAGIC );
# MAGIC
# MAGIC ------------------------------------------
# MAGIC -- BRONZE FLOW - LUMINA SPORTS
# MAGIC ------------------------------------------
# MAGIC -- Read CSV files from the lumina_sports_orders volume
# MAGIC CREATE FLOW lumina_sports_orders_flow
# MAGIC AS INSERT INTO multi_flow_1_bronze.orders_bronze_flows_demo BY NAME
# MAGIC SELECT
# MAGIC   CAST(subsidiary_id AS STRING) AS subsidiary_id,
# MAGIC   CAST(order_id AS STRING) AS order_id,
# MAGIC   CAST(order_timestamp AS STRING) AS order_timestamp,
# MAGIC   CAST(customer_id AS STRING) AS customer_id,
# MAGIC   CAST(region AS STRING) AS region,
# MAGIC   CAST(country AS STRING) AS country,
# MAGIC   CAST(city AS STRING) AS city,
# MAGIC   CAST(channel AS STRING) AS channel,
# MAGIC   CAST(sku AS STRING) AS sku,
# MAGIC   CAST(category AS STRING) AS category,
# MAGIC   CAST(qty AS STRING) AS qty,
# MAGIC   CAST(unit_price AS STRING) AS unit_price,
# MAGIC   CAST(discount_pct AS STRING) AS discount_pct,
# MAGIC   CAST(coupon_code AS STRING) AS coupon_code,
# MAGIC   CAST(total_amount AS STRING) AS total_amount,
# MAGIC   CAST(order_date AS STRING) AS order_date,
# MAGIC   _metadata.file_name AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${lumina_sports_orders_source}',   -- Uses the configuration parameter to point to the lumina sports volume
# MAGIC   format => 'csv',
# MAGIC   header => true
# MAGIC );
# MAGIC
# MAGIC ------------------------------------------
# MAGIC -- BRONZE FLOW - NORTHSTAR OUTFITTERS
# MAGIC ------------------------------------------
# MAGIC -- Read JSON files from the northstar_outfitters_orders volume
# MAGIC CREATE FLOW northstar_outfitters_orders_flow
# MAGIC AS INSERT INTO multi_flow_1_bronze.orders_bronze_flows_demo BY NAME
# MAGIC SELECT
# MAGIC   CAST(subsidiary_id AS STRING) AS subsidiary_id,
# MAGIC   CAST(order_id AS STRING) AS order_id,
# MAGIC   CAST(order_timestamp AS STRING) AS order_timestamp,
# MAGIC   CAST(customer_id AS STRING) AS customer_id,
# MAGIC   CAST(region AS STRING) AS region,
# MAGIC   CAST(country AS STRING) AS country,
# MAGIC   CAST(city AS STRING) AS city,
# MAGIC   CAST(channel AS STRING) AS channel,
# MAGIC   CAST(sku AS STRING) AS sku,
# MAGIC   CAST(category AS STRING) AS category,
# MAGIC   CAST(qty AS STRING) AS qty,
# MAGIC   CAST(unit_price AS STRING) AS unit_price,
# MAGIC   CAST(discount_pct AS STRING) AS discount_pct,
# MAGIC   CAST(coupon_code AS STRING) AS coupon_code,
# MAGIC   CAST(total_amount AS STRING) AS total_amount,
# MAGIC   CAST(order_date AS STRING) AS order_date,
# MAGIC   _metadata.file_name AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${northstar_outfitters_orders_source}',  -- Uses the configuration parameter to point to the northstar volume
# MAGIC   format => 'json'
# MAGIC );
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the code**
# MAGIC - The code defines three flows: `bright_home_orders_flow`, `lumina_sports_orders_flow` and `northstar_outfitters_orders_flow`. Each one uses `INSERT INTO multi_flow_1_bronze.orders_bronze_flows_demo BY NAME`, so all three write to the same bronze table.
# MAGIC - All business columns are cast to `STRING` so every flow aligns with the unified bronze schema, while the **metadata columns** (`source_file` and `file_mod_time`) keep their native types. They capture the source **file name and modification time**, which helps with lineage and debugging.
# MAGIC - The `FROM STREAM read_files('${...}', ...)` clause uses your configuration parameter to reference the volume path and relies on Auto Loader for incremental ingestion.
# MAGIC
# MAGIC The flows differ only in their name, the configuration parameter they read, and the file format:
# MAGIC
# MAGIC | Flow | Configuration parameter | File format |
# MAGIC |---|---|---|
# MAGIC | `bright_home_orders_flow` | `${bright_home_orders_source}` | `csv` (`header => true`) |
# MAGIC | `lumina_sports_orders_flow` | `${lumina_sports_orders_source}` | `csv` (`header => true`) |
# MAGIC | `northstar_outfitters_orders_flow` | `${northstar_outfitters_orders_source}` | `json` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it runs successfully. 
# MAGIC
# MAGIC 2. Explore the run in the Lakeflow Pipelines Editor.
# MAGIC   - Confirm **449** rows were ingested into the bronze table from the three volumes (**157 + 110 + 182**)
# MAGIC   - Preview the data in the editor (Select **orders_bronze_flows_demo** -> **Data** tab). Notice data from each volume was incrementally ingested into the bronze table.
# MAGIC
# MAGIC > **TROUBLESHOOTING:** If your pipeline does not run successfully, confirm that your volumes were created and that your configuration parameters are set correctly.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multi_flow/checkpoint_bronze_flows.png" alt="Bronze Flow" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### D5. Query the Bronze Streaming Table
# MAGIC 1. The query below groups records by the captured **source_file** column so you can see the number of ingested rows per file.
# MAGIC
# MAGIC     Review how many files were ingested from each source confirming each cloud storage flow was ingested successfully.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC | Source File                 | Total Ingested by Source  |
# MAGIC |-----------------------------|---------------------------|
# MAGIC | nso_orders_2025-11-01.json  | 182                       |
# MAGIC | bsh_orders_2025-11-01.csv   | 157                       |
# MAGIC | lms_orders_2025-11-01.csv   | 110                       |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, count(*) AS `Total Ingested by Source`
# MAGIC FROM multi_flow_1_bronze.orders_bronze_flows_demo
# MAGIC GROUP BY source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Silver Table Data Quality, Optimization, and Transformation
# MAGIC
# MAGIC In the silver layer, we refine the **raw bronze table** into a clean and consistent **single source of truth (silver table)**. 
# MAGIC
# MAGIC This is where we standardize fields, enforce data quality, and prepare the dataset for downstream analytics.
# MAGIC
# MAGIC In this section, you will:
# MAGIC
# MAGIC - Apply data quality constraints  
# MAGIC - Clean and standardize fields  
# MAGIC - Enable liquid clustering for on the streaming table for optimized performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Create a New SQL File in your Pipeline
# MAGIC
# MAGIC 1. Click the kebab menu next to your `ingest_multiple_flows` folder and select **Create file**.
# MAGIC 2. Select the language as **SQL**.
# MAGIC 3. Name the file `silver_transformation.sql`.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Create Silver Transactions Table with Liquid Clustering and Data Quality Rules
# MAGIC
# MAGIC 1. Add the following code to your `silver_transformation.sql` file to create a Silver table with:
# MAGIC - a defined schema, 
# MAGIC - enforce consistent column types through `TRY_CAST` 
# MAGIC - and enable liquid clustering for performance

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC CREATE OR REFRESH STREAMING TABLE multi_flow_2_silver.orders_silver_flows_demo
# MAGIC (
# MAGIC   -- A: Define a fixed schema to prevent schema evolution.
# MAGIC   subsidiary_id   STRING,
# MAGIC   order_id        STRING,
# MAGIC   order_timestamp TIMESTAMP,
# MAGIC   order_date      DATE,
# MAGIC   customer_id     STRING,
# MAGIC   region          STRING,
# MAGIC   country         STRING,
# MAGIC   city            STRING,
# MAGIC   channel         STRING,
# MAGIC   sku             STRING,
# MAGIC   category        STRING,
# MAGIC   qty             INT,
# MAGIC   unit_price      DOUBLE,
# MAGIC   discount_pct    DOUBLE,
# MAGIC   total_amount    DOUBLE,
# MAGIC   coupon_code     STRING,
# MAGIC
# MAGIC   -- B: Data quality constraints to drop or flag or drop invalid rows.
# MAGIC   CONSTRAINT qty_valid          EXPECT (qty >= 0) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT total_amount_valid EXPECT (total_amount >= 0) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT timestamp_not_null EXPECT (order_timestamp IS NOT NULL) ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC -- C: Adds a table comment
# MAGIC COMMENT 'Clean and standardize data from the multiple-flow bronze table'
# MAGIC
# MAGIC -- D: Enable liquid clustering to improve performance on common filters.
# MAGIC CLUSTER BY AUTO
# MAGIC
# MAGIC AS
# MAGIC -- E: Select and clean data from the Bronze table. Uses TRY_CAST to enforce consistent types across all subsidiaries.
# MAGIC SELECT
# MAGIC   subsidiary_id,
# MAGIC   order_id,
# MAGIC   TRY_CAST(order_timestamp AS TIMESTAMP) AS order_timestamp, 
# MAGIC   TRY_CAST(order_date      AS DATE)      AS order_date,
# MAGIC   customer_id,
# MAGIC   region,
# MAGIC   country,
# MAGIC   city,
# MAGIC   channel,
# MAGIC   sku,
# MAGIC   category,
# MAGIC   TRY_CAST(qty          AS INT)    AS qty,
# MAGIC   TRY_CAST(unit_price   AS DOUBLE) AS unit_price,
# MAGIC   TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
# MAGIC   TRY_CAST(total_amount AS DOUBLE) AS total_amount,
# MAGIC   coupon_code
# MAGIC -- F: Incrementally reads data from the bronze table that contains data from three volumes
# MAGIC FROM STREAM multi_flow_1_bronze.orders_bronze_flows_demo;
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the code**
# MAGIC - A: `CREATE OR REFRESH STREAMING TABLE` - Creates or refreshes the **multi_flow_2_silver.orders_silver_flows_demo** Silver table and applies a fixed schema to prevent schema drift across multiple flows.
# MAGIC
# MAGIC - B: `CONSTRAINT ... EXPECT`  Applies data quality rules that drop rows with invalid quantities, negative totals, or missing timestamps.
# MAGIC
# MAGIC - C: `COMMENT`  Adds descriptive metadata explaining the purpose of the Silver table.
# MAGIC
# MAGIC - D: `CLUSTER BY AUTO`  Enables automatic liquid clustering where Databricks intelligently chooses clustering keys to optimize your query performance. You can also specify your own clustering keys if you'd like.
# MAGIC
# MAGIC - E: `TRY_CAST`  Enforces consistent column types across all subsidiaries by converting raw values into standardized data types.
# MAGIC
# MAGIC - F: `SELECT ... FROM STREAM`  Incrementally reads, selects, and cleans records from the **multi_flow_1_bronze.orders_bronze_flows_demo** Bronze streaming table, which contains data from three separate volumes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Run and Explore the Pipeline 
# MAGIC 1. Select **Run pipeline** to create the Silver table.  
# MAGIC    - This run executes the transformation logic and applies the data quality expectations.
# MAGIC
# MAGIC 2. Explore the pipeline in the Lakeflow Pipeline Editor. Notice the following:
# MAGIC    - Since the pipeline has already ingested the source files into Bronze, **0** rows are processed in the Bronze table.
# MAGIC    - All **449** rows are processed in the Silver table.
# MAGIC    - In the **Expectations** column, select `3 met` to view the data quality rules. All rows pass the expectations.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ![Silver SDP Checkpoint](https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multi_flow/checkpoint_silver.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Explore the Silver Streaming table
# MAGIC
# MAGIC 1. Run the command to view the table metadata. When the results appear, notice the following:
# MAGIC
# MAGIC - The silver table has the exact **column data types** that were defined.
# MAGIC - **Liquid clustering** is enabled:
# MAGIC   - In the **# Clustering Information** section no columns are specified since Databricks hasn't optimized the keys yet (requires historical query analysis on the table to optimize the clustered columns)
# MAGIC   - In the **Table Properties** row you will see cluster by auto is enabled (`clusterByAuto=true`).
# MAGIC
# MAGIC **NOTE:** SDP supports Liquid clustering. Liquid clustering automatically organizes data based on frequently filtered columns to improve query performance. For more information, view the [Use liquid clustering for tables](https://docs.databricks.com/aws/en/delta/clustering).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED multi_flow_2_silver.orders_silver_flows_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the code below to view the data in your **silver table**. 
# MAGIC
# MAGIC     Confirm that the records look clean, standardized, and ready for gold-level analysis. 
# MAGIC
# MAGIC     Look for consistent data types, valid numeric values, and properly cast timestamps and dates.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM multi_flow_2_silver.orders_silver_flows_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Business Intelligence Materialized Views
# MAGIC
# MAGIC Materialized views in Lakeflow provide precomputed results that power fast, reliable analytics for downstream users. Unlike regular views, they automatically refresh as new data arrives, so stakeholders always see up-to-date insights without requiring on-demand computation.
# MAGIC
# MAGIC **NOTE:** This section assumes prior familiarity with materialized views.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Create a New SQL File in your Pipeline
# MAGIC
# MAGIC 1. Click the kebab menu next to your `ingest_multiple_flows` folder and select **Create file**.
# MAGIC 2. Select the language as **SQL**.
# MAGIC 3. Name the file `gold_mvs.sql`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Create Simple Gold Materialized Views
# MAGIC
# MAGIC 1. Next you will add two small simple materialized views for your consumers. These are only used to confirm that your silver data is clean and ready for analysis. Materialized views are not the focus here, so we will keep them simple.
# MAGIC
# MAGIC    a. The first materialized view gives a **daily summary by subsidiary**, letting you quickly check revenue, units, and order counts over time.  
# MAGIC
# MAGIC    b. The second view highlights **basic product performance** so you can see which categories and SKUs are selling within each subsidiary.
# MAGIC
# MAGIC
# MAGIC 2. Copy the code below into your `gold_mvs.sql` file to create both materialized views.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ------------------------------------------
# MAGIC -- a. GOLD MATERIALIZED VIEW: DAILY SUBSIDIARY SCORECARD
# MAGIC -- Simple daily summary by subsidiary
# MAGIC ------------------------------------------
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW multi_flow_3_gold.mv_daily_subsidiary_scorecard_demo
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   subsidiary_id,
# MAGIC   COUNT(DISTINCT order_id)    AS order_count,   -- how many unique orders occurred
# MAGIC   ROUND(SUM(total_amount),2)  AS total_revenue, -- total revenue for the day
# MAGIC   SUM(qty)                    AS total_units    -- total units sold
# MAGIC FROM multi_flow_2_silver.orders_silver_flows_demo
# MAGIC WHERE order_date IS NOT NULL
# MAGIC GROUP BY order_date, subsidiary_id;
# MAGIC
# MAGIC
# MAGIC ------------------------------------------
# MAGIC -- b. GOLD MATERIALIZED VIEW: PRODUCT PERFORMANCE BY SUBSIDIARY
# MAGIC -- Basic units and revenue by product and subsidiary
# MAGIC ------------------------------------------
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW multi_flow_3_gold.mv_product_performance_by_subsidiary_demo
# MAGIC AS
# MAGIC SELECT
# MAGIC   subsidiary_id,
# MAGIC   category,
# MAGIC   sku,
# MAGIC   SUM(qty)                   AS units_sold,  -- total units sold for each SKU
# MAGIC   ROUND(SUM(total_amount),2) AS revenue      -- total revenue for each SKU
# MAGIC FROM multi_flow_2_silver.orders_silver_flows_demo
# MAGIC GROUP BY subsidiary_id, category, sku;
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### F3. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Select **Run pipeline** to create the two Gold materialized views.
# MAGIC
# MAGIC 2. Explore the pipeline in the Lakeflow Pipeline Editor. Notice the following:
# MAGIC
# MAGIC    - Since the pipeline has already ingested the raw data into the **Bronze** and **Silver** streaming tables, and no new files were added, there are no new records for the streaming tables to process.
# MAGIC
# MAGIC    - Both materialized views are created with **3** and **15** rows respectively.
# MAGIC
# MAGIC    - In the **Tables** window at the bottom:  
# MAGIC      - Select the **Show and hide columns** button   
# MAGIC      - Then show the **Incrementalization** column to see whether the materialized views were **fully recomputed** or **incrementally computed**.
# MAGIC
# MAGIC    - On this initial run, both materialized views show **Full recompute** on the initial creation. Later, we will see these views compute incrementally.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ![MVs SDP Checkpoint](https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multi_flow/checkpoint_mvs_pipeline.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### F4. Display the Materialized Views

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Querying the **multi_flow_3_gold.mv_daily_subsidiary_scorecard_demo** materialized view returns a daily scorecard for each subsidiary. 
# MAGIC
# MAGIC     It summarizes order volume, total revenue, and total units so you can quickly compare performance across the three subsidiaries and order date.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM multi_flow_3_gold.mv_daily_subsidiary_scorecard_demo

# COMMAND ----------

# MAGIC %md
# MAGIC 2. The **multi_flow_3_gold.mv_product_performance_by_subsidiary_demo**  materialized view provides a simple product performance breakdown, showing which categories and SKUs are selling within each subsidiary. 
# MAGIC
# MAGIC     It helps you compare units sold and revenue across product lines.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM multi_flow_3_gold.mv_product_performance_by_subsidiary_demo
# MAGIC ORDER BY subsidiary_id, category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Run the Spark Declarative Pipeline with New Files
# MAGIC
# MAGIC Now that the pipeline is built, let's add the daily drop for the **2025-11-02** orders for each subsidiary.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Land a New File in Each Volume
# MAGIC
# MAGIC 1. Run the cell below to add the next daily file (**2025-11-02**) to each subsidiary volume.  
# MAGIC
# MAGIC 2. After the cell runs, confirm that each volume now contains two files: **2025-11-01** and **2025-11-02**.

# COMMAND ----------

## Copy a second CSV file into the bright_home_orders volume
marketplace_share_path = f'/Volumes/{your_marketplace_share_catalog_name}/v02/subsidiary_daily_orders'

copy_files(
    copy_from = f'{marketplace_share_path}/bright_home_orders', 
    copy_to = f'/Volumes/{my_catalog}/multi_flow_1_bronze/bright_home_orders', 
    n = 2
)

## Copy a second CSV file into the lumina_sports_orders volume
copy_files(
    copy_from = f'{marketplace_share_path}/lumina_sports_orders', 
    copy_to = f'/Volumes/{my_catalog}/multi_flow_1_bronze/lumina_sports_orders', 
    n = 2
)

## Copy a second JSON file into the northstar_outfitters_orders volume
copy_files(
    copy_from = f'{marketplace_share_path}/northstar_outfitters_orders', 
    copy_to = f'/Volumes/{my_catalog}/multi_flow_1_bronze/northstar_outfitters_orders', 
    n = 2
)


## List files in your volumes (confirm two files exist)
spark.sql(f'LIST "{my_vol_path}/bright_home_orders"').display()
spark.sql(f'LIST "{my_vol_path}/lumina_sports_orders"').display()
spark.sql(f'LIST "{my_vol_path}/northstar_outfitters_orders"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Explore the New Daily Drop (2025-11-02) File in Each Volume
# MAGIC 1. Simply run the cell below to count the number of records in the raw daily drop file **2025-11-02** within each volume.
# MAGIC
# MAGIC 2. Confirm that the output shows **502** total rows across all three subsidiaries for the **2025-11-02** orders drop.
# MAGIC
# MAGIC | Volume                      | TotalRows | FileName                    |
# MAGIC |-----------------------------|-----------|------------------------------|
# MAGIC | bright_home_orders          | 191       | bsh_orders_2025-11-02.csv    |
# MAGIC | lumina_sports_orders        | 170       | lms_orders_2025-11-02.csv    |
# MAGIC | northstar_outfitters_orders | 141       | nso_orders_2025-11-02.json   |
# MAGIC | TOTAL                       | 502       |                              |

# COMMAND ----------

from pyspark.sql.functions import lit, sum as _sum

df_all = spark.sql(f"""
    SELECT 
        'bright_home_orders' AS Volume,
        COUNT(*) AS TotalRows,
        'bsh_orders_2025-11-02.csv' AS FileName
    FROM read_files('{my_vol_path}/bright_home_orders/bsh_orders_2025-11-02.csv')

    UNION ALL
    SELECT 
        'lumina_sports_orders' AS Volume,
        COUNT(*) AS TotalRows,
        'lms_orders_2025-11-02.csv' AS FileName
    FROM read_files('{my_vol_path}/lumina_sports_orders/lms_orders_2025-11-02.csv')

    UNION ALL
    SELECT 
        'northstar_outfitters_orders' AS Volume,
        COUNT(*) AS TotalRows,
        'nso_orders_2025-11-02.json' AS FileName
    FROM read_files('{my_vol_path}/northstar_outfitters_orders/nso_orders_2025-11-02.json')
""")

# Build TOTAL row
total_row = (
    df_all
    .agg(_sum("TotalRows").alias("TotalRows"))
    .withColumn("Volume", lit("TOTAL"))
    .withColumn("FileName", lit(""))
    .select("Volume", "TotalRows", "FileName")  # match column order
)

# Append TOTAL row to the bottom
df_with_total = df_all.unionByName(total_row)

display(df_with_total)

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline to **incrementally** ingest, process and aggregate the **new daily sales drop**. Confirm it runs successfully. 
# MAGIC
# MAGIC 2. Explore the run in the Lakeflow Pipelines Editor.
# MAGIC   - Confirm **502** rows were ingested into the **bronze table** from the new daily orders drop in the three volumes.
# MAGIC   - Confirm all **502** were processed and passed the data quality checks in the **silver table**.
# MAGIC   - Confirm the materialized views:
# MAGIC     - Contain **6** and **15** rows respectively
# MAGIC     - Were both incrementally refreshed (**Incremental**). 
# MAGIC       - **NOTE:** For more information view the [Incremental refresh for materialized views](https://docs.databricks.com/aws/en/optimizations/incremental-refresh) documentation.
# MAGIC
# MAGIC > **TROUBLESHOOTING:** If your pipeline does does not match the output below make sure you have landed the second file in each volume from the 'Land a New File in Each Volume' section above.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ![SDP Run 2 Files in All Volumes](https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multi_flow/checkpoint_run_daily_drop_2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### G4. Explore the Final Pipeline Objects

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to view the **bronze** table. 
# MAGIC
# MAGIC     Notice that the table contains **951 rows** (The total number of rows after both runs)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM multi_flow_1_bronze.orders_bronze_flows_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Count the number of rows ingested by each **source_file** in the **bronze** streaming table. 
# MAGIC
# MAGIC     Notice that we can easily examine how many rows were ingested by each source file (**daily orders drop**).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, count(*) AS TotalRows
# MAGIC FROM multi_flow_1_bronze.orders_bronze_flows_demo
# MAGIC GROUP BY source_file
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. View the data in the **silver table**. Notice that the data is clean, adheres to our defined schema and contains our 'single source of truth'.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM multi_flow_2_silver.orders_silver_flows_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. View the gold level materialized view **mv_daily_subsidiary_scorecard_demo**. 
# MAGIC
# MAGIC     Notice downstream consumers can easily examine orders by date for each subsidiary.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM multi_flow_3_gold.mv_daily_subsidiary_scorecard_demo
# MAGIC ORDER BY order_date, subsidiary_id;

# COMMAND ----------

# MAGIC %md
# MAGIC 5. View the gold level materialized view **mv_product_performance_by_subsidiary_demo**. 
# MAGIC
# MAGIC     Notice downstream consumers can easily examine detailed order metrics by each **subsidiary_id** and **sku**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM multi_flow_3_gold.mv_product_performance_by_subsidiary_demo
# MAGIC ORDER BY subsidiary_id, category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Introduction to Adding Tags to Bronze, Silver and Gold Objects
# MAGIC #### This requires the necessary permissions to add tags
# MAGIC
# MAGIC In this step you add semantic metadata to your tables and materialized views. Tags make it easier to organize, search and govern objects in Unity Catalog. They help downstream teams quickly understand what each object represents and how it should be used.
# MAGIC
# MAGIC You apply two types of tags:
# MAGIC
# MAGIC - **Custom demo tags**  
# MAGIC   These describe the department that owns the data and the quality level in the medallion architecture.  
# MAGIC   Examples:  
# MAGIC   - `demo_tag_Department = 'Sales'`  
# MAGIC   - `demo_tag_Quality = 'bronze' | 'silver' | 'gold'`
# MAGIC
# MAGIC - **System tags**  
# MAGIC   These are built-in Unity Catalog tags. Here you add the `system.Certified` tag to identify trusted, production-ready objects.
# MAGIC
# MAGIC
# MAGIC **NOTE**: For more information view the [Apply tags to Unity Catalog securable objects](https://docs.databricks.com/aws/en/database-objects/tags)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. The code below creates the following tags:
# MAGIC   - Updates the **bronze table** with department and quality tags so users can see this is raw, ingested data owned by Sales.
# MAGIC   - Updates the **silver table** with the same tags, then adds the `system.Certified` tag to mark it as a clean, trusted dataset.
# MAGIC   - Updates both **gold materialized views** with department and quality tags, then marks each one as `system.Certified` because these are curated analytics objects intended for broad consumption.
# MAGIC
# MAGIC **NOTE:** [ALTER TABLE]()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ------------------------------------
# MAGIC -- Bronze table tags
# MAGIC ----------------------------------
# MAGIC ALTER TABLE multi_flow_1_bronze.orders_bronze_flows_demo
# MAGIC SET TAGS (
# MAGIC   'demo_tag_Department' = 'Sales',
# MAGIC   'demo_tag_Quality' = 'bronze'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC ----------------------------------
# MAGIC -- Silver table tags
# MAGIC ----------------------------------
# MAGIC ALTER TABLE multi_flow_2_silver.orders_silver_flows_demo
# MAGIC SET TAGS (
# MAGIC   'demo_tag_Department' = 'Sales',
# MAGIC   'demo_tag_Quality' = 'silver'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC ----------------------------------
# MAGIC -- Materialized views table tags
# MAGIC ------------------------------------
# MAGIC ALTER TABLE multi_flow_3_gold.mv_product_performance_by_subsidiary_demo
# MAGIC SET TAGS (
# MAGIC   'demo_tag_Department' = 'Sales',
# MAGIC   'demo_tag_Quality' = 'gold'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE multi_flow_3_gold.mv_daily_subsidiary_scorecard_demo
# MAGIC SET TAGS (
# MAGIC   'demo_tag_Department' = 'Sales',
# MAGIC   'demo_tag_Quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 2. After you add tags to a table or materialized view, you can query them directly using the **information_schema.table_tags** view within the specific catalog, or the **system.information_schema** schema. 
# MAGIC
# MAGIC     The query below queries **your-catalog.information_schema**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM information_schema.table_tags;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. You can also view tags in **Catalog Explorer**. Follow these steps to inspect the tags on one of your objects:
# MAGIC
# MAGIC    a. Right click **Catalog** and select **Open in New Tab**  
# MAGIC
# MAGIC    b. Navigate to your catalog  
# MAGIC
# MAGIC    c. Open the **multi_flow_3_gold** schema  
# MAGIC
# MAGIC    d. Select the **mv_daily_subsidiary_scorecard_demo** materialized view  
# MAGIC
# MAGIC    e. In the right pane, locate the **Tags** section and confirm the three tags that were added
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ![Tagging Checkpoint](https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multi_flow/checkpoint_tagging.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. (OPTIONAL) Land Additional Files in your Volumes
# MAGIC
# MAGIC If you'd like to continue practicing, use the demonstration function `copy_files` to dynamically add another JSON file (file number 3) to your cloud storage location.  
# MAGIC
# MAGIC **NOTES:** 
# MAGIC - Ensure the variables you defined at the start of this demonstration: `your_marketplace_share_catalog_name`, `my_catalog` are still active for the function to work properly.
# MAGIC - There are a total of 7 available files you can continue practicing with.

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC ## Copy a second CSV file into the bright_home_orders volume
# MAGIC marketplace_share_path = f'/Volumes/{your_marketplace_share_catalog_name}/v02/subsidiary_daily_orders'
# MAGIC
# MAGIC copy_files(
# MAGIC     copy_from = f'{marketplace_share_path}/bright_home_orders', 
# MAGIC     copy_to = f'/Volumes/{my_catalog}/multi_flow_1_bronze/bright_home_orders', 
# MAGIC     n = 3 # <-- Add a third file to the volume
# MAGIC )
# MAGIC
# MAGIC ## Copy a second CSV file into the lumina_sports_orders volume
# MAGIC copy_files(
# MAGIC     copy_from = f'{marketplace_share_path}/lumina_sports_orders', 
# MAGIC     copy_to = f'/Volumes/{my_catalog}/multi_flow_1_bronze/lumina_sports_orders', 
# MAGIC     n = 3 # <-- Add a third file to the volume
# MAGIC )
# MAGIC
# MAGIC ## Copy a second JSON file into the northstar_outfitters_orders volume
# MAGIC copy_files(
# MAGIC     copy_from = f'{marketplace_share_path}/northstar_outfitters_orders', 
# MAGIC     copy_to = f'/Volumes/{my_catalog}/multi_flow_1_bronze/northstar_outfitters_orders', 
# MAGIC     n = 3 # <-- Add a third file to the volume
# MAGIC )
# MAGIC
# MAGIC
# MAGIC ## List files in your volumes
# MAGIC spark.sql(f'LIST "{my_vol_path}/bright_home_orders"').display()
# MAGIC spark.sql(f'LIST "{my_vol_path}/lumina_sports_orders"').display()
# MAGIC spark.sql(f'LIST "{my_vol_path}/northstar_outfitters_orders"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Clean up
# MAGIC 1. Feel free to delete the schemas you create in this demonstration by running cell below and confirming the delete (**Y**).

# COMMAND ----------

delete_schemas(
    catalog = my_catalog, ## <--- Your catalog name using the variable you set earlier
    schemas = ['multi_flow_1_bronze', 'multi_flow_2_silver', 'multi_flow_3_gold']
)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Delete your Spark Declarative Pipeline through the **Jobs & Pipelines** UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## K. Summary and Key Takeaways 
# MAGIC
# MAGIC - **Multi-Flow Ingestion**: Successfully ingested data from three different subsidiaries (`CSV` and `JSON` formats) into a single bronze streaming table using separate flows
# MAGIC - **Schema Standardization**: Resolved format conflicts by casting all columns to STRING in bronze, then applying proper data types in silver
# MAGIC - **Data Quality & Performance**: Implemented constraint-based data quality checks and enabled liquid clustering for optimized query performance
# MAGIC - **Incremental Processing**: Demonstrated true incremental processing across the entire medallion architecture with automatic materialized view refresh
# MAGIC
# MAGIC #### Business Value Delivered
# MAGIC
# MAGIC The pipeline processed **951 total records** from six source files across two daily drops, creating a unified view of sales data that enables:
# MAGIC - Cross-subsidiary performance analysis
# MAGIC - Real-time business intelligence through auto-refreshing materialized views
# MAGIC - Scalable data quality enforcement as new subsidiaries are added
# MAGIC
# MAGIC This architecture provides a foundation for enterprise-scale data consolidation while maintaining data lineage, quality, and performance optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>