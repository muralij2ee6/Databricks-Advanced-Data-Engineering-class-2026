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
# MAGIC # Demo - Advanced Data Quality Checks and Expectations in Spark Declarative Pipelines
# MAGIC
# MAGIC #### Duration: ~25-30 Minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This demonstration showcases enterprise-grade data quality management using Spark Declarative Pipelines (Lakeflow) with advanced quality expectations and quarantine patterns. You will build a comprehensive medallion architecture that implements three critical data engineering patterns: range-based validation, automatic schema evolution, and zero-loss quarantine processing.
# MAGIC
# MAGIC The demo simulates a real-world scenario where order data arrives with varying quality levels and evolving schemas. Using official Databricks patterns, you will process this data through bronze, silver, and gold layers while implementing robust quality controls that catch data anomalies, handle schema changes seamlessly, and ensure zero data loss through intelligent quarantine mechanisms.
# MAGIC
# MAGIC This demonstration uses a two-run approach: Run 1 establishes a clean baseline with 157 perfect records to validate the quality framework, while Run 2 introduces 60 records containing intentional quality issues and schema evolution to demonstrate the system's resilience and monitoring capabilities.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demonstration, you will be able to:
# MAGIC
# MAGIC - **Implement comprehensive data validation** using 6 quality expectations covering NOT NULL checks, numeric ranges, and date validations with proper constraint syntax
# MAGIC - **Handle automatic schema evolution** without pipeline code changes using flexible bronze layer design and schema hints
# MAGIC - **Build quarantine patterns** using official Databricks inverse logic to capture invalid records while maintaining zero data loss and detailed failure tracking
# MAGIC - **Monitor data quality metrics** and analyze expectation performance to identify specific data quality issues and develop remediation strategies

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

# MAGIC %md-sandbox
# MAGIC ## Demo Architecture Overview
# MAGIC
# MAGIC This demo demonstrates how to build a robust data pipeline using Databricks medallion architecture, focusing on data quality enforcement and schema flexibility.
# MAGIC
# MAGIC ### Demo Pipeline Architecture
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1690 560" width="100%" role="img" aria-label="Advanced data quality pipeline overview: sales CSV files flow through bronze raw and clean tables into a silver table with quality checks, then valid, quarantined and analytics outputs" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif;max-width:1690px;display:block;margin:12px auto;"><defs><marker id="od-ah-g" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#7C8B94"/></marker><marker id="od-ah-n" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B3139"/></marker><marker id="od-ah-t" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B5062"/></marker><filter id="od-sh" x="-8%" y="-8%" width="116%" height="130%"><feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#0B2026" flood-opacity="0.13"/></filter></defs><text x="14" y="30" font-size="20" font-weight="400" fill="#1B3139" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Cloud Storage</text><rect x="10" y="42" width="168" height="400" rx="18" fill="#fff" stroke="#9AA5AB" stroke-width="1.6" /><rect x="24" y="62" width="140" height="360" rx="8" fill="none" stroke="#8C98A0" stroke-width="1.5" stroke-dasharray="5 4" /><text x="94" y="90" font-size="17" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Sales</text><text x="94" y="110" font-size="14.5" font-weight="400" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV Files</text><g transform="translate(78,130) scale(1.0)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><text x="94" y="196" font-size="13" font-weight="400" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >File 1</text><g transform="translate(78,226) scale(1.0)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><text x="94" y="292" font-size="13" font-weight="400" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >File 2</text><g transform="translate(78,322) scale(1.0)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >CSV</text></g><text x="94" y="388" font-size="13" font-weight="400" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >File 3</text><path d="M166,281 H214" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#od-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="216" y="218" width="34" height="31.0" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="258" y="244" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="216" y="258" width="214" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#od-sh)"/><text x="323.0" y="286" font-size="12.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >sales_bronze_raw_demo</text><path d="M432,281 H468" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#od-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="470" y="218" width="34" height="31.0" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="512" y="244" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="470" y="258" width="226" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#od-sh)"/><text x="583.0" y="286" font-size="12.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >sales_bronze_clean_demo</text><path d="M698,281 H736" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#od-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><text x="928" y="150" font-size="22" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Applying Data</text><text x="928" y="176" font-size="22" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Quality Checks</text><rect x="738" y="190" width="380" height="224" rx="0" fill="#F6F4F1" stroke="#8C98A0" stroke-width="1.2" /><svg x="752" y="218" width="34" height="31.0" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="794" y="244" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="752" y="258" width="352" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#od-sh)"/><text x="928.0" y="286" font-size="14" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >sales_silver_dq_demo</text><text x="928" y="352" font-size="15.5" text-anchor="middle" fill="#1B3139" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">Applying Data Quality Checks</text><text x="928" y="374" font-size="15.5" text-anchor="middle" fill="#1B3139" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">and Writing Quarantine logic</text><svg x="1150" y="54" width="34" height="31.0" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="1192" y="80" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="1150" y="94" width="232" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#od-sh)"/><text x="1266.0" y="122" font-size="12.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >sales_silver_valid_demo</text><path d="M1120,281 H1126 V125 Q1126,117 1134,117 H1145" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#od-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="1150" y="324" width="34" height="31.0" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="1192" y="350" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="1150" y="364" width="264" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#od-sh)"/><text x="1282.0" y="392" font-size="12.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >sales_silver_quarantined_demo</text><path d="M1120,281 H1126 V379 Q1126,387 1134,387 H1145" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#od-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="1448" y="54" width="34" height="34" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><rect x="32.015" y="26.436" width="10.5" height="3.5" transform="rotate(131 37.265 28.186)"/><rect x="32.5" y="31" width="15" height="3"/><rect x="37.485" y="35.064" width="10.5" height="3.5" transform="rotate(131 42.735 36.814)"/></svg><text x="1490" y="80" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Materialized View</text><rect x="1448" y="94" width="214" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#od-sh)"/><text x="1555.0" y="122" font-size="12.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >sales_analytics_demo</text><path d="M1384,117 H1444" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#od-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><path d="M216,452 H1064 V434 L1146,478 L1064,522 V504 H216 z" fill="#1B5062"/><text x="640" y="486" font-size="18" text-anchor="middle" fill="#fff" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif"><tspan font-weight="700">Incrementally</tspan> process <tspan font-weight="700">new data</tspan> in the streaming tables (ST)</text></svg>
# MAGIC
# MAGIC ### Demo Execution Flow
# MAGIC
# MAGIC - **Run 1:** Clean baseline data (157 records) — all pass quality checks, establishes 100% quality baseline
# MAGIC - **Run 2:** Data with quality issues (60 records, 2 new columns) — demonstrates issue detection and schema evolution

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Setup

# COMMAND ----------

# MAGIC %md
# MAGIC > **Workspace setup:** Using a Databricks Academy (Vocareum) workspace? Your catalog is created for you, so you can start. Using your own workspace or Databricks Free Edition? Complete **Course Overview → B. Workspace Setup** first (catalog creation and permissions, Marketplace data). Run this notebook only in a development or sandbox workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Configure Your Catalog and Schema
# MAGIC
# MAGIC Run the cell below to initialize your environment. This setup step performs the following actions:
# MAGIC
# MAGIC - **Creates or validates catalog access** when running outside of a Databricks provided Vocareum workspace
# MAGIC - **Creates three schemas** in your specified catalog:
# MAGIC     - **dq_1_bronze** - Raw data ingestion layer
# MAGIC     - **dq_2_silver** - Validated and transformed data layer  
# MAGIC     - **dq_3_gold** - Analytics-ready aggregated data layer
# MAGIC - **Creates volume storage** for source data processing:
# MAGIC     - `sales` volume: Active processing location for pipeline consumption
# MAGIC     - `ops` volume: Staging location containing multiple data files
# MAGIC - **Verifies compute environment** compatibility with Lakeflow pipelines
# MAGIC
# MAGIC This ensures that all schemas, tables, and objects are created in your designated catalog with proper isolation.
# MAGIC
# MAGIC **Important:** You must have permission to create catalogs in your own non-Vocareum workspace. If you do not have the required permissions, this step will fail. If you cannot create catalogs, see **Course Overview → B2. Troubleshooting: Missing Create Catalog Permissions** before continuing.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-dq

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Verify Volume Path Configuration
# MAGIC
# MAGIC Run the cell below to view the value of the `my_vol_path` variable and confirm that it references your **your-catalog.dq_1_bronze** path. This path will be used to dynamically reference your source volumes throughout this demonstration.

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Source Data Characteristics
# MAGIC
# MAGIC Before building the pipeline, examine the source data to understand its structure, quality characteristics, and schema evolution patterns. This analysis will inform our quality expectation design.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Examine the Source Files
# MAGIC
# MAGIC The **ops** volume holds several files that are moved incrementally into the **sales** volume, which the pipeline reads. This simulates real-world data arrival.
# MAGIC
# MAGIC Run the cell below to list both volumes. Notice that **ops** contains multiple files, while **sales** currently contains only one.

# COMMAND ----------

# List the files in the ops and sales volumes
for volume in ['ops', 'sales']:
    print(f'--- {volume}')
    display(spark.sql(f"LIST '{my_vol_path}/{volume}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Compare the First Two Files
# MAGIC
# MAGIC | | `sales_1.csv` | `sales_2.csv` |
# MAGIC |---|---|---|
# MAGIC | **Records** | 157 | 60 |
# MAGIC | **Columns** | 16 | 18 (the 16 original columns plus the new `order_status` and `shipping_cost`) |
# MAGIC | **Quality** | Clean: no missing business keys, discount percentages between 0 and 100, recent order dates | Intentional quality issues (below) |
# MAGIC | **Purpose** | Quality baseline for the first run | Schema evolution and quality issues for the second run |
# MAGIC
# MAGIC **Intentional quality issues in `sales_2.csv`:**
# MAGIC - Invalid discount percentages (for example 120, outside the 0-100 range)
# MAGIC - Historical order dates (for example 1930-12-31)
# MAGIC - Missing `customer_id` values
# MAGIC - Invalid shipping costs (above 100 or below 0)
# MAGIC
# MAGIC About 85% of the 60 records are expected to pass all checks, and about 15% to be quarantined with the failure reasons.
# MAGIC
# MAGIC Run the cell below to preview both files. The previews also show a `_rescued_data` column that `read_files` adds, which is not counted in the column numbers above.

# COMMAND ----------

# Preview the first file (clean baseline) and the second file (quality issues + new columns)
for file in ['sales_1.csv', 'sales_2.csv']:
    print(f'--- {file}')
    display(spark.sql(f"SELECT * FROM read_files('{my_vol_path}/ops/{file}')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Create the Spark Declarative Pipeline
# MAGIC
# MAGIC Now that we understand the data characteristics and quality challenges, create the Spark Declarative Pipeline with comprehensive data quality expectations and quarantine handling.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Create a Apache Spark™ Declarative Pipeline
# MAGIC
# MAGIC Complete the following steps to create your Spark Declarative Pipeline using the Lakeflow Pipelines Editor:
# MAGIC
# MAGIC 1. In the main navigation pane, right-click **Jobs & Pipelines** and select **Open link in New Tab**.  
# MAGIC
# MAGIC 2. In the new tab, select **Create → ETL Pipeline**.  
# MAGIC
# MAGIC 3. Configure the pipeline settings:
# MAGIC    - **Pipeline Name**: `demo_dq_yourname`
# MAGIC    - **Default Catalog**: `YOUR_LABUSER_CATALOG`
# MAGIC    - **Default Schema**: `dq_1_bronze`  
# MAGIC    **NOTE:** Clear the selected schema using the cross icon to view all schemas.
# MAGIC
# MAGIC 4. **Rename pipeline components**:
# MAGIC    - Rename the **transformations** folder to `dq_pipeline`
# MAGIC    - Rename **my_transformation.py** file to `bronze_ingestion.sql`
# MAGIC
# MAGIC 5. Leave the **Lakeflow Pipelines Editor** page open for the next steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Configure Pipeline Parameters
# MAGIC
# MAGIC 1. Run the cell below to retrieve the key-value pairs needed to set your pipeline configuration parameters for the **source volume**.

# COMMAND ----------

config_parameters = [
    ('source', f'{my_vol_path}/sales')
]

for key, value in config_parameters:
    print(f"Key: {key}\nValue: {value}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Add the parameter to your **Spark Declarative Pipeline**: open **Settings**, under **Configuration** select **Add configuration**, enter the key and value shown above, and select **Save**. This lets your pipeline reference the volume through a parameter.
# MAGIC
# MAGIC **NOTE:** For more details on configuration parameters, see the Databricks documentation: [Use parameters with Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Create Bronze Layer - Raw Ingestion with Schema Evolution
# MAGIC
# MAGIC The bronze layer implements schema evolution by using the `read_files` function with flexible column handling and schema hints for future column compatibility.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Create Bronze Table with Schema Evolution Support
# MAGIC
# MAGIC Copy the code below into your `bronze_ingestion.sql` file. This implementation uses a STRING-based approach for maximum schema flexibility and includes schema hints for future columns.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC
# MAGIC ------------------------------------------
# MAGIC -- BRONZE LAYER: RAW DATA INGESTION
# MAGIC ------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_1_bronze.sales_bronze_raw_demo
# MAGIC AS
# MAGIC SELECT 
# MAGIC   -- Core business fields (all as STRING for schema flexibility)
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
# MAGIC
# MAGIC   -- New columns for schema evolution (will be null initially)
# MAGIC   CAST(order_status AS STRING) AS order_status,
# MAGIC   CAST(shipping_cost AS STRING) AS shipping_cost,
# MAGIC
# MAGIC   -- Rescued data column for parsing issues
# MAGIC   CAST(_rescued_data AS STRING) AS _rescued_data,
# MAGIC
# MAGIC   -- Metadata columns for lineage tracking
# MAGIC   _metadata.file_name AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${source}',
# MAGIC   format => 'csv',
# MAGIC   schemaHints => 'order_status STRING, shipping_cost STRING'
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
# MAGIC **Review the Bronze Layer Design Patterns:**
# MAGIC
# MAGIC **Schema Evolution Strategy:**
# MAGIC - **STRING storage**: All business columns stored as STRING for maximum flexibility and compatibility
# MAGIC - **Pre-defined schema**: Includes both original and two future columns for seamless evolution
# MAGIC - **Schema hints**: Declares expected new columns (`order_status`, `shipping_cost`) even when not yet present in data
# MAGIC - **Null handling**: New columns will be NULL for existing files, populated for future files
# MAGIC
# MAGIC **Data Lineage and Protection:**
# MAGIC - **Auto Loader**: `read_files` function enables incremental processing with checkpoint management
# MAGIC - **Metadata capture**: Source file names and modification timestamps for complete data lineage
# MAGIC - **Rescued data**: `_rescued_data` column captures any parsing issues for investigation and recovery
# MAGIC - **Parameter reference**: `${source}` enables dynamic path configuration
# MAGIC
# MAGIC **Technical Benefits:**
# MAGIC - No pipeline code changes required when new columns arrive
# MAGIC - Backward compatibility with existing data files
# MAGIC - Forward compatibility with evolving business requirements
# MAGIC
# MAGIC For more information about schema hints functionality, see the [Databricks documentation on schema hints](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema#override-schema-inference-with-schema-hints).

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Create Clean Bronze Table
# MAGIC
# MAGIC Create a streaming intermediate Bronze table that performs safe type casting from the raw ingested data, without applying any quality checks. This step ensures clean type conversion before data validation.
# MAGIC
# MAGIC - Creates a clean streaming Bronze table from the source data stream.
# MAGIC - Uses `TRY_CAST` to safely convert data types (dates, numbers) without failing on bad data.
# MAGIC - Passes through all columns without applying any data quality filters yet.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- BRONZE LAYER: INTERMEDIATE TRANSFORMATION TABLE
# MAGIC -- Purpose: Clean type casting from Bronze Raw (no quality checks yet)
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_1_bronze.sales_bronze_clean_demo
# MAGIC COMMENT "Intermediate table - type casting only, no quality checks"
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Direct pass-through and safe type casting
# MAGIC   subsidiary_id,
# MAGIC   order_id,
# MAGIC   TRY_CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
# MAGIC   TRY_CAST(order_date AS DATE) AS order_date,
# MAGIC   customer_id,
# MAGIC   region,
# MAGIC   country,
# MAGIC   city,
# MAGIC   channel,
# MAGIC   sku,
# MAGIC   category,
# MAGIC   TRY_CAST(qty AS INT) AS qty,
# MAGIC   TRY_CAST(unit_price AS DOUBLE) AS unit_price,
# MAGIC   TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
# MAGIC   TRY_CAST(total_amount AS DOUBLE) AS total_amount,
# MAGIC   coupon_code,
# MAGIC   order_status,
# MAGIC   TRY_CAST(shipping_cost AS DOUBLE) AS shipping_cost,
# MAGIC   source_file
# MAGIC
# MAGIC FROM STREAM dq_1_bronze.sales_bronze_raw_demo;
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Run and Validate the Bronze Layer
# MAGIC
# MAGIC 1. **Run the pipeline** and confirm that it completes successfully with no errors.
# MAGIC
# MAGIC 2. **Validate ingestion results** in the Lakeflow Pipelines editor:
# MAGIC    - Verify that **157 records** were ingested into the sales_bronze_raw_demo table from the first file
# MAGIC    - Select the **sales_bronze_raw_demo** table and open the **Data** tab to preview the ingested sales data
# MAGIC    - Confirm that new columns (`order_status`, `shipping_cost`) exist but contain NULL values
# MAGIC
# MAGIC **Troubleshooting:** If your pipeline fails, verify that:
# MAGIC - Volume paths are correctly configured in pipeline parameters
# MAGIC - The `source` configuration parameter matches your volume path
# MAGIC - Serverless compute is selected and running
# MAGIC
# MAGIC **Expected Checkpoint Results:**

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/dq/checkpoint_bronze.png" alt="Bronze Layer Results" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Query and Analyze Bronze Layer
# MAGIC
# MAGIC Run these queries to validate the bronze layer tables:
# MAGIC
# MAGIC - Confirm that all records are ingested into the raw bronze table
# MAGIC - Check that `schema_hints` worked successfully

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify record ingestion by source file (should show only sales_1.csv)
# MAGIC SELECT 
# MAGIC   source_file,
# MAGIC   COUNT(*) AS record_count,
# MAGIC   'Clean baseline data' AS data_profile
# MAGIC FROM dq_1_bronze.sales_bronze_raw_demo
# MAGIC GROUP BY source_file
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify schema evolution readiness
# MAGIC -- Total Column should be 21
# MAGIC DESCRIBE TABLE dq_1_bronze.sales_bronze_raw_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Create Silver Layer with Data Quality Expectations and Quarantine Logic
# MAGIC
# MAGIC - In this layer, we will first type-cast columns to their appropriate data formats.
# MAGIC - Next, we will apply data quality expectations.
# MAGIC - Finally, based on these expectations, we will separate the data into two tables: one for clean records and one for invalid records, using inverse logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Understanding the Data Quality Expectations
# MAGIC
# MAGIC Before creating the table with expectations, review what we check and why. You already know the `CONSTRAINT ... EXPECT ... ON VIOLATION` syntax and the WARN, DROP ROW and FAIL UPDATE actions.
# MAGIC
# MAGIC **Our strategy:** use **WARN** (the default) for all six expectations, so the pipeline keeps running and every record is retained for quarantine analysis.
# MAGIC
# MAGIC **Our 6 expectations, in three categories:**
# MAGIC - **Business keys (critical for data integrity):** `check_subsidiary_id`, `check_customer_id` and `check_sku` must be NOT NULL, so every order can be attributed to a subsidiary, a customer and a product.
# MAGIC - **Range-based business rules:** `valid_discount_range` (discount between 0% and 100%, which catches negative and impossible values) and `valid_date_range` (order date within the last 4 years, which catches archived data and future dates).
# MAGIC   - **NOTE:** Since our data is static, `valid_date_range` is checked against **01-01-2026**.
# MAGIC - **Schema evolution:** `valid_shipping_cost` (shipping cost between $0 and $100) applies to a column that is not in the first file. Once the column appears, the expectation enforces it automatically. For details, see [Schema Evolution Pattern](https://docs.databricks.com/aws/en/ldp/expectation-patterns?language=SQL#schema-evolution-pattern).

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Understanding the Quarantine Records Pattern
# MAGIC
# MAGIC You saw this pattern in the lecture. Records that violate expectations are routed to a separate quarantine table for analysis and remediation instead of being dropped, so **no data is lost**. The primary table keeps every record and uses WARN expectations to log violations, inverse logic (a NOT of all the rules) flags the failed records, and valid and invalid records then flow to separate tables.
# MAGIC
# MAGIC > **Learn More:**  
# MAGIC > For more on quarantine frameworks and best practices, see the [Databricks documentation on expectation patterns and quarantining invalid records](https://docs.databricks.com/aws/en/ldp/expectation-patterns?language=Python%20Module#quarantine-invalid-records).

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Create a New SQL File in your Pipeline
# MAGIC
# MAGIC In your `dq_pipeline` folder, open the kebab menu, select **Create file**, choose **SQL**, and name it `silver_transformation.sql`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Create Quarantine Table with Quality Expectations
# MAGIC
# MAGIC
# MAGIC Add the following code to your `silver_transformation.sql` file. This creates the main quarantine table with all 6 data quality expectations defined.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- SILVER LAYER - STEP 1: QUARANTINE TABLE WITH EXPECTATIONS
# MAGIC -- Purpose: Define table structure and 6 data quality expectations
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_dq_demo
# MAGIC (
# MAGIC   -- Business columns with proper data types
# MAGIC   subsidiary_id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_timestamp TIMESTAMP,
# MAGIC   order_date DATE,
# MAGIC   customer_id STRING,
# MAGIC   region STRING,
# MAGIC   country STRING,
# MAGIC   city STRING,
# MAGIC   channel STRING,
# MAGIC   sku STRING,
# MAGIC   category STRING,
# MAGIC   qty INT,
# MAGIC   unit_price DOUBLE,
# MAGIC   discount_pct DOUBLE,
# MAGIC   total_amount DOUBLE,
# MAGIC   coupon_code STRING,
# MAGIC   order_status STRING,
# MAGIC   shipping_cost DOUBLE,
# MAGIC   source_file STRING,
# MAGIC
# MAGIC   -- Quality tracking columns for quarantine analysis
# MAGIC   is_quarantined BOOLEAN,
# MAGIC   quarantine_reason STRING,
# MAGIC
# MAGIC   --========================================================================
# MAGIC   -- 6 DATA QUALITY EXPECTATIONS (WARN ACTION FOR QUARANTINE PATTERN)
# MAGIC   --========================================================================
# MAGIC
# MAGIC   CONSTRAINT check_subsidiary_id 
# MAGIC     EXPECT (subsidiary_id IS NOT NULL),
# MAGIC
# MAGIC   CONSTRAINT check_customer_id 
# MAGIC     EXPECT (customer_id IS NOT NULL),
# MAGIC
# MAGIC   CONSTRAINT check_sku 
# MAGIC     EXPECT (sku IS NOT NULL),
# MAGIC
# MAGIC   CONSTRAINT valid_discount_range
# MAGIC     EXPECT (discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100)),
# MAGIC
# MAGIC   -- Since the data is static, we check that order_date falls between 01-01-2026 and 4 years prior, instead of using the current date.
# MAGIC   CONSTRAINT valid_date_range 
# MAGIC     EXPECT (order_date IS NULL OR 
# MAGIC         (order_date >= DATE_SUB(DATE '2026-01-01', 1460) AND 
# MAGIC           order_date <= DATE '2026-01-01')),
# MAGIC
# MAGIC   CONSTRAINT valid_shipping_cost EXPECT (
# MAGIC     CASE WHEN shipping_cost IS NOT NULL THEN shipping_cost > 0 AND shipping_cost < 100 ELSE TRUE END
# MAGIC   )
# MAGIC )
# MAGIC COMMENT "Quarantine table with 6 expectations - supports inverse logic pattern"
# MAGIC TBLPROPERTIES (
# MAGIC   'quality.layer' = 'silver_quarantine',
# MAGIC   'quality.pattern' = 'inverse_logic'
# MAGIC )
# MAGIC PARTITIONED BY (is_quarantined);
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the Expectation Framework:**
# MAGIC
# MAGIC **Expectations Summary:**
# MAGIC
# MAGIC | # | Expectation Name | Validates | Range/Rule | Violation Example |
# MAGIC |---|-----------------|-----------|------------|---------------------|
# MAGIC | 1 | check_subsidiary_id | Business Key | NOT NULL | Missing subsidiary IDs |
# MAGIC | 2 | check_customer_id | Business Key | NOT NULL | Missing customer IDs |
# MAGIC | 3 | check_sku | Business Key | NOT NULL | Missing SKUs |
# MAGIC | 4 | valid_discount_range | Discount | 0%-100% | discount = -10.73% or 120% |
# MAGIC | 5 | valid_date_range | Date | Last 4 years | date = 1930-12-31 |
# MAGIC | 6 | valid_shipping_cost | Shipping | $0-$100 | shipping_cost = -5 or 150 |
# MAGIC
# MAGIC **Quality Tracking Design:**
# MAGIC - **is_quarantined**: Boolean flag indicating whether record failed any expectation
# MAGIC - **quarantine_reason**: Detailed string listing all failed validations for remediation
# MAGIC - **Partitioning**: Separates quarantined and valid data for performance optimization
# MAGIC
# MAGIC **Table Properties:**
# MAGIC - Metadata tags identify this as a quarantine pattern implementation
# MAGIC - Enables monitoring and governance tools to recognize the quality framework

# COMMAND ----------

# MAGIC %md
# MAGIC ### E5. Create Inverse Logic Flow for Quarantine Population
# MAGIC
# MAGIC Add the inverse logic flow to populate the quarantine table with quality tracking information. This flow implements the official Databricks quarantine pattern by using inverse logic to identify and flag records that fail any quality expectations.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- SILVER LAYER - STEP 2: FLOW TO APPLY INVERSE LOGIC
# MAGIC -- Purpose: Calculate is_quarantined flag and populate quarantine table
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE FLOW apply_inverse_logic_flow
# MAGIC AS
# MAGIC INSERT INTO dq_2_silver.sales_silver_dq_demo BY NAME
# MAGIC SELECT
# MAGIC   -- Pass through all business columns
# MAGIC   subsidiary_id,
# MAGIC   order_id,
# MAGIC   order_timestamp,
# MAGIC   order_date,
# MAGIC   customer_id,
# MAGIC   region,
# MAGIC   country,
# MAGIC   city,
# MAGIC   channel,
# MAGIC   sku,
# MAGIC   category,
# MAGIC   qty,
# MAGIC   unit_price,
# MAGIC   discount_pct,
# MAGIC   total_amount,
# MAGIC   coupon_code,
# MAGIC   order_status,
# MAGIC   shipping_cost,
# MAGIC   source_file,
# MAGIC
# MAGIC   --========================================================================
# MAGIC   -- INVERSE LOGIC: Mark as quarantined if ANY expectation fails
# MAGIC   --========================================================================
# MAGIC   NOT (
# MAGIC     (subsidiary_id IS NOT NULL) AND
# MAGIC     (customer_id IS NOT NULL) AND
# MAGIC     (sku IS NOT NULL) AND
# MAGIC     (discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100)) AND
# MAGIC     (order_date IS NULL OR 
# MAGIC      (order_date >= DATE_SUB(DATE '2026-01-01', 1460) AND 
# MAGIC       order_date <= DATE '2026-01-01')) AND
# MAGIC     (shipping_cost IS NULL OR (shipping_cost > 0 AND shipping_cost < 100))
# MAGIC   ) AS is_quarantined,
# MAGIC
# MAGIC   --========================================================================
# MAGIC   -- BUILD DETAILED QUARANTINE REASON STRING
# MAGIC   --========================================================================
# MAGIC   CONCAT_WS('; ',
# MAGIC     CASE WHEN subsidiary_id IS NULL 
# MAGIC          THEN 'Missing subsidiary_id' END,
# MAGIC     CASE WHEN customer_id IS NULL 
# MAGIC          THEN 'Missing customer_id' END,
# MAGIC     CASE WHEN sku IS NULL 
# MAGIC          THEN 'Missing sku' END,
# MAGIC     CASE WHEN discount_pct IS NOT NULL AND (discount_pct < 0 OR discount_pct > 100) 
# MAGIC          THEN 'Invalid discount_pct (must be 0-100)' END,
# MAGIC     CASE WHEN order_date IS NOT NULL AND 
# MAGIC               (order_date < DATE_SUB(DATE '2026-01-01', 1460) OR order_date > DATE '2026-01-01') 
# MAGIC          THEN 'Invalid order_date (outside 4-year range)' END,
# MAGIC     CASE WHEN shipping_cost IS NOT NULL AND (shipping_cost <= 0 OR shipping_cost >= 100)
# MAGIC          THEN 'Invalid shipping_cost (must be 0-100)' END
# MAGIC   ) AS quarantine_reason
# MAGIC
# MAGIC FROM STREAM dq_1_bronze.sales_bronze_clean_demo;
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the Inverse Logic Implementation:**
# MAGIC
# MAGIC **Quarantine Flag Logic:**
# MAGIC - **NOT()** wrapper around all expectations combined with AND logic
# MAGIC - If ANY expectation fails, the record is marked `is_quarantined = TRUE`
# MAGIC - If ALL expectations pass, the record is marked `is_quarantined = FALSE`
# MAGIC
# MAGIC **Detailed Failure Tracking:**
# MAGIC - **CONCAT_WS()** builds semicolon-separated failure reasons
# MAGIC - Each CASE statement checks for specific violation types
# MAGIC - Provides actionable information for data remediation teams
# MAGIC - Empty string for records that pass all validations
# MAGIC
# MAGIC **Streaming Flow Benefits:**
# MAGIC - Processes records continuously as they arrive
# MAGIC - Maintains low latency for real-time quality monitoring
# MAGIC - Enables immediate quarantine identification for operational workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ### E6. Create Separate Tables for Valid and Quarantined Records
# MAGIC
# MAGIC Create separate tables for valid and quarantined records to enable zero data loss and parallel processing workflows. Add this code to your `silver_transformation.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- SILVER LAYER - STEP 3: SEPARATE VALID AND INVALID DATA PATHS
# MAGIC --========================================================================
# MAGIC
# MAGIC --========================================================================
# MAGIC -- VALID RECORDS PATH - Clean data for analytics
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_valid_demo
# MAGIC COMMENT "Clean records passing all 6 quality checks - ready for analytics"
# MAGIC AS
# MAGIC SELECT * EXCEPT (is_quarantined, quarantine_reason)
# MAGIC FROM STREAM dq_2_silver.sales_silver_dq_demo
# MAGIC WHERE is_quarantined = FALSE;
# MAGIC
# MAGIC --========================================================================
# MAGIC -- QUARANTINED RECORDS PATH - Invalid data for remediation
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_quarantined_demo
# MAGIC COMMENT "Invalid records with quality violations - requires remediation"
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM STREAM dq_2_silver.sales_silver_dq_demo
# MAGIC WHERE is_quarantined = TRUE;
# MAGIC
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the code:**
# MAGIC - The source table keeps every record, because its WARN expectations only log violations.
# MAGIC - **Valid path:** `sales_silver_valid_demo` holds the records where `is_quarantined = FALSE`. **Quarantine path:** `sales_silver_quarantined_demo` holds the records where `is_quarantined = TRUE`.
# MAGIC - Total records = valid records + quarantined records, so no data is lost. Analytics reads only the valid table, and remediation works from the quarantined table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Create Gold Layer - Production-Ready Analytics Data
# MAGIC
# MAGIC The gold layer contains pre-aggregated analytics from validated silver data, providing fast query performance for business intelligence and reporting applications.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Create Gold Layer Analytics Table
# MAGIC
# MAGIC In your `dq_pipeline` folder, create a new SQL file named `gold_analytics.sql` (kebab menu → **Create file** → **SQL**) and paste the code below.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC --========================================================================
# MAGIC -- GOLD LAYER: MATERIALIZED VIEW FOR BUSINESS ANALYTICS
# MAGIC --========================================================================
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dq_3_gold.sales_analytics_demo
# MAGIC COMMENT "Business metrics aggregated from validated sales data"
# MAGIC AS
# MAGIC SELECT
# MAGIC   region,
# MAGIC   country,
# MAGIC   category,
# MAGIC   COUNT(DISTINCT order_id) AS total_orders,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC   SUM(qty) AS total_quantity_sold,
# MAGIC   ROUND(SUM(total_amount), 2) AS total_revenue,
# MAGIC   ROUND(AVG(total_amount), 2) AS avg_order_value,
# MAGIC   ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
# MAGIC   MIN(order_date) AS earliest_order_date,
# MAGIC   MAX(order_date) AS latest_order_date,
# MAGIC   CURRENT_TIMESTAMP() AS last_refreshed_at
# MAGIC FROM dq_2_silver.sales_silver_valid_demo
# MAGIC GROUP BY region, country, category;
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
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the code:** The gold materialized view pre-computes orders, unique customers, quantity, revenue, average order value, average discount and order date range by region, country and category. It reads only the **valid** silver table, so quarantined records never reach analytics, and it refreshes automatically as new valid data arrives.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Run and Analyze Pipeline Results - First Run (Clean Baseline)
# MAGIC
# MAGIC Execute the complete pipeline and analyze the results to validate that the quality framework correctly processes clean baseline data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Execute the Complete Pipeline
# MAGIC
# MAGIC 1. **Run the pipeline** and confirm that it completes successfully with no errors
# MAGIC 2. **Observe the pipeline graph** to see data flowing through all layers: Bronze → Silver → Gold
# MAGIC 3. **Monitor execution time** and resource utilization in the pipeline UI
# MAGIC
# MAGIC **Troubleshooting:** If your pipeline fails, verify that:
# MAGIC - All SQL files are properly created in the `dq_pipeline` folder
# MAGIC - Configuration parameters are correctly set
# MAGIC - Serverless compute is selected and running
# MAGIC - Volume paths are accessible and contain data files
# MAGIC
# MAGIC **Expected Pipeline Graph:**

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/dq/checkpoint_run_1.png" alt="Complete Pipeline Run 1" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Validate First Run Results - Clean Baseline
# MAGIC
# MAGIC Review the pipeline execution to confirm that all data quality checks passed and the pipeline established a perfect baseline. This run demonstrates that the pipeline framework correctly ingests, transforms, and validates clean data.
# MAGIC
# MAGIC **Expected Results for Run 1 (Clean Baseline):**
# MAGIC
# MAGIC | Table | Expected Records | Quality Status | Notes |
# MAGIC |-------|------------------|----------------|-------|
# MAGIC | Bronze (sales_bronze_raw_demo) | 157 | All ingested | Raw data from sales_1.csv |
# MAGIC | Silver (sales_bronze_clean_demo) | 157 | Type-cast success | All conversions successful |
# MAGIC | Silver (sales_silver_dq_demo) | 157 | All tracked | All records with is_quarantined = FALSE |
# MAGIC | Silver (sales_silver_valid_demo) | 157 | 100% quality | Perfect baseline established |
# MAGIC | Silver (sales_silver_quarantined_demo) | 0 | No violations | Quality framework validated |
# MAGIC | Gold (sales_analytics_demo) | 55 | Aggregated | Business metrics by region/category |
# MAGIC
# MAGIC **Validation Steps in Pipeline UI:**
# MAGIC 1. Select the **sales_silver_dq_demo** table and open the **Expectations** tab to verify "6 constraints tracked"
# MAGIC 2. Open the **Data** tab to confirm all records have `is_quarantined = FALSE`
# MAGIC 3. Check the **sales_silver_quarantined_demo** table to ensure it contains 0 records
# MAGIC 4. Verify the **sales_analytics_demo** gold table contains aggregated business metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Query and Analyze Results
# MAGIC
# MAGIC Execute the following queries to verify silver and gold layers result:
# MAGIC - Records in **sales_silver_quarantined_demo** should be zero
# MAGIC - Quality Score should be **100%**
# MAGIC - Querying on **sales_analytics_demo** MV for insight

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify quarantine is empty for Run 1 (should return 0 - perfect quality baseline)
# MAGIC SELECT 
# MAGIC   COUNT(*) AS quarantined_count,
# MAGIC   'Expected: 0 records for clean baseline' AS validation_note
# MAGIC FROM dq_2_silver.sales_silver_quarantined_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all records passed quality checks
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_records,
# MAGIC   SUM(CASE WHEN is_quarantined = FALSE THEN 1 ELSE 0 END) AS valid_records,
# MAGIC   SUM(CASE WHEN is_quarantined = TRUE THEN 1 ELSE 0 END) AS quarantined_records,
# MAGIC   ROUND(SUM(CASE WHEN is_quarantined = FALSE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS quality_score_pct
# MAGIC FROM dq_2_silver.sales_silver_dq_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Querying on sales_analytics materailized view
# MAGIC SELECT * FROM dq_3_gold.sales_analytics_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Prepare and Execute Second Run - Quality Issues and Schema Evolution
# MAGIC
# MAGIC Introduce the second data file containing quality issues and new schema columns to demonstrate the pipeline's resilience and monitoring capabilities.

# COMMAND ----------

# MAGIC %md
# MAGIC ### H1. Land the Second File
# MAGIC
# MAGIC Copy the second file (`sales_2.csv`) from the **ops** location to the **sales** location, then list the **sales** volume to confirm both files are there. The pipeline will process the new file incrementally.

# COMMAND ----------

ops_path = f'/Volumes/{my_catalog}/dq_1_bronze/ops'
sales_path = f'/Volumes/{my_catalog}/dq_1_bronze/sales'

# Copy files from ops location to user's source volume (now including sales_2.csv)
copy_files(copy_from=ops_path, copy_to=sales_path, n=2)

# Confirm both files are in the sales volume
spark.sql(f"LIST '{sales_path}'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### H2. Execute Pipeline Run 2 - Quality Issues and Schema Evolution
# MAGIC
# MAGIC 1. **Run the pipeline again** and confirm that it completes successfully despite quality issues
# MAGIC 2. **Observe incremental processing** as the pipeline detects and processes the new file
# MAGIC 3. **Monitor quality metrics** in the pipeline UI to see expectation violations being tracked
# MAGIC
# MAGIC **Expected Behavior:**
# MAGIC - Pipeline continues running despite quality violations (WARN action)
# MAGIC - New columns are automatically incorporated (schema evolution)
# MAGIC - Invalid records are systematically quarantined with detailed failure reasons
# MAGIC - Valid records continue to flow to analytics tables
# MAGIC
# MAGIC **Expected Pipeline Results:**

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/dq/checkpoint_run_2.png" alt="Pipeline Run 2 with Quality Issues" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3. Analyze Run 2 Results - Quality Issues Detected
# MAGIC
# MAGIC Review the pipeline execution to see how quality issues and schema evolution were successfully handled by the quarantine framework.
# MAGIC
# MAGIC **Expected Results for Run 2 (Incremental with Issues):**
# MAGIC
# MAGIC | Table | New Records | Total Records | Quality Notes |
# MAGIC |-------|-------------|---------------|---------------|
# MAGIC | Bronze (sales_bronze_raw_demo) | +60 | 217 | Schema evolved! (2 new columns populated) |
# MAGIC | Bronze (sales_bronze_clean_demo) | +60 | 217 | Type casting successful for all records |
# MAGIC | Silver (sales_silver_dq_demo) | +60 | 217 | All records tracked with quality flags |
# MAGIC | Silver (sales_silver_valid_demo) | +51 | 208 | 9 records failed expectations |
# MAGIC | Silver (sales_silver_quarantined_demo) | +9 | 9 | Invalid records captured with failure details |
# MAGIC | Gold (sales_analytics_demo) | Updated | 60 | Metrics updated with new valid data |
# MAGIC
# MAGIC **Key Validation Points:**
# MAGIC 1. **Zero Data Loss**: Bronze (217) = Valid (208) + Quarantined (9)
# MAGIC 2. **Schema Evolution**: New columns populated automatically
# MAGIC 3. **Quality Detection**: Specific violations identified and quarantined
# MAGIC 4. **Pipeline Resilience**: Continues processing despite quality issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### H4. Analyze the Quality Results
# MAGIC
# MAGIC Run the queries below to analyze the quality results of both files:
# MAGIC 1. **Overall quality and zero data loss:** total ingested = valid + quarantined.
# MAGIC 2. **Quarantined records:** the specific failure reason for each record.
# MAGIC 3. **Quality by source file:** the clean baseline file compared with the file that has quality issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall quality metrics and zero data loss verification
# MAGIC WITH quality_stats AS (
# MAGIC   SELECT 
# MAGIC     (SELECT COUNT(*) FROM dq_1_bronze.sales_bronze_raw_demo) AS total_ingested,
# MAGIC     (SELECT COUNT(*) FROM dq_2_silver.sales_silver_valid_demo) AS total_valid,
# MAGIC     (SELECT COUNT(*) FROM dq_2_silver.sales_silver_quarantined_demo) AS total_quarantined
# MAGIC )
# MAGIC SELECT 
# MAGIC   total_ingested,
# MAGIC   total_valid,
# MAGIC   total_quarantined,
# MAGIC   ROUND(total_valid * 100.0 / total_ingested, 2) AS quality_score_pct,
# MAGIC   ROUND(total_quarantined * 100.0 / total_ingested, 2) AS failure_rate_pct,
# MAGIC   CASE WHEN total_ingested = total_valid + total_quarantined 
# MAGIC        THEN '✅ ZERO DATA LOSS VERIFIED' 
# MAGIC        ELSE '❌ DATA LOSS DETECTED' 
# MAGIC   END AS data_loss_check
# MAGIC FROM quality_stats;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze specific quarantined records with detailed failure reasons
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   discount_pct,
# MAGIC   order_date,
# MAGIC   shipping_cost,
# MAGIC   source_file,
# MAGIC   quarantine_reason
# MAGIC FROM dq_2_silver.sales_silver_quarantined_demo
# MAGIC ORDER BY quarantine_reason, order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality score analysis by source file
# MAGIC SELECT 
# MAGIC   COALESCE(b.source_file, q.source_file) AS source_file,
# MAGIC   COUNT(DISTINCT COALESCE(b.order_id, q.order_id)) AS total_records,
# MAGIC   COUNT(DISTINCT s.order_id) AS valid_records,
# MAGIC   COUNT(DISTINCT q.order_id) AS quarantined_records,
# MAGIC   ROUND(COUNT(DISTINCT s.order_id) * 100.0 / 
# MAGIC         NULLIF(COUNT(DISTINCT COALESCE(b.order_id, q.order_id)), 0), 2) AS quality_score_pct,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(b.source_file, q.source_file) LIKE '%sales_1%' THEN 'Clean baseline data'
# MAGIC     WHEN COALESCE(b.source_file, q.source_file) LIKE '%sales_2%' THEN 'Quality issues + schema evolution'
# MAGIC     ELSE 'Unknown file pattern'
# MAGIC   END AS file_profile
# MAGIC FROM dq_1_bronze.sales_bronze_raw_demo b
# MAGIC FULL OUTER JOIN dq_2_silver.sales_silver_valid_demo s 
# MAGIC   ON b.order_id = s.order_id
# MAGIC FULL OUTER JOIN dq_2_silver.sales_silver_quarantined_demo q 
# MAGIC   ON COALESCE(b.order_id, s.order_id) = q.order_id
# MAGIC GROUP BY COALESCE(b.source_file, q.source_file)
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %md
# MAGIC **Key observations:**
# MAGIC - **Zero data loss:** bronze (217) = valid (208) + quarantined (9).
# MAGIC - **Specific failure reasons:** each quarantined record lists why it failed (invalid discount, order date outside the 4-year range, invalid shipping cost or missing customer_id), so teams can prioritize remediation.
# MAGIC - **Quality by file:** `sales_1.csv` scores 100%, `sales_2.csv` scores 85% (51 of 60), and the combined score is 95.85% (208 of 217).

# COMMAND ----------

# MAGIC %md
# MAGIC ### H5. Validate Schema Evolution
# MAGIC
# MAGIC Verify that schema evolution worked without any pipeline code changes. Records from the first file have NULL values for the new columns (**shipping_cost** and **order_status**), while records from the second file have populated values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dq_2_silver.sales_silver_valid_demo

# COMMAND ----------

# MAGIC %md
# MAGIC **What this shows:**
# MAGIC - The schema grew from 16 to 18 business columns without any pipeline code changes, because the schema hints in the bronze table already declared the new columns.
# MAGIC - Records from `sales_1.csv` keep NULL in the new columns, and records from `sales_2.csv` populate them.
# MAGIC - The new `shipping_cost` column is automatically included in the quality checks (`valid_shipping_cost`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Summary and Key Takeaways
# MAGIC
# MAGIC **Data Quality Management:**
# MAGIC - Successfully implemented 6 comprehensive data quality expectations covering business keys, numeric ranges, and temporal validations
# MAGIC - Achieved 100% quality score on clean baseline data (Run 1: 157/157 records)
# MAGIC - Detected and quarantined 9 quality violations in problematic data (Run 2: 51/60 records passed)
# MAGIC - Overall pipeline quality score: 95.85% (208/217 total records processed)
# MAGIC
# MAGIC **Schema Evolution:**
# MAGIC - Seamlessly handled schema evolution from 16 to 18 core business columns from source without pipeline code changes
# MAGIC - Bronze layer STRING strategy enabled backward compatibility and forward flexibility
# MAGIC - New business columns (**order_status, shipping_cost**) automatically integrated
# MAGIC
# MAGIC **Zero Data Loss Quarantine:**
# MAGIC - Implemented official Databricks inverse logic pattern for quarantine processing
# MAGIC - Achieved mathematical zero data loss: Bronze Records = Valid Sales Records + Quarantine Sales Records
# MAGIC - Provided detailed failure analysis and remediation insights for each quarantined record
# MAGIC
# MAGIC ### Production Readiness
# MAGIC
# MAGIC This demonstration showcased enterprise-grade patterns that provide:
# MAGIC - **Robust data quality controls** that catch anomalies before they reach analytics
# MAGIC - **Flexible schema handling** that adapts to evolving business requirements
# MAGIC - **Complete data lineage** with zero loss and full audit capabilities
# MAGIC - **Actionable quality insights** for continuous data improvement

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>