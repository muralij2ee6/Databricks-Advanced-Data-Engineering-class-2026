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

# MAGIC %md-sandbox
# MAGIC # Demo - Automating SCD Type 2 with AUTO CDC in Apache Spark Declarative Pipelines
# MAGIC
# MAGIC #### Duration: ~38-47 Minutes
# MAGIC
# MAGIC ## Overview 
# MAGIC
# MAGIC This demonstration showcases how to implement automated Change Data Capture (CDC) for Slowly Changing Dimension (SCD) Type 2 patterns using Apache Spark™ Declarative Pipelines. 
# MAGIC
# MAGIC Learners will learn to build an end-to-end streaming pipeline that automatically processes customer data changes (inserts, updates, and deletes) while maintaining complete historical records. The demo uses real-world retail customer data from Databricks Marketplace and demonstrates how AUTO CDC INTO simplifies complex CDC operations that traditionally required manual coding. 
# MAGIC
# MAGIC Learners will create a multi-layered architecture (Bronze → Silver → Gold) with automated data quality checks, incremental processing, and materialized views for both current and historical customer analytics.
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC
# MAGIC - **Configure and implement AUTO CDC INTO** for automated Change Data Capture processing in Apache Spark™ Declarative Pipelines with SCD Type 2 patterns
# MAGIC
# MAGIC - **Design multi-layered data architectures** using Bronze (raw ingestion), Silver (CDC processing), and Gold (analytics-ready) layers with streaming tables and materialized views
# MAGIC
# MAGIC - **Apply comprehensive data quality constraints** using pipeline expectations with WARN, DROP, and FAIL actions to ensure data integrity across CDC operations
# MAGIC
# MAGIC - **Process streaming JSON files incrementally** using Auto Loader and manage customer lifecycle events (INSERT, UPDATE, DELETE) while preserving historical versions
# MAGIC
# MAGIC - **Create analytics ready materialized views** that automatically maintain current customer states and track removed customers for downstream reporting and business intelligence
# MAGIC
# MAGIC
# MAGIC ### CDC Pipeline Overview
# MAGIC
# MAGIC In this demonstration, you'll build a Apache Spark™ Declarative Pipeline that performs the full medallion flow from raw ingestion to curated analytics:
# MAGIC
# MAGIC 1. **Ingest JSON source files** from cloud storage into a **Bronze raw streaming table** using Auto Loader.  
# MAGIC 2. **Apply data quality expectations** to the raw table to produce a **clean Bronze table** ready for downstream processing.  
# MAGIC 3. **Implement CDC SCD Type 2** logic with `AUTO CDC INTO` to maintain an up-to-date **Silver customer table** with full change history.  
# MAGIC 4. **Create Gold materialized views** for **current customers** and **deleted customers**, enabling simplified analytics and reporting.
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1440 540" width="100%" role="img" aria-label="CDC pipeline overview: JSON files feed a raw and a clean bronze table, then an SCD Type 2 silver table and two gold materialized views" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif;max-width:1440px;display:block;margin:12px auto;"><defs><marker id="oc-ah-g" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#7C8B94"/></marker><marker id="oc-ah-n" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B3139"/></marker><marker id="oc-ah-t" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B5062"/></marker><filter id="oc-sh" x="-8%" y="-8%" width="116%" height="130%"><feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#0B2026" flood-opacity="0.13"/></filter></defs><text x="14" y="30" font-size="20" font-weight="400" fill="#1B3139" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Cloud Storage</text><rect x="10" y="42" width="168" height="400" rx="18" fill="#fff" stroke="#9AA5AB" stroke-width="1.6" /><rect x="24" y="62" width="140" height="360" rx="8" fill="none" stroke="#8C98A0" stroke-width="1.5" stroke-dasharray="5 4" /><text x="94" y="90" font-size="17" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Customers</text><text x="94" y="110" font-size="14.5" font-weight="400" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >JSON Files</text><g transform="translate(78,130) scale(1.0)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >JSON</text></g><text x="94" y="196" font-size="13" font-weight="400" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >File 1</text><g transform="translate(78,226) scale(1.0)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >JSON</text></g><text x="94" y="292" font-size="13" font-weight="400" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >File 2</text><g transform="translate(78,322) scale(1.0)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >JSON</text></g><text x="94" y="388" font-size="13" font-weight="400" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >File 3</text><path d="M166,281 H214" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#oc-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="216" y="218" width="34" height="31.0" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="258" y="244" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="216" y="258" width="214" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#oc-sh)"/><text x="323.0" y="286" font-size="12.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >customers_bronze_raw_demo</text><path d="M432,281 H468" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#oc-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="470" y="218" width="34" height="31.0" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="512" y="244" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="470" y="258" width="226" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#oc-sh)"/><text x="583.0" y="286" font-size="12.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >customers_bronze_clean_demo</text><path d="M698,281 H736" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#oc-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><text x="928" y="130" font-size="22" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Change Data Capture</text><text x="928" y="154" font-size="17" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >SCD Type 2</text><rect x="738" y="170" width="380" height="244" rx="0" fill="#F6F4F1" stroke="#8C98A0" stroke-width="1.2" /><svg x="752" y="218" width="34" height="31.0" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="794" y="244" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="752" y="258" width="352" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#oc-sh)"/><text x="928.0" y="286" font-size="14" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >customers_silver_scd2_demo</text><text x="928" y="342" font-size="15.5" text-anchor="middle" fill="#1B3139" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">Update customer information in</text><text x="928" y="365" font-size="15.5" text-anchor="middle" fill="#1B3139" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">the table (<tspan font-weight="700">inserts</tspan>, <tspan font-weight="700">updates</tspan> and</text><text x="928" y="388" font-size="15.5" text-anchor="middle" fill="#1B3139" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif"><tspan font-weight="700">deletes)</tspan> using <tspan font-weight="700">SCD Type 2</tspan></text><svg x="1160" y="80" width="34" height="34" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><rect x="32.015" y="26.436" width="10.5" height="3.5" transform="rotate(131 37.265 28.186)"/><rect x="32.5" y="31" width="15" height="3"/><rect x="37.485" y="35.064" width="10.5" height="3.5" transform="rotate(131 42.735 36.814)"/></svg><text x="1202" y="106" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Materialized View</text><rect x="1160" y="120" width="262" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#oc-sh)"/><text x="1291.0" y="148" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >current_customers_gold_demo</text><path d="M1120,281 H1127 V151 Q1127,143 1135,143 H1155" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#oc-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="1160" y="324" width="34" height="34" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><rect x="32.015" y="26.436" width="10.5" height="3.5" transform="rotate(131 37.265 28.186)"/><rect x="32.5" y="31" width="15" height="3"/><rect x="37.485" y="35.064" width="10.5" height="3.5" transform="rotate(131 42.735 36.814)"/></svg><text x="1202" y="350" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Materialized View</text><rect x="1160" y="364" width="262" height="46" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#oc-sh)"/><text x="1291.0" y="392" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >removed_customers_gold_demo</text><path d="M1120,281 H1127 V379 Q1127,387 1135,387 H1155" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#oc-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><path d="M216,452 H1064 V434 L1146,478 L1064,522 V504 H216 z" fill="#1B5062"/><text x="640" y="486" font-size="18" text-anchor="middle" fill="#fff" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif"><tspan font-weight="700">Incrementally</tspan> process <tspan font-weight="700">new data</tspan> in the streaming tables (ST)</text><rect x="1160" y="440" width="262" height="80" rx="0" fill="#1B5062" stroke="none" stroke-width="1" /><text x="1172" y="462" font-size="12.5" fill="#fff" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">Filter the <tspan font-weight="700">customers_silver_scd2_demo</tspan></text><text x="1172" y="482" font-size="12.5" fill="#fff" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">table to create the gold materialized views</text><text x="1172" y="502" font-size="12.5" fill="#fff" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">for <tspan font-weight="700">active</tspan> and <tspan font-weight="700">deleted</tspan> customers</text></svg>

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
# MAGIC ## A. REQUIRED - Classroom Setup
# MAGIC Follow the cells below to set up the notebook for your specific Workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC > **Workspace setup:** Using a Databricks Academy (Vocareum) workspace? Your catalog is created for you, so you can start. Using your own workspace or Databricks Free Edition? Complete **Course Overview → B. Workspace Setup** first (catalog creation and permissions, Marketplace data). Run this notebook only in a development or sandbox workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to initialize your environment. This setup step does the following:
# MAGIC
# MAGIC     - **Assumes you have permission to create a catalog** when running outside of a Databricks provided Vocareum workspace
# MAGIC
# MAGIC     - Creates a catalog named **labuser_user_name** when running outside of a Databricks Vocareum provided workspace  
# MAGIC       - Uses **labuser_idnumber** when running in Vocareum
# MAGIC
# MAGIC     - Create three schemas in your specified catalog:  
# MAGIC       - **sdp_cdc_1_bronze**
# MAGIC       - **sdp_cdc_2_silver**
# MAGIC       - **sdp_cdc_3_gold**  
# MAGIC     - Creates a volume named **YOUR_LABUSER_CATALOG.sdp_cdc_1_bronze.customer_source_files** and adds a single JSON file into the volume.
# MAGIC     - Checks your specified compute
# MAGIC
# MAGIC     This ensures that all schemas, tables and objects are created in your catalog.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-auto-cdc-demo

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Confirm the variable `my_vol_path` created in the classroom setup points to **your-catalog.sdp_cdc_1_bronze.customer_source_files** volume path.
# MAGIC
# MAGIC     **Example:** `/Volumes/YOUR_LABUSER_CATALOG/sdp_cdc_1_bronze/customer_source_files`

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Customer Data Source File(s) in Your Volume

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to programmatically view the files in your `/Volumes/your-catalog/sdp_cdc_1_bronze/customer_source_files/` volume **path**.
# MAGIC
# MAGIC     Confirm you only see one file for customers (**00.json**).

# COMMAND ----------

spark.sql(f'LIST "{my_vol_path}"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the query below to explore the customers **00.json** file located in your **customer_source_files** volume within **your-catalog.sdp_cdc_1_bronze** schema. 
# MAGIC
# MAGIC     Note the following:
# MAGIC
# MAGIC       a. The file contains **939 customers** (remember this number).
# MAGIC
# MAGIC       b. It includes general customer information such as **email**, **name**, and **address**.
# MAGIC
# MAGIC       c. The **timestamp** column specifies the logical order (sequence) of customer events in the source data as a UNIX timestamp.
# MAGIC
# MAGIC       d. The **operation** column indicates whether the entry is for a new customer, a deletion, or an update.
# MAGIC
# MAGIC       - **NOTE:** Since this is the first JSON file, all rows will be considered new customers.
# MAGIC
# MAGIC   **NOTE:** The `my_vol_path` SQL variable was created for you in the classroom setup.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   my_vol_path || '/00.json',  -- my_vol_path is the path to your source JSON files in the volume your-catalog.sdp_cdc_1_bronze.customer_source_files
# MAGIC   format => "JSON"
# MAGIC )
# MAGIC ORDER BY operation;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question
# MAGIC How can we ingest new raw JSON source files with customer updates into our pipeline to update a **silver table** when inserts, updates, or deletes occur, while also maintaining historical records (SCD Type 2)?
# MAGIC
# MAGIC ### Answer
# MAGIC Use `AUTO CDC INTO` with Spark Declarative Pipelines!
# MAGIC
# MAGIC View the Databricks documentation [Processing a change data feed: Keep only the latest data vs. keep historical versions of data](https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture#processing-a-change-data-feed-keep-only-the-latest-data-vs-keep-historical-versions-of-data) for more information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Building the CDC Spark Declarative Pipeline with `AUTO CDC INTO`

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
# MAGIC    - Name your pipeline `demo_auto_cdc_pipeline_yourname`
# MAGIC    - Select your default **catalog** and **schema**:  
# MAGIC         - **Catalog:** The catalog specified in the setup script (example: `labuser_xxx`) 
# MAGIC         - **Schema:** **sdp_cdc_1_bronze**  
# MAGIC       **NOTE:** Clear the selected schema using the cross icon to view all schemas.
# MAGIC
# MAGIC 4. Rename the `transformations` folder to `my_pipeline`.
# MAGIC
# MAGIC 5. Rename the `my_transformation.py` file to `cdc_pipeline.sql`.
# MAGIC
# MAGIC 6. Leave the **Lakeflow Pipelines Editor** page open.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ![Create SDP Checkpoint](https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/cdc_create_initial_pipeline.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Configure Pipeline Parameter
# MAGIC
# MAGIC 1. Run the cell below to get the path to your raw data source JSON files in your **customer_source_files** volume within your **labuser.sdp_cdc_1_bronze** schema.

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Copy the path above and add it as a configuration parameter in your **Spark Declarative Pipeline**. This will reference your **sdp_cdc_1_bronze.customer_source_files** volume that contains your raw JSON file(s) that you explored earlier:
# MAGIC
# MAGIC    a. Select **Settings** in your pipeline tab.
# MAGIC
# MAGIC    b. Under **Configuration** select **Add configuration**.
# MAGIC
# MAGIC    c. For **Key** add `source`
# MAGIC
# MAGIC    d. For **Value** add the `path to your volume` from above.
# MAGIC
# MAGIC    e. Select **Save**.
# MAGIC
# MAGIC    **NOTE:** For more information on configuration parameters, check out the Databricks documentation [Use parameters with Apache Spark™ Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/parameters)
# MAGIC
# MAGIC #### Checkpoint (your path will vary)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/cdc_config_parameter_value.png" alt="Config Parameter Checkpoint" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Create Bronze Layer with Auto Loader for Incremental Ingestion
# MAGIC
# MAGIC 1. In this step, you'll define the **Bronze raw** layer of your **Spark Declarative Pipeline** to create the table: **sdp_cdc_1_bronze.customers_bronze_raw_demo**.  
# MAGIC
# MAGIC    This layer will incrementally ingest raw **JSON** files from cloud storage into a **streaming table** using **Auto Loader**.  
# MAGIC
# MAGIC    Copy the SQL code below and paste it into your `cdc_pipeline.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ------------------------------------------------------
# MAGIC -- STEP 1: JSON -> Bronze Ingestion
# MAGIC ------------------------------------------------------
# MAGIC -- Ingest the JSON files from cloud storage into a streaming table using Auto Loader
# MAGIC CREATE OR REFRESH STREAMING TABLE sdp_cdc_1_bronze.customers_bronze_raw_demo
# MAGIC   COMMENT "Raw data from customers CDC feed"
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() processing_time,  -- Obtain the ingestion processing time for the rows
# MAGIC   _metadata.file_name as source_file    -- Obtain the file name of the record
# MAGIC FROM STREAM read_files(
# MAGIC   "${source}",  -- References your SDP parameter that points to your demonstration volume containing your JSON data source files
# MAGIC   format => "json");
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
# MAGIC **Review the code:**
# MAGIC
# MAGIC - `CREATE OR REFRESH STREAMING TABLE` - Creates a managed streaming table that automatically updates as new data arrives.  
# MAGIC - `COMMENT` - Describes the purpose of the table for easier discovery and documentation.  
# MAGIC - `current_timestamp()` - Captures the ingestion timestamp for data lineage and auditing.  
# MAGIC - `_metadata.file_name` - Adds the source file name for traceability and debugging.  
# MAGIC - `FROM STREAM read_files()` - Uses Auto Loader to incrementally read new JSON files from the specified (triggered or continuous)  `${source}` path you added in the previous step.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Select **Dry Run** to verify that your pipeline and SQL code are configured correctly.  
# MAGIC
# MAGIC    This will validate your settings without actually running the full pipeline.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC > **TROUBLESHOOTING:** If the dry run did not work, make sure you configured your `source` pipeline parameter correctly in the **Configure Pipeline Parameter** step above.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/cdc_dry_run_ingest.png" alt="JSON Ingest" width="1100">

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. Apply Data Quality Rules to the Bronze Table
# MAGIC
# MAGIC With the raw JSON CDC data now set up to stream into **sdp_cdc_1_bronze.customers_bronze_raw_demo**, we'll add another **bronze** quality layer before moving to **Silver**.  
# MAGIC
# MAGIC You'll create an intermediate table **sdp_cdc_1_bronze.customers_bronze_clean** and apply **data quality constraints** to filter and flag invalid records while preserving expected `NULL` values for `DELETE` operations.  
# MAGIC
# MAGIC In this step, you'll:  
# MAGIC - Apply **multiple constraints** to check key fields.  
# MAGIC - Use **WARN**, **DROP**, and **FAIL** actions to define how violations are handled.  
# MAGIC - Add **conditional logic** and **regex validation** inside constraints.  
# MAGIC
# MAGIC For details, see [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### C4.1 - About the Data Source
# MAGIC
# MAGIC    - The CDC feed contains `INSERT`, `UPDATE`, and `DELETE` operations for customer records. 
# MAGIC
# MAGIC    - `INSERT` and `UPDATE` events include valid values for all key columns.  
# MAGIC
# MAGIC    - `DELETE` events have `NULL` values for all non-key fields such as **name**, **email**, **address**, **city**, and **state**.  
# MAGIC
# MAGIC    **Example of a Dropped Record:**
# MAGIC
# MAGIC    | **address** | **city** | **customer_id** | **email** | **name** | **operation** | **state** |
# MAGIC    |--------------|-----------|------------------|-------------|------------|----------------|-------------|
# MAGIC    | `NULL` | `NULL` | `23617` | `NULL` | `NULL` | `DELETE` | `NULL` |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### C4.2 - Create the Clean Bronze Table Using Pipeline Data Quality Expectations
# MAGIC
# MAGIC 1. Now that we have a general understanding of the data, let's define the following **data quality constraints** that enforce those rules in a secondary Bronze table named **sdp_cdc_1_bronze.customers_bronze_clean_demo** before the data moves to the Silver stage.
# MAGIC
# MAGIC Below is a table of the data quality constraints:
# MAGIC
# MAGIC   | **Comment ID** | **Constraint** | **Rule / Expectation** | **Violation Action** | **Purpose** |
# MAGIC   |:------:|----------------|------------------------|----------------------|--------------|
# MAGIC   | **A** | **`valid_id`** | **customer_id** `IS NOT NULL` | **FAIL** | Fails the transaction if a record has a missing **customer_id**. *Will required manual intervention to fix it.* |
# MAGIC   | **B** | **`valid_operation`** | **operation** `IS NOT NULL` | **DROP ROW** | Drops any record missing an **operation** type. |
# MAGIC   | **C** | **`valid_name`** | **name** `IS NOT NULL OR operation = "DELETE"` | **WARN** (default) | Flags missing **name** values unless it's a **DELETE** operation. |
# MAGIC   | **D** | **`valid_address`** | All address fields (**address**, **city**, **state**, **zip_code**) must be `non-null` unless it's a **DELETE** | **WARN** (default) | Ensures complete address information for **INSERT** and **UPDATE** events. |
# MAGIC   | **E** | **`valid_email`** | **email** must match a valid format unless it's a **DELETE** | **DROP ROW** | Uses regex to validate the **email** format and drops invalid records. |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Copy the SQL code below and append it into your `cdc_pipeline.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC  ------------------------------------------------------
# MAGIC -- STEP 2: Bronze Raw -> Bronze Clean
# MAGIC ------------------------------------------------------
# MAGIC CREATE STREAMING TABLE sdp_cdc_1_bronze.customers_bronze_clean_demo
# MAGIC   (
# MAGIC
# MAGIC     -- A. Require a valid customer_id, fail the transaction if missing
# MAGIC     CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) 
# MAGIC       ON VIOLATION FAIL UPDATE,
# MAGIC
# MAGIC     -- B. Require a valid operation, drop any record with NULL operation
# MAGIC     CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) 
# MAGIC       ON VIOLATION DROP ROW,
# MAGIC
# MAGIC     -- C. Require name to be present unless the operation is DELETE
# MAGIC     CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = "DELETE"),
# MAGIC
# MAGIC     -- D. Require full address fields unless operation is DELETE
# MAGIC     CONSTRAINT valid_address EXPECT (
# MAGIC       (address IS NOT NULL 
# MAGIC        AND city IS NOT NULL 
# MAGIC        AND state IS NOT NULL 
# MAGIC        AND zip_code IS NOT NULL)
# MAGIC        OR operation = "DELETE"),
# MAGIC
# MAGIC     -- E. Require valid email format (regex), skip check for DELETE, drop invalid rows
# MAGIC     CONSTRAINT valid_email EXPECT (
# MAGIC       rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') 
# MAGIC       OR operation = "DELETE") 
# MAGIC       ON VIOLATION DROP ROW
# MAGIC   )
# MAGIC   COMMENT "Clean raw bronze data and apply quality constraints"
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime -- Convert unix timestamp
# MAGIC FROM STREAM sdp_cdc_1_bronze.customers_bronze_raw_demo;
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
# MAGIC **Review the code:**
# MAGIC
# MAGIC   - This query creates the **sdp_cdc_1_bronze.customers_bronze_clean_demo** table from the **sdp_cdc_1_bronze.customers_bronze_raw_demo** table.  
# MAGIC
# MAGIC   - It applies the **data quality constraints** defined above to ensure only valid CDC records progress to the next stage before reaching Silver.
# MAGIC
# MAGIC   - Also **Converts UNIX timestamp to readable timestamp** using `CAST(from_unixtime(timestamp) AS timestamp)` as `timestamp_datetime`.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Select **Dry Run** to verify that your pipeline and SQL code are configured correctly.  
# MAGIC
# MAGIC    This will validate your settings without actually running the full pipeline.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/cdc_dry_run_bronze_clean.png" alt="Bronze Clean" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### C5.Implement `AUTO CDC INTO` for the Silver Table
# MAGIC
# MAGIC Now that the **Bronze** layer (**sdp_cdc_1_bronze.customers_bronze_raw_demo -> sdp_cdc_1_bronze.customers_bronze_clean_demo**) data is clean, we'll create a **Silver** table that automatically applies inserts, updates, and deletes as new changes arrive.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Why `AUTO CDC INTO`
# MAGIC
# MAGIC Hand-written `MERGE INTO` logic for CDC is brittle: it has to handle inserts, updates and deletes, duplicate events and replays, late-arriving changes and the right match conditions, and it gets harder to maintain as rules change. `AUTO CDC INTO` handles this declaratively: you define the **keys**, the **delete condition** and the **sequencing column**, and the pipeline applies the changes incrementally. It defaults to **SCD Type 1**, and we will use **SCD Type 2** in this demonstration.
# MAGIC
# MAGIC We will complete this in two steps:
# MAGIC
# MAGIC 1. **Create an empty Silver target table** to store historical customer data using the SCD Type 2 pattern
# MAGIC 2. **Use `AUTO CDC INTO`** to automatically apply inserts, updates, and deletes as new changes arrive
# MAGIC
# MAGIC **NOTE:** For more details, see the official documentation: [**The AUTO CDC APIs: Simplify change data capture with Apache Spark™ Declarative Pipelines**](https://docs.databricks.com/aws/en/ldp/cdc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### C5.1 - Create an Empty Silver Target Table
# MAGIC Before we can use AUTO CDC INTO, we need to create the target table that will store our customer data with historical tracking.
# MAGIC
# MAGIC 1. The SQL command below creates or refreshes a **streaming Silver table** named **sdp_cdc_2_silver.customers_silver_scd2_demo**.  
# MAGIC
# MAGIC    This table will continuously receive incremental updates from the **customers_bronze_clean_demo** table as new change data arrives.  
# MAGIC
# MAGIC    Copy and paste the code below into your `cdc_pipeline.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ---------------------------------------------------------------------------------------
# MAGIC -- STEP 3: Processing CDC Data with AUTO CDC INTO
# MAGIC ---------------------------------------------------------------------------------------
# MAGIC
# MAGIC -- a. Create the streaming target table if it's not already created
# MAGIC CREATE OR REFRESH STREAMING TABLE sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC   COMMENT 'SCD Type 2 Historical Customer Data';
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
# MAGIC
# MAGIC - `CREATE OR REFRESH STREAMING TABLE` - Creates the table **sdp_cdc_2_silver.customers_silver_scd2_demo** if it doesn't exist, or refreshes it if it does, ensuring it's ready for streaming updates.
# MAGIC
# MAGIC - `COMMENT` - Adds a description to document the table's purpose, identifying it as an **SCD Type 2** table used to track historical customer changes over time.
# MAGIC
# MAGIC Once the Silver target table exists, we can automatically propagate changes from the cleaned Bronze table using Apache Spark™ Declarative Pipelines with the `AUTO CDC INTO` command.

# COMMAND ----------

# MAGIC %md
# MAGIC #### C5.2 - Perform SCD Type 2 with `AUTO CDC INTO`
# MAGIC
# MAGIC
# MAGIC 1. Before we run `AUTO CDC INTO`, let's take a closer look at the key columns in our cleaned Bronze table (**sdp_cdc_1_bronze.customers_bronze_clean_demo**). 
# MAGIC
# MAGIC     Understanding these columns helps explain how **Spark Declarative Pipelines** detects and applies changes across `INSERT`, `UPDATE`, and `DELETE` operations.
# MAGIC
# MAGIC     Below is a simplified preview of the data. We've included only the columns that are important for **Change Data Capture (CDC)**.
# MAGIC
# MAGIC | **customer_id** (*primary key*) | **name** | **email** | **operation** (*type of change*) | **timestamp_datetime** (*sequence column*) | **source_file** | ... |
# MAGIC |------------------|-----------|------------|----------------|------------------------|------------------|-----|
# MAGIC | 23056 | Brent Chavez | nelsonjoy@example.com | `NEW` | 2021-09-23T17:26:21.000+00:00 | 00.json | ... |
# MAGIC | 23057 | James Cruz | perkinsdeborah@example.net | `UPDATE` | 2021-09-23T18:21:45.000+00:00 | 00.json | ... |
# MAGIC | 23058 | Jennifer Christensen | jmccullough@example.net | `DELETE` | 2021-09-23T00:19:44.000+00:00 | 00.json | ... |
# MAGIC | ... | ... | ... | ... | ... | ... | ... |
# MAGIC
# MAGIC **Key Columns**
# MAGIC
# MAGIC - **customer_id**- Serves as the **primary key** used to identify each unique customer record. CDC logic depends on this column to determine which row should be `updated` or `deleted` in the target table.
# MAGIC
# MAGIC - **operation** - Indicates the type of change from the source system (`NEW`, `UPDATE`, or `DELETE`).  
# MAGIC   - `DELETE` operations remove the corresponding record from the target table.  
# MAGIC   - `NEW` and `UPDATE` operations are automatically processed and merged by `AUTO CDC` using the **customer_id** primary key column.
# MAGIC
# MAGIC - **timestamp_datetime** - Defines the **order of operations**. This ensures `updates` are applied in the correct sequence when multiple changes occur for the same record.
# MAGIC
# MAGIC - **source_file** - Helps trace which file each record came from, useful for debugging and validation during pipeline runs.
# MAGIC
# MAGIC Next, we'll use these columns to perform `AUTO CDC INTO`, allowing Spark Declarative Pipelines **to automatically manage incremental updates between Bronze and Silver**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. The `AUTO CDC INTO` command defines a **Spark Declarative Pipeline flow** that applies **SCD Type 2** Change Data Capture logic to keep the **Silver** table continuously up to date.  
# MAGIC
# MAGIC    It automatically manages how `INSERTS`, `UPDATES`, and `DELETES` are applied between the following:  
# MAGIC
# MAGIC    - `Source:` **sdp_cdc_1_bronze.customers_bronze_clean_demo** - The streaming Bronze table containing cleaned CDC records.  
# MAGIC
# MAGIC    - `Target:` **sdp_cdc_2_silver.customers_silver_scd2_demo** - The Silver table that stores the current and historical customer data.
# MAGIC
# MAGIC
# MAGIC     Copy and paste the code below into your `cdc_pipeline.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC -- b. Perform SCD Type 2 into the silver table
# MAGIC CREATE FLOW customers_scd_type_2_flow AS 
# MAGIC AUTO CDC INTO sdp_cdc_2_silver.customers_silver_scd2_demo  -- Target: Where processed records are stored
# MAGIC FROM STREAM sdp_cdc_1_bronze.customers_bronze_clean_demo   -- Source: Clean CDC records from Bronze layer
# MAGIC   KEYS (customer_id)                                       -- Primary key: Used to match records for updates/deletes
# MAGIC   APPLY AS DELETE WHEN operation = "DELETE"                -- Delete logic: Remove records marked as DELETE
# MAGIC   SEQUENCE BY timestamp_datetime                           -- Ordering: Ensures changes are applied in correct sequence
# MAGIC   COLUMNS * EXCEPT (timestamp, _rescued_data, operation)   -- Column selection: Include all except metadata fields
# MAGIC   STORED AS SCD TYPE 2;                                    -- SCD Type 2: Maintains historical versions with __START_AT and __END_AT
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
# MAGIC
# MAGIC - `CREATE FLOW customers_scd_type_2_flow AS` - Creates a named flow (`customers_scd_type_2_flow`) that defines how CDC changes will be processed.  
# MAGIC - `AUTO CDC INTO sdp_cdc_2_silver.customers_silver_scd2_demo` - Applies the CDC logic to the **target** Silver table.  
# MAGIC - `FROM STREAM sdp_cdc_1_bronze.customers_bronze_clean_demo` - Reads the streaming **source data** that includes new inserts, updates, and deletes.  
# MAGIC - `KEYS (customer_id)` - Identifies the unique customer record by its primary key.  
# MAGIC - `APPLY AS DELETE WHEN operation = "DELETE"` - Ensures records with a delete operation are removed from the target.  
# MAGIC - `SEQUENCE BY timestamp_datetime` - Orders incoming records so late-arriving data is processed correctly.  
# MAGIC - `COLUMNS * EXCEPT (timestamp, _rescued_data, operation)` - Selects all columns from the source except metadata or system fields.  
# MAGIC - `STORED AS SCD TYPE 2` - Specifies the **Slowly Changing Dimension Type 2** method, which keeps the historical versions of changed records.
# MAGIC
# MAGIC - **NOTE:** For more information view the Databricks documentation [AUTO CDC INTO (Apache Spark™ Declarative Pipelines)](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-apply-changes-into).

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Select **Dry Run** to verify that your pipeline and SQL code are configured correctly.  
# MAGIC
# MAGIC    This will validate your settings without actually running the full pipeline.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/cdc_dry_run_auto_cdc_run1.png" alt="AUTO CDC" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Pipeline Execution and Analysis (Run with 1 JSON File)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Run the Spark Declarative Pipeline
# MAGIC
# MAGIC 1. In the **Lakeflow Pipelines Editor**, run your **Spark Declarative Pipeline**. The first run may take a few minutes.  
# MAGIC
# MAGIC 2. Once the pipeline finishes, verify it looks similar to the example below.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/cdc_01_pipeline-run.png" alt="Pipeline Run 1" width="1200">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. Quick Review: AUTO CDC INTO (SCD Type 2 Behavior)
# MAGIC
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1020 566" width="100%" role="img" aria-label="SCD Type 2 example table: current rows have a null end date, historical rows have an end date" style="font-family:'DM Sans',Arial,Helvetica,sans-serif;max-width:1020px;display:block;margin:12px auto;"><defs><marker id="s2i-ah-g" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#5A6F77"/></marker><marker id="s2i-ah-l" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#FF3621"/></marker><marker id="s2i-ah-b" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#2272B4"/></marker><marker id="s2i-ah-n" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B3139"/></marker><marker id="s2i-ah-k" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#00A972"/></marker><filter id="s2i-sh" x="-8%" y="-8%" width="116%" height="130%"><feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#0B2026" flood-opacity="0.13"/></filter></defs><rect x="36" y="14" width="400" height="40" rx="0" fill="#1B5062" stroke="none" stroke-width="1.5" /><text x="48" y="42" font-size="17" font-weight="700" fill="#fff" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >SCD Type 2 (Historical Tracking/Versioning)</text><circle cx="76" cy="86" r="4" fill="#0B2026" stroke="none" stroke-width="1.5"/><text x="100" y="92" font-size="18" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan font-style="italic">When a record changes:</tspan></text><circle cx="112" cy="120" r="3" fill="none" stroke="none" stroke-width="1.5"/><circle cx="112" cy="118" r="3.5" fill="none" stroke="#0B2026" stroke-width="1.6"/><text x="130" y="124" font-size="15.5" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan>The </tspan><tspan font-weight="700">old record is preserved</tspan><tspan> with an additional column indicating its validity period (start date, end</tspan></text><text x="130" y="146" font-size="15.5" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan>date, or a current flag) and a </tspan><tspan font-weight="700">new record is inserted</tspan><tspan> with the updated information.</tspan></text><circle cx="112" cy="176" r="3.5" fill="none" stroke="#0B2026" stroke-width="1.6"/><text x="130" y="182" font-size="15.5" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan>When a </tspan><tspan font-weight="700">record is marked as deleted,</tspan><tspan> the record is kept and a column indicates the record is </tspan><tspan font-weight="700">inactive</tspan><tspan>.</tspan></text><circle cx="76" cy="226" r="4" fill="#0B2026" stroke="none" stroke-width="1.5"/><text x="100" y="232" font-size="18" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan>Used when </tspan><tspan font-weight="700">historical data is important</tspan><tspan>, and the system needs to track </tspan><tspan font-weight="700">how attributes change</tspan></text><text x="100" y="258" font-size="18" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan font-weight="700">over time</tspan><tspan> (like tracking changes in a customer's address or status).</tspan></text><text x="40" y="336" font-size="19" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Example customers target table with SCD Type 2</text><rect x="500" y="296" width="200" height="60" fill="#00A972" stroke="none"/><text x="600.0" y="321.325" font-size="14.5" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan>Columns indicate </tspan><tspan font-weight="700">active</tspan></text><text x="600.0" y="340.825" font-size="14.5" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan>and </tspan><tspan font-weight="700">inactive</tspan><tspan> rows</tspan></text><path d="M562,358 V372" fill="none" stroke="#00A972" stroke-width="1.8" marker-end="url(#s2i-ah-k)" stroke-linecap="round" stroke-linejoin="round"/><path d="M648,358 V372" fill="none" stroke="#00A972" stroke-width="1.8" marker-end="url(#s2i-ah-k)" stroke-linecap="round" stroke-linejoin="round"/><rect x="36" y="374" width="104" height="34" rx="0" fill="#1273C5" stroke="none" stroke-width="1.5" /><text x="45" y="396.44" font-size="14.5" font-weight="700" fill="#fff" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >CustomerID</text><rect x="140" y="374" width="100" height="34" rx="0" fill="#1B5062" stroke="none" stroke-width="1.5" /><text x="149" y="396.44" font-size="14.5" font-weight="700" fill="#fff" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Name</text><rect x="240" y="374" width="122" height="34" rx="0" fill="#1B5062" stroke="none" stroke-width="1.5" /><text x="249" y="396.44" font-size="14.5" font-weight="700" fill="#fff" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Address</text><rect x="362" y="374" width="118" height="34" rx="0" fill="#1B5062" stroke="none" stroke-width="1.5" /><text x="371" y="396.44" font-size="14.5" font-weight="700" fill="#fff" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >ProcessDate</text><rect x="480" y="374" width="118" height="34" rx="0" fill="#00A972" stroke="none" stroke-width="1.5" /><text x="489" y="396.44" font-size="14.5" font-weight="700" fill="#fff" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >__START_AT</text><rect x="598" y="374" width="118" height="34" rx="0" fill="#00A972" stroke="none" stroke-width="1.5" /><text x="607" y="396.44" font-size="14.5" font-weight="700" fill="#fff" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >__END_AT</text><rect x="36" y="408" width="680" height="34" rx="0" fill="#fff" stroke="#E4E7EA" stroke-width="0.8" /><text x="45" y="430.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">1</text><text x="149" y="430.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">Peter</text><text x="249" y="430.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">123 Main St.</text><text x="371" y="430.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/20/2025</text><text x="489" y="430.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/20/2025</text><text x="607" y="430.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">null</text><rect x="36" y="442" width="680" height="34" rx="0" fill="#F4F5F6" stroke="#E4E7EA" stroke-width="0.8" /><text x="45" y="464.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">1</text><text x="149" y="464.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">Peter</text><text x="249" y="464.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">1 Blue Rd.</text><text x="371" y="464.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/1/2025</text><text x="489" y="464.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/1/2025</text><text x="607" y="464.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/20/2025</text><rect x="36" y="476" width="680" height="34" rx="0" fill="#F4F5F6" stroke="#E4E7EA" stroke-width="0.8" /><text x="45" y="498.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">2</text><text x="149" y="498.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">Samarth</text><text x="249" y="498.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">22 Front St</text><text x="371" y="498.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/1/2025</text><text x="489" y="498.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/1/2025</text><text x="607" y="498.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/20/2025</text><rect x="36" y="510" width="680" height="34" rx="0" fill="#F4F5F6" stroke="#E4E7EA" stroke-width="0.8" /><text x="45" y="532.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">3</text><text x="149" y="532.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">Kostas</text><text x="249" y="532.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">100 Athens Dr.</text><text x="371" y="532.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/20/2025</text><text x="489" y="532.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">5/20/2025</text><text x="607" y="532.44" font-size="14.5" font-weight="400" fill="#0B2026" style="font-family:'DM Sans',Arial,Helvetica,sans-serif">null</text><rect x="36" y="408" width="674" height="34" fill="none" stroke="#00A972" stroke-width="2.4"/><rect x="36" y="442" width="674" height="68" fill="none" stroke="#FF5F46" stroke-width="2.4"/><rect x="36" y="510" width="674" height="34" fill="none" stroke="#00A972" stroke-width="2.4"/><rect x="734" y="396" width="252" height="56" fill="#00A972" stroke="none"/><text x="860.0" y="419.475" font-size="13.5" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan font-weight="700">Null</tspan><tspan> values indicate the </tspan><tspan font-weight="700">current</tspan></text><text x="860.0" y="437.975" font-size="13.5" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan font-weight="700">row</tspan></text><rect x="734" y="462" width="252" height="62" fill="#FF5F46" stroke="none"/><text x="860.0" y="488.475" font-size="13.5" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan font-weight="700">Non null</tspan><tspan> values indicate </tspan><tspan font-weight="700">inactive</tspan></text><text x="860.0" y="506.975" font-size="13.5" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" xml:space="preserve" ><tspan font-weight="700">(historic)</tspan><tspan> rows</tspan></text><path d="M734,424 H714" fill="none" stroke="#00A972" stroke-width="1.8" marker-end="url(#s2i-ah-k)" stroke-linecap="round" stroke-linejoin="round"/><path d="M734,492 H714" fill="none" stroke="#FF5F46" stroke-width="1.8" marker-end="url(#s2i-ah-l)" stroke-linecap="round" stroke-linejoin="round"/></svg>
# MAGIC
# MAGIC Recall `AUTO CDC INTO` in Spark Declarative Pipelines automatically tracks historical changes without manual merge logic. With **SCD Type 2** it creates two new columns:
# MAGIC
# MAGIC - **__START_AT** - Timestamp when the record becomes active. 
# MAGIC
# MAGIC - **__END_AT** - Timestamp when the record is closed (after an update or delete).  
# MAGIC
# MAGIC When a row changes, the old version gets an **__END_AT** timestamp, and a new version starts with a fresh **__START_AT**. 
# MAGIC
# MAGIC This keeps a full SCD Type 2 history automatically.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Explore the Spark Declarative Pipeline Results
# MAGIC
# MAGIC After the pipeline completes, review the outputs to confirm data flow and CDC behavior on the first run with **one** JSON file.
# MAGIC
# MAGIC 1. In the pipeline graph:
# MAGIC    - **939** rows were streamed through all layers, passing Bronze data quality checks and upserting into **sdp_cdc_2_silver.customers_silver_scd2_demo**.
# MAGIC
# MAGIC 2. In the **Lakeflow Pipeline Editor** table window:
# MAGIC    - View the **Upserted records** column in **sdp_cdc_2_silver.customers_silver_scd2_demo** and confirm all **939** rows were inserted as new customers.
# MAGIC
# MAGIC 3. In the **Tables** view, select the **customers_silver_scd2_demo** table:
# MAGIC    - Preview the table to inspect the data.
# MAGIC    - Scroll right to see the **__START_AT** and **__END_AT** columns  were automatically added by the `AUTO CDC INTO` SCD Type 2 process indicating active, inactive and removed data (right now all rows are active - **__END_AT** = `null`).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Explore the Pipeline CDC Silver Streaming Table

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to confirm that `current_catalog()` references your catalog and that your settings are still active (they might have been cleared).  
# MAGIC
# MAGIC     If they were cleared, reset your default catalog using: `USE CATALOG your_catalog_name;`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS `Your Current Catalog`

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the query below to view the **sdp_cdc_2_silver.customers_silver_scd2_demo** CDC streaming table (SCD Type 2).  
# MAGIC
# MAGIC Observe the following:
# MAGIC
# MAGIC - The table contains all **939 rows** from the **00.json** file, as all customers are new.  
# MAGIC
# MAGIC - Scroll to the right to see that `AUTO CDC INTO (SCD Type 2)` added two columns:  
# MAGIC   - **__START_AT**: Timestamp showing when the current version of each record became active. This matches the sequence column **timestamp_datetime**.  
# MAGIC   - **__END_AT**: 
# MAGIC     - Timestamp showing when the record became inactive due to a **DELETE** or **UPDATE**. 
# MAGIC     - Filtering where **__END_AT** is `NULL` returns all active customers.  
# MAGIC
# MAGIC Since this is the first ingestion from **00.json**, all records are active and **__END_AT** contains only `NULL` values.
# MAGIC
# MAGIC **NOTES:**  
# MAGIC - These columns implement **Slowly Changing Dimension (SCD) Type 2** tracking.  
# MAGIC - `AUTO CDC INTO` automatically manages **__START_AT** and **__END_AT** to record the valid time range of each version.  
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, email, timestamp_datetime, processing_time, source_file, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below to query the CDC table for **all current customers** by filtering where **__END_AT** is `NULL`. 
# MAGIC
# MAGIC    Recall a `NULL` value in the **__END_AT** column indicates all active (current) records.
# MAGIC
# MAGIC    Review the results and confirm that on the first run all **939** records are active.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, email, timestamp_datetime, processing_time, source_file, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE __END_AT IS NULL;    -- Find all current customers

# COMMAND ----------

# MAGIC %md
# MAGIC 4. To find all **deleted customer records** in an SCD Type 2 table created with `AUTO CDC INTO`, you need to locate the **most recent version of each record** and check if the **__END_AT** `IS NOT NULL` (contains a value).  
# MAGIC
# MAGIC     - **NOTE:** Recall a non-null **__END_AT** value indicates that the record has been marked as deleted.  
# MAGIC
# MAGIC     The example query below demonstrates one way to find these records. Run the cell and confirm that **0** results are returned, since no deletions have occurred yet in the **sdp_cdc_2_silver.customers_silver_scd2_demo** CDC table. 
# MAGIC
# MAGIC
# MAGIC **Code details**
# MAGIC - This identifies **removed customers** by grouping all versions, selecting each customer's latest record, and keeping only those marked as deleted, showing their final details before removal.
# MAGIC
# MAGIC     - **Use MAX_BY to find the latest record** - The function `MAX_BY(value, __START_AT)` returns the value from the row with the most recent `__START_AT` timestamp.  
# MAGIC         - `MAX_BY(name, __START_AT)` - most recent name  
# MAGIC         - `MAX_BY(address, __START_AT)` - most recent address  
# MAGIC         - `MAX_BY(__END_AT, __START_AT)` - most recent deletion marker from the most recent record
# MAGIC         - etc.
# MAGIC
# MAGIC     - `GROUP BY customer_id` - Collects all historical versions of each customer into a single group so we can analyze changes over time.
# MAGIC
# MAGIC     - `HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL` - Condition keeps only customers whose latest record has a value for `__END_AT`, meaning they have been removed.
# MAGIC
# MAGIC     - [max_by aggregate function](https://docs.databricks.com/aws/en/sql/language-manual/functions/max_by)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Currently this query will return 0 results since no customers were marked as deleted
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   MAX_BY(name, __START_AT)      AS name,
# MAGIC   MAX_BY(address, __START_AT)   AS address,
# MAGIC   MAX_BY(city, __START_AT) AS city,
# MAGIC   MAX_BY(state, __START_AT)   AS state,
# MAGIC   MAX_BY(zip_code, __START_AT)   AS zip_code,
# MAGIC   MAX_BY(__START_AT, __START_AT) AS __START_AT,
# MAGIC   MAX_BY(__END_AT, __START_AT)  AS __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC GROUP BY customer_id
# MAGIC HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Create Gold Materialized Views
# MAGIC
# MAGIC Rather than having users manually query the CDC table to **find current or deleted customers**, you can create **Gold materialized views** that surface this information automatically as an object.  
# MAGIC
# MAGIC These views simplify access for business users and can be added directly to your **Spark Declarative Pipeline** for continuous updates.
# MAGIC
# MAGIC In this step we'll create two views:
# MAGIC
# MAGIC    - a. **current_customers_gold_demo** - Returns only the most recent, active customer records (where `__END_AT IS NULL`).  
# MAGIC
# MAGIC    - b. **removed_customers_gold_demo** - Creates a **Gold Materialized View** that lists all customers who have been **removed (deleted)** from the SCD Type 2 table.  (It uses `MAX_BY` on multiple columns like `name`, `address`, `city`, `state`, and `zip_code` to return the most recent record per customer where the latest `__END_AT` value is not `NULL`.)
# MAGIC
# MAGIC **NOTES:**
# MAGIC
# MAGIC - Materialized views in **Spark Declarative Pipelines** provide an automatically updated layer for reporting and dashboards. They make it easy to query only the **current customer state** or identify **removed customers** without scanning the entire SCD history.
# MAGIC
# MAGIC - When possible, **materialized views** are incrementally updated. For more information, see the [Incremental refresh for materialized views](https://docs.databricks.com/aws/en/optimizations/incremental-refresh).
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Create the Current Customers Materialized View
# MAGIC
# MAGIC 1. Copy and paste the code below into your `cdc_pipeline.sql` file to create your gold materialized views. Explore the code.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ---------------------------------------------------------------------------------------
# MAGIC -- STEP 4: Create Materialized View for Current (Active) Customers
# MAGIC ---------------------------------------------------------------------------------------
# MAGIC
# MAGIC -- a. Create Gold Materialized View for Current Customers
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.current_customers_gold_demo
# MAGIC COMMENT "Current updated list of active customers"
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   * EXCEPT (processing_time),
# MAGIC   current_timestamp() updated_at
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE `__END_AT` IS NULL;      -- Filter for only rows that contain a null value for __END_AT, which indicates the current version of the record
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
# MAGIC ### E2. Create the Deleted Customers Materialized View
# MAGIC
# MAGIC 1. Copy and paste the code below into your `cdc_pipeline.sql` file to create your gold materialized views. Explore the code.
# MAGIC
# MAGIC - **NOTE:** Because this uses `GROUP BY` with aggregates instead of window functions, Lakeflow can update the view incrementally as new CDC data arrives instead of recomputing everything.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC -- b. Create Gold Materialized View that lists all customers who have been removed (deleted) from the SCD Type 2 table.
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.removed_customers_gold_demo AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   MAX_BY(name, __START_AT)      AS name,
# MAGIC   MAX_BY(address, __START_AT)   AS address,
# MAGIC   MAX_BY(city, __START_AT) AS city,
# MAGIC   MAX_BY(state, __START_AT)   AS state,
# MAGIC   MAX_BY(zip_code, __START_AT)   AS zip_code,
# MAGIC   MAX_BY(__START_AT, __START_AT) AS __START_AT,
# MAGIC   MAX_BY(__END_AT, __START_AT)  AS __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC GROUP BY customer_id
# MAGIC HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL;  -- Find the latest records __END_AT value and only return if not null
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
# MAGIC ### E3. Run the Final Spark Declarative Pipeline with the Materialized Views
# MAGIC
# MAGIC 1. **REQUIRED:** Since you're developing the pipeline, run it using **Run pipeline with full table refresh** to remove all checkpoints, truncate the tables, and run the pipeline from scratch.
# MAGIC     - Select the **Run Pipeline** dropdown arrow > **Run pipeline with full table refresh**.  
# MAGIC
# MAGIC    This action will:  
# MAGIC    - Remove all existing checkpoints  
# MAGIC    - Drop previously created tables  
# MAGIC    - Restart the pipeline from scratch 
# MAGIC
# MAGIC 2. While the pipeline is running, update the bottom table view to show the **Incrementalization** column.  
# MAGIC    - **NOTE:** The **Incrementalization** column shows whether each materialized view was processed as a **Full recompute** or updated **incrementally**: [Incremental refresh for materialized views](https://docs.databricks.com/aws/en/optimizations/incremental-refresh).  
# MAGIC
# MAGIC
# MAGIC <br></br>
# MAGIC 3. After the run completes, verify the pipeline results match the example below.
# MAGIC
# MAGIC #### Checkpoint
# MAGIC
# MAGIC > **TROUBLESHOOTING:** If you see zero rows processed in your streaming tables, you likely ran a normal pipeline run instead of **Run pipeline with full table refresh**. A standard run will not reprocess data that was already ingested and processed.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/incremental_mvs_column2.png" alt="MV Incremental Column" width="1000">
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/cdc_02_pipeline-run-with-mvs-one-file.png" alt="Pipeline Run 2 - MVs" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Explore the Spark Declarative Pipeline Run

# COMMAND ----------

# MAGIC %md
# MAGIC After the pipeline completes, review the outputs to confirm data flow and CDC behavior.
# MAGIC
# MAGIC 1. In the pipeline graph:
# MAGIC    - **939** rows were streamed through all streaming tables, passing Bronze data quality checks and upserting into **sdp_cdc_2_silver.customers_silver_scd2_demo**.
# MAGIC    - **sdp_cdc_3_gold.current_customers_gold_demo** shows **939** active customers, as no updates or deletes have occurred yet and all customers are active.
# MAGIC    - **sdp_cdc_3_gold.removed_customers_gold_demo** shows **0** rows since no customers have been removed.
# MAGIC
# MAGIC 2. In the **Lakeflow Pipeline Editor** table window:
# MAGIC    - The **Upserted records** column in **sdp_cdc_2_silver.customers_silver_scd2_demo** confirms all **939** rows were inserted as new customers.
# MAGIC    - Both Gold materialized views display `Full recompute` in the **Incrementalization** column because this was their first computation.
# MAGIC
# MAGIC 3. In the **Tables** view, select **customers_silver_scd2_demo**:
# MAGIC    - Preview the table to inspect the data.
# MAGIC    - Scroll right to see the **__START_AT** and **__END_AT** columns automatically added by the `AUTO CDC INTO` SCD Type 2 process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Incremental Data Processing
# MAGIC The final pipeline is built, let's add another file to cloud storage!

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Land Additional Data
# MAGIC 1. Before landing additional data to cloud storage, let's query the **sdp_cdc_2_silver.customers_silver_scd2_demo** streaming table for customers *23225* and *23617*.  
# MAGIC
# MAGIC    Review the output and notice the following:
# MAGIC
# MAGIC    - **customer_id = 23225**  
# MAGIC      - **Address:** `76814 Jacqueline Mountains Suite 815`  
# MAGIC      - **State:** `TX`  
# MAGIC      - **__END_AT:** `null`, indicating this is the current active record for the customer.
# MAGIC
# MAGIC    - **customer_id = 23617**  
# MAGIC      - This record currently exists in the table and is active (**__END_AT** is `null`).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE customer_id IN (23225, 23617);

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to land a new JSON file (**01.json**) **in** your **customer_source_files** volume to simulate new files being added to your cloud storage location.
# MAGIC
# MAGIC     Confirm that your **customer_source_files** volume contains two JSON files (**00.json and 01.json**).

# COMMAND ----------

## Copy a second JSON file into the user's volume
copy_files(
    copy_from = f'/Volumes/{my_catalog}/sdp_cdc_1_bronze/staging/customers', 
    copy_to = f'/Volumes/{my_catalog}/sdp_cdc_1_bronze/customer_source_files', 
    n = 2
)

## List files in your volume (confirm two files exist)
spark.sql(f'LIST "{my_vol_path}"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below to view the raw data in the **01.json** file before ingesting it into your pipeline.  
# MAGIC
# MAGIC    Key points to notice:
# MAGIC
# MAGIC    - The **01.json** file contains **23** rows.  
# MAGIC
# MAGIC    - The **operation** column includes **UPDATE**, **DELETE**, and **NEW** values:
# MAGIC      - 12 customers marked as **UPDATE**  
# MAGIC      - 1 customer marked as **DELETE**  
# MAGIC      - 10 customers marked as **NEW**  
# MAGIC
# MAGIC    - For **customer_id = 23225** (Sandy Adams):  
# MAGIC      - Original address (from **00.json**): `76814 Jacqueline Mountains Suite 815`, `TX`  
# MAGIC      - Updated address (in this file): `512 John Stravenue Suite 239`, `TN`  
# MAGIC
# MAGIC    - For **customer_id = 23617**:  
# MAGIC      - The **operation** is **DELETE**.  
# MAGIC      - All other columns contain `NULL` values.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   my_vol_path || '/01.json',
# MAGIC   format => "JSON"
# MAGIC )
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Process Updates and Deletes

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Return to your **Spark Declarative Pipeline** and select **Run pipeline** to incrementally process the new JSON file (**01.json**) and apply SCD Type 2 logic to your Silver table.  
# MAGIC
# MAGIC 2. Once complete, verify the results match the example below.
# MAGIC
# MAGIC ### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/cdc_lecture/cdc_run_01json.png" alt="Pipeline Run 2" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC 3. After running the pipeline, review the **Pipeline graph**:
# MAGIC
# MAGIC    - **23 rows** were incrementally ingested from the new **01.json** file into both **customers_bronze_raw_demo** and **customers_bronze_clean_demo** , passing all data quality checks. 
# MAGIC
# MAGIC    - In the **customers_silver_scd2_demo** streaming table (SCD Type 2), **35 rows** were processed and **upserted**.  
# MAGIC      - Breakdown of the **35** upserts from the **01.json** file:  
# MAGIC         - **12 updates** - each update adds a new version(row) and marks the old one inactive (**24 total upserts**)  
# MAGIC         - **1 delete** - sets a value in the **__END_AT** column  
# MAGIC         - **10 new** customer inserts 
# MAGIC
# MAGIC      **Total changes:** 24 + 1 + 10 = **35**
# MAGIC
# MAGIC    - The **current_customers_gold_demo** materialized view contains **948** active customers.
# MAGIC
# MAGIC    - The **removed_customers_gold_demo** materialized view contains **1** deleted customer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Pipeline Object Analysis and Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Query the CDC Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Query the data in the **sdp_cdc_2_silver.customers_silver_scd2_demo** streaming table with SCD Type 2 and observe the following:
# MAGIC
# MAGIC    a. The table contains **961 rows**
# MAGIC
# MAGIC       - **initial 939 customers**  
# MAGIC       - \+ **12 updates** to existing customers 
# MAGIC       - \+ **10 new customers**
# MAGIC
# MAGIC    b. Scroll to the right and locate the **__END_AT** column. Then scroll down to rows **82** and **83**. Notice there are two rows for customer **22668**, the original record and the updated record.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC ORDER BY customer_id, __END_AT;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the query on the **sdp_cdc_2_silver.customers_silver_scd2_demo** streaming table for all rows where **__END_AT** `IS NOT NULL` to view all rows where those customers rows are now inactive.
# MAGIC
# MAGIC Notice the following:
# MAGIC   - **13 rows** are returned (**12 UPDATES** + **1 DELETE**)
# MAGIC   - The **__END_AT** column indicates the date and time that the row was either updated or marked as removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE __END_AT IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Query the **sdp_cdc_2_silver.customers_silver_scd2_demo** table for the **customer_id** *23225* (one of the customers that was updated). 
# MAGIC
# MAGIC Notice the following:
# MAGIC
# MAGIC - There are **two records** for that customer in the table.
# MAGIC - The original record from the **00.json** file now has a value in the **__END_AT** column, indicating that it is now inactive.
# MAGIC - The new record from the **01.json** file is now the active row and contains a `null` value in the **__END_AT** column.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, state, source_file, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE customer_id = 23225;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. In the **01.json** file, recall **customer_id** *23617* was marked as deleted. 
# MAGIC
# MAGIC     Let's query the **sdp_cdc_2_silver.customers_silver_scd2_demo** table for that customer and view the results. 
# MAGIC
# MAGIC     Notice that when a customer is marked as deleted, the **__END_AT** column contains the value of when that customer was deleted and became inactive, but the customer records STILL EXISTS in the **sdp_cdc_2_silver.customers_silver_scd2_demo** with SCD Type 2.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, __START_AT, __END_AT
# MAGIC FROM sdp_cdc_2_silver.customers_silver_scd2_demo
# MAGIC WHERE customer_id = 23617

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Query the Current Customers Materialized View
# MAGIC
# MAGIC 1. To view your organization's most up-to-date customer data, you can query the materialized view **sdp_cdc_3_gold.current_customers_gold_demo**. 
# MAGIC
# MAGIC     Remember, the query to create the materialized view filters for all **__END_AT** values that are `null` (active rows).
# MAGIC
# MAGIC     Run the cell and view the results. Notice the following:
# MAGIC    - The current updated list of customers contains **948 rows**:
# MAGIC      - **939** from the initial file (**00.json**)
# MAGIC      - **+10** new customers from the update file (**01.json**)
# MAGIC      - **-1** deleted customer from the update file (**01.json**)
# MAGIC      - The table also contains the updated records from the **01.json** file.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, address, name, __START_AT, __END_AT, source_file
# MAGIC FROM sdp_cdc_3_gold.current_customers_gold_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Query the Removed Customers Materialized View
# MAGIC 1. To view your organization's deleted customers, you can query the materialized view **sdp_cdc_3_gold.removed_customers_gold_demo**. 
# MAGIC
# MAGIC     Run the cell and view the results. Notice that there has been **1** customer marked as deleted (**customer_id** = *23617*) in the CDC silver table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_cdc_3_gold.removed_customers_gold_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. (Optional) Continue Incrementally Processing New Data
# MAGIC
# MAGIC If you'd like to continue practicing, use the demonstration function `copy_files` to dynamically add another JSON file to your cloud storage location. 
# MAGIC
# MAGIC Simply remove the `%skip` and run the cell to add a new file into your source volume.
# MAGIC
# MAGIC **NOTE:** Ensure the variable `my_catalog`, defined by the classroom setup at the start of this demonstration, is still active for the function to work properly.

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC ## Copy a another JSON file into the user's volume
# MAGIC copy_files(
# MAGIC     copy_from = f'/Volumes/{my_catalog}/sdp_cdc_1_bronze/staging/customers', 
# MAGIC     copy_to = f'/Volumes/{my_catalog}/sdp_cdc_1_bronze/customer_source_files', 
# MAGIC     n = 3 # <-- This value determines how many files should be in the volume. This will add the third JSON file (02.json) to the volume. You can continue using this until you run out of files.
# MAGIC )
# MAGIC
# MAGIC ## List files in your volume
# MAGIC spark.sql(f'LIST "{my_vol_path}"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Lab Clean Up
# MAGIC 1. Feel free to delete the schemas you create in this demonstration by running cell below and confirming the delete (**Y**).

# COMMAND ----------

delete_schemas(
    catalog = my_catalog, ## <--- Your catalog name using the variable you set earlier
    schemas = ['sdp_cdc_1_bronze','sdp_cdc_2_silver','sdp_cdc_3_gold']
)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Delete your Spark Declarative Pipeline through the **Jobs & Pipelines** UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Summary and Key Takeaways
# MAGIC
# MAGIC You have successfully implemented an end-to-end **AUTO CDC for SCD Type 2** pipeline using **Apache Spark™ Declarative Pipelines**. This demonstration showcased how modern data engineering can dramatically simplify complex Change Data Capture operations that traditionally required extensive manual coding.
# MAGIC
# MAGIC **Simplified CDC Implementation:**
# MAGIC - Replaced complex manual `MERGE` operations with declarative `AUTO CDC INTO` syntax
# MAGIC - Automatic handling of late-arriving data using sequence columns
# MAGIC - Built-in SCD Type 2 logic without manual custom coding
# MAGIC
# MAGIC **Robust Data Quality:**
# MAGIC - Multi-layered validation with conditional constraints for DELETE operations
# MAGIC - Flexible violation handling (WARN, DROP, FAIL) based on business requirements
# MAGIC - Comprehensive email validation using regex patterns
# MAGIC
# MAGIC **Incremental Processing Efficiency:**
# MAGIC - Auto Loader for incremental file ingestion (**triggered or continuous**) from cloud storage
# MAGIC - Incremental materialized view updates where possible
# MAGIC - Streaming architecture that processes only new changes

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>