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
# MAGIC # Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads
# MAGIC
# MAGIC #### Duration: ~31-36 Minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This demonstration showcases how to build a multiplex data pipeline using Spark Declarative Pipelines (Lakeflow). You will learn how to ingest multiplexed data from a single source into a bronze streaming table, then demultiplex that data into multiple silver streaming tables based on event types, and finally create aggregated gold-layer views for analytics.
# MAGIC
# MAGIC The demo simulates a real-world scenario where multiple business domains (store operations, marketing, and logistics) generate events that are ingested into a single data stream. Using the medallion architecture pattern, you will process this multiplexed data through bronze, silver, and gold layers while enabling Iceberg compatibility for cross-platform analytics.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demonstration, you will be able to:
# MAGIC
# MAGIC - **Build multiplex data pipelines** using Spark Declarative Pipelines to handle multiple event types from a single source
# MAGIC - **Demultiplex streaming data** by filtering and transforming events into separate domain-specific tables
# MAGIC - **Build Delta sinks** using the Python API to create analytics-ready tables from streaming sources
# MAGIC - **Enable Iceberg compatibility** on Delta tables to support cross-platform data access and analytics

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
# MAGIC ### Multiplex Pipeline Demonstration Overview
# MAGIC In this demonstration, you'll build a Apache Spark™ Declarative Pipeline that implements the full medallion architecture, from raw data ingestion to curated analytics.
# MAGIC
# MAGIC 1. **Ingest multiplexed files** from cloud storage and write them into a single bronze table.  
# MAGIC     - These files contain multiplexed data, meaning they include events for **marketing**, **logistics**, and **store operations**, all dumped into a single cloud location.
# MAGIC 2. **Demultiplex the bronze table** into intermediate tables based on event group. Three intermediate tables will be created in the bronze layer.
# MAGIC 3. **Transform intermediate tables** to create silver tables for each event group, adding new columns as required by business logic and casting columns to the appropriate data types.
# MAGIC 4. **Create gold materialized views** from the marketing data that automatically refresh and provide analytics-ready results.
# MAGIC 5. **Write the logistics table to a Delta sink** to enable external access via Iceberg reads.
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1300 610" width="100%" role="img" aria-label="Multiplex pipeline overview: one business-events source feeds a bronze table that is de-multiplexed into intermediate, silver, then a Delta sink and a gold materialized view" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif;max-width:1300px;display:block;margin:12px auto;"><defs><marker id="om-ah-g" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#7C8B94"/></marker><marker id="om-ah-n" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B3139"/></marker><marker id="om-ah-t" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B5062"/></marker><filter id="om-sh" x="-8%" y="-8%" width="116%" height="130%"><feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#0B2026" flood-opacity="0.13"/></filter></defs><text x="14" y="30" font-size="20" font-weight="400" fill="#1B3139" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Cloud Storage</text><rect x="10" y="42" width="150" height="330" rx="18" fill="#fff" stroke="#9AA5AB" stroke-width="1.6" /><rect x="22" y="177" width="126" height="150" rx="8" fill="#fff" stroke="#8C98A0" stroke-width="1.5" stroke-dasharray="5 4" /><text x="85" y="201" font-size="12.5" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Business Events</text><text x="85" y="218" font-size="12.5" font-weight="700" fill="#0B2026" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Source</text><g transform="translate(33,247) scale(0.78)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >PQ</text></g><g transform="translate(71,247) scale(0.78)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >PQ</text></g><g transform="translate(109,247) scale(0.78)"><path d="M0,0 h22 l12,12 v32 h-34 z" fill="#fff" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><path d="M22,0 v12 h12" fill="none" stroke="#FF5F46" stroke-width="2" stroke-linejoin="round"/><rect x="2" y="25" width="30" height="14" rx="3" fill="#FF5F46"/><text x="17" y="35.5" font-size="9.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >PQ</text></g><text x="85" y="410" font-size="14" font-weight="400" fill="#1B3139" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Files are landed into</text><text x="85" y="429" font-size="14" font-weight="700" fill="#1B3139" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >volume daily</text><rect x="196" y="180" width="200" height="200" rx="2" fill="none" stroke="#FFAB00" stroke-width="1.6" stroke-dasharray="6 5" /><svg x="208" y="188" width="30" height="27.3" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="246" y="208" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="208" y="230" width="176" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="296.0" y="257" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >bronze_demo</text><rect x="204" y="292" width="184" height="80" rx="0" fill="#F6F4F1" stroke="none" stroke-width="1" /><text x="296" y="322" font-size="14.5" text-anchor="middle" fill="#1B3139" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">Ingesting <tspan font-weight="700">files</tspan> from</text><text x="296" y="344" font-size="14.5" text-anchor="middle" fill="#1B3139" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif"><tspan font-weight="700">source</tspan></text><path d="M162,252 H194" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><rect x="440" y="30" width="262" height="470" rx="2" fill="none" stroke="#FFAB00" stroke-width="1.6" stroke-dasharray="6 5" /><svg x="456" y="58" width="30" height="27.3" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="494" y="78" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="456" y="100" width="230" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="571.0" y="127" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >logistics_intermediate</text><path d="M398,252 H410 V130 Q410,122 418,122 H451" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="456" y="188" width="30" height="27.3" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="494" y="208" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="456" y="230" width="230" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="571.0" y="257" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >marketing_intermediate</text><path d="M398,252 H451" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="456" y="318" width="30" height="27.3" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="494" y="338" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="456" y="360" width="230" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="571.0" y="387" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >store_ops_intermediate</text><path d="M398,252 H410 V374 Q410,382 418,382 H451" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><rect x="452" y="440" width="238" height="50" rx="0" fill="#F6F4F1" stroke="none" stroke-width="1" /><text x="571" y="462" font-size="13.5" font-weight="400" fill="#1B3139" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >De-multiplexing bronze table</text><text x="571" y="480" font-size="13.5" font-weight="400" fill="#1B3139" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >into intermediate tables</text><rect x="740" y="30" width="262" height="470" rx="2" fill="none" stroke="#8C98A0" stroke-width="1.6" stroke-dasharray="6 5" /><svg x="756" y="58" width="30" height="27.3" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="794" y="78" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="756" y="100" width="230" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="871.0" y="127" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >logistics_silver_demo</text><path d="M688,122 H752" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="756" y="188" width="30" height="27.3" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="794" y="208" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="756" y="230" width="230" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="871.0" y="257" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >marketing_silver_demo</text><path d="M688,252 H752" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="756" y="318" width="30" height="27.3" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#1B3139" stroke-width="2.6" stroke-linecap="round"/></svg><text x="794" y="338" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Streaming Table</text><rect x="756" y="360" width="230" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="871.0" y="387" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >store_ops_silver_demo</text><path d="M688,382 H752" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><rect x="752" y="440" width="238" height="50" rx="0" fill="#F6F4F1" stroke="none" stroke-width="1" /><text x="871" y="470" font-size="14.5" text-anchor="middle" fill="#1B3139" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif">Performing <tspan font-weight="700">transformations</tspan></text><rect x="1040" y="30" width="250" height="340" rx="2" fill="none" stroke="#FFAB00" stroke-width="1.6" stroke-dasharray="6 5" /><svg x="1052" y="58" width="28" height="28" viewBox="1.265 25.6 47.748 47.748" fill="none"><path d="M43.4295 61.3529L26.3179 31.8768C25.9325 31.2299 24.9818 31.2299 24.5964 31.8768L10.0541 56.9276C9.79712 57.3676 10.2596 57.8851 10.7221 57.6522C10.9019 57.5746 11.1332 57.4711 11.3644 57.3934C11.3644 57.3934 11.7498 57.2382 12.4435 57.0311C13.1886 56.7982 13.9851 56.5912 14.7302 56.41C16.7857 55.9442 19.4835 55.556 21.8986 55.9442C24.1339 56.3065 25.7012 56.9276 26.8317 57.5228C27.3456 57.8075 27.8338 57.1088 27.4227 56.6947C25.804 55.1161 23.2861 53.1752 19.4835 52.3729C19.3293 52.347 19.2265 52.14 19.3293 52.0106L25.0332 42.1508C25.213 41.8143 25.6755 41.8143 25.8811 42.1508L36.8264 60.9906C37.0576 61.3788 36.8521 61.8964 36.441 62.0516C35.5931 62.3363 34.5654 62.5433 33.4092 62.5433C32.6898 62.5433 31.9447 62.4657 31.1996 62.2845C31.1996 62.2845 29.0927 61.9481 26.2408 60.2401C23.4145 58.5321 17.9419 56.3583 11.8269 59.8778C9.20618 61.5599 7.45904 63.889 6.53408 65.3383C6.37992 65.5712 6.63686 65.83 6.8681 65.7006C8.97494 64.5619 14.6531 61.8964 19.7661 62.8539C22.7979 63.4232 26.2922 65.7264 29.8892 67.0721C35.8757 69.2977 41.7338 65.5712 41.7338 65.5453C42.3504 65.1312 42.7101 64.8207 42.7872 64.7689L42.8643 64.6913C43.8406 63.8632 44.0719 62.4657 43.4295 61.3529Z" fill="#00ADD4"/></svg><text x="1088" y="78" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Delta Sink</text><rect x="1052" y="100" width="226" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="1165.0" y="127" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >delta_sink_logistics</text><path d="M988,122 H1048" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><svg x="1052" y="188" width="30" height="30" viewBox="0 0 56 51" fill="#1B3139"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><rect x="32.015" y="26.436" width="10.5" height="3.5" transform="rotate(131 37.265 28.186)"/><rect x="32.5" y="31" width="15" height="3"/><rect x="37.485" y="35.064" width="10.5" height="3.5" transform="rotate(131 42.735 36.814)"/></svg><text x="1090" y="208" font-size="14.5" font-weight="700" fill="#0B2026" text-anchor="start" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >Materialized View</text><rect x="1052" y="230" width="226" height="44" rx="8" fill="#FF5F46" stroke="none" stroke-width="0" filter="url(#om-sh)"/><text x="1165.0" y="257" font-size="13" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif" >marketing_campaign_summary</text><path d="M988,252 H1048" fill="none" stroke="#1B5062" stroke-width="2.2" marker-end="url(#om-ah-t)" stroke-linecap="round" stroke-linejoin="round"/><path d="M196,528 H980 V508 L1070,552 L980,596 V576 H196 z" fill="#1B5062"/><text x="560" y="559" font-size="16.5" text-anchor="middle" fill="#fff" style="font-family:'DM Sans',-apple-system,Arial,Helvetica,sans-serif"><tspan font-weight="700">Incrementally</tspan> process <tspan font-weight="700">new files</tspan> in the streaming tables (ST)</text></svg>

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
# MAGIC 1. Run the cell below to initialize your environment. This setup step does the following:
# MAGIC     - **Assumes you have permission to create a catalog** when running outside of a Databricks provided Vocareum workspace
# MAGIC     - Create three schemas in your specified catalog:
# MAGIC         - **multiplex_1_bronze**
# MAGIC         - **multiplex_2_silver**
# MAGIC         - **multiplex_3_gold**
# MAGIC     - Create `business_events` volume in your **YOUR_LABUSER_CATALOG.multiplex_1_bronze** schema, and adds a single file to your volume.
# MAGIC     - Verifies your selected compute environment
# MAGIC
# MAGIC     This ensures that all schemas, tables and objects are created in your catalog.
# MAGIC
# MAGIC > **Important:** You must have permission to create catalogs in your own non Vocareum workspace. If you do not have the required permissions, this step will fail. If you cannot create catalogs, see **Course Overview → B2. Troubleshooting: Missing Create Catalog Permissions** before continuing.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-multiplex

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to view the value of the `my_vol_path` variable.
# MAGIC
# MAGIC     Confirm that the value references your **your-catalog.multiplex_1_bronze** path. This will be used to dynamically reference your source volumes throughout this demonstration.

# COMMAND ----------

print(my_vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Raw Source Data for the Multiplex Data Ingestion
# MAGIC
# MAGIC Before building the multiplex pipeline, start by exploring the raw source data stored in the volume.
# MAGIC
# MAGIC The source volume receives multiplexed data from a Kafka stream, containing three distinct event groups:
# MAGIC - **store_ops**
# MAGIC - **marketing**
# MAGIC - **logistics**
# MAGIC
# MAGIC All event types are ingested into a single file within a volume, simulating a real-world scenario where multiple business domains generate events concurrently.
# MAGIC
# MAGIC For this demo, we process each file individually from the source location to demonstrate streaming ingestion and multiplexing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Explore Business Events Daily Volume

# COMMAND ----------

# MAGIC %md
# MAGIC 1. View the files in the **multiplex_1_bronze.business_events** volume. 
# MAGIC
# MAGIC     You will see one parquet file exists in this volume which contains event data.

# COMMAND ----------

spark.sql(f"LIST '{my_vol_path}/business_events' ").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Query the raw Parquet in volume to view the raw data and note the following:
# MAGIC     - The data is packed in **BINARY** format
# MAGIC       - The **key** columns holds a unique identifier for the data 
# MAGIC       - The **volume** column holds the actual data.
# MAGIC       - The **topic** column indicates the different event types present in this source location

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/business_events'
# MAGIC )
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. View the data by **casting `BINARY` columns to `STRING`** to view the values.
# MAGIC
# MAGIC     Notice the following:
# MAGIC     - The **value_str** column contains a JSON string of key-value pairs, which vary depending on the business event group
# MAGIC     - The **topic** column indicates the different event types present in this source location
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CAST(key AS STRING) AS key,
# MAGIC   CAST(value AS STRING) AS value_str,
# MAGIC   topic,
# MAGIC   partition,
# MAGIC   offset
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/business_events'
# MAGIC )
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Introduction to the Variant Data Type
# MAGIC
# MAGIC 1. The incoming event data is stored as JSON strings. Because each business event has a different structure, we use the **VARIANT** data type to handle this flexibility.
# MAGIC
# MAGIC ##### Why VARIANT:
# MAGIC
# MAGIC - Supports semi structured JSON data
# MAGIC - Handles multiple event shapes in a single column
# MAGIC - Allows fields to be extracted when needed
# MAGIC - Works well for multiplex streaming data
# MAGIC
# MAGIC ##### What This Query Is Doing
# MAGIC
# MAGIC This query converts raw JSON into a usable format.
# MAGIC
# MAGIC - Reads raw event files with `read_files`
# MAGIC - Casts the raw value to a string
# MAGIC - Parses the JSON string into a VARIANT column
# MAGIC - Extracts `event_id` as a string
# MAGIC - Extracts and casts `timestamp` to a TIMESTAMP
# MAGIC - Keeps both raw and extracted fields for downstream use
# MAGIC
# MAGIC VARIANT lets us ingest once, then shape the data later as part of the pipeline.
# MAGIC
# MAGIC **NOTE:** This is a **quick introduction** to the VARIANT data type used in this notebook. We will use the `VARIANT` data type in your Spark Declarative Pipeline (SDP).
# MAGIC
# MAGIC - [VARIANT type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type)
# MAGIC - [Variant Data Type - Making Semi-Structured Data Fast and Simple - Deep Dive](https://www.youtube.com/watch?v=jtjOfggD4YY)
# MAGIC - [Introducing the Open Variant Data Type in Delta Lake and Apache Spark](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   -- Raw JSON payload read from the file and cast to a string
# MAGIC   CAST(value AS STRING) AS value_str,
# MAGIC
# MAGIC   -- Parse the JSON string into a VARIANT column for flexible field access
# MAGIC   parse_json(value_str) AS event_data_variant,
# MAGIC
# MAGIC   -- Extract the event_id field from the VARIANT and cast it to STRING
# MAGIC   CAST(event_data_variant:event_id AS STRING) AS extracted_event_id,
# MAGIC
# MAGIC   -- Extract the timestamp field from the VARIANT and cast it to TIMESTAMP
# MAGIC   event_data_variant:timestamp::TIMESTAMP AS extracted_timestamp
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/business_events'
# MAGIC )
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Analyze Raw Data Statistics
# MAGIC
# MAGIC 1. Explore the raw data for **business_events**. The cell below performs the following:
# MAGIC     - Counts the number of records in the file within the volume  
# MAGIC     - Counts the number of records in the file for each event group in our volume
# MAGIC
# MAGIC     In the output, notice the following:
# MAGIC     - **372** rows are present in this file  
# MAGIC     - You will see counts for each of the three different sources

# COMMAND ----------

# a. Total row count
df_count = spark.sql(f"""
    SELECT COUNT(*) AS total_rows
    FROM  read_files('{my_vol_path}/business_events')
""")
display(df_count)

# b. Data source count by topic
df_data_source_count = spark.sql(f"""
    SELECT 
        topic,
        COUNT(*) AS total_rows
    FROM read_files('{my_vol_path}/business_events')
    GROUP BY topic
    ORDER BY topic
""")
display(df_data_source_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Create the Spark Declarative Pipeline
# MAGIC
# MAGIC Now that we have explored the data and reviewed casting from `BINARY` to `STRING` along with the `VARIANT` data type, we are ready to create the Spark Declarative Pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Create a Apache Spark™ Declarative Pipeline Using the Lakeflow Pipelines Editor
# MAGIC
# MAGIC Complete the following steps to create your Spark Declarative Pipeline:
# MAGIC
# MAGIC 1. In the main navigation pane, right-click **Jobs & Pipelines** and select **Open link in New Tab**.  
# MAGIC
# MAGIC 2. In the new tab, select **Create → ETL Pipeline**.  
# MAGIC
# MAGIC 3. At the top, complete the following:
# MAGIC    - Name your pipeline `demo_multiplex_yourname`
# MAGIC    - Select your default **catalog** and **schema**:  
# MAGIC         - **Catalog:** `YOUR_LABUSER_CATALOG`
# MAGIC         - **Schema:** **multiplex_1_bronze**  
# MAGIC       **NOTE:** Clear the selected schema using the cross icon to view all schemas.
# MAGIC
# MAGIC 4. Rename the **transformations** folder to `multiplex_pipeline`.
# MAGIC
# MAGIC 5. Rename the **my_transformation.py** file to `ingestion.sql`.
# MAGIC
# MAGIC 6. Leave the **Lakeflow Pipelines Editor** page open.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Create the First Bronze Table for All Business Events Data
# MAGIC
# MAGIC Start by creating the **bronze streaming table** to ingest data from the source.
# MAGIC - This table will capture raw business event data streaming in from multiple sources.
# MAGIC - We will use the `VARIANT` data type to parse JSON formatting strings.
# MAGIC - Two metadata columns are added to track when new data is ingested.
# MAGIC
# MAGIC 1. Copy the SQL code below and paste it into your `ingestion.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE THE BRONZE TABLE FOR BUSINESS EVENTS DATA
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.bronze_demo
# MAGIC TBLPROPERTIES (
# MAGIC   'pipelines.reset.allowed' = false
# MAGIC ) 
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(key AS STRING) AS event_id,
# MAGIC   PARSE_JSON(CAST(value AS STRING)) AS event_data_variant,
# MAGIC   CAST(topic AS STRING) AS event_group,
# MAGIC   CAST(partition AS STRING) AS partition,
# MAGIC   CAST(offset AS STRING) AS offset,
# MAGIC   -- Adding metadata columns
# MAGIC   _metadata.file_name AS source_file,
# MAGIC   _metadata.file_modification_time AS file_mod_time
# MAGIC FROM STREAM read_files(
# MAGIC   '${business_events_source}'
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
# MAGIC **Review the Code**
# MAGIC
# MAGIC The `TBLPROPERTIES` statement configures the following setting:
# MAGIC - **Reset Protection**: The `'pipelines.reset.allowed' = false` property prevents full refreshes on the streaming table, which helps avoid accidentally removing checkpoints and truncating the streaming table data
# MAGIC
# MAGIC #### IMPORTANT: Understanding Full Table Refresh Protection
# MAGIC
# MAGIC This protection is particularly important when your raw data source automatically removes files after a certain timeframe. Without this setting, data that is no longer present in the source directory would not be re-ingested into the target table during a **Run pipeline with full table refresh** operation.
# MAGIC
# MAGIC **NOTE:** For guidance on when to use full refreshes, see the [Should I use a full refresh?](https://docs.databricks.com/aws/en/ldp/updates#should-i-use-a-full-refresh) documentation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Fan Out the Bronze Table by Business Event
# MAGIC
# MAGIC At this stage, we started with a single multiplexed bronze streaming table. The goal is to demultiplex this data by fanning it out into separate intermediate tables, one for each business event type.
# MAGIC
# MAGIC This step creates three domain specific intermediate tables:
# MAGIC
# MAGIC - **marketing_intermediate**
# MAGIC - **logistics_intermediate**
# MAGIC - **store_ops_intermediate**
# MAGIC
# MAGIC Each table contains only the events relevant to its business domain, making downstream processing and analytics simpler and more focused.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Create the Marketing Bronze Table
# MAGIC
# MAGIC Begin by creating **the** marketing intermediate table by:
# MAGIC - Filtering for records where `event_group = 'business_events_marketing'`
# MAGIC - Converting columns to their appropriate data types
# MAGIC - Including metadata columns to track ingested records
# MAGIC
# MAGIC
# MAGIC 1. Copy the SQL code below and paste it into your `ingestion.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE MARKETING INTERMEDIATE TABLE
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.marketing_intermediate
# MAGIC AS SELECT 
# MAGIC   event_id,
# MAGIC   event_data_variant,
# MAGIC   event_group,
# MAGIC   event_data_variant:event_id::STRING AS extracted_event_id,
# MAGIC   event_data_variant:timestamp::TIMESTAMP AS timestamp,
# MAGIC   event_data_variant:event_type::STRING AS event_type,
# MAGIC   event_data_variant:subsidiary_id::STRING AS subsidiary_id,
# MAGIC   event_data_variant:campaign_id::STRING AS campaign_id,
# MAGIC   event_data_variant:channel::STRING AS channel,
# MAGIC   event_data_variant:impressions::LONG AS impressions,
# MAGIC   event_data_variant:clicks::LONG AS clicks,
# MAGIC   event_data_variant:conversions::LONG AS conversions,
# MAGIC   event_data_variant:spend_usd::DOUBLE AS spend_usd,
# MAGIC   source_file,
# MAGIC   file_mod_time
# MAGIC FROM STREAM multiplex_1_bronze.bronze_demo
# MAGIC WHERE event_group = 'business_events_marketing';
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
# MAGIC ### E2. Create the Logistics and Store Operations Bronze Tables
# MAGIC
# MAGIC Now create the other two intermediate tables the same way you created the marketing table. Each one:
# MAGIC - Filters for its own events: `event_group = 'business_events_logistics'` for logistics and `event_group = 'business_events_store_ops'` for store operations
# MAGIC - Converts columns to their appropriate data types
# MAGIC - Includes metadata columns to track ingested records
# MAGIC
# MAGIC **NOTE:** The store operations table names its parsed timestamp column `extracted_timestamp` (the marketing and logistics tables call it `timestamp`). The silver step renames it.
# MAGIC
# MAGIC 1. Copy the SQL code below and paste it into your `ingestion.sql` file. It creates **logistics_intermediate** and **store_ops_intermediate**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE LOGISTICS INTERMEDIATE TABLE
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.logistics_intermediate
# MAGIC AS SELECT 
# MAGIC   event_id,
# MAGIC   event_data_variant,
# MAGIC   event_group,
# MAGIC   event_data_variant:event_id::STRING AS extracted_event_id,
# MAGIC   event_data_variant:timestamp::TIMESTAMP AS timestamp,
# MAGIC   event_data_variant:event_type::STRING AS event_type,
# MAGIC   event_data_variant:subsidiary_id::STRING AS subsidiary_id,
# MAGIC   event_data_variant:warehouse_id::STRING AS warehouse_id,
# MAGIC   event_data_variant:carrier::STRING AS carrier,
# MAGIC   event_data_variant:batch_id::STRING AS batch_id,
# MAGIC   event_data_variant:num_packages::LONG AS num_packages,
# MAGIC   event_data_variant:destination_region::STRING AS destination_region,
# MAGIC   source_file,
# MAGIC   file_mod_time
# MAGIC FROM STREAM multiplex_1_bronze.bronze_demo
# MAGIC WHERE event_group = 'business_events_logistics';
# MAGIC
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE STORE OPERATIONS INTERMEDIATE TABLE
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.store_ops_intermediate
# MAGIC AS SELECT 
# MAGIC   event_id,
# MAGIC   event_data_variant,
# MAGIC   event_group,
# MAGIC   event_data_variant:event_id::STRING AS extracted_event_id,
# MAGIC   event_data_variant:timestamp::TIMESTAMP AS extracted_timestamp,
# MAGIC   event_data_variant:event_type::STRING AS event_type,
# MAGIC   event_data_variant:subsidiary_id::STRING AS subsidiary_id,
# MAGIC   event_data_variant:store_id::STRING AS store_id,
# MAGIC   event_data_variant:city::STRING AS city,
# MAGIC   event_data_variant:region::STRING AS region,
# MAGIC   event_data_variant:opened_by_employee_id::STRING AS opened_by_employee_id,
# MAGIC   source_file,
# MAGIC   file_mod_time
# MAGIC FROM STREAM multiplex_1_bronze.bronze_demo
# MAGIC WHERE event_group = 'business_events_store_ops';
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
# MAGIC ### E3. Configure the Pipeline Parameters
# MAGIC
# MAGIC 1. Run the cell below to retrieve the key-value pairs needed to set your pipeline configuration parameters for the **source volume**.

# COMMAND ----------

config_parameters = [
    ('business_events_source', f'{my_vol_path}/business_events'),
    ('my_catalog',my_catalog)
]

for key, value in config_parameters:
    print(f"Key: {key}\nValue: {value}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Copy the paths above and add each one as a configuration parameter in your **Spark Declarative Pipeline**.
# MAGIC
# MAGIC     This will allow your pipeline to reference each volume through parameters.
# MAGIC
# MAGIC    a. Select **Settings** in your pipeline tab  
# MAGIC
# MAGIC    b. Under **Configuration**, select **Add configuration**
# MAGIC
# MAGIC    c. For each **Key**, enter the key name shown above  
# MAGIC
# MAGIC    d. For each **Value**, enter the corresponding volume path  
# MAGIC
# MAGIC    e. Select **Save**
# MAGIC
# MAGIC **NOTE:** For more details on configuration parameters, see the Databricks documentation: [Use parameters with Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm that it completes successfully.
# MAGIC
# MAGIC 2. In the Lakeflow Pipelines editor, explore the pipeline run:
# MAGIC    - Verify that **372** rows were ingested into the bronze demo table from the `business_events` source volume
# MAGIC    - Confirm the row counts for each intermediate streaming table:
# MAGIC      - **194** rows in **logistics_intermediate**
# MAGIC      - **105** rows in **marketing_intermediate**
# MAGIC      - **73** rows in **store_ops_intermediate**
# MAGIC    - Select the **bronze_demo** table and open the **Data** tab to preview all of the ingested records business event data.
# MAGIC
# MAGIC **TROUBLESHOOTING:** If your pipeline does not run successfully, make sure your volumes are created and your configuration parameters are set correctly.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multiplex/checkpoint_bronze.png" alt="Bronze" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Create Silver Layer Tables
# MAGIC
# MAGIC Next, you will create the silver layer tables that process and clean the bronze data for each business domain. Start by creating a new file in your **multiplex_pipeline**:
# MAGIC
# MAGIC 1. Click the kebab menu next to your **multiplex_pipeline** folder.
# MAGIC
# MAGIC 2. Select **Create file**
# MAGIC
# MAGIC 3. Select the language as **SQL**
# MAGIC
# MAGIC 4. Name the file `silver_transformation.sql`

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Create the Silver Table for Marketing Data
# MAGIC
# MAGIC In this step, we transform the raw marketing events into a curated silver table, **multiplex_2_silver.marketing_silver_demo**, which is optimized for analytics.
# MAGIC
# MAGIC Here's what the silver transformation does:
# MAGIC
# MAGIC - Reads data from the **marketing_intermediate** streaming table.
# MAGIC - Uses `COALESCE` to create a reliable **event_id**, ensuring each row has a unique identifier.
# MAGIC - Selects key business fields needed for analysis, such as campaign, channel, impressions, clicks, and spend.
# MAGIC - Keeps the data at the same level of detail (no aggregation or filtering that changes the number of rows).
# MAGIC - Calculates important marketing metrics:
# MAGIC   - **click_through_rate**: Measures how often people click after seeing an ad (clicks divided by impressions).
# MAGIC   - **cost_per_click**: Shows how much each click costs (spend divided by clicks).
# MAGIC - Safely handles cases where impressions or clicks are zero to prevent errors in metric calculations.
# MAGIC - Produces a clean, structured streaming table ready for dashboards and reporting.
# MAGIC
# MAGIC This silver table ensures consistent data and trusted metrics for all downstream analytics.
# MAGIC
# MAGIC
# MAGIC 1. Copy the code into your `silver_transformation.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE THE SILVER TABLE FOR MARKETING DATA
# MAGIC ------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_2_silver.marketing_silver_demo
# MAGIC AS SELECT 
# MAGIC   COALESCE(extracted_event_id, event_id) AS event_id,
# MAGIC   timestamp,
# MAGIC   event_group,
# MAGIC   event_type,
# MAGIC   subsidiary_id,
# MAGIC   campaign_id,
# MAGIC   channel,
# MAGIC   impressions,
# MAGIC   clicks,
# MAGIC   conversions,
# MAGIC   spend_usd,
# MAGIC   CASE WHEN impressions > 0 
# MAGIC     THEN clicks / impressions 
# MAGIC     ELSE 0 
# MAGIC   END AS click_through_rate,
# MAGIC   CASE WHEN clicks > 0 
# MAGIC     THEN spend_usd / clicks 
# MAGIC     ELSE 0 
# MAGIC   END AS cost_per_click
# MAGIC FROM STREAM multiplex_1_bronze.marketing_intermediate;
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
# MAGIC ### F2. Create the Silver Tables for Logistics and Store Operations Data
# MAGIC
# MAGIC Now create the other two silver tables the same way as the marketing table. Each one reads from its intermediate streaming table, uses `COALESCE` to build a reliable **event_id**, applies data quality filters and adds calculated columns.
# MAGIC
# MAGIC **multiplex_2_silver.logistics_silver_demo** (reads **logistics_intermediate**)
# MAGIC - Filters out incomplete records: rows where **warehouse_id** or **batch_id** are missing.
# MAGIC - Selects the warehouse, carrier, batch ID, package count and destination region.
# MAGIC - Adds **is_valid_shipment** (true if `num_packages` > 0, which helps identify ghost shipments or system errors) and **event_date** (the date from the timestamp).
# MAGIC
# MAGIC **multiplex_2_silver.store_ops_silver_demo** (reads **store_ops_intermediate**)
# MAGIC - Filters out records that are missing the timestamp, store ID or event type.
# MAGIC - Renames **extracted_timestamp** to the common **timestamp** column.
# MAGIC - Adds **event_date**, **event_hour** and **store_number** (parsed from **store_id** with a split function).
# MAGIC
# MAGIC 1. Copy the code into your `silver_transformation.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE THE SILVER TABLE FOR LOGISTICS DATA
# MAGIC ------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_2_silver.logistics_silver_demo
# MAGIC AS SELECT 
# MAGIC   COALESCE(extracted_event_id, event_id) AS event_id,
# MAGIC   timestamp,
# MAGIC   event_group,
# MAGIC   event_type,
# MAGIC   subsidiary_id,
# MAGIC   warehouse_id,
# MAGIC   carrier,
# MAGIC   batch_id,
# MAGIC   num_packages,
# MAGIC   destination_region,
# MAGIC   CASE WHEN num_packages > 0 THEN TRUE ELSE FALSE END AS is_valid_shipment,
# MAGIC   DATE(timestamp) AS event_date
# MAGIC FROM STREAM multiplex_1_bronze.logistics_intermediate
# MAGIC WHERE warehouse_id IS NOT NULL
# MAGIC   AND batch_id IS NOT NULL;
# MAGIC
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE THE SILVER TABLE FOR STORE OPERATIONS DATA
# MAGIC ------------------------------------------------------
# MAGIC CREATE OR REFRESH STREAMING TABLE multiplex_2_silver.store_ops_silver_demo
# MAGIC AS SELECT 
# MAGIC   COALESCE(extracted_event_id, event_id) AS event_id,
# MAGIC   extracted_timestamp AS timestamp,
# MAGIC   event_group,
# MAGIC   event_type,
# MAGIC   subsidiary_id,
# MAGIC   store_id,
# MAGIC   city,
# MAGIC   region,
# MAGIC   opened_by_employee_id,
# MAGIC   DATE(extracted_timestamp) AS event_date,
# MAGIC   HOUR(extracted_timestamp) AS event_hour,
# MAGIC   SPLIT(store_id, '_')[2] AS store_number
# MAGIC FROM STREAM multiplex_1_bronze.store_ops_intermediate
# MAGIC WHERE extracted_timestamp IS NOT NULL
# MAGIC   AND store_id IS NOT NULL
# MAGIC   AND event_type IS NOT NULL;
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
# MAGIC ### F3. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it runs successfully 
# MAGIC
# MAGIC 2. In the Lakeflow Pipelines editor, explore the pipeline run:
# MAGIC    - Confirm the row counts for each silver streaming table:
# MAGIC      - **194** rows in **logistics_silver_demo**
# MAGIC      - **105** rows in **marketing_silver_demo**
# MAGIC      - **73** rows in **store_ops_silver_demo**
# MAGIC    - Select any silver table and open the **Data** tab to preview the results
# MAGIC
# MAGIC **TROUBLESHOOTING:** If your pipeline does not run successfully, confirm that your volumes were created and that your configuration parameters are set correctly.
# MAGIC
# MAGIC #### Checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multiplex/checkpoint_silver.png" alt="silver" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### F4. Query the Silver Streaming Tables
# MAGIC
# MAGIC 1. Verify that the data has been properly demultiplexed into the silver layer tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM multiplex_2_silver.marketing_silver_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM multiplex_2_silver.logistics_silver_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM multiplex_2_silver.store_ops_silver_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Create Gold Layer Views
# MAGIC
# MAGIC With the silver tables in place, we can now build the gold layer.
# MAGIC
# MAGIC The gold layer consists of materialized views that:
# MAGIC
# MAGIC - Aggregate data from the silver tables
# MAGIC - Apply business friendly calculations and groupings
# MAGIC - Provide stable, analytics ready datasets for dashboards and reporting
# MAGIC - Reduce query complexity for downstream consumers
# MAGIC
# MAGIC In this section, we will create a materialized view on top of the marketing silver table to support common business questions.
# MAGIC
# MAGIC **NOTE:** For this training, we will create a single materialized view. In a real production scenario, you would typically create multiple materialized views to support different analytics needs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Create a New SQL File in Your Pipeline
# MAGIC
# MAGIC 1. On your **multiplex_pipeline** folder select the kebab menu and select **Create File**
# MAGIC
# MAGIC 2. Select the language as **SQL**
# MAGIC
# MAGIC 3. Name the file `gold_view.sql`

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Create Gold View for Marketing Campaign Summary
# MAGIC In this step, we elevate the data to the Gold layer by creating a materialized view, **multiplex_3_gold.marketing_campaign_summary**. This view aggregates the granular event data into high-level performance metrics, optimized for executive reporting and dashboards.
# MAGIC
# MAGIC Here's what this transformation does:
# MAGIC
# MAGIC - Aggregates data from the **marketing_silver_demo** table, grouping it by Campaign, Subsidiary, and Channel to provide a clear performance summary.
# MAGIC - **Calculates Totals:** Sums up key volume metrics (events, impressions, clicks, conversions) and financial metrics (total spend) for each group.
# MAGIC - Derives Strategic KPIs:
# MAGIC   - **ctr_percentage**: The Click-Through Rate formatted as a percentage.
# MAGIC   - **conversion_rate_percentage**: The percentage of clicks that resulted in a successful conversion.
# MAGIC   - **cost_per_conversion**: The average amount spent to acquire a single conversion (CPA).
# MAGIC - Ensures Data Quality:
# MAGIC   - Uses `NULLIF` to prevent "division by zero" errors when calculating ratios.
# MAGIC   - Uses `ROUND` to format currency and percentages to two decimal places for cleaner presentation.
# MAGIC
# MAGIC This materialized view serves as the "source of truth" for campaign performance dashboards, enabling stakeholders to compare channel effectiveness and ROI instantly.
# MAGIC
# MAGIC 1. Copy the code below in your `gold_view.sql` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC ------------------------------------------------------
# MAGIC -- CREATE MATERIALIZED VIEW FOR MARKETING DATA
# MAGIC ------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW multiplex_3_gold.marketing_campaign_summary
# MAGIC AS SELECT
# MAGIC   campaign_id,
# MAGIC   subsidiary_id,
# MAGIC   channel,
# MAGIC   COUNT(*) AS total_events,
# MAGIC   SUM(impressions) AS total_impressions,
# MAGIC   SUM(clicks) AS total_clicks,
# MAGIC   SUM(conversions) AS total_conversions,
# MAGIC   ROUND(SUM(spend_usd), 2) AS total_spend_usd,
# MAGIC   ROUND((SUM(clicks) * 1.0 / NULLIF(SUM(impressions), 0)) * 100, 2) AS ctr_percentage,
# MAGIC   ROUND((SUM(conversions) * 1.0 / NULLIF(SUM(clicks), 0)) * 100, 2) AS conversion_rate_percentage,
# MAGIC   ROUND(SUM(spend_usd) / NULLIF(SUM(conversions), 0), 2) AS cost_per_conversion
# MAGIC FROM multiplex_2_silver.marketing_silver_demo
# MAGIC GROUP BY campaign_id, subsidiary_id, channel;
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
# MAGIC ### G3. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it runs successfully 
# MAGIC
# MAGIC 2. Explore the run in the Lakeflow Pipelines Editor:
# MAGIC    - Confirm **96** rows were ingested into the **marketing_campaign_summary** materialized view
# MAGIC    - Preview the data in the editor (Select **marketing_campaign_summary** → **Data** tab)
# MAGIC
# MAGIC **TROUBLESHOOTING:** If your pipeline does not run successfully, confirm that your volumes were created and that your configuration parameters are set correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM multiplex_3_gold.marketing_campaign_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Enable Iceberg Reads
# MAGIC
# MAGIC Now, we want to enable Iceberg reads on our final streaming table to support cross-platform analytics and data sharing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### H1. Create a Delta Sink
# MAGIC
# MAGIC We need to create a Delta sink table because Iceberg reads (UniForm with Iceberg v2) cannot be enabled directly on streaming tables or views. To do this, we will create a Delta sink from one of our streaming tables.
# MAGIC
# MAGIC 1. What are sinks in Spark Declarative Pipelines, and how do we use them?
# MAGIC
# MAGIC - [Sinks in Apache Spark™ Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/sinks)
# MAGIC
# MAGIC - [Using sinks in pipelines](https://docs.databricks.com/aws/en/ldp/ldp-sinks)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Begin by creating a new Python File in Your Pipeline to create the **delta_sink_logistics** sink . 
# MAGIC     - **NOTE: Only the Python API is supported. SQL is not supported.**
# MAGIC
# MAGIC    a. Click the kebab menu on your **multiplex_pipeline** folder and select **Create file**
# MAGIC
# MAGIC    b. Select the language as **Python**
# MAGIC
# MAGIC    c. Name the file `delta_sink.py`
# MAGIC
# MAGIC    d. Copy the code below and paste in your `delta_sink.py` file.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Here is what the code below does:
# MAGIC - **Defines a Destination (Sink):** The `create_sink` function registers a specific endpoint for your data—a Delta table named `logistics_delta_sink` in the Gold schema—separately from the logic that populates it.
# MAGIC - **Establishes a Streaming Flow:** The `@dp.append_flow` decorator creates a continuous data pipeline that links your processing logic directly to the defined sink, automating the movement of data.
# MAGIC - **Enables Append-Only Logic:** The append flow optimizes performance by adding new records as they arrive, rather than re-processing or overwriting existing data.
# MAGIC - **Automates Incremental Processing:** By using `readStream`, the pipeline automatically tracks which data has already been processed (using checkpointing), ensuring that only new records from the Silver table are moved to Gold.
# MAGIC
# MAGIC 4. Copy and paste into your `delta_sink.py` file.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC # ------------------------------------------------------
# MAGIC #       CREATE DELTA SINK
# MAGIC # ------------------------------------------------------
# MAGIC
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC my_catalog = spark.conf.get("my_catalog")
# MAGIC
# MAGIC dp.create_sink(
# MAGIC   name = "delta_sink_logistics",
# MAGIC   format = "delta",
# MAGIC   options = { "tableName": f"{my_catalog}.multiplex_3_gold.logistics_delta_sink" }
# MAGIC )
# MAGIC
# MAGIC @dp.append_flow(name = "delta_sink_logistics_flow", target="delta_sink_logistics")
# MAGIC def delta_sink_logistics_flow():
# MAGIC   return(
# MAGIC   spark.readStream.table("multiplex_2_silver.logistics_silver_demo")
# MAGIC )
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
# MAGIC ### H2. Run and Explore the Pipeline
# MAGIC
# MAGIC 1. Run the Spark Declarative Pipeline and confirm it runs successfully 
# MAGIC
# MAGIC 2. Review the pipeline results in the Lakeflow Pipelines Editor:
# MAGIC    - Ensure the **marketing_campaign_summary** materialized view contains **96** records.
# MAGIC    - Check that the record count in **logistics_silver_demo** matches the output in **delta_sink_logistics** (should be **194** records).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Pipeline Final State

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/advanced-techniques-with-apache-spark-declarative-pipelines-en_us-1.1.0/images/20261029T211415Z/Advanced Techniques with Apache Spark Declarative Pipelines/Includes/images/multiplex/checkpoint_final.png" alt="final" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3. Examine Table Properties

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Review the table properties and note the following:
# MAGIC - In **Detailed Table Information**, confirm that the **type** is **managed** and the **provider** is **delta**. This indicates it is a managed Delta table.
# MAGIC - In the **table_properties** column of the same section, you will see that deletion vectors are enabled: `delta.enableDeletionVectors=true`.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED multiplex_3_gold.logistics_delta_sink

# COMMAND ----------

# MAGIC %md
# MAGIC ### H4. Enable Iceberg Reads
# MAGIC
# MAGIC 1. To enable Iceberg reads on a Delta table, you must **disable deletion vectors**. Deletion vectors allow soft deletes, but Iceberg requires hard deletes for compatibility.
# MAGIC
# MAGIC       **How to enable Iceberg reads:**
# MAGIC
# MAGIC       a. **Disable deletion vectors:** Turn off deletion vectors for your Delta table.
# MAGIC
# MAGIC
# MAGIC       b. **Enable Iceberg compatibility:** Set the table property to allow Iceberg reads.
# MAGIC
# MAGIC > **NOTE:**  
# MAGIC > With [Iceberg v3](https://docs.databricks.com/aws/en/iceberg/iceberg-v3) (generally available since May 21, 2026; requires Databricks Runtime 18 LTS or above), you do **not** need to disable deletion vectors. 
# MAGIC
# MAGIC For more details, see the [Databricks documentation on deletion vectors](https://docs.databricks.com/aws/en/delta/deletion-vectors).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Disable deletion vectors
# MAGIC ALTER TABLE multiplex_3_gold.logistics_delta_sink 
# MAGIC   SET TBLPROPERTIES (
# MAGIC     'delta.enableDeletionVectors' = 'false'
# MAGIC   );
# MAGIC
# MAGIC -- Step 2: Enable Iceberg compatibility
# MAGIC ALTER TABLE multiplex_3_gold.logistics_delta_sink 
# MAGIC   SET TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode' = 'name',
# MAGIC     'delta.enableIcebergCompatV2' = 'true',
# MAGIC     'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Let's review the table properties and observe the changes:
# MAGIC - You will see a new section called **Delta Uniform Iceberg**, which includes details such as 
# MAGIC   - **metadata location**, 
# MAGIC   - **converted delta version**,
# MAGIC   - **converted delta timestamp**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED multiplex_3_gold.logistics_delta_sink;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Let's view the table properties:
# MAGIC - `delta.universalFormat.enabledFormats = iceberg` confirms that Iceberg format is enabled.
# MAGIC - `delta.enableDeletionVectors = false` shows that deletion vectors is not enabled.
# MAGIC - The table currently supports Iceberg version 2: `delta.feature.icebergCompatV2 = supported`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES multiplex_3_gold.logistics_delta_sink;

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Land Another File in Cloud Storage
# MAGIC
# MAGIC Earlier in the workshop, we ingested a single file. In this step, we will land an additional file into the cloud storage volume to demonstrate how Spark Declarative Pipelines respond when new data is detected.
# MAGIC
# MAGIC 1. Run the command below to add the new file to your source volume, then confirm that the volume now contains two files.
# MAGIC

# COMMAND ----------

ops_path = f'/Volumes/{my_catalog}/multiplex_1_bronze/ops'
business_events_source_path = f'/Volumes/{my_catalog}/multiplex_1_bronze/business_events'


#copy files from Ops location to user's source volume
copy_files(copy_from = f'{ops_path}', copy_to = business_events_source_path, n = 2)

spark.sql(f"LIST '{business_events_source_path}' ").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Let's review the number of rows present in the newly landed file that will be ingested by our pipeline:
# MAGIC - **Marketing**: 62 new records
# MAGIC - **Store Ops**: 80 new records
# MAGIC - **Logistics**: 158 new records
# MAGIC - **Total**: 300 new records in this file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     topic,
# MAGIC     count(*) as number_of_records_per_event_group , 
# MAGIC     sum(count(*)) OVER () as total_records_in_raw_file
# MAGIC FROM read_files(
# MAGIC     my_vol_path || '/business_events/part-02_business_events.parquet'
# MAGIC )
# MAGIC GROUP BY topic;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the Spark Declarative Pipeline to incrementally process the new file and confirm that it completes successfully.
# MAGIC
# MAGIC 4. In the Lakeflow Pipelines editor, review the pipeline run:
# MAGIC    - Verify that **300** rows were ingested into the bronze demo table from the `business_events` source volume with the new landed file.
# MAGIC    - Confirm the row counts for each intermediate bronze and silver streaming table:
# MAGIC      - **158** rows in both **logistics_intermediate** and **logistics_silver_demo**
# MAGIC      - **80** rows in both **store_ops_intermediate** and **store_ops_silver_demo**
# MAGIC      - **62** rows in both **marketing_intermediate** and **marketing_silver_demo**
# MAGIC    - Verify that the materialized view **marketing_campaign_summary** contains **141** output records.
# MAGIC    - Ensure that **delta_sink_logistics** has the same number of records as **logistics_silver_demo** - **158** records.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Summary and Key Takeaways 
# MAGIC - **Multiplex Data Pipeline Architecture**: Successfully built a complete multiplex streaming pipeline using Spark Declarative Pipelines that ingested data from a single source and demultiplexed it into three separate business domain tables (marketing, logistics, and store operations)
# MAGIC - **VARIANT Data Type Implementation**: Leveraged the VARIANT data type to efficiently handle semi-structured JSON data with different schemas across multiple event types in a single bronze table
# MAGIC - **Medallion Architecture with Streaming**: Implemented the full medallion pattern (bronze → silver → gold) using streaming tables, intermediate transformations, and materialized views for real-time data processing
# MAGIC - **Delta Sinks and Iceberg Compatibility**: Created Delta sinks using the Python API and enabled Iceberg compatibility for cross-platform analytics by disabling deletion vectors and configuring universal format properties
# MAGIC
# MAGIC #### Business Value Delivered
# MAGIC
# MAGIC The pipeline processed **672 total records** from two source files containing multiplexed business events, creating domain-specific analytics tables that enable:
# MAGIC - Real-time operational insights across marketing campaigns, logistics shipments, and store operations
# MAGIC - Cross-platform data access through Iceberg compatibility for broader ecosystem integration  
# MAGIC - Scalable event processing architecture that can handle additional business domains and event types
# MAGIC - Automated incremental processing with streaming checkpoints ensuring exactly-once delivery
# MAGIC
# MAGIC This multiplex architecture provides a foundation for enterprise-scale event processing while maintaining data lineage, real-time capabilities, and cross-platform compatibility through Delta Universal Format.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>