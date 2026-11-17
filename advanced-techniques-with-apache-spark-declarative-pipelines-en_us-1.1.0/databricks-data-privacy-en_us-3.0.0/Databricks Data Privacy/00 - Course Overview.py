# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">00 Lecture - Course Overview</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/common/databricks_academy_logo.png)

# COMMAND ----------

# DBTITLE 1,Overview and Terminal Objectives
# MAGIC %md
# MAGIC # Databricks Data Privacy
# MAGIC #### Duration: 5 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In a typical lakehouse, personal data spreads from source tables into downstream copies and aggregates. Obligations such as restricted access and the right to erasure hold only when every copy is found, protected by purpose and risk and accounted for with evidence. Databricks Data Privacy treats privacy as a set of technical decisions and controls built on Unity Catalog and Delta Lake. It separates privacy from security, governance and compliance, then organizes the work around an identify, protect, manage and verify lifecycle and a decision model applied to every technique.
# MAGIC
# MAGIC The course follows one retailer's data estate through that lifecycle. Data Classification and governed tags identify sensitive columns and record how each should be treated. Tag-driven attribute-based access control (ABAC) policies mask those columns across a schema, with manual column masks and row filters for the cases a central policy can't cover. De-identification goes further with salted hashing, tokenization through a separately governed vault and column encryption, with the salt and encryption key held in Databricks Secrets. For the right to erasure, the course tracks a request in a control plane, finds the subject's data with classification and lineage, deletes and propagates the change with `DELETE` and `MERGE`, then purges the underlying files with `REORG TABLE ... APPLY (PURGE)` and retention-aware `VACUUM`. Lineage, audit logs, query history and Delta table history then combine into an evidence trail an auditor can check.
# MAGIC
# MAGIC The course is technical and does not provide legal advice.
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC
# MAGIC By the end of this course, you will be able to:
# MAGIC
# MAGIC 1. **Explain** how privacy differs from security, governance and compliance and how the identify, protect, manage and verify lifecycle organizes privacy work on Databricks.
# MAGIC 2. **Choose** a privacy control for a given purpose and risk using a structured privacy decision model.
# MAGIC 3. **Use** Data Classification and governed tags to find sensitive columns and record how each should be treated.
# MAGIC 4. **Configure** tag-driven ABAC policies, manual column masks and row filters to restrict access to sensitive columns.
# MAGIC 5. **Apply** de-identification techniques, including salted hashing, tokenization with a vault and column encryption, with salts and keys stored in Databricks Secrets.
# MAGIC 6. **Execute** a right-to-erasure request from discovery with classification and lineage through deletion, downstream propagation and physical file purge.
# MAGIC 7. **Construct** an evidence trail from classification, lineage, audit logs, query history and Delta table history that shows whether a privacy control worked.

# COMMAND ----------

# DBTITLE 1,A. Prerequisites
# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC - Basic familiarity with the Databricks workspace, including navigating it, opening notebooks and running notebook cells.
# MAGIC - Familiarity with Unity Catalog concepts, including catalogs, schemas, tables and granting privileges.
# MAGIC - Intermediate SQL, including joins, aggregations and `MERGE` statements.
# MAGIC - Basic familiarity with Delta Lake tables, including table history and time travel.
# MAGIC - Familiarity with layered data pipelines such as the medallion (bronze, silver, gold) architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Workspace Setup Information

# COMMAND ----------

# DBTITLE 1,Workspace Prerequisites
# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #c62828; font-size: 1.1em;">Prerequisites</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">This content assumes you have:</p>
# MAGIC
# MAGIC - Access to <strong>Serverless Compute</strong> (all notebooks run on serverless notebook compute, environment version 6).
# MAGIC - A <strong>serverless SQL warehouse</strong> (2X-Small is enough) for the SQL you run across the course.
# MAGIC - <strong>Unity Catalog</strong> enabled in your workspace.
# MAGIC - The governed <strong>sensitivity_level</strong> tag and the <strong>dbacademy_privacy</strong> secret scope in place. The course's workspace setup creates both (see B1 and B2).
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Vocareum Workspace
# MAGIC %md-sandbox
# MAGIC ### B1. Databricks Provided Vocareum Workspace (Recommended)
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If you are running this course in a <strong>Databricks Academy provided Vocareum workspace</strong>, your Unity Catalog catalog is already created for you.
# MAGIC
# MAGIC - Your catalog name matches your Vocareum username and looks like: <strong>labuser12345</strong> (series of unique numbers)
# MAGIC
# MAGIC - The workspace setup has already created the governed <strong>sensitivity_level</strong> tag and the <strong>dbacademy_privacy</strong> secret scope the demos use.
# MAGIC
# MAGIC - This course does not require <strong>Marketplace</strong> data. All demo and lab data is generated synthetically by the setup scripts.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Free Edition or Outside Workspaces
# MAGIC %md-sandbox
# MAGIC ### B2. Databricks Free Edition or Outside Workspaces (*as is*)
# MAGIC
# MAGIC ##### Databricks Free Edition or Other Workspaces may work for this, but it is provided **as is** and support is not guaranteed.
# MAGIC
# MAGIC Some features may not be available depending on the capabilities of Databricks Free Edition or your Workspace. Governed tags, ABAC policies and Databricks Secrets in particular may not be available everywhere.
# MAGIC
# MAGIC Please read below to setup your environment
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC #### Catalog Information
# MAGIC
# MAGIC - If you are running this course in your own Databricks workspace or Databricks Free Edition, the setup will <strong>create a Unity Catalog catalog and schema for you</strong>.
# MAGIC
# MAGIC - The <strong>Create Catalog</strong> permission is required.
# MAGIC
# MAGIC - The catalog name is derived from your Databricks username and follows this pattern: <strong>labuser_username</strong>
# MAGIC
# MAGIC #### Run the Workspace Setup Once
# MAGIC
# MAGIC - A workspace admin runs <strong>Includes/Workspace-Setup</strong> once before learners start. It creates the governed <strong>sensitivity_level</strong> tag, grants <strong>ASSIGN</strong> on it so learners can apply it to their own tables and creates the <strong>dbacademy_privacy</strong> secret scope with demo values.
# MAGIC
# MAGIC - The ABAC demo and the first lab also use an <strong>account privileged</strong> account group as the policy exception. It has to be created at the account level and is optional. Without it, those notebooks show the same contrast with a deterministic fallback.
# MAGIC
# MAGIC #### Access Marketplace Data
# MAGIC
# MAGIC Marketplace data is not required.
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Troubleshooting Setup - Your Workspace Can't Create Catalogs
# MAGIC   </strong>
# MAGIC <details>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC If you do not have permission to create a new catalog but already have one available, you can explicitly specify an existing catalog:
# MAGIC
# MAGIC - Open the <strong>Classroom-Setup-NN</strong> notebook in <strong>./Includes</strong> for the notebook you're running, for example <strong>Classroom-Setup-03</strong>.
# MAGIC   - Set the catalog name here: `FORCED_CATALOG = 'YOUR_CATALOG_NAME'`, then rerun that notebook's setup cell.
# MAGIC
# MAGIC   </div>
# MAGIC </details>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Do Not Run in Production Environments</strong>
# MAGIC
# MAGIC <div style="color:#333;">
# MAGIC <ul>
# MAGIC <li>Only run this course in <strong>development or sandbox workspaces</strong>.</li>
# MAGIC <li>Do not run in production environments. The setup scripts create catalogs, schemas and tables in your workspace.</li>
# MAGIC <li>The workspace setup also creates an account-level governed tag and a workspace secret scope with demo values.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Course Agenda
# MAGIC %md-sandbox
# MAGIC ## C. Course Agenda
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC
# MAGIC <style>
# MAGIC .agenda-table td, .agenda-table th {
# MAGIC   font-size: 14pt !important;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 50px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 80px;">Type</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Module Name</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">1</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">REQUIRED - Project Overview and Setup</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">2</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Privacy, Security, Governance and Compliance</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">3</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Discover and Classify Sensitive Data</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">4</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">ABAC and Manual Masks</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">5</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e8f5e9; color: #2e7d32; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lab</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Discover, Tag and Protect PII</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">6</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">De-identification and Key Management</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">7</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Right to Erasure: Delete, Propagate, Purge</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">8</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Privacy Evidence with System Tables</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">9</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Summary and Next Steps</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">10</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e8f5e9; color: #2e7d32; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lab</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Fulfil and Verify an Erasure Request</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - Use Databricks Runtime version: **`Serverless V6`** for running all demo and lab notebooks.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>