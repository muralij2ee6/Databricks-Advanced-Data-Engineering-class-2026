# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">09 Lecture - Summary and Next Steps</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/common/databricks_academy_logo.png)

# COMMAND ----------

# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Lecture - Summary and Next Steps
# MAGIC #### Duration: 5 minutes

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1200px; margin: 0 auto 16px auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #1B5162; color: white; border-radius: 8px 8px 4px 4px; padding: 16px 24px; text-align: center; margin-bottom: 12px;">
# MAGIC <div style="font-size: 22pt; font-weight: 700; margin-bottom: 6px;">What You Learned in This Course</div>
# MAGIC <div style="font-size: 16pt; font-weight: 400; line-height: 1.4; opacity: 0.95;">Four phases to find personal data, protect it, honor an erasure request and prove each control worked.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: rgba(27,81,98,0.08); border-radius: 10px; padding: 10px 14px; margin-bottom: 10px;">
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #1B5162; letter-spacing: 2px; text-align: center; margin-bottom: 8px;">IDENTIFY</div>
# MAGIC <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; max-width: 700px; margin: 0 auto;">
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #1B5162; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #1B5162; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Discover and Classify</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">Data Classification, confidence, samples</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #1B5162; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #1B5162; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Governed Tags</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">One sensitivity_level per column</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="text-align: center; font-size: 20pt; color: #5A6F77; line-height: 1; margin: 4px 0 6px 0;">&darr;</div>
# MAGIC
# MAGIC <div style="background: rgba(0,169,114,0.08); border-radius: 10px; padding: 10px 14px; margin-bottom: 10px;">
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #00A972; letter-spacing: 2px; text-align: center; margin-bottom: 8px;">PROTECT</div>
# MAGIC <div style="display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px;">
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #00A972; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #00A972; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Tag-Driven ABAC</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">Column masks across a schema</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #00A972; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #00A972; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Manual Masks</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">SET MASK and SET ROW FILTER</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #00A972; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #00A972; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">De-identification</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">Hashing, tokens, encryption</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #00A972; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #00A972; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Secrets</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">Salts and keys kept out of code</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="text-align: center; font-size: 20pt; color: #5A6F77; line-height: 1; margin: 4px 0 6px 0;">&darr;</div>
# MAGIC
# MAGIC <div style="background: rgba(255,95,70,0.10); border-radius: 10px; padding: 10px 14px; margin-bottom: 10px;">
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #FF5F46; letter-spacing: 2px; text-align: center; margin-bottom: 8px;">MANAGE</div>
# MAGIC <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; max-width: 700px; margin: 0 auto;">
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #FF5F46; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #FF5F46; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Right to Erasure</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">Track, delete, propagate</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #FF5F46; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #FF5F46; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Physical Purge</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">REORG PURGE and VACUUM</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="text-align: center; font-size: 20pt; color: #5A6F77; line-height: 1; margin: 4px 0 6px 0;">&darr;</div>
# MAGIC
# MAGIC <div style="background: rgba(97,135,148,0.10); border-radius: 10px; padding: 10px 14px; margin-bottom: 10px;">
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #618794; letter-spacing: 2px; text-align: center; margin-bottom: 8px;">VERIFY</div>
# MAGIC <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; max-width: 700px; margin: 0 auto;">
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #618794; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #618794; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Evidence Trail</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">Lineage, audit, query history</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #FFFFFF; border: 1.5px solid #618794; border-radius: 8px; padding: 10px 10px; text-align: center;">
# MAGIC <div style="color: #618794; font-size: 28pt; font-weight: 700; line-height: 1;">&#10003;</div>
# MAGIC <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin: 4px 0 2px 0;">Table History</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; line-height: 1.35;">DESCRIBE HISTORY as evidence</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 8px; padding: 12px 22px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); font-size: 15pt; color: #0b2026; line-height: 1.5; text-align: center;">
# MAGIC You framed privacy against security, governance and compliance. You then ran the <strong>privacy decision model</strong> on every technique and closed each one with evidence that the control worked.
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Additional Resources
# MAGIC
# MAGIC Explore these resources to keep learning and stay up to date with data privacy on Databricks.
# MAGIC
# MAGIC #### Discover and classify
# MAGIC
# MAGIC - Data Classification. Detect sensitive columns and apply `class.*` tags:
# MAGIC [AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/data-classification)
# MAGIC
# MAGIC - Supported classification tags. The `class.*` tags a scan can apply:
# MAGIC [AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-tags) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification-tags) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/data-classification-tags)
# MAGIC
# MAGIC - Custom classifiers (Beta). Detect identifiers specific to your organization:
# MAGIC [AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-custom-classifiers) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification-custom-classifiers) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/data-classification-custom-classifiers)
# MAGIC
# MAGIC - Data classification system table. Query the detections directly:
# MAGIC [AWS](https://docs.databricks.com/aws/en/admin/system-tables/data-classification) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/data-classification) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/admin/system-tables/data-classification)
# MAGIC
# MAGIC - Governed tags. Define a tag key and its allowed values centrally:
# MAGIC [AWS](https://docs.databricks.com/aws/en/admin/governed-tags) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/governed-tags) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/admin/governed-tags)
# MAGIC
# MAGIC - Apply tags to Unity Catalog securable objects:
# MAGIC [AWS](https://docs.databricks.com/aws/en/database-objects/tags) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/database-objects/tags) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/database-objects/tags)
# MAGIC
# MAGIC #### Protect by purpose and risk
# MAGIC
# MAGIC - Attribute-based access control (ABAC). Tag-driven policies that mask or filter data across a catalog or schema:
# MAGIC [AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/abac/)
# MAGIC
# MAGIC - Best practices for ABAC policies:
# MAGIC [AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/best-practices) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/best-practices) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/abac/best-practices)
# MAGIC
# MAGIC - Row filters and column masks. The manual alternative applied to a single table:
# MAGIC [AWS](https://docs.databricks.com/aws/en/tables/row-and-column-filters) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/tables/row-and-column-filters) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/tables/row-and-column-filters)
# MAGIC
# MAGIC #### De-identify and manage keys
# MAGIC
# MAGIC - Secret management. Keep salts and keys out of notebooks and code:
# MAGIC [AWS](https://docs.databricks.com/aws/en/security/secrets/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/security/secrets/)
# MAGIC
# MAGIC - `secret` function. Read a secret from SQL:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/functions/secret) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/secret) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/functions/secret)
# MAGIC
# MAGIC - `aes_encrypt` function. Encrypt a column with a key:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/functions/aes_encrypt) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/aes_encrypt) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/functions/aes_encrypt)
# MAGIC
# MAGIC - `aes_decrypt` function. Authorized reversal of an encrypted value:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/functions/aes_decrypt) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/aes_decrypt) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/functions/aes_decrypt)
# MAGIC
# MAGIC - `sha2` function. The hash behind a salted pseudonym:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/functions/sha2) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/sha2) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/functions/sha2)
# MAGIC
# MAGIC #### Erasure and physical purge
# MAGIC
# MAGIC - Prepare your data for GDPR compliance. Patterns for deleting a subject's data on request:
# MAGIC [AWS](https://docs.databricks.com/aws/en/ldp/gdpr) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/gdpr) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ldp/gdpr)
# MAGIC
# MAGIC - Deletion vectors. Why a `DELETE` marks rows instead of rewriting files:
# MAGIC [AWS](https://docs.databricks.com/aws/en/delta/deletion-vectors) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/delta/deletion-vectors) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/delta/deletion-vectors)
# MAGIC
# MAGIC - `REORG TABLE`. Rewrite files with `APPLY (PURGE)` so deleted rows are physically removed:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/delta-reorg-table) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-reorg-table) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/delta-reorg-table)
# MAGIC
# MAGIC - `VACUUM`. Remove files that are no longer referenced, after the retention window:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-vacuum) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/delta-vacuum)
# MAGIC
# MAGIC - `DESCRIBE HISTORY`. Table history as one source of erasure evidence:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/delta-describe-history) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-describe-history) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/delta-describe-history)
# MAGIC
# MAGIC #### Evidence with system tables
# MAGIC
# MAGIC - Lineage system tables. Follow a column to the downstream tables it reaches:
# MAGIC [AWS](https://docs.databricks.com/aws/en/admin/system-tables/lineage) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/lineage) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/admin/system-tables/lineage)
# MAGIC
# MAGIC - Audit log system table. Who did what, on which object and when:
# MAGIC [AWS](https://docs.databricks.com/aws/en/admin/system-tables/audit-logs) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/audit-logs) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/admin/system-tables/audit-logs)
# MAGIC
# MAGIC - Query history system table. The statements behind an action:
# MAGIC [AWS](https://docs.databricks.com/aws/en/admin/system-tables/query-history) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/query-history) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/admin/system-tables/query-history)
# MAGIC
# MAGIC #### Trust and compliance
# MAGIC
# MAGIC - [Databricks Security and Trust Center](https://www.databricks.com/trust) and its [Compliance](https://www.databricks.com/trust/compliance) page. The live source for the certifications and attestations Databricks holds.
# MAGIC
# MAGIC #### Beyond this course
# MAGIC
# MAGIC These capabilities sit next to data privacy on Databricks, but the course doesn't teach them.
# MAGIC
# MAGIC - Customer-managed keys. Encryption at rest with a key you hold protects storage transparently. The course taught selective, in-query column encryption instead:
# MAGIC [AWS](https://docs.databricks.com/aws/en/security/keys/customer-managed-keys) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/security/keys/customer-managed-keys) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/security/keys/customer-managed-keys)
# MAGIC
# MAGIC - Workspace-catalog binding. Workspace, catalog and cloud-account isolation separate environments structurally. The course worked inside one catalog with fine-grained controls:
# MAGIC [AWS](https://docs.databricks.com/aws/en/catalogs/binding) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/catalogs/binding) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/catalogs/binding)
# MAGIC
# MAGIC - Databricks Clean Rooms. Run approved analysis across organizations without exposing the underlying rows:
# MAGIC [AWS](https://docs.databricks.com/aws/en/clean-rooms/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/clean-rooms/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/clean-rooms/)
# MAGIC
# MAGIC - OpenSharing (formerly Delta Sharing). Share live data across organizations without copying it:
# MAGIC [AWS](https://docs.databricks.com/aws/en/delta-sharing/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/delta-sharing/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/delta-sharing/)
# MAGIC
# MAGIC - `ai_mask` function. Mask personal data inside unstructured text with AI Functions. The course scoped itself to structured data:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_mask) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/ai_mask) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/functions/ai_mask)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Next Steps
# MAGIC
# MAGIC Continue building your Databricks skills with additional training and certification resources.
# MAGIC
# MAGIC ### B1. Continue Your Learning
# MAGIC
# MAGIC Expand your data and AI knowledge through Databricks self-paced and instructor-led training. These courses help you deepen your technical skills on the Databricks platform.
# MAGIC
# MAGIC Visit the [Databricks Training and Certification](https://www.databricks.com/learn/training/home)
# MAGIC
# MAGIC - [Advanced Data Engineering with Databricks](https://www.databricks.com/training/catalog/advanced-data-engineering-with-databricks-971). The professional series this course belongs to.
# MAGIC
# MAGIC - [Advanced Techniques with Apache Spark Declarative Pipelines](https://www.databricks.com/training/catalog/advanced-techniques-with-apache-spark-declarative-pipelines-2972)
# MAGIC
# MAGIC - [Databricks Performance Optimization](https://www.databricks.com/training/catalog/databricks-performance-optimization-2967)
# MAGIC
# MAGIC - [Automated Deployment with Declarative Automation Bundles](https://www.databricks.com/training/catalog/automated-deployment-with-declarative-automation-bundles-3489)
# MAGIC
# MAGIC - [DevOps Essentials for Data Engineering](https://www.databricks.com/training/catalog/devops-essentials-for-data-engineering-3605)
# MAGIC
# MAGIC
# MAGIC ### B2. Earn a Certification
# MAGIC
# MAGIC Validate your Databricks expertise by earning an official credential. Certifications demonstrate your ability to apply Databricks technologies in real-world data and AI workloads.
# MAGIC
# MAGIC Visit the [Databricks Certification and Badging](https://www.databricks.com/learn/training/certification)
# MAGIC
# MAGIC - [Databricks Certified Data Engineer Professional](https://www.databricks.com/learn/certification/data-engineer-professional)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>