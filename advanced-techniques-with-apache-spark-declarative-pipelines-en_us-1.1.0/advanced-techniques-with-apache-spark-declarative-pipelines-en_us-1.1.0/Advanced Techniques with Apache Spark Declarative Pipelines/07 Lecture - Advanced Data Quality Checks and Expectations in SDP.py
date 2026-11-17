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
# MAGIC # Lecture — Advanced Data Quality Checks and Expectations in Spark Declarative Pipelines
# MAGIC
# MAGIC #### Duration: ~10-12 Minutes
# MAGIC
# MAGIC In production data pipelines, data quality is paramount. While Spark Declarative Pipelines provide built-in expectations with three violation modes — <strong>WARN</strong>, <strong>DROP ROW</strong>, and <strong>FAIL UPDATE</strong> — real-world scenarios demand more sophisticated approaches. Production pipelines must validate business rules, handle schema evolution gracefully, and preserve every record for audit and remediation purposes.
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC - Explain the limitations of basic violation modes for production-grade data quality
# MAGIC - Implement range-based and NULL-tolerant constraint expressions using advanced SQL patterns
# MAGIC - **Apply advanced patterns** including row-count validation, missing record detection, and primary key uniqueness
# MAGIC - Describe **schema evolution** and design bronze layers that handle it safely without pipeline failures
# MAGIC - Apply the **quarantine pattern** **using inverse logic** to achieve zero data loss with detailed failure tracking
# MAGIC - Choose appropriate strategies between DROP ROW and quarantine based on business requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Why Advanced Quality Expectations?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. The Limits of Basic Expectations
# MAGIC
# MAGIC A <code>NOT NULL</code> check confirms that a field is <em>present</em> — but presence alone does not mean a value is <em>correct</em>. The table below shows the range of quality problems that appear in real production data, and whether a basic <code>NOT NULL</code> check catches them.
# MAGIC
# MAGIC <div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:16px;margin-bottom:20px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#0b2026;color:#F9F7F4;">
# MAGIC       <th style="padding:12px 16px;text-align:left;">Problem Type</th>
# MAGIC       <th style="padding:12px 16px;text-align:left;">Example</th>
# MAGIC       <th style="padding:12px 16px;text-align:center;">NOT NULL catches it?</th>
# MAGIC       <th style="padding:12px 16px;text-align:center;">Advanced Expectation?</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Numeric anomaly</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">Negative quantity in an order</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Temporal inconsistency</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">Event date set to year 1970 (system default)</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Range violation</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">Discount rate = 120%</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Optional field rule</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">Field may be NULL, but when present must be &gt;= 0</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;"><strong>Schema evolution</strong></td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;">New column added mid-stream breaks existing rules</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;"><strong>Data loss</strong></td>
# MAGIC       <td style="padding:11px 16px;">Invalid records permanently dropped — no audit trail</td>
# MAGIC       <td style="padding:11px 16px;text-align:center;color:#98102A;">❌</td>
# MAGIC       <td style="padding:11px 16px;text-align:center;color:#00A972;">✅</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table></div><!--ox-->
# MAGIC
# MAGIC <div style="background:#FFF8E8;border-left:4px solid #FFAB00;border-radius:4px;padding:14px 20px;font-size:16px;color:#0b2026;line-height:1.9;">
# MAGIC   <strong style="color:#FF5F46;"> Real-World Example:</strong> A <code>discount_rate</code> field containing <code>120</code> passes every <code>NOT NULL</code> check without complaint — but it represents an impossible discount that will silently corrupt downstream revenue calculations. Only a range constraint like <code>discount_rate BETWEEN 0 AND 100</code> catches this before it flows downstream.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Advanced Expectation Patterns
# MAGIC
# MAGIC Beyond row-level constraints, Databricks expectations support <strong>cross-table validation</strong>. These patterns let you verify row counts, detect missing records, and enforce primary key uniqueness across datasets — catching problems that single-row checks cannot see.
# MAGIC
# MAGIC <div style="margin: 20px 0; display: flex; flex-direction: column; gap: 20px;">
# MAGIC
# MAGIC   <!-- Card 1 -->
# MAGIC   <div style="background:#E3F2FD; border-top: 4px solid #4299E0; border-radius: 8px; padding: 20px;">
# MAGIC     <div style="font-weight: bold; color: #4299E0; font-size: 15px; margin-bottom: 8px;">Row Count Validation</div>
# MAGIC     <div style="font-size: 15px; color: #618794; margin-bottom: 14px; line-height: 1.7;">Validates that row counts match between two tables — useful after joins, aggregations, or pipeline fan-out to ensure no records were silently dropped.</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">MATERIALIZED</span> <span style="color:#C92A18;font-weight:600;">VIEW</span> count_verification (
# MAGIC   <span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> no_rows_dropped <span style="color:#C92A18;font-weight:600;">EXPECT</span> (a_count == b_count)
# MAGIC     <span style="color:#C92A18;font-weight:600;">ON</span> <span style="color:#C92A18;font-weight:600;">VIOLATION</span> <span style="color:#C92A18;font-weight:600;">FAIL</span> <span style="color:#C92A18;font-weight:600;">UPDATE</span>
# MAGIC )
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> * <span style="color:#C92A18;font-weight:600;">FROM</span>
# MAGIC   (<span style="color:#C92A18;font-weight:600;">SELECT</span> <span style="color:#C92A18;font-weight:600;">COUNT</span>(*) <span style="color:#C92A18;font-weight:600;">AS</span> a_count <span style="color:#C92A18;font-weight:600;">FROM</span> table_a),
# MAGIC   (<span style="color:#C92A18;font-weight:600;">SELECT</span> <span style="color:#C92A18;font-weight:600;">COUNT</span>(*) <span style="color:#C92A18;font-weight:600;">AS</span> b_count <span style="color:#C92A18;font-weight:600;">FROM</span> table_b)</pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Card 2 -->
# MAGIC   <div style="background:#FFF3E0; border-top: 4px solid #FFAB00; border-radius: 8px; padding: 20px;">
# MAGIC     <div style="font-weight: bold; color: #FFAB00; font-size: 15px; margin-bottom: 8px;">Missing Record Detection</div>
# MAGIC     <div style="font-size: 15px; color: #618794; margin-bottom: 14px; line-height: 1.7;">Uses a LEFT OUTER JOIN to identify records present in a validation copy but absent in the report table — catching completeness failures that row-level checks miss entirely.</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">MATERIALIZED</span> <span style="color:#C92A18;font-weight:600;">VIEW</span> report_compare_tests (
# MAGIC   <span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> no_missing_records <span style="color:#C92A18;font-weight:600;">EXPECT</span> (r_key <span style="color:#C92A18;font-weight:600;">IS</span> <span style="color:#C92A18;font-weight:600;">NOT</span> <span style="color:#C92A18;font-weight:600;">NULL</span>)
# MAGIC     <span style="color:#C92A18;font-weight:600;">ON</span> <span style="color:#C92A18;font-weight:600;">VIOLATION</span> <span style="color:#C92A18;font-weight:600;">FAIL</span> <span style="color:#C92A18;font-weight:600;">UPDATE</span>
# MAGIC )
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> v.*, r.key <span style="color:#C92A18;font-weight:600;">AS</span> r_key
# MAGIC <span style="color:#C92A18;font-weight:600;">FROM</span> validation_copy v
# MAGIC <span style="color:#C92A18;font-weight:600;">LEFT</span> <span style="color:#C92A18;font-weight:600;">OUTER</span> <span style="color:#C92A18;font-weight:600;">JOIN</span> report r <span style="color:#C92A18;font-weight:600;">ON</span> v.key = r.key</pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Card 3 -->
# MAGIC   <div style="background:#FFEBEE; border-top: 4px solid #FF5F46; border-radius: 8px; padding: 20px;">
# MAGIC     <div style="font-weight: bold; color: #FF5F46; font-size: 15px; margin-bottom: 8px;">Primary Key Uniqueness</div>
# MAGIC     <div style="font-size: 15px; color: #618794; margin-bottom: 14px; line-height: 1.7;">Groups by the primary key and checks that every group has exactly one entry. Catches duplicate key violations before they corrupt downstream joins or aggregations.</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">MATERIALIZED</span> <span style="color:#C92A18;font-weight:600;">VIEW</span> report_pk_tests (
# MAGIC   <span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> unique_pk <span style="color:#C92A18;font-weight:600;">EXPECT</span> (num_entries = <span style="color:#2272B4;">1</span>)
# MAGIC     <span style="color:#C92A18;font-weight:600;">ON</span> <span style="color:#C92A18;font-weight:600;">VIOLATION</span> <span style="color:#C92A18;font-weight:600;">FAIL</span> <span style="color:#C92A18;font-weight:600;">UPDATE</span>
# MAGIC )
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> pk, <span style="color:#C92A18;font-weight:600;">COUNT</span>(*) <span style="color:#C92A18;font-weight:600;">AS</span> num_entries
# MAGIC <span style="color:#C92A18;font-weight:600;">FROM</span> report
# MAGIC <span style="color:#C92A18;font-weight:600;">GROUP</span> <span style="color:#C92A18;font-weight:600;">BY</span> pk</pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#F9F7F4; border-left: 4px solid #618794; border-radius: 4px; padding: 14px 20px; font-size: 16px; color: #0B2026; line-height: 1.9;">
# MAGIC   <strong style="color:#1B5162;">For more details, refer to the documentation:</strong>
# MAGIC   <a href="https://docs.databricks.com/aws/en/ldp/expectation-patterns?language=SQL" style="color:#4299E0; font-weight: bold;">Databricks Expectation Patterns documentation</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. Writing NULL-Tolerant Constraints
# MAGIC
# MAGIC Each <code>CONSTRAINT</code> block should validate a <strong>single logical rule</strong>. This gives the pipeline UI precise per-constraint metrics. But there is a critical trap: <strong>NULL evaluates to NOT TRUE</strong> in SQL, so a basic range check treats every NULL as a violation.
# MAGIC
# MAGIC <div style="display:flex;flex-direction:column;gap:20px;margin:20px 0;">
# MAGIC
# MAGIC   <!-- Card 1: Naive -->
# MAGIC   <div style="background:#FFEBEE;border-top:4px solid #98102A;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#98102A;font-size:15px;margin-bottom:10px;">Naive — NULLs treated as violations</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> valid_discount
# MAGIC <span style="color:#C92A18;font-weight:600;">EXPECT</span> (
# MAGIC   discount_rate &gt;= <span style="color:#2272B4;">0</span>
# MAGIC   <span style="color:#C92A18;font-weight:600;">AND</span> discount_rate &lt;= <span style="color:#2272B4;">100</span>
# MAGIC )
# MAGIC <span style="color:#6A737D;">-- Every NULL record is flagged as a violation</span></pre></div>
# MAGIC     <div style="margin-top:12px;font-size:14px;color:#618794;line-height:1.7;">After schema evolution, all historic records that predate the <code>discount_rate</code> column will have <code>NULL</code> — and every one will fail this constraint, flooding the pipeline UI with false violations.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Card 2: NULL-Tolerant -->
# MAGIC   <div style="background:#E8F5E9;border-top:4px solid #00A972;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#00A972;font-size:15px;margin-bottom:10px;">NULL-Tolerant — validates only when present</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> valid_discount
# MAGIC <span style="color:#C92A18;font-weight:600;">EXPECT</span> (
# MAGIC   <span style="color:#C92A18;font-weight:600;">CASE</span>
# MAGIC     <span style="color:#C92A18;font-weight:600;">WHEN</span> discount_rate <span style="color:#C92A18;font-weight:600;">IS</span> <span style="color:#C92A18;font-weight:600;">NOT</span> <span style="color:#C92A18;font-weight:600;">NULL</span>
# MAGIC     <span style="color:#C92A18;font-weight:600;">THEN</span> discount_rate &gt;= <span style="color:#2272B4;">0</span>
# MAGIC          <span style="color:#C92A18;font-weight:600;">AND</span> discount_rate &lt;= <span style="color:#2272B4;">100</span>
# MAGIC     <span style="color:#C92A18;font-weight:600;">ELSE</span> <span style="color:#C92A18;font-weight:600;">TRUE</span>   <span style="color:#6A737D;">-- NULL is acceptable</span>
# MAGIC   <span style="color:#C92A18;font-weight:600;">END</span>
# MAGIC )</pre></div>
# MAGIC     <div style="margin-top:12px;font-size:14px;color:#618794;line-height:1.7;">NULL records pass cleanly. Only records where <code>discount_rate</code> is present <em>and</em> out of range are flagged — giving you accurate, noise-free violation metrics.</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #7b1fa2;
# MAGIC   background: #f3e5f5;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#4a148c; margin-bottom:6px; font-size: 1.2em;">Rule of thumb</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     <b></b></strong> Whenever a column may be absent — because it is optional or was added after the pipeline started — wrap its constraint in a <code>'CASE WHEN ... IS NOT NULL THEN ... ELSE TRUE END'</code> block.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Resilient Pipeline Design

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Ingest Everything as STRING
# MAGIC
# MAGIC The most resilient bronze layer design <strong>never rejects a record due to type mismatch</strong>. By storing all incoming fields as <code>STRING</code>, you accept whatever the source sends — integers, decimals, mixed types — and defer type enforcement to silver where <code>TRY_CAST</code> handles failures gracefully.
# MAGIC
# MAGIC <div style="display:flex;flex-direction:column;gap:16px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="background:#E8F5E9;border-top:4px solid #00A972;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#00A972;font-size:15px;margin-bottom:8px;"> Bronze — Accept Everything</div>
# MAGIC     All fields inferred as STRING. Type mismatches never fail the pipeline — an integer in a string field is just a string.
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">STREAMING</span> <span style="color:#C92A18;font-weight:600;">TABLE</span> bronze_events
# MAGIC COMMENT <span style="color:#0A7A4B;">&quot;Bronze: all fields as STRING, schema rescue enabled&quot;</span>
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> *
# MAGIC <span style="color:#C92A18;font-weight:600;">FROM</span> <span style="color:#C92A18;font-weight:600;">STREAM</span> read_files(
# MAGIC   <span style="color:#0A7A4B;">&#x27;/path/to/source&#x27;</span>,
# MAGIC   format =&gt; <span style="color:#0A7A4B;">&#x27;json&#x27;</span>,
# MAGIC   schemaEvolutionMode =&gt; <span style="color:#0A7A4B;">&#x27;rescue&#x27;</span>
# MAGIC )</pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="text-align:center;font-size:22px;line-height:1;color:#1B5162;">&#9660;</div>
# MAGIC
# MAGIC   <div style="background:#F9F7F4;border-top:4px solid #1B5162;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#1B5162;font-size:15px;margin-bottom:8px;">Silver — Enforce Types Safely</div>
# MAGIC     <div style="font-size:15px;color:#618794;margin-bottom:10px;line-height:1.7;"><code>TRY_CAST</code> returns NULL on cast failure instead of halting the pipeline. The NULL-tolerant constraint pattern then handles the rest.</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">STREAMING</span> <span style="color:#C92A18;font-weight:600;">TABLE</span> silver_events (
# MAGIC   <span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> valid_amount <span style="color:#C92A18;font-weight:600;">EXPECT</span> (
# MAGIC     <span style="color:#C92A18;font-weight:600;">CASE</span> <span style="color:#C92A18;font-weight:600;">WHEN</span> amount <span style="color:#C92A18;font-weight:600;">IS</span> <span style="color:#C92A18;font-weight:600;">NOT</span> <span style="color:#C92A18;font-weight:600;">NULL</span>
# MAGIC     <span style="color:#C92A18;font-weight:600;">THEN</span> amount &gt;= <span style="color:#2272B4;">0</span> <span style="color:#C92A18;font-weight:600;">ELSE</span> <span style="color:#C92A18;font-weight:600;">TRUE</span> <span style="color:#C92A18;font-weight:600;">END</span>
# MAGIC   ) <span style="color:#C92A18;font-weight:600;">ON</span> <span style="color:#C92A18;font-weight:600;">VIOLATION</span> <span style="color:#C92A18;font-weight:600;">DROP</span> <span style="color:#C92A18;font-weight:600;">ROW</span>
# MAGIC )
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> *,
# MAGIC   <span style="color:#C92A18;font-weight:600;">TRY_CAST</span>(amount_str <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">DOUBLE</span>) <span style="color:#C92A18;font-weight:600;">AS</span> amount
# MAGIC <span style="color:#C92A18;font-weight:600;">FROM</span> <span style="color:#C92A18;font-weight:600;">STREAM</span> bronze_events</pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Schema Evolution Tools at the Bronze Layer
# MAGIC
# MAGIC <div style="font-size:16px;color:#1B5162;line-height:1.9;margin-bottom:20px;">
# MAGIC Two built-in mechanisms cover the full lifecycle of schema change — <code>schemaHints</code> for columns you know are coming, and <code>_rescued_data</code> as the last line of defence for anything unexpected.
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:flex;flex-direction:column;gap:16px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="background:#E8F5E9;border-top:4px solid #00A972;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#00A972;font-size:15px;margin-bottom:8px;"> schemaHints — Declare Future Columns Today</div>
# MAGIC     <div style="font-size:15px;color:#618794;margin-bottom:10px;line-height:1.7;">Declare columns expected in <em>upcoming</em> files before they arrive. When the new column appears, it populates automatically. Records before the evolution carry <code>NULL</code> — backward and forward compatible simultaneously.</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">STREAMING</span> <span style="color:#C92A18;font-weight:600;">TABLE</span> bronze_events
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> *
# MAGIC <span style="color:#C92A18;font-weight:600;">FROM</span> <span style="color:#C92A18;font-weight:600;">STREAM</span> read_files(
# MAGIC   <span style="color:#0A7A4B;">&#x27;/path/to/source&#x27;</span>,
# MAGIC   format =&gt; <span style="color:#0A7A4B;">&#x27;json&#x27;</span>,
# MAGIC   schemaHints =&gt; <span style="color:#0A7A4B;">&#x27;loyalty_tier STRING, region_code STRING&#x27;</span>,
# MAGIC )
# MAGIC <span style="color:#6A737D;">-- Old records: loyalty_tier = NULL (acceptable)</span>
# MAGIC <span style="color:#6A737D;">-- New records: loyalty_tier populated automatically</span></pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="background:#FFF3E0;border-top:4px solid #FFAB00;border-radius:8px;padding:20px;">
# MAGIC     <div style="font-weight:bold;color:#FFAB00;font-size:15px;margin-bottom:8px;"> _rescued_data — Last Line of Defence</div>
# MAGIC     <div style="font-size:15px;color:#618794;margin-bottom:10px;line-height:1.7;">Any field arriving outside the declared schema — unexpected columns, type mismatches — is captured as JSON in <code>_rescued_data</code>. Nothing is silently discarded. Query it at any time for investigation or recovery.</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#6A737D;">-- Inspect rescued fields after the fact</span>
# MAGIC <span style="color:#C92A18;font-weight:600;">SELECT</span>
# MAGIC   event_id,
# MAGIC   _rescued_data:unexpected_field  <span style="color:#C92A18;font-weight:600;">AS</span> unexpected_field,
# MAGIC   _rescued_data:new_column        <span style="color:#C92A18;font-weight:600;">AS</span> new_column
# MAGIC <span style="color:#C92A18;font-weight:600;">FROM</span> bronze_events
# MAGIC <span style="color:#C92A18;font-weight:600;">WHERE</span> _rescued_data <span style="color:#C92A18;font-weight:600;">IS</span> <span style="color:#C92A18;font-weight:600;">NOT</span> <span style="color:#C92A18;font-weight:600;">NULL</span></pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#FFF8E8;border-left:4px solid #FFAB00;border-radius:4px;padding:14px 20px;font-size:16px;color:#0b2026;line-height:1.9;">
# MAGIC   <strong style="color:#FF5F46;">⚠️ Critical interaction:</strong> When a column is added via schema evolution, all records ingested <em>before</em> the evolution carry <code>NULL</code> for that column. Any constraint written for that column <strong>must use the NULL-tolerant <code>CASE WHEN</code> pattern</strong> — otherwise every historic record fails the constraint, causing widespread false violations in the pipeline UI.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. The Quarantine Pattern

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. How the Quarantine Pattern Works
# MAGIC
# MAGIC The quarantine pattern routes every incoming record through quality evaluation, then splits output into two paths based on results—clean path for analytics and quarantine path for remediation.
# MAGIC
# MAGIC **Key Guarantee**: No record is ever dropped
# MAGIC
# MAGIC **Mathematical Relationship**: `Total Records In = Clean Records + Quarantine Records`
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1214 322" width="100%" role="img" aria-label="Quarantine pattern: each record is evaluated against all expectations and routed to clean records or quarantine records" style="font-family:'DM Sans',Arial,Helvetica,sans-serif;max-width:1214px;display:block;margin:12px auto;"><defs><filter id="qf-sh" x="-8%" y="-8%" width="116%" height="130%"><feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#0B2026" flood-opacity="0.13"/></filter><marker id="qf-ah-g" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#5A6F77"/></marker><marker id="qf-ah-l" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#FF3621"/></marker><marker id="qf-ah-b" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#2272B4"/></marker><marker id="qf-ah-n" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#1B3139"/></marker><marker id="qf-ah-k" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#00A972"/></marker><filter id="qf-sh" x="-8%" y="-8%" width="116%" height="130%"><feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#0B2026" flood-opacity="0.13"/></filter></defs><rect x="10" y="120" width="150" height="70" rx="12" fill="#1565C0" stroke="#0D47A1" stroke-width="2" filter="url(#qf-sh)"/><text x="85.0" y="149.0" font-size="17" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Incoming</text><text x="85.0" y="171.0" font-size="17" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Record</text><path d="M162,155 H196" fill="none" stroke="#1B3139" stroke-width="2.6" marker-end="url(#qf-ah-n)" stroke-linecap="round" stroke-linejoin="round"/><rect x="200" y="112" width="200" height="86" rx="12" fill="#37474F" stroke="#263238" stroke-width="2" filter="url(#qf-sh)"/><text x="300.0" y="138.0" font-size="16" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Evaluate all</text><text x="300.0" y="160.0" font-size="14" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >expectations</text><text x="300.0" y="182.0" font-size="14" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >against the record</text><path d="M402,155 H430" fill="none" stroke="#1B3139" stroke-width="2.6" marker-end="url(#qf-ah-n)" stroke-linecap="round" stroke-linejoin="round"/><path d="M530,80 L640,155 L530,230 L420,155 z" fill="#F9A825" stroke="#C98A00" stroke-width="2" stroke-linejoin="round" filter="url(#qf-sh)"/><text x="530" y="150" font-size="15" font-weight="700" fill="#111" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Do ALL</text><text x="530" y="170" font-size="14" font-weight="600" fill="#111" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >expectations pass?</text><path d="M530,80 V58 H690" fill="none" stroke="#1B3139" stroke-width="2.4" marker-end="url(#qf-ah-n)" stroke-linecap="round" stroke-linejoin="round"/><text x="560" y="48" font-size="14" font-weight="700" fill="#2E7D32" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Yes</text><path d="M530,230 V262 H690" fill="none" stroke="#1B3139" stroke-width="2.4" marker-end="url(#qf-ah-n)" stroke-linecap="round" stroke-linejoin="round"/><text x="560" y="286" font-size="14" font-weight="700" fill="#B71C1C" text-anchor="start" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >No — any rule fails</text><rect x="694" y="18" width="250" height="80" rx="12" fill="#2E7D32" stroke="#1B5E20" stroke-width="2" filter="url(#qf-sh)"/><text x="819.0" y="52.0" font-size="14.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >is_quarantined = FALSE</text><text x="819.0" y="74.0" font-size="13.5" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >quarantine_reason = empty</text><rect x="694" y="222" width="250" height="80" rx="12" fill="#B71C1C" stroke="#7F0000" stroke-width="2" filter="url(#qf-sh)"/><text x="819.0" y="245.0" font-size="14.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >is_quarantined = TRUE</text><text x="819.0" y="267.0" font-size="13.5" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >quarantine_reason = list of</text><text x="819.0" y="289.0" font-size="13.5" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >failed rules</text><path d="M946,58 H978" fill="none" stroke="#1B3139" stroke-width="2.6" marker-end="url(#qf-ah-n)" stroke-linecap="round" stroke-linejoin="round"/><path d="M946,262 H978" fill="none" stroke="#1B3139" stroke-width="2.6" marker-end="url(#qf-ah-n)" stroke-linecap="round" stroke-linejoin="round"/><rect x="982" y="18" width="216" height="80" rx="12" fill="#1B5E20" stroke="#0B3D0B" stroke-width="2" filter="url(#qf-sh)"/><rect x="994" y="40.0" width="36" height="36" rx="8.64" fill="#fff" stroke="none" stroke-width="0" filter="url(#qf-sh)"/><svg x="999.76" y="46.983999999999995" width="24.48" height="22.3" viewBox="0 0 56 51" fill="#FF5F46"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#FF5F46" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#FF5F46" stroke-width="2.6" stroke-linecap="round"/></svg><text x="1114.0" y="52.0" font-size="16" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Clean Records</text><text x="1114.0" y="74.0" font-size="13" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Ready for analytics</text><rect x="982" y="222" width="216" height="80" rx="12" fill="#7F0000" stroke="#4A0000" stroke-width="2" filter="url(#qf-sh)"/><rect x="994" y="244.0" width="36" height="36" rx="8.64" fill="#fff" stroke="none" stroke-width="0" filter="url(#qf-sh)"/><svg x="999.76" y="250.98399999999998" width="24.48" height="22.3" viewBox="0 0 56 51" fill="#FF5F46"><rect x="14" y="6" width="34" height="3"/><rect x="14" y="6" width="3" height="33"/><rect x="45" y="6" width="3" height="19"/><rect x="14" y="17" width="34" height="3"/><rect x="24" y="20" width="3" height="17"/><rect x="14" y="37" width="17" height="3"/><path d="M32,29 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#FF5F46" stroke-width="2.6" stroke-linecap="round"/><path d="M32,36 q2.1,-4.6 4.2,0 t4.2,0 t4.2,0 t4.2,0" fill="none" stroke="#FF5F46" stroke-width="2.6" stroke-linecap="round"/></svg><text x="1114.0" y="245.0" font-size="14.5" font-weight="700" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >Quarantine Records</text><text x="1114.0" y="267.0" font-size="12.5" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >For investigation</text><text x="1114.0" y="289.0" font-size="12.5" font-weight="400" fill="#fff" text-anchor="middle" style="font-family:'DM Sans',Arial,Helvetica,sans-serif" >and reprocessing</text></svg>
# MAGIC
# MAGIC <div style="background:#FFF3E0;border-left:4px solid #FF9800;border-radius:4px;padding:14px 18px;margin:16px 0 10px 0;">
# MAGIC <div style="font-weight:bold;font-size:14px;color:#E65100;margin-bottom:6px;">⚠️ The Quality Tracking Table Must Always Use WARN</div>
# MAGIC <div style="font-size:14px;color:#333;">If <code>DROP ROW</code> or <code>FAIL UPDATE</code> is applied to the quality tracking table, invalid records are removed <em>before</em> the quarantine flag can be calculated — silently breaking the zero-data-loss guarantee. The <code>WARN</code> action is used here for one reason only: to surface per-constraint violation metrics in the pipeline UI. All actual routing logic is handled separately by the inverse logic flow.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#E3F2FD;border-left:4px solid #1976D2;border-radius:4px;padding:14px 18px;margin:10px 0;">
# MAGIC <div style="font-weight:bold;font-size:14px;color:#0D47A1;margin-bottom:6px;">ℹ️ Why Partition the Quality Tracking Table by is_quarantined?</div>
# MAGIC <div style="font-size:14px;color:#333;">Partitioning the quality tracking table by the <code>is_quarantined</code> column physically separates clean and failed records on storage. The downstream queries that split the two paths — filtering on <code>is_quarantined = FALSE</code> and <code>is_quarantined = TRUE</code> — then benefit from partition pruning, reading only the relevant partition rather than scanning the entire table.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. Zero Data Loss with Inverse Logic
# MAGIC
# MAGIC <code>DROP ROW</code> permanently deletes invalid records — there is no recovery path. The <strong>quarantine pattern</strong> eliminates data loss entirely by routing <em>every</em> record into the table, then using an <code>is_quarantined</code> flag and inverse logic to split clean and failed records into separate downstream views.
# MAGIC <div style="display:flex;flex-direction:column;gap:16px;margin:20px 0;">
# MAGIC
# MAGIC   <div style="background:#E3F2FD;border-top:4px solid #4299E0;border-radius:8px;padding:18px;">
# MAGIC     <div style="font-weight:bold;color:#4299E0;font-size:15px;margin-bottom:8px;"> Step 1 — Quarantine Table with Inverse Logic</div>
# MAGIC     <div style="font-size:15px;color:#618794;line-height:1.8;margin-bottom:10px;">All records are written using <code>WARN</code> — no records are dropped. The <code>is_quarantined</code> flag is derived using <code>NOT(all rules)</code>: if any rule fails, the record is flagged. <code>WARN</code> is used solely to surface per-constraint metrics in the pipeline UI.</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">STREAMING</span> <span style="color:#C92A18;font-weight:600;">TABLE</span> trips_quarantine (
# MAGIC   <span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> valid_distance <span style="color:#C92A18;font-weight:600;">EXPECT</span> (trip_distance &gt; <span style="color:#2272B4;">0</span>),
# MAGIC   <span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> valid_fare     <span style="color:#C92A18;font-weight:600;">EXPECT</span> (fare_amount &gt;= <span style="color:#2272B4;">0</span>),
# MAGIC   <span style="color:#C92A18;font-weight:600;">CONSTRAINT</span> valid_pax      <span style="color:#C92A18;font-weight:600;">EXPECT</span> (passenger_count <span style="color:#C92A18;font-weight:600;">BETWEEN</span> <span style="color:#2272B4;">1</span> <span style="color:#C92A18;font-weight:600;">AND</span> <span style="color:#2272B4;">9</span>)
# MAGIC   <span style="color:#6A737D;">-- WARN: surfaces metrics in UI, no records dropped</span>
# MAGIC )
# MAGIC <span style="color:#C92A18;font-weight:600;">PARTITIONED</span> <span style="color:#C92A18;font-weight:600;">BY</span> (is_quarantined)
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> *,
# MAGIC   <span style="color:#C92A18;font-weight:600;">NOT</span>(
# MAGIC     trip_distance &gt; <span style="color:#2272B4;">0</span>
# MAGIC     <span style="color:#C92A18;font-weight:600;">AND</span> fare_amount &gt;= <span style="color:#2272B4;">0</span>
# MAGIC     <span style="color:#C92A18;font-weight:600;">AND</span> passenger_count <span style="color:#C92A18;font-weight:600;">BETWEEN</span> <span style="color:#2272B4;">1</span> <span style="color:#C92A18;font-weight:600;">AND</span> <span style="color:#2272B4;">9</span>
# MAGIC   ) <span style="color:#C92A18;font-weight:600;">AS</span> is_quarantined
# MAGIC <span style="color:#C92A18;font-weight:600;">FROM</span> <span style="color:#C92A18;font-weight:600;">STREAM</span> bronze_trips</pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="text-align:center;font-size:22px;line-height:1;color:#1B5162;">&#9660;</div>
# MAGIC
# MAGIC   <div style="background:#E8F5E9;border-top:4px solid #00A972;border-radius:8px;padding:18px;">
# MAGIC     <div style="font-weight:bold;color:#00A972;font-size:15px;margin-bottom:8px;"> Step 2 — Split into Clean and Failed Views</div>
# MAGIC     <div style="font-size:15px;color:#618794;line-height:1.8;margin-bottom:10px;">Two Materialized Views filter on the <code>is_quarantined</code> partition. Because the quarantine table is <strong>partitioned by <code>is_quarantined</code></strong>, each view benefits from full partition pruning — only its partition is scanned, not the whole table.</div>
# MAGIC     <div style="position:relative;"><button type="button" onclick="var b=this,t=document.createElement('textarea');t.value=b.parentNode.querySelector('pre').textContent.replace(/\u00a0/g,'');document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);b.textContent='✓ Copied!';setTimeout(function(){b.textContent='Copy';},2000);" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:14px;background:#fff;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;">Copy</button><pre style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:14px 16px;padding-right:90px;margin:0;font-size:15px;line-height:1.6;overflow-x:auto;white-space:pre;font-family:Consolas,Monaco,monospace;"><span style="color:#6A737D;">-- Clean records for downstream analytics</span>
# MAGIC <span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">MATERIALIZED</span> <span style="color:#C92A18;font-weight:600;">VIEW</span> valid_trips_data
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> * <span style="color:#C92A18;font-weight:600;">FROM</span> trips_quarantine
# MAGIC <span style="color:#C92A18;font-weight:600;">WHERE</span> is_quarantined = <span style="color:#C92A18;font-weight:600;">FALSE</span>;
# MAGIC &nbsp;
# MAGIC <span style="color:#6A737D;">-- Failed records preserved for remediation and audit</span>
# MAGIC <span style="color:#C92A18;font-weight:600;">CREATE</span> <span style="color:#C92A18;font-weight:600;">OR</span> <span style="color:#C92A18;font-weight:600;">REFRESH</span> <span style="color:#C92A18;font-weight:600;">MATERIALIZED</span> <span style="color:#C92A18;font-weight:600;">VIEW</span> invalid_trips_data
# MAGIC <span style="color:#C92A18;font-weight:600;">AS</span> <span style="color:#C92A18;font-weight:600;">SELECT</span> * <span style="color:#C92A18;font-weight:600;">FROM</span> trips_quarantine
# MAGIC <span style="color:#C92A18;font-weight:600;">WHERE</span> is_quarantined = <span style="color:#C92A18;font-weight:600;">TRUE</span>;</pre></div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#F9F7F4;border-left:4px solid #FFAB00;border-radius:4px;padding:14px 20px;font-size:16px;color:#0b2026;line-height:1.9;">
# MAGIC   <strong style="color:#FFAB00;">💡 Why WARN and not DROP ROW on the quarantine table?</strong> If <code>DROP ROW</code> or <code>FAIL UPDATE</code> were applied to the quarantine table, invalid records would be removed before the <code>is_quarantined</code> flag is evaluated — defeating the entire pattern. <code>WARN</code> ensures every record is written and routed correctly by the inverse logic column.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. Choosing Between DROP ROW and Quarantine
# MAGIC
# MAGIC Both strategies enforce data quality, but they differ fundamentally in what happens to invalid records. The right choice depends on whether your business needs an audit trail, recovery capability, and whether false violations from schema evolution are a concern.
# MAGIC
# MAGIC <div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:16px;margin-bottom:20px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#0b2026;color:#F9F7F4;">
# MAGIC       <th style="padding:12px 16px;text-align:left;"></th>
# MAGIC       <th style="padding:12px 16px;text-align:center;">DROP ROW</th>
# MAGIC       <th style="padding:12px 16px;text-align:center;">Quarantine Pattern</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Invalid records</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;">Permanently deleted</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;">Preserved in quarantine table</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Audit trail</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;"> None</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;"> Full — queryable</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Data recovery</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#98102A;"> Not possible</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;"> Fix rule → re-route from quarantine</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">UI violation metrics</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;">Visible in pipeline UI</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;"> Per-constraint metrics via WARN</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Read performance</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;">Full table scan on clean data</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;color:#00A972;"> Partition pruning on <code>is_quarantined</code></td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#EEEDE9;">
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;font-weight:bold;color:#1B5162;">Pipeline complexity</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;">Low — single table</td>
# MAGIC       <td style="padding:11px 16px;border-bottom:1px solid #EEEDE9;text-align:center;">Moderate — temp table + 2 views</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:11px 16px;font-weight:bold;color:#1B5162;">Best for</td>
# MAGIC       <td style="padding:11px 16px;text-align:center;">Non-critical streams with well-established rules</td>
# MAGIC       <td style="padding:11px 16px;text-align:center;">Production pipelines with compliance, audit, or remediation needs</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table></div><!--ox-->
# MAGIC
# MAGIC <div style="background:#F9F7F4;border-left:4px solid #4299E0;border-radius:4px;padding:14px 20px;font-size:16px;color:#0b2026;line-height:1.9;">
# MAGIC   <strong style="color:#4299E0;">Recommendation:</strong> For enterprise production pipelines, the quarantine pattern is the preferred approach. It gives you zero data loss, per-constraint violation metrics in the pipeline UI, partition-pruned reads on clean data, and a recoverable quarantine table for root cause analysis and reprocessing — capabilities that <code>DROP ROW</code> cannot provide.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>