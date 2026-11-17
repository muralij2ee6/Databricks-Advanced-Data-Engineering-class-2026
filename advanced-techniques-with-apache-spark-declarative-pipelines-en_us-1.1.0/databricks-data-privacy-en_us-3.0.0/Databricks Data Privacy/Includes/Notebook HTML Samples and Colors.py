# Databricks notebook source
# MAGIC %md
# MAGIC # DE-PRO-2 Notebook HTML Samples and Colors
# MAGIC
# MAGIC Reference cells for authors. The approved palette and the callout/admonition
# MAGIC patterns below match the shared curriculum design system. Compose slides from
# MAGIC the Card-Template-Catalog blocks. Never invent colors, fonts, sizes or spacing.
# MAGIC
# MAGIC Rules:
# MAGIC - Always use `%md-sandbox` for HTML (regular `%md` strips it).
# MAGIC - 14pt is the absolute minimum font size.
# MAGIC - Accent rotation on cards: blue -> green -> coral -> amber -> red.
# MAGIC - No em-dashes or en-dashes. No AI icons or rocket ships.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Approved palette - Theme Light (primary)
# MAGIC
# MAGIC | Color | Hex | Usage |
# MAGIC |------|------|------|
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#0b2026;border:1px solid #ccc;"></span> | #0b2026 | Primary text, dark headers |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#F9F7F4;border:1px solid #ccc;"></span> | #F9F7F4 | Card backgrounds |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FF5F46;border:1px solid #ccc;"></span> | #FF5F46 | Coral accent, highlights, alerts |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#EEEDE9;border:1px solid #ccc;"></span> | #EEEDE9 | Borders, alternating rows |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#1B5162;border:1px solid #ccc;"></span> | #1B5162 | Header backgrounds, borders |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#00A972;border:1px solid #ccc;"></span> | #00A972 | Success, accent bars |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#98102A;border:1px solid #ccc;"></span> | #98102A | Warnings, danger accent |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FFAB00;border:1px solid #ccc;"></span> | #FFAB00 | Amber accent, highlights |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#618794;border:1px solid #ccc;"></span> | #618794 | Secondary text, muted elements |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#4299E0;border:1px solid #ccc;"></span> | #4299E0 | Blue accent, links, process boxes |
# MAGIC
# MAGIC **Fixed UI tints (structural, don't treat as swap colors):** `#F8F9FC` (note callout background), `#FFF6F4` (key-point callout background).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <!-- NOTE callout: use for asides the learner should keep in mind -->
# MAGIC <div style="border-left:4px solid #1B5162;background:#F8F9FC;padding:14px 18px;border-radius:4px;margin:16px 0;font-family:sans-serif;">
# MAGIC   <strong style="display:block;color:#1B5162;margin-bottom:6px;font-size:15pt;">Note</strong>
# MAGIC   <div style="color:#0b2026;font-size:14pt;">Use this style to highlight an important remark or observation the reader should keep in mind.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <!-- KEY POINT callout: use for the one thing to remember -->
# MAGIC <div style="border-left:4px solid #FF5F46;background:#FFF6F4;padding:14px 18px;border-radius:4px;margin:16px 0;font-family:sans-serif;">
# MAGIC   <strong style="display:block;color:#98102A;margin-bottom:6px;font-size:15pt;">Key Point</strong>
# MAGIC   <div style="color:#0b2026;font-size:14pt;">Use this style for the single most important takeaway on the slide.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <!-- SUCCESS callout: use for a recommended pattern / the right way -->
# MAGIC <div style="border-left:4px solid #00A972;background:#e8f5ee;padding:14px 18px;border-radius:4px;margin:16px 0;font-family:sans-serif;">
# MAGIC   <strong style="display:block;color:#00A972;margin-bottom:6px;font-size:15pt;">Do This</strong>
# MAGIC   <div style="color:#0b2026;font-size:14pt;">Use this style to mark the recommended, platform-first pattern.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <!-- CAUTION callout: use for a genuine warning or a common mistake to avoid -->
# MAGIC <div style="border-left:4px solid #98102A;background:#FDE0DC;padding:14px 18px;border-radius:4px;margin:16px 0;font-family:sans-serif;">
# MAGIC   <strong style="display:block;color:#98102A;margin-bottom:6px;font-size:15pt;">Caution</strong>
# MAGIC   <div style="color:#0b2026;font-size:14pt;">Use this style for a genuine warning or a common mistake to avoid.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <!-- REQUIRED COMPUTE red callout: paste near the top of every notebook -->
# MAGIC <div style="border:2px solid #98102A;background:#FDE0DC;padding:16px 20px;border-radius:8px;margin:16px 0;font-family:sans-serif;">
# MAGIC   <strong style="display:block;color:#98102A;margin-bottom:8px;font-size:16pt;">REQUIRED - Select a Compute Environment</strong>
# MAGIC   <div style="color:#0b2026;font-size:14pt;">
# MAGIC     This notebook was tested on <strong>Serverless (version 6)</strong>. Attach this notebook to that compute before running.
# MAGIC     <br>Other compute may work but is not guaranteed.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expandable script pattern
# MAGIC
# MAGIC Place below each visual, in the same `%md-sandbox` cell. Explains the WHY.
# MAGIC Do not title it "Instructor Script". No AI icons.
# MAGIC
# MAGIC ```
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC ### <matching section header>
# MAGIC - why this matters
# MAGIC - common mistake
# MAGIC - real-world context
# MAGIC
# MAGIC </details>
# MAGIC ```