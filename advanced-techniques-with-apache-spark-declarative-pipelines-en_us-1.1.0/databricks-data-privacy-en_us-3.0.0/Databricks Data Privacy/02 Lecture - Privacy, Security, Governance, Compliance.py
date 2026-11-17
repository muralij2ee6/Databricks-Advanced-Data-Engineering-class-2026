# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Databricks Data Privacy
# MAGIC </span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">02 Lecture - Privacy, Security, Governance, Compliance</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/databricks-data-privacy-en_us-3.0.0/images/20261110T075937Z/Databricks Data Privacy/Includes/Images/common/databricks_academy_logo.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Privacy, Security, Governance and Compliance
# MAGIC #### Duration: 15 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture we'll frame what privacy means on Databricks before you start building controls. 
# MAGIC
# MAGIC People use <b>privacy</b>, <b>security</b>, <b>governance</b> and <b>compliance</b> as if they were the same thing, so this lecture separates them first, then shows where they overlap.
# MAGIC
# MAGIC From there you meet the lifecycle the whole course reuses, identify, protect, manage and verify and the privacy decision model, a short set of questions you run on every technique. 
# MAGIC
# MAGIC We'll turn vague privacy obligations into concrete technical questions you can answer on Databricks and sets the compliance framing. 
# MAGIC
# MAGIC Keep in mind that this course is <b>non-legal</b> and the live Trust and Compliance Center is the source of truth for certifications.
# MAGIC
# MAGIC > Stance: privacy is a set of decisions and controls you implement and verify on Databricks, not a compliance certificate.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you'll be able to:
# MAGIC
# MAGIC 1. **Distinguish** privacy from security, governance and compliance and **explain** where they overlap.
# MAGIC 2. **Explain** the identify, protect, manage and verify lifecycle and the role each phase plays.
# MAGIC 3. **Translate** privacy obligations into technical questions you can answer on Databricks.
# MAGIC 4. **Describe** how the certifications Databricks holds differ from the regulations it helps you meet and **locate** the live Trust and Compliance Center.
# MAGIC 5. **Reuse** the running scenario and the six-question privacy decision model that every later demo and lab returns to.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Privacy, Security, Governance and Compliance
# MAGIC
# MAGIC These four words get used interchangeably and <b>they aren't the same</b>. Each answers a different question and <b>privacy is the one this course is about</b>.
# MAGIC <br/><br/>
# MAGIC <div class="pqg">
# MAGIC <style>
# MAGIC .pqg { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .pqg * { box-sizing: border-box; }
# MAGIC .pqg-head { display: flex; justify-content: space-between; align-items: baseline; flex-wrap: wrap; gap: 6px 16px; margin-bottom: 16px; }
# MAGIC .pqg-head .t { font-size: 16pt; font-weight: 600; color: #0b2026; }
# MAGIC .pqg-head .s { font-size: 14pt; color: #5E7077; }
# MAGIC .pqg-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
# MAGIC .pqg-card { background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px 22px; }
# MAGIC .pqg-eb { display: flex; align-items: baseline; flex-wrap: wrap; gap: 4px 12px; }
# MAGIC .pqg-dom { font-size: 17pt; font-weight: 700; }
# MAGIC .pqg-imp { font-size: 14pt; font-weight: 600; color: #0b2026; }
# MAGIC .pqg-tag { margin-left: auto; align-self: center; font-size: 12px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #fff; background: #FF5F46; border-radius: 20px; padding: 3px 12px; }
# MAGIC .pqg-desc { font-size: 14pt; line-height: 1.55; margin-top: 10px; }
# MAGIC .pqg-asks { margin-top: 14px; padding: 12px 16px; border-radius: 8px; }
# MAGIC .pqg-asks .l { display: block; font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; margin-bottom: 4px; }
# MAGIC .pqg-asks .q { font-size: 14pt; line-height: 1.5; color: #0b2026; }
# MAGIC .pqg-foot { margin-top: 20px; padding-top: 16px; border-top: 2px solid #EEEDE9; color: #5E7077; font-size: 14pt; line-height: 1.5; }
# MAGIC @media (max-width: 700px) { .pqg-grid { grid-template-columns: 1fr; } }
# MAGIC </style>
# MAGIC
# MAGIC <div class="pqg-head"><span class="t">Four domains, four questions</span><span class="s">Privacy is where they meet</span></div>
# MAGIC
# MAGIC <div class="pqg-grid">
# MAGIC   <div class="pqg-card" style="border-top: 8px solid #4299E0;">
# MAGIC   <div class="pqg-eb"><span class="pqg-dom" style="color: #2272B4;">Security</span><span class="pqg-imp">Keep data safe</span></div>
# MAGIC   <div class="pqg-desc">Protect data and systems from unauthorized access, with access controls, encryption and network controls.</div>
# MAGIC   <div class="pqg-asks" style="background: rgba(66,153,224,0.10);"><span class="l" style="color: #2272B4;">It answers</span><span class="q">Is the data protected from people who shouldn't reach it?</span></div>
# MAGIC   </div>
# MAGIC   <div class="pqg-card" style="border-top: 8px solid #00A972;">
# MAGIC   <div class="pqg-eb"><span class="pqg-dom" style="color: #00A972;">Governance</span><span class="pqg-imp">Know your data</span></div>
# MAGIC   <div class="pqg-desc">Own, catalog and set policy for data: what you hold, where it lives, who is accountable and who can grant access.</div>
# MAGIC   <div class="pqg-asks" style="background: rgba(0,169,114,0.10);"><span class="l" style="color: #00A972;">It answers</span><span class="q">Do we know what data we have and who is responsible for it?</span></div>
# MAGIC   </div>
# MAGIC   <div class="pqg-card" style="border-top: 8px solid #FFAB00;">
# MAGIC   <div class="pqg-eb"><span class="pqg-dom" style="color: #B27A00;">Compliance</span><span class="pqg-imp">Meet obligations</span></div>
# MAGIC   <div class="pqg-desc">Satisfy external laws, regulations and standards and be able to demonstrate it.</div>
# MAGIC   <div class="pqg-asks" style="background: rgba(255,171,0,0.14);"><span class="l" style="color: #B27A00;">It answers</span><span class="q">Can we meet our obligations and show that we did?</span></div>
# MAGIC   </div>
# MAGIC   <div class="pqg-card" style="border-top: 8px solid #FF5F46;">
# MAGIC   <div class="pqg-eb"><span class="pqg-dom" style="color: #FF5F46;">Privacy</span><span class="pqg-imp">Handle personal data right</span><span class="pqg-tag">This course</span></div>
# MAGIC   <div class="pqg-desc">Handle personal data so individuals' rights are respected: find it, minimize it, protect it, delete it and prove it.</div>
# MAGIC   <div class="pqg-asks" style="background: rgba(255,95,70,0.10);"><span class="l" style="color: #FF5F46;">It answers</span><span class="q">Are we handling personal data responsibly and can we prove it?</span></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="pqg-foot">Each domain answers a different question. Privacy draws on the other three and adds its own focus: personal data and individual rights.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Privacy draws on the other three
# MAGIC
# MAGIC Privacy isn't a separate silo. It uses security to protect personal data, governance to know where that data lives and compliance to know which obligations apply. 
# MAGIC Privacy sits where the three meet and it adds its own focus: <b>personal data and individual rights</b>.
# MAGIC <br/><br/>
# MAGIC <div style="max-width: 760px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <svg width="100%" viewBox="0 0 720 500" role="img" style="font-family: sans-serif;">
# MAGIC   <circle cx="280" cy="215" r="150" fill="#4299E0" fill-opacity="0.16" stroke="#4299E0" stroke-width="2"/>
# MAGIC   <circle cx="440" cy="215" r="150" fill="#00A972" fill-opacity="0.16" stroke="#00A972" stroke-width="2"/>
# MAGIC   <circle cx="360" cy="345" r="150" fill="#FFAB00" fill-opacity="0.16" stroke="#FFAB00" stroke-width="2"/>
# MAGIC   <text x="205" y="185" text-anchor="middle" font-size="20" font-weight="700" fill="#2272B4">Security</text>
# MAGIC   <text x="205" y="208" text-anchor="middle" font-size="13" fill="#5E7077">keep data safe</text>
# MAGIC   <text x="515" y="185" text-anchor="middle" font-size="20" font-weight="700" fill="#00A972">Governance</text>
# MAGIC   <text x="515" y="208" text-anchor="middle" font-size="13" fill="#5E7077">know your data</text>
# MAGIC   <text x="360" y="410" text-anchor="middle" font-size="20" font-weight="700" fill="#B27A00">Compliance</text>
# MAGIC   <text x="360" y="433" text-anchor="middle" font-size="13" fill="#5E7077">meet obligations</text>
# MAGIC   <text x="360" y="270" text-anchor="middle" font-size="22" font-weight="800" fill="#FF5F46">Privacy</text>
# MAGIC   <text x="360" y="293" text-anchor="middle" font-size="13" fill="#0b2026">personal data and rights</text>
# MAGIC </svg>
# MAGIC <div style="margin-top: 14px; padding: 16px 20px; background: #F9F7F4; border: 2px solid #1B5162; border-radius: 8px; font-size: 14pt; line-height: 1.55; text-align: left;">
# MAGIC   The overlap is why the course reuses <strong>Unity Catalog</strong> controls. You'll use <b>governed tags</b> and <b>access controls</b> (security and governance) to deliver privacy and you'll treat compliance as obligations that drive the controls.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why the distinction matters</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Security is necessary but not sufficient for privacy.** A perfectly secured table can still expose personal data to people inside the company who shouldn't see it and it can still hold data you were supposed to delete. Privacy adds purpose and lifecycle on top of access control.
# MAGIC - **Governance is the substrate.** You can't protect or delete personal data you haven't cataloged and can't locate. Discovery and classification (Demo 03) is a governance capability the privacy work depends on.
# MAGIC - **Compliance and privacy are not the same.** A regulation says a person can request erasure. Privacy is the technical work that finds the data, deletes it everywhere and proves it.
# MAGIC - **The trap to avoid:** treating a compliance certificate as proof of privacy. A certificate describes the platform's posture, not whether your specific tables are handled correctly. That's your job and it's what you'll learn to do here.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. The Identify, Protect, Manage, Verify Lifecycle
# MAGIC
# MAGIC The course is organized as a lifecycle you apply to personal data: you identify it, protect it, manage it over its life and verify that each control worked.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="lc">
# MAGIC <style>
# MAGIC .lc { font-family: sans-serif; max-width: 1000px; margin: 0 auto; color: #0b2026; }
# MAGIC .lc * { box-sizing: border-box; }
# MAGIC .lc-flow { display: flex; align-items: stretch; justify-content: center; flex-wrap: wrap; gap: 10px; }
# MAGIC .lc-step { flex: 1 1 200px; min-width: 190px; max-width: 240px; background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 16px 18px; }
# MAGIC .lc-step .name { font-size: 17pt; font-weight: 700; color: #0b2026; }
# MAGIC .lc-step .d { font-size: 14pt; line-height: 1.45; color: #0b2026; margin-top: 6px; }
# MAGIC .lc-arrow { display: flex; align-items: center; font-size: 22pt; color: #90A5B1; }
# MAGIC @media (max-width: 760px) { .lc-flow { flex-direction: column; align-items: stretch; } .lc-step { max-width: none; } .lc-arrow { transform: rotate(90deg); } }
# MAGIC </style>
# MAGIC <div class="lc-flow">
# MAGIC   <div class="lc-step" style="border-top: 6px solid #4299E0;"><div class="name">Identify</div><div class="d">Find personal data across the estate.</div></div>
# MAGIC   <div class="lc-arrow">&rarr;</div>
# MAGIC   <div class="lc-step" style="border-top: 6px solid #00A972;"><div class="name">Protect</div><div class="d">Restrict or transform it by purpose.</div></div>
# MAGIC   <div class="lc-arrow">&rarr;</div>
# MAGIC   <div class="lc-step" style="border-top: 6px solid #FF5F46;"><div class="name">Manage</div><div class="d">Delete and purge over its lifecycle.</div></div>
# MAGIC   <div class="lc-arrow">&rarr;</div>
# MAGIC   <div class="lc-step" style="border-top: 6px solid #FFAB00;"><div class="name">Verify</div><div class="d">Prove the control worked.</div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">What each phase means</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **Identify:** find the personal data across the estate. Demo 03 uses Data Classification and governed tags.
# MAGIC - **Protect:** restrict or transform it by purpose. Demo 04 uses tag-driven ABAC and manual masks. Demo 06 uses de-identification and key management.
# MAGIC - **Manage:** correct, delete and purge personal data over its lifecycle. Demo 07 is the erasure workflow. (De-identification and key management sit under Protect, in Demo 06, because they transform the value to protect it.)
# MAGIC - **Verify:** produce evidence that the control worked. Demo 08 assembles it from system tables, lineage and history.
# MAGIC - **Why verify matters:** applying a control isn't the end of the job. A regulator, an auditor or a customer will ask you to show that a person's data was actually protected or deleted. Every demo and lab in this course ends by producing that evidence, so verify is a first-class phase rather than an afterthought.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Turn Obligations Into Technical Questions
# MAGIC
# MAGIC A privacy obligation is usually stated in plain language. To act on it, restate it as a technical question you can answer on Databricks. Each one maps to a phase of the lifecycle.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .fc-card { background: #F9F7F4; border-radius: 10px; padding: 20px 24px; border-top: 8px solid #4299E0; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .fc-item { display: grid; grid-template-columns: 230px 1fr; gap: 20px; padding: 14px 0; border-bottom: 1px solid #EEEDE9; align-items: start; }
# MAGIC .fc-item:first-child { padding-top: 4px; }
# MAGIC .fc-item:last-child { border-bottom: none; padding-bottom: 4px; }
# MAGIC .fc-key { font-size: 14pt; font-weight: 700; color: #1B5162; line-height: 1.35; }
# MAGIC .fc-key span { display: block; font-size: 13px; font-weight: 600; letter-spacing: 1px; text-transform: uppercase; color: #618794; margin-top: 4px; }
# MAGIC .fc-val { font-size: 14pt; color: #0b2026; line-height: 1.55; }
# MAGIC @media (max-width: 600px) { .fc-item { grid-template-columns: 1fr; gap: 4px; } }
# MAGIC </style>
# MAGIC
# MAGIC <div class="fc-card">
# MAGIC   <div class="fc-item"><div class="fc-key">Find a person's data<span>Identify, Demo 03</span></div><div class="fc-val">Can we locate every column and row that holds this person's data, across the estate?</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">Restrict or transform it<span>Protect, Demos 04 and 06</span></div><div class="fc-val">Can we hide or transform it by purpose, so an analyst gets useful data and the original stays out of reach?</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">Correct or delete it<span>Manage, Demo 07</span></div><div class="fc-val">Can we delete a person's data everywhere it spread, from raw tables through to gold and purge the old files?</div></div>
# MAGIC   <div class="fc-item"><div class="fc-key">Prove what happened<span>Verify, Demo 08</span></div><div class="fc-val">Can we show the evidence: a tag, a masked result, an empty erasure query, an audit event, lineage or table history?</div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">Why restate obligations this way</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - A plain obligation like "respect the right to erasure" is hard to act on directly. The technical question "can we delete this subject everywhere their data spread and prove it" tells you exactly what to build and how to check it.
# MAGIC - The four questions line up with the four phases, so the lifecycle isn't abstract. Each phase exists to answer one of these questions on your own data.
# MAGIC - These four obligations are the plain-language starting point. Section E1 turns them into the six-question decision model every demo runs and each demo ends by answering the last one, proving what happened.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Compliance
# MAGIC
# MAGIC Compliance is where privacy work meets external standards. This course stays non-legal. The distinction that matters is between what Databricks is certified for and what it helps you build.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="hs">
# MAGIC <style>
# MAGIC .hs { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .hs * { box-sizing: border-box; }
# MAGIC .hs-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
# MAGIC .hs-card { background: #F9F7F4; border-radius: 12px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); padding: 20px 22px; }
# MAGIC .hs-top { display: flex; align-items: center; gap: 12px; }
# MAGIC .hs-badge { flex: none; width: 40px; height: 40px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 20px; font-weight: 700; }
# MAGIC .hs-verb { font-size: 19pt; font-weight: 800; }
# MAGIC .hs-flag { margin-left: auto; font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: #B27A00; border: 1px solid #FFAB00; border-radius: 20px; padding: 3px 10px; }
# MAGIC .hs-def { font-size: 14pt; line-height: 1.5; color: #0b2026; margin: 10px 0 14px; }
# MAGIC .hs-chips { display: flex; flex-direction: column; gap: 8px; }
# MAGIC .hs-chip { display: flex; align-items: center; justify-content: space-between; gap: 10px; background: #ffffff; border: 1px solid #EEEDE9; border-radius: 8px; padding: 9px 12px; }
# MAGIC .hs-std { font-size: 14pt; font-weight: 700; color: #0b2026; }
# MAGIC .hs-ind { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: #5E7077; background: #EEEDE9; border-radius: 20px; padding: 3px 10px; white-space: nowrap; }
# MAGIC .hs-note { margin-top: 16px; text-align: center; font-size: 14pt; line-height: 1.5; color: #5E7077; }
# MAGIC .hs-note strong { color: #0b2026; }
# MAGIC @media (max-width: 700px) { .hs-grid { grid-template-columns: 1fr; } }
# MAGIC </style>
# MAGIC <div class="hs-grid">
# MAGIC   <div class="hs-card" style="border-top: 8px solid #00A972;">
# MAGIC   <div class="hs-top"><span class="hs-badge" style="background: #00A972; color: #ffffff;">&#10003;</span><span class="hs-verb" style="color: #00A972;">Holds</span></div>
# MAGIC   <div class="hs-def">Attestations Databricks has earned for its own platform, audited by a third party.</div>
# MAGIC   <div class="hs-chips">
# MAGIC     <div class="hs-chip"><span class="hs-std">ISO 27001</span><span class="hs-ind">Broad</span></div>
# MAGIC     <div class="hs-chip"><span class="hs-std">SOC 2 Type II</span><span class="hs-ind">Broad</span></div>
# MAGIC     <div class="hs-chip"><span class="hs-std">HITRUST</span><span class="hs-ind">Healthcare</span></div>
# MAGIC   </div>
# MAGIC   </div>
# MAGIC   <div class="hs-card" style="border-top: 8px solid #FFAB00;">
# MAGIC   <div class="hs-top"><span class="hs-badge" style="border: 2px solid #FFAB00; color: #B27A00;">&#8594;</span><span class="hs-verb" style="color: #B27A00;">Supports</span><span class="hs-flag">not certified</span></div>
# MAGIC   <div class="hs-def">Databricks helps you meet these with platform features and guidance, but does not hold them itself.</div>
# MAGIC   <div class="hs-chips">
# MAGIC     <div class="hs-chip"><span class="hs-std">HIPAA</span><span class="hs-ind">Healthcare</span></div>
# MAGIC     <div class="hs-chip"><span class="hs-std">DORA</span><span class="hs-ind">Finance</span></div>
# MAGIC   </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div class="hs-note">Scope changes by cloud and over time, so the <strong><a href="https://www.databricks.com/trust" target="_blank">Trust and Compliance Center</a></strong> is the source of truth rather than a fixed list.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1B5162; background: #F8F9FC; padding: 14px 18px; border-radius: 4px; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <strong style="display:block; color:#1B5162; margin-bottom:6px; font-size:15pt;">Non-legal reminder</strong>
# MAGIC   <div style="color:#0b2026; font-size:14pt; line-height:1.55;">
# MAGIC   This course works from the controls you implement and verify and points you to the obligation they serve. 
# MAGIC   <br /><br />
# MAGIC   For formal determinations, go to your data protection officer or legal team and the <a href="https://www.databricks.com/trust" target="_blank">Trust and Compliance Center</a>, not to a course notebook.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">What each attestation and regulation involves</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **ISO 27001.** An information-security management certification Databricks holds, published with a Statement of Applicability.
# MAGIC - **SOC 2 Type II.** An audited controls report, available through your account team.
# MAGIC - **HITRUST (HITRUST CSF).** A healthcare-oriented certification whose scope varies by cloud.
# MAGIC - **HIPAA.** A US healthcare regulation Databricks supports with a compliance security profile and a signed agreement, not a certification it holds.
# MAGIC - **DORA.** An EU financial-sector regulation Databricks supports with guidance and a shared-responsibility whitepaper.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
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
# MAGIC   <div class="cl-docs-sub">The current certifications, their scope by cloud and the due-diligence materials live here and are kept up to date. Treat this as the source of truth and reach the SOC 2 Type II report and other documents through your account team.</div>
# MAGIC   <div class="cl-docs-links">
# MAGIC     <a class="cl-doc-btn" href="https://www.databricks.com/trust" target="_blank">Security and Trust Center &rarr;</a>
# MAGIC     <a class="cl-doc-btn" href="https://www.databricks.com/trust/compliance" target="_blank">Compliance &rarr;</a>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. The Running Scenario
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; display: flex; flex-direction: column; gap: 20px; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .ds-banner { display: flex; align-items: center; gap: 20px; width: 100%; border-radius: 10px; padding: 24px 28px; background: var(--bnr-bg); border-left: 8px solid var(--bnr-accent); box-shadow: 0 2px 10px rgba(27,49,57,0.08); }
# MAGIC .ds-banner-icon { width: 60px; height: 60px; border-radius: 50%; flex-shrink: 0; background: var(--bnr-accent); color: var(--bnr-icon-fg); font-size: 30pt; font-weight: 700; line-height: 1; display: flex; align-items: center; justify-content: center; }
# MAGIC .ds-banner-title { font-size: 22pt; font-weight: 700; color: var(--bnr-title); line-height: 1.2; margin-bottom: 4px; }
# MAGIC .ds-banner-tagline { font-size: 15pt; color: var(--bnr-tagline); line-height: 1.4; }
# MAGIC .ds-banner.info { --bnr-bg: #1B5162; --bnr-accent: #4299E0; --bnr-icon-fg: #FFFFFF; --bnr-title: #FFFFFF; --bnr-tagline: #F9F7F4; }
# MAGIC .ds-row { display: flex; gap: 24px; align-items: stretch; flex-wrap: wrap; }
# MAGIC .ds-row > * { flex: 1; min-width: 260px; }
# MAGIC .blue{--accent:#4299E0;} .green{--accent:#00A972;} .coral{--accent:#FF5F46;} .amber{--accent:#FFAB00;} .red{--accent:#98102A;}
# MAGIC .ds-scenario { background: #F9F7F4; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 8px solid var(--accent, #4299E0); display: flex; flex-direction: column; }
# MAGIC .ds-scenario-band { background: var(--accent); color: #fff; padding: 14px 20px; text-align: center; font-size: 18pt; font-weight: 700; }
# MAGIC .ds-scenario-body { padding: 18px 20px; display: flex; flex-direction: column; flex: 1; }
# MAGIC .ds-scenario-q { font-size: 16pt; font-style: italic; color: #0b2026; margin-bottom: 12px; text-align: center; }
# MAGIC .ds-scenario-text { font-size: 14pt; color: #0b2026; line-height: 1.6; }
# MAGIC .ds-scenario-text ul { margin: 12px 0; padding-left: 18px; }
# MAGIC .ds-scenario-text li { margin-bottom: 8px; }
# MAGIC .ds-viz { margin-top: auto; padding-top: 14px; border-top: 1px dashed #E0DED9; }
# MAGIC .ds-viz-cap { font-size: 14px; color: #618794; text-align: center; margin-top: 10px; line-height: 1.4; }
# MAGIC .ds-chip { font-size: 13px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 3px 8px; color: #5E7077; }
# MAGIC .ds-chip.pii { border-color: #FF5F46; color: #98102A; font-weight: 700; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ds-banner info">
# MAGIC   <div class="ds-banner-icon">i</div>
# MAGIC   <div>
# MAGIC     <div class="ds-banner-title">You work at a multinational retailer</div>
# MAGIC     <div class="ds-banner-tagline">The course follows three people on one retailer estate, each with a privacy problem to solve.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ds-row">
# MAGIC   <div class="ds-scenario blue">
# MAGIC     <div class="ds-scenario-band">The data engineer</div>
# MAGIC     <div class="ds-scenario-body">
# MAGIC       <div class="ds-scenario-q">"Which of my columns hold sensitive data?"</div>
# MAGIC       <div class="ds-scenario-text">You inherited the retailer estate.
# MAGIC         <ul>
# MAGIC           <li>Access controls exist, but nobody labeled what is sensitive.</li>
# MAGIC           <li>Emails, phone numbers and national ids are mixed in with harmless columns.</li>
# MAGIC         </ul>
# MAGIC         <strong>You have to find the sensitive data before you can protect it.</strong>
# MAGIC       </div>
# MAGIC       <div class="ds-viz">
# MAGIC         <div style="display:flex; gap:6px; justify-content:center; flex-wrap:wrap;">
# MAGIC           <span class="ds-chip">country</span>
# MAGIC           <span class="ds-chip pii">email ?</span>
# MAGIC           <span class="ds-chip">tier</span>
# MAGIC           <span class="ds-chip pii">national_id ?</span>
# MAGIC           <span class="ds-chip">city</span>
# MAGIC         </div>
# MAGIC         <div class="ds-viz-cap">Which columns are <span style="color:#98102A;font-weight:700;">sensitive</span>?</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-scenario green">
# MAGIC     <div class="ds-scenario-band">Your analyst</div>
# MAGIC     <div class="ds-scenario-body">
# MAGIC       <div class="ds-scenario-q">"Can I have useful data without the identifiers?"</div>
# MAGIC       <div class="ds-scenario-text">An analyst needs trends by country, tier and revenue.
# MAGIC         <ul>
# MAGIC           <li>They must not see <strong>email</strong> or <strong>national_id</strong>.</li>
# MAGIC           <li>They still need rows that join across tables.</li>
# MAGIC         </ul>
# MAGIC         <strong>The data has to stay useful without identifying anyone.</strong>
# MAGIC       </div>
# MAGIC       <div class="ds-viz">
# MAGIC         <div style="display:flex; align-items:center; justify-content:center; gap:10px;">
# MAGIC           <span style="font-size:13px; font-family:monospace; background:rgba(152,16,42,0.10); border:1px solid #98102A; border-radius:4px; padding:4px 8px; color:#98102A; text-decoration:line-through;">j.smith@ex.com</span>
# MAGIC           <span style="color:#00A972; font-size:16px; font-weight:700;">&rarr;</span>
# MAGIC           <span style="font-size:13px; font-family:monospace; background:rgba(0,169,114,0.12); border:1px solid #00A972; border-radius:4px; padding:4px 8px; color:#00A972;">NULL</span>
# MAGIC         </div>
# MAGIC         <div class="ds-viz-cap">Original hidden, rows still <span style="color:#00A972;font-weight:700;">join</span></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ds-scenario coral">
# MAGIC     <div class="ds-scenario-band">A customer</div>
# MAGIC     <div class="ds-scenario-body">
# MAGIC       <div class="ds-scenario-q">"Please delete my personal data."</div>
# MAGIC       <div class="ds-scenario-text">An erasure request arrives.
# MAGIC         <ul>
# MAGIC           <li>The customer's data spread from raw tables into silver and gold.</li>
# MAGIC           <li>You must delete it, propagate the delete, purge old files and prove it.</li>
# MAGIC         </ul>
# MAGIC         <strong>It has to be gone from every layer and you have to prove it.</strong>
# MAGIC       </div>
# MAGIC       <div class="ds-viz">
# MAGIC         <div style="display:flex; align-items:center; justify-content:center; gap:6px; flex-wrap:wrap;">
# MAGIC           <span style="font-size:12px; background:rgba(205,127,50,0.14); border:1px solid #CD7F32; border-radius:4px; padding:4px 8px; color:#0b2026;">raw</span>
# MAGIC           <span style="color:#618794;">&rarr;</span>
# MAGIC           <span style="font-size:12px; background:rgba(90,111,119,0.16); border:1px solid #5A6F77; border-radius:4px; padding:4px 8px; color:#0b2026;">silver</span>
# MAGIC           <span style="color:#618794;">&rarr;</span>
# MAGIC           <span style="font-size:12px; background:rgba(255,171,0,0.16); border:1px solid #FFAB00; border-radius:4px; padding:4px 8px; color:#0b2026;">gold</span>
# MAGIC           <span style="color:#98102A; font-size:18px; font-weight:700; margin-left:4px;">&#10005;</span>
# MAGIC         </div>
# MAGIC         <div class="ds-viz-cap">Delete across <span style="color:#98102A;font-weight:700;">every layer</span>, then prove it</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">The scenario behind the course</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - **One estate, reused everywhere.** Every demo works the same multinational-retailer data, so the techniques build on each other instead of resetting each demo.
# MAGIC - **The three people map to the course arc:**
# MAGIC   - The engineer's problem drives **discovery and classification** (Demo 3) and **protection** (Demo 4).
# MAGIC   - The analyst's problem drives **de-identification and key management** (Demo 6): masking, pseudonymization and encryption chosen by purpose.
# MAGIC   - The customer's request drives **right to erasure** (Demo 7) and the **evidence** you assemble to prove it (Demo 8).
# MAGIC - **Always end on evidence.** Each demo and lab finishes by showing the control worked: a tag, a masked result, an empty erasure query, an audit row, lineage or table history.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. The privacy decision model
# MAGIC
# MAGIC For each of those problems and every technique in the course, you run the same short set of questions. This decision model expands the four obligations from Section C into the specific questions you ask of each technique and it is the spine every demo and lab returns to, ending on evidence.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <div class="cw">
# MAGIC <style>
# MAGIC .cw { font-family: sans-serif; max-width: 1100px; margin: 0 auto; color: #0b2026; }
# MAGIC .cw * { box-sizing: border-box; }
# MAGIC .dm-card { background: #F9F7F4; border-radius: 10px; padding: 20px 24px; border-top: 8px solid #1B5162; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .dm-item { display: grid; grid-template-columns: 44px 1fr 150px; gap: 16px; padding: 12px 0; border-bottom: 1px solid #EEEDE9; align-items: center; }
# MAGIC .dm-item:last-child { border-bottom: none; }
# MAGIC .dm-num { width: 34px; height: 34px; border-radius: 50%; background: var(--accent, #4299E0); color: #fff; font-size: 15pt; font-weight: 800; display: flex; align-items: center; justify-content: center; }
# MAGIC .dm-q { font-size: 14pt; color: #0b2026; line-height: 1.5; }
# MAGIC .dm-phase { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: var(--accent, #4299E0); text-align: right; }
# MAGIC @media (max-width: 600px) { .dm-item { grid-template-columns: 44px 1fr; } .dm-phase { display: none; } }
# MAGIC </style>
# MAGIC
# MAGIC <div class="dm-card">
# MAGIC   <div class="dm-item" style="--accent:#4299E0;"><div class="dm-num" style="background:#4299E0;">1</div><div class="dm-q">What sensitive data exists here?</div><div class="dm-phase">Identify</div></div>
# MAGIC   <div class="dm-item" style="--accent:#00A972;"><div class="dm-num" style="background:#00A972;">2</div><div class="dm-q">Why is it needed and for what purpose?</div><div class="dm-phase">Protect</div></div>
# MAGIC   <div class="dm-item" style="--accent:#00A972;"><div class="dm-num" style="background:#00A972;">3</div><div class="dm-q">Who may see the original value?</div><div class="dm-phase">Protect</div></div>
# MAGIC   <div class="dm-item" style="--accent:#00A972;"><div class="dm-num" style="background:#00A972;">4</div><div class="dm-q">Is linkage required, or can we break it?</div><div class="dm-phase">Protect</div></div>
# MAGIC   <div class="dm-item" style="--accent:#FF5F46;"><div class="dm-num" style="background:#FF5F46;">5</div><div class="dm-q">Where did it propagate downstream?</div><div class="dm-phase">Manage</div></div>
# MAGIC   <div class="dm-item" style="--accent:#FFAB00;"><div class="dm-num" style="background:#FFAB00;">6</div><div class="dm-q">How will deletion and verification work?</div><div class="dm-phase">Verify</div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL DETAILS
# MAGIC
# MAGIC <details>
# MAGIC <summary style="font-size:14pt; font-weight:600; color:#1B5162; cursor:pointer;">How to use the decision model</summary>
# MAGIC
# MAGIC <div style="font-size:14pt; line-height:1.6; color:#0b2026; padding:12px 0;">
# MAGIC
# MAGIC - The six questions map onto the four phases. Question 1 is Identify, questions 2 to 4 are Protect, question 5 is Manage and question 6 is Verify.
# MAGIC - You don't answer these once. You re-run them for each new technique and each newly discovered or tagged column.
# MAGIC - The engineer's problem starts at question 1, the analyst's at questions 2 to 4 and the customer's erasure request at questions 5 and 6.
# MAGIC - Every demo and lab ends on question 6, by producing the evidence: a tag, a masked result, an empty erasure query, an audit event, lineage or a history row.
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this lecture, you:
# MAGIC
# MAGIC 1. **Separated** privacy from security, governance and compliance and saw that privacy sits where the three meet and adds personal data and individual rights.
# MAGIC 2. Learned the **identify, protect, manage, verify** lifecycle and why verify is a first-class phase.
# MAGIC 3. Turned privacy **obligations into technical questions** you can answer on Databricks.
# MAGIC 4. Set the **non-legal compliance framing**, the certification versus regulation distinction and where the live Trust and Compliance Center fits.
# MAGIC 5. Met the **running scenario** and the **six-question privacy decision model** that every later demo and lab reuses.
# MAGIC
# MAGIC Next, in **Discover and Classify Sensitive Data**, you start the lifecycle at identify: you use Data Classification and governed tags to find the sensitive columns in the retailer estate and end by tracing a detection to a tag a later control can act on.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC