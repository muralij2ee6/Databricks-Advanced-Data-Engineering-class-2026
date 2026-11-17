# Training Ops & TA Info (platform-wide, reusable)

The `knowledge:faq-and-best-practices` skill consults this training-ops / platform info **after** the course-specific FAQ and
**before** answering from scratch. Platform-general items that recur across every course — sourced from
the **DCI Readiness — Teaching Assistants (KBYG)** deck and platform docs. Course-specific entries live
in `specific_course_skills/<course>/faq.md`.

> Each entry is vetted content. The TA answers from it and cites it; it never invents an FAQ answer.

## FAQ (platform-wide)
- **Where is the lab / a specific notebook? / "link me to it".** The course notebooks are **already in
  this Databricks workspace** (the TA runs here). Open the one you need from the **workspace file
  browser**, in the course folder — the TA can name the exact notebook (see the course's `course_pack.md`).
  You do **not** go back to Vocareum to find a notebook you already have open in the workspace; Vocareum
  is only the launcher that got you here.
- **How long do I have access to the lab (and slides)?** You keep access for **4 days after the session
  ends** to revisit and complete the hands-on lab exercises. Note the lab environment also has a **timer** —
  when it runs out the lab terminates and unsaved progress is lost, so save your work.
- **Can I record the session / get the slides?** No recording is allowed (GDPR/CCPA). The TA can share
  documentation links and resources, but **not the course slides**.
- **I can't open / log in to the lab (Vocareum).** This is a lab-environment issue — flag the instructor,
  who escalates lab-access emergencies to **#ext-tr-emergency** (Training Ops). Non-emergency ILT
  questions go to **#help-trops-ilt**.
- **Lab won't open — common fixes:** use a **Chrome-based** browser (avoid Safari); **allow cookies**
  (incl. third-party, needed for SSO); **disable the pop-up blocker** (a "spinning V" that never opens is
  usually a blocked pop-up); try an **incognito** window; do **not** connect via corporate VPN.
- **"No workspace found" / blank page:** click **Databricks Details → Workspace link**, then retry.
- **"Sorry, there was an error opening the application":** your Academy account is missing a first/last
  name — add both in Databricks Academy, then reopen.
- **What is Vocareum?** The hands-on lab platform, reached via Databricks Academy (or Console for
  troubleshooting).
- **Will this class prepare me for the certification?** The session is **not exam prep**. Exam guides are
  the source of truth for exam content, and certifications assume ~6 months of hands-on experience — don't
  promise cert readiness from the class alone.

## Best Practices (platform-wide)
- Always filter on a date/time column so data skipping can prune files (less I/O).
- Name only the columns you need instead of `SELECT *` on large tables.
- Prefer a view for logic reused across queries; a one-off query is fine for exploration.
- Never run write statements (UPDATE/DELETE/MERGE) during a read-only analytics lab.

## Maintenance
Sourced from the DCI Readiness — Teaching Assistants deck + platform docs. Re-review each term. Platform-
general only — anything course-specific belongs in the course folder so it can be updated with the course.
<!-- NOTE: the deck is internally inconsistent on access duration — slide 6 (HDC logistics) says 4 days,
slide 24 (FAQ) says 7 days. Set to 4 per instruction; reconcile with L&E. -->
