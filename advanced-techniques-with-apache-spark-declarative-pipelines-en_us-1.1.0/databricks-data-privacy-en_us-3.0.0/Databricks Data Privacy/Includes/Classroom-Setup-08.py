# Databricks notebook source
# Classroom-Setup-08
# DEMO 08: Privacy Evidence with System Tables
# ---------------------------------------------------------------------
# Idempotent, serverless, safe to re-run.
#
# Steps
# - Common -> catalog -> Required-Data -> 5 snapshots (classification, lineage x2, audit, query history; built if missing) -> sensitivity_level tag (best effort) -> already-erased estate + verified register (rebuilt) -> validate -> config.
#
# Creates
# - priv_demo (6 demo tables), 5 snap_* tables (if missing), erasure_bronze, erasure_silver, erasure_requests, erasure_request_statements (rebuilt, subject absent).
#
# Catalog override: set FORCED_CATALOG below.

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Common-Python

# COMMAND ----------

# Catalog override for restricted workspaces. Leave as None to auto-resolve:
# - Vocareum: your pre-provisioned labuserNNN catalog.
# - Other workspaces: labuser_<username>, created if you have privileges.
# Set to an EXISTING catalog name (string) if you can't create catalogs.
FORCED_CATALOG = None

my_catalog = build_user_catalog(catalog_forced=FORCED_CATALOG)

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Required-Data

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType,
    ArrayType, MapType, TimestampType, DateType,
)

DEMO_SCHEMA = "priv_demo" # shared retailer estate (read only in the demo)

# The five deterministic snapshots the demo reads.
CLASS_SNAPSHOT = "snap_classification_results"
TABLE_LINEAGE_SNAP = "snap_table_lineage"
COLUMN_LINEAGE_SNAP = "snap_column_lineage"
AUDIT_SNAP = "snap_audit"
QUERY_SNAP = "snap_query_history"

# The demo-owned tables and register (rebuilt each run).
BRONZE_TABLE = "erasure_bronze" # authoritative layer, already erased
SILVER_TABLE = "erasure_silver" # downstream copy, already erased
REGISTER_TABLE = "erasure_requests"
REQUEST_STMTS_TABLE = "erasure_request_statements" # the register's per-statement action log

# The request and subject the evidence row is assembled for. Pseudonymous; the
# register never stores raw PII. SUBJECT_ID is the concrete internal id the
# subject_key resolves to (an access-controlled lookup), and it's absent from
# the erasure tables so the verification returns an empty result.
REQUEST_ID = "DSR-2026-0412"
SUBJECT_KEY = "SUBJ-7C4A9E"
SUBJECT_ID = 4212
SEED_LO, SEED_HI = 4100, 4300 # the customer slice the erasure tables mirror

# The two actors in the evidence. The register records the statement id of each erasure
# action, so the register joins to query.history on statement_id. A Unity Catalog table
# write carries no statement id in the audit log, so the audit event lines up with a
# statement by table, actor and time instead.
ENGINEER_EMAIL = "privacy.engineer@dbacademy.example"
ANALYST_EMAIL = "data.analyst@dbacademy.example"
DEL_STMT_ID = "01ef9c2a7b3f4e10"
MRG_STMT_ID = "01ef9c2a7b3f4e11"
REORG_STMT_ID = "01ef9c2a7b3f4e12"
VERIFY_STMT_ID = "01ef9c2a7b3f4e13"
DECOY_STMT_ID = "01ef9c2a7b3f4e14"

# Fabricated Unity Catalog table ids (uuid shape) for the audit credential-vend rows.
BRONZE_TABLE_ID = "b7c9e1a2-3d4f-5061-7a8b-9c0d1e2f3a4b"
SILVER_TABLE_ID = "c8d0f2b3-4e5a-6172-8b9c-0d1e2f3a4b5c"

# COMMAND ----------

def _distinct_keys(catalog: str, schema: str, table: str, cols):
    # The DISTINCT tuples of `cols` in a snapshot, or None when it can't be read.
    try:
        found = spark.sql(
            f"SELECT DISTINCT {', '.join(cols)} FROM `{catalog}`.`{schema}`.`{table}`"
        ).collect()
    except Exception:
        return None
    return {tuple(r[c] for c in cols) for r in found}


def _snapshot_has(catalog: str, schema: str, table: str, cols, required) -> bool:
    # True only when the snapshot exists AND carries every required key. A missing,
    # empty, partial or live-derived copy that lacks any required key fails the check,
    # so the caller rebuilds the deterministic copy instead of keeping a partial one.
    have = _distinct_keys(catalog, schema, table, cols)
    return have is not None and set(required).issubset(have)


def _has_statement_ids(catalog: str, schema: str, table: str, id_expr: str, required_ids) -> bool:
    # True only when the snapshot carries every required statement id. The audit and
    # query-history snapshots are a scripted erasure, so a live-derived copy of the real
    # system table will lack these ids and is replaced by the deterministic copy.
    try:
        found = spark.sql(
            f"SELECT DISTINCT {id_expr} AS sid FROM `{catalog}`.`{schema}`.`{table}`"
        ).collect()
    except Exception:
        return False
    return set(required_ids).issubset({r["sid"] for r in found})

# COMMAND ----------

# Classification snapshot (same 11 detections as Setup-03/07). Rebuilt if incomplete.
def ensure_classification_snapshot(catalog: str, schema: str = DEMO_SCHEMA):
    rows = [
        ("customer_profile", "first_name", "string", "class.name", "HIGH", 1.00,
         ["James", "Mary", "Aiko", "Mateus", "Priya"]),
        ("customer_profile", "last_name", "string", "class.name", "HIGH", 1.00,
         ["Smith", "Garcia", "Nakamura", "Muller", "Patel"]),
        ("customer_profile", "email", "string", "class.email_address", "HIGH", 1.00,
         ["james.smith@example.com", "mary.garcia@example.net", "aiko.nakamura@mail.example.org"]),
        ("customer_profile", "phone", "string", "class.phone_number", "HIGH", 0.99,
         ["+14155550142", "+442071838750", "+498912345678"]),
        ("customer_profile", "street_address", "string", "class.location", "HIGH", 0.97,
         ["482 Main St", "1290 Oak Ave", "73 Cedar Ln"]),
        ("customer_profile", "date_of_birth", "date", "class.date_of_birth", "HIGH", 1.00,
         ["1974-03-12", "1988-11-02", "1969-07-25"]),
        ("customer_profile", "national_id", "string", "class.us_ssn", "HIGH", 0.10,
         ["123-45-6789", "987-65-4321", "456-78-9012"]),
        ("customer_profile", "ip_address", "string", "class.ip_address", "HIGH", 1.00,
         ["203.0.113.5", "198.51.100.23", "192.0.2.146"]),
        ("customer_profile", "created_at", "timestamp", "class.date_of_birth", "LOW", 0.28,
         ["2021-06-02T10:22:14", "2022-01-17T08:03:51", "2020-11-30T19:45:10"]),
        ("support_records", "body_text", "string", "class.email_address", "HIGH", 0.50,
         ["My email is r.lopez@example.net.", "You can reach me at jane.doe@example.com or +12025550143."]),
        ("support_records", "body_text", "string", "class.phone_number", "LOW", 0.29,
         ["You can reach me at jane.doe@example.com or +12025550143.", "call me on +13105550187"]),
    ]
    # Complete only when every (table_name, column_name, class_tag) detection is
    # present, so a missing, empty, partial or live-derived snapshot is rebuilt
    # deterministically instead of a partial copy being kept.
    required_detections = {(r[0], r[1], r[3]) for r in rows}
    if _snapshot_has(catalog, schema, CLASS_SNAPSHOT,
                     ["table_name", "column_name", "class_tag"], required_detections):
        print(f"[OK] {schema}.{CLASS_SNAPSHOT} already carries all "
              f"{len(required_detections)} detections. Keeping it.")
        return
    detected = datetime.datetime(2026, 9, 2, 3, 14, 0)
    latest = datetime.datetime(2026, 9, 3, 3, 21, 0)
    schema_def = StructType([
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("data_type", StringType(), False),
        StructField("class_tag", StringType(), False),
        StructField("confidence", StringType(), False),
        StructField("frequency", DoubleType(), False),
        StructField("samples", ArrayType(StringType()), False),
    ])
    df = (
        spark.createDataFrame(rows, schema_def)
        .withColumn("catalog_name", F.lit(catalog))
        .withColumn("schema_name", F.lit(schema))
        .withColumn("first_detected_time", F.lit(detected).cast("timestamp"))
        .withColumn("latest_detected_time", F.lit(latest).cast("timestamp"))
        .select("catalog_name", "schema_name", "table_name", "column_name", "data_type",
                "class_tag", "samples", "confidence", "frequency",
                "first_detected_time", "latest_detected_time")
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"`{catalog}`.`{schema}`.{CLASS_SNAPSHOT}"
    )
    print(f"[OK] Built {schema}.{CLASS_SNAPSHOT} (11 deterministic detections, synthetic samples).")


ensure_classification_snapshot(my_catalog)

# COMMAND ----------

# Deterministic lineage snapshots. Rebuilt only if missing or incomplete.
# Estate: customer_profile + loyalty -> customer_enriched -> customer_metrics_gold.
def _lineage_ready(catalog: str, schema: str = DEMO_SCHEMA) -> bool:
    # Ready only when BOTH snapshots carry every estate edge the demo reads: the three
    # table edges, and the key column edges (identifiers reaching customer_enriched and
    # the aggregate reaching customer_metrics_gold). A missing, empty, single-edge, or
    # live-derived snapshot that lacks any required edge fails, so both are rebuilt.
    req_table_edges = {
        ("customer_profile", "customer_enriched"),
        ("loyalty", "customer_enriched"),
        ("customer_enriched", "customer_metrics_gold"),
    }
    req_col_edges = {
        ("customer_profile", "email", "customer_enriched", "email"),
        ("customer_profile", "national_id", "customer_enriched", "national_id"),
        ("customer_profile", "phone", "customer_enriched", "phone"),
        ("customer_enriched", "est_revenue", "customer_metrics_gold", "total_est_revenue"),
    }
    return (
        _snapshot_has(catalog, schema, TABLE_LINEAGE_SNAP,
                      ["source_table_name", "target_table_name"], req_table_edges)
        and _snapshot_has(catalog, schema, COLUMN_LINEAGE_SNAP,
                          ["source_table_name", "source_column_name",
                           "target_table_name", "target_column_name"], req_col_edges)
    )


def ensure_lineage_snapshots(catalog: str, schema: str = DEMO_SCHEMA):
    if _lineage_ready(catalog, schema):
        print(f"[OK] {schema}.{TABLE_LINEAGE_SNAP} already carries the estate edges. Keeping it.")
        return

    def full(tbl):
        return f"{catalog}.{schema}.{tbl}"

    event_time = datetime.datetime(2026, 9, 2, 3, 30, 0)
    event_date = datetime.date(2026, 9, 2)
    created_by = "workspace-setup@dbacademy.example"

    # --- table lineage: real column names from system.access.table_lineage ----
    t_edges = [
        ("customer_profile", "customer_enriched"),
        ("loyalty", "customer_enriched"),
        ("customer_enriched", "customer_metrics_gold"),
    ]
    t_rows = [
        (full(s), schema, s, "TABLE", full(t), schema, t, "TABLE", "NOTEBOOK", created_by)
        for s, t in t_edges
    ]
    t_schema = StructType([
        StructField("source_table_full_name", StringType(), False),
        StructField("source_table_schema", StringType(), False),
        StructField("source_table_name", StringType(), False),
        StructField("source_type", StringType(), False),
        StructField("target_table_full_name", StringType(), False),
        StructField("target_table_schema", StringType(), False),
        StructField("target_table_name", StringType(), False),
        StructField("target_type", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("created_by", StringType(), False),
    ])
    (
        spark.createDataFrame(t_rows, t_schema)
        .withColumn("event_time", F.lit(event_time).cast(TimestampType()))
        .withColumn("event_date", F.lit(event_date).cast(DateType()))
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"`{catalog}`.`{schema}`.{TABLE_LINEAGE_SNAP}")
    )

    # --- column lineage: real column names from system.access.column_lineage --
    # Identifiers propagate profile -> enriched; the gold aggregate keeps no identifier.
    c_edges = [
        ("customer_profile", "customer_id", "customer_enriched", "customer_id"),
        ("customer_profile", "email", "customer_enriched", "email"),
        ("customer_profile", "national_id", "customer_enriched", "national_id"),
        ("customer_profile", "phone", "customer_enriched", "phone"),
        ("customer_profile", "country", "customer_enriched", "country"),
        ("customer_enriched", "country", "customer_metrics_gold", "country"),
        ("customer_enriched", "est_revenue", "customer_metrics_gold", "total_est_revenue"),
    ]
    c_rows = [
        (full(s), schema, s, sc, "TABLE", full(t), schema, t, tc, "TABLE", "NOTEBOOK", created_by)
        for s, sc, t, tc in c_edges
    ]
    c_schema = StructType([
        StructField("source_table_full_name", StringType(), False),
        StructField("source_table_schema", StringType(), False),
        StructField("source_table_name", StringType(), False),
        StructField("source_column_name", StringType(), False),
        StructField("source_type", StringType(), False),
        StructField("target_table_full_name", StringType(), False),
        StructField("target_table_schema", StringType(), False),
        StructField("target_table_name", StringType(), False),
        StructField("target_column_name", StringType(), False),
        StructField("target_type", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("created_by", StringType(), False),
    ])
    (
        spark.createDataFrame(c_rows, c_schema)
        .withColumn("event_time", F.lit(event_time).cast(TimestampType()))
        .withColumn("event_date", F.lit(event_date).cast(DateType()))
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"`{catalog}`.`{schema}`.{COLUMN_LINEAGE_SNAP}")
    )
    print(f"[OK] Built {schema}.{TABLE_LINEAGE_SNAP} ({len(t_rows)} edges) and "
          f"{schema}.{COLUMN_LINEAGE_SNAP} ({len(c_rows)} column edges), matching the system-table columns.")


ensure_lineage_snapshots(my_catalog)

# COMMAND ----------

# Deterministic audit snapshot (real column names, scripted erasure events). Rebuilt if missing.
def ensure_audit_snapshot(catalog: str, schema: str = DEMO_SCHEMA):
    if _has_statement_ids(catalog, schema, AUDIT_SNAP, "request_params['table_full_name']",
                          {f"{catalog}.{schema}.{BRONZE_TABLE}", f"{catalog}.{schema}.{SILVER_TABLE}"}):
        print(f"[OK] {schema}.{AUDIT_SNAP} already carries the erasure credential-vend events. Keeping it.")
        return

    def ui(email):
        return (email, email) # user_identity: (email, subject_name)

    ok = (200, "", "") # response: (status_code, error_message, result)
    d = datetime.date(2026, 8, 21)

    def full_arg(tbl):
        return f"{catalog}.{schema}.{tbl}"

    # A table write (DELETE, MERGE, REORG) is audited by Unity Catalog as a
    # generateTemporaryTableCredential with operation READ_WRITE, keyed by table_full_name
    # and table_id. It carries no statement id, so it lines up with a statement by table,
    # actor and time. A metadata read is a getTable, keyed by full_name_arg.
    # (event_time, source_ip, user_identity, service_name, action_name, request_id,
    #  request_params, response, event_id)
    rows = [
        (datetime.datetime(2026, 8, 21, 9, 14, 5), "203.0.113.51", ui(ENGINEER_EMAIL),
         "unityCatalog", "generateTemporaryTableCredential", "req-8f1a2b3c",
         {"table_full_name": full_arg(BRONZE_TABLE), "table_id": BRONZE_TABLE_ID, "operation": "READ_WRITE"},
         ok, "evt-aa01"),
        (datetime.datetime(2026, 8, 21, 9, 15, 10), "203.0.113.51", ui(ENGINEER_EMAIL),
         "unityCatalog", "generateTemporaryTableCredential", "req-8f1a2b3d",
         {"table_full_name": full_arg(SILVER_TABLE), "table_id": SILVER_TABLE_ID, "operation": "READ_WRITE"},
         ok, "evt-aa02"),
        (datetime.datetime(2026, 8, 21, 9, 16, 55), "203.0.113.51", ui(ENGINEER_EMAIL),
         "unityCatalog", "generateTemporaryTableCredential", "req-8f1a2b3e",
         {"table_full_name": full_arg(BRONZE_TABLE), "table_id": BRONZE_TABLE_ID, "operation": "READ_WRITE"},
         ok, "evt-aa03"),
        (datetime.datetime(2026, 8, 21, 8, 40, 0), "203.0.113.77", ui(ANALYST_EMAIL),
         "unityCatalog", "getTable", "req-7c0a1120",
         {"full_name_arg": full_arg("customer_enriched")},
         ok, "evt-aa04"),
    ]
    audit_schema = StructType([
        StructField("event_time", TimestampType(), False),
        StructField("source_ip_address", StringType(), True),
        StructField("user_identity", StructType([
            StructField("email", StringType(), True),
            StructField("subject_name", StringType(), True),
        ]), True),
        StructField("service_name", StringType(), True),
        StructField("action_name", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("request_params", MapType(StringType(), StringType()), True),
        StructField("response", StructType([
            StructField("status_code", IntegerType(), True),
            StructField("error_message", StringType(), True),
            StructField("result", StringType(), True),
        ]), True),
        StructField("event_id", StringType(), True),
    ])
    (
        spark.createDataFrame(rows, audit_schema)
        .withColumn("account_id", F.lit("acct-dbacademy"))
        .withColumn("workspace_id", F.lit("1234567890123456"))
        .withColumn("version", F.lit("2.0"))
        .withColumn("event_date", F.lit(d).cast(DateType()))
        .withColumn("user_agent", F.lit("databricks-sql/1.0"))
        .withColumn("session_id", F.lit("sess-erasure-0412"))
        .withColumn("audit_level", F.lit("ACCOUNT_LEVEL"))
        .select("account_id", "workspace_id", "version", "event_time", "event_date",
                "source_ip_address", "user_agent", "session_id", "user_identity",
                "service_name", "action_name", "request_id", "request_params",
                "response", "audit_level", "event_id")
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"`{catalog}`.`{schema}`.{AUDIT_SNAP}")
    )
    print(f"[OK] Built {schema}.{AUDIT_SNAP} ({len(rows)} rows), matching the system.access.audit columns.")


ensure_audit_snapshot(my_catalog)

# COMMAND ----------

# Deterministic query-history snapshot (scripted erasure statements). Rebuilt if missing.
def ensure_query_history_snapshot(catalog: str, schema: str = DEMO_SCHEMA):
    if _has_statement_ids(catalog, schema, QUERY_SNAP, "statement_id",
                          {DEL_STMT_ID, MRG_STMT_ID, REORG_STMT_ID, VERIFY_STMT_ID}):
        print(f"[OK] {schema}.{QUERY_SNAP} already carries the erasure statements. Keeping it.")
        return

    b = f"{schema}.{BRONZE_TABLE}"
    s = f"{schema}.{SILVER_TABLE}"

    # (statement_id, executed_by, statement_text, statement_type, execution_status,
    #  start_time, end_time, read_rows, produced_rows)
    rows = [
        (DEL_STMT_ID, ENGINEER_EMAIL,
         f"/* {REQUEST_ID} */ DELETE FROM {b} WHERE customer_id = {SUBJECT_ID}",
         "DELETE", "FINISHED",
         datetime.datetime(2026, 8, 21, 9, 14, 2), datetime.datetime(2026, 8, 21, 9, 14, 7), 201, 0),
        (MRG_STMT_ID, ENGINEER_EMAIL,
         f"/* {REQUEST_ID} */ MERGE INTO {s} t USING (SELECT {SUBJECT_ID} AS customer_id) x "
         f"ON t.customer_id = x.customer_id WHEN MATCHED THEN DELETE",
         "MERGE", "FINISHED",
         datetime.datetime(2026, 8, 21, 9, 15, 5), datetime.datetime(2026, 8, 21, 9, 15, 12), 201, 0),
        (REORG_STMT_ID, ENGINEER_EMAIL,
         f"/* {REQUEST_ID} */ REORG TABLE {b} APPLY (PURGE)",
         "OPTIMIZE", "FINISHED",
         datetime.datetime(2026, 8, 21, 9, 16, 30), datetime.datetime(2026, 8, 21, 9, 16, 58), 201, 200),
        (VERIFY_STMT_ID, ENGINEER_EMAIL,
         f"/* {REQUEST_ID} */ SELECT count(*) FROM {b} WHERE customer_id = {SUBJECT_ID}",
         "SELECT", "FINISHED",
         datetime.datetime(2026, 8, 21, 9, 17, 10), datetime.datetime(2026, 8, 21, 9, 17, 11), 200, 1),
        (DECOY_STMT_ID, ANALYST_EMAIL,
         f"SELECT country, customer_count FROM {schema}.customer_metrics_gold ORDER BY customer_count DESC",
         "SELECT", "FINISHED",
         datetime.datetime(2026, 8, 21, 8, 40, 0), datetime.datetime(2026, 8, 21, 8, 40, 1), 40, 40),
    ]
    qh_schema = StructType([
        StructField("statement_id", StringType(), False),
        StructField("executed_by", StringType(), True),
        StructField("statement_text", StringType(), True),
        StructField("statement_type", StringType(), True),
        StructField("execution_status", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("read_rows", LongType(), True),
        StructField("produced_rows", LongType(), True),
    ])
    (
        spark.createDataFrame(rows, qh_schema)
        .withColumn("account_id", F.lit("acct-dbacademy"))
        .withColumn("workspace_id", F.lit("1234567890123456"))
        .withColumn("session_id", F.lit("sess-erasure-0412"))
        .withColumn("executed_by_user_id", F.lit("42000042"))
        .withColumn("error_message", F.lit(None).cast(StringType()))
        .withColumn("read_bytes", (F.col("read_rows") * F.lit(512)).cast(LongType()))
        .select("account_id", "workspace_id", "statement_id", "session_id",
                "execution_status", "executed_by", "executed_by_user_id",
                "statement_text", "statement_type", "error_message",
                "start_time", "end_time", "read_rows", "produced_rows", "read_bytes")
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"`{catalog}`.`{schema}`.{QUERY_SNAP}")
    )
    print(f"[OK] Built {schema}.{QUERY_SNAP} ({len(rows)} rows), matching the system.query.history columns.")


ensure_query_history_snapshot(my_catalog)

# COMMAND ----------

# Best-effort governed sensitivity_level tag on customer_profile (mirrors Setup-03).
def ensure_sensitivity_tag_context(catalog: str, schema: str = DEMO_SCHEMA):
    cp = f"`{catalog}`.`{schema}`.customer_profile"
    stmts = [
        ("tag email=restricted", f"ALTER TABLE {cp} ALTER COLUMN email SET TAGS ('sensitivity_level' = 'restricted')"),
        ("tag national_id=restricted", f"ALTER TABLE {cp} ALTER COLUMN national_id SET TAGS ('sensitivity_level' = 'restricted')"),
        ("tag phone=confidential", f"ALTER TABLE {cp} ALTER COLUMN phone SET TAGS ('sensitivity_level' = 'confidential')"),
    ]
    applied = 0
    for label, sql in stmts:
        try:
            spark.sql(sql)
            applied += 1
        except Exception:
            pass
    if applied == len(stmts):
        print(f"[OK] Sensitivity tag context applied ({applied}/{len(stmts)} statements).")
    elif applied:
        print(f"[OK] Sensitivity tag context applied ({applied}/{len(stmts)} statements); the demo uses the classification snapshot for the rest.")
    else:
        print("[SKIPPED] The sensitivity_level tag wasn't applied here; the demo uses the classification snapshot for the 'what exists' evidence.")


ensure_sensitivity_tag_context(my_catalog)

# COMMAND ----------

# Already-erased estate (subject absent) + verified register. Always rebuilt.
def ensure_erasure_evidence_estate(catalog: str, schema: str = DEMO_SCHEMA):
    bronze = f"`{catalog}`.`{schema}`.{BRONZE_TABLE}"
    silver = f"`{catalog}`.`{schema}`.{SILVER_TABLE}"
    register = f"`{catalog}`.`{schema}`.{REGISTER_TABLE}"

    # Authoritative layer, already erased: the seed slice minus the subject.
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {bronze} AS
        SELECT customer_id, first_name, last_name, email, phone, national_id,
               country, created_at
        FROM `{catalog}`.`{schema}`.customer_profile
        WHERE customer_id BETWEEN {SEED_LO} AND {SEED_HI}
          AND customer_id <> {SUBJECT_ID}
        """
    )
    # Downstream copy, already erased.
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {silver} AS
        SELECT p.customer_id, p.email, p.national_id, p.country,
               l.points_balance, l.tier AS loyalty_program_tier, l.enrolled_at
        FROM `{catalog}`.`{schema}`.customer_profile p
        LEFT JOIN `{catalog}`.`{schema}`.loyalty l USING (customer_id)
        WHERE p.customer_id BETWEEN {SEED_LO} AND {SEED_HI}
          AND p.customer_id <> {SUBJECT_ID}
        """
    )

    # The request register, staged as a completed, verified request (no raw PII).
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {register} (
          request_id STRING,
          subject_key STRING,
          request_type STRING,
          status STRING,
          received_date DATE,
          deadline_date DATE,
          scope STRING,
          affected_asset_count INT,
          completed_at TIMESTAMP,
          evidence_ref STRING
        )
        """
    )
    spark.sql(
        f"""
        INSERT INTO {register} VALUES (
          '{REQUEST_ID}', '{SUBJECT_KEY}', 'erasure', 'verified',
          DATE'2026-08-20', DATE'2026-09-19',
          'priv_demo customer estate: erasure_bronze (authoritative), erasure_silver (downstream)',
          2, TIMESTAMP'2026-08-21 09:17:11',
          'DESCRIBE HISTORY {schema}.{BRONZE_TABLE}/{SILVER_TABLE}; system.query.history + system.access.audit for {REQUEST_ID}'
        )
        """
    )

    # The register's per-statement action log: which statement ran, of which type, on which
    # table. This is how the request ties to query.history (join on statement_id) and how the
    # audit credential-vend event lines up (match on target_table, actor and an overlapping time).
    stmt_log = f"`{catalog}`.`{schema}`.{REQUEST_STMTS_TABLE}"
    b_full = f"{catalog}.{schema}.{BRONZE_TABLE}"
    s_full = f"{catalog}.{schema}.{SILVER_TABLE}"
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {stmt_log} (
          request_id STRING,
          statement_id STRING,
          statement_type STRING,
          target_table STRING,
          operation STRING
        )
        """
    )
    spark.sql(
        f"""
        INSERT INTO {stmt_log} VALUES
          ('{REQUEST_ID}', '{DEL_STMT_ID}', 'DELETE', '{b_full}', 'READ_WRITE'),
          ('{REQUEST_ID}', '{MRG_STMT_ID}', 'MERGE', '{s_full}', 'READ_WRITE'),
          ('{REQUEST_ID}', '{REORG_STMT_ID}', 'OPTIMIZE', '{b_full}', 'READ_WRITE')
        """
    )

    b = spark.sql(f"SELECT count(*) AS c FROM {bronze}").first()["c"]
    present = spark.sql(f"SELECT count(*) AS c FROM {bronze} WHERE customer_id = {SUBJECT_ID}").first()["c"]
    print(f"[OK] Built {schema}.{BRONZE_TABLE} ({b} rows) and {schema}.{SILVER_TABLE}, subject already erased.")
    print(f"Subject customer_id {SUBJECT_ID} rows in {BRONZE_TABLE}: {present} (expect 0, the verification result).")
    print(f"[OK] Built {schema}.{REGISTER_TABLE} with 1 verified request ({REQUEST_ID}).")
    print(f"[OK] Built {schema}.{REQUEST_STMTS_TABLE} with the 3 erasure statements for {REQUEST_ID}.")


ensure_erasure_evidence_estate(my_catalog)

# COMMAND ----------

compute_validation(recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)

# COMMAND ----------

display_config_values([
    ("Your Catalog", my_catalog, my_catalog),
    ("Shared Demo Schema (read only)", DEMO_SCHEMA),
    ("Evidence snapshots", f"{DEMO_SCHEMA}.{CLASS_SNAPSHOT}, {TABLE_LINEAGE_SNAP}, {COLUMN_LINEAGE_SNAP}, {AUDIT_SNAP}, {QUERY_SNAP}"),
    ("Request register (verified)", f"{DEMO_SCHEMA}.{REGISTER_TABLE}, {DEMO_SCHEMA}.{REQUEST_STMTS_TABLE}"),
    ("Already-erased tables (verification)", f"{DEMO_SCHEMA}.{BRONZE_TABLE}, {DEMO_SCHEMA}.{SILVER_TABLE}"),
    ("Request / subject", f"{REQUEST_ID} / {SUBJECT_KEY} (customer_id {SUBJECT_ID})"),
])
setup_complete_msg()