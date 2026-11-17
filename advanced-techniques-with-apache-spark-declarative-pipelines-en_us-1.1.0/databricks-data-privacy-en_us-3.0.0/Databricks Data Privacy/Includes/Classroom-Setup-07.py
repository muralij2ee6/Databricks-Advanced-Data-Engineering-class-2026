# Databricks notebook source
# Classroom-Setup-07
# DEMO 07: Right to Erasure
# ---------------------------------------------------------------------
# Idempotent, serverless, safe to re-run.
#
# Steps
# - Common -> catalog -> Required-Data -> classification snapshot (if missing) -> lineage snapshots (if missing) -> isolated erasure estate (rebuilt, deletion vectors on, subject seeded) -> validate -> config.
#
# Creates
# - priv_demo (6 demo tables), snap_classification_results, snap_table_lineage, snap_column_lineage (if missing), erasure_bronze, erasure_silver (rebuilt).
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

from pyspark.sql import functions as F

DEMO_SCHEMA = "priv_demo" # shared retailer estate (read only in the find step)

# The isolated, demo-owned tables the delete/purge steps mutate. Deletion vectors on.
BRONZE_TABLE = "erasure_bronze" # authoritative layer (mirrors customer_profile)
SILVER_TABLE = "erasure_silver" # downstream copy (mirrors customer_enriched)

# Deterministic snapshots the find step reads.
CLASS_SNAPSHOT = "snap_classification_results"
TABLE_LINEAGE_SNAP = "snap_table_lineage"
COLUMN_LINEAGE_SNAP = "snap_column_lineage"

# Subject of the erasure request. SUBJECT_ID sits inside the seed range.
SUBJECT_ID = 4212
SEED_LO, SEED_HI = 4100, 4300 # ~201 customers, one small file, includes the subject

# COMMAND ----------

# Deterministic classification snapshot (same 11 detections as Setup-03). Rebuilt if missing.
import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType


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
from pyspark.sql.types import TimestampType, DateType


def _lineage_ready(catalog: str, schema: str = DEMO_SCHEMA) -> bool:
    # Ready only when BOTH snapshots carry every estate edge the find step reads: the
    # three table edges, and the key column edges (identifiers reaching customer_enriched
    # and the aggregate reaching customer_metrics_gold). A missing, empty, single-edge,
    # or live-derived snapshot that lacks any required edge fails, so both are rebuilt.
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

# Isolated erasure tables with deletion vectors. Always rebuilt, seeded with the subject.


def ensure_erasure_estate(catalog: str, schema: str = DEMO_SCHEMA):
    bronze = f"`{catalog}`.`{schema}`.{BRONZE_TABLE}"
    silver = f"`{catalog}`.`{schema}`.{SILVER_TABLE}"

    # Authoritative layer: a PII-shaped slice of the customer estate.
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {bronze}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true') AS
        SELECT customer_id, first_name, last_name, email, phone, national_id,
               country, created_at
        FROM `{catalog}`.`{schema}`.customer_profile
        WHERE customer_id BETWEEN {SEED_LO} AND {SEED_HI}
        """
    )
    # Downstream copy: the same customers joined to loyalty (mirrors customer_enriched).
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {silver}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true') AS
        SELECT p.customer_id, p.email, p.national_id, p.country,
               l.points_balance, l.tier AS loyalty_program_tier, l.enrolled_at
        FROM `{catalog}`.`{schema}`.customer_profile p
        LEFT JOIN `{catalog}`.`{schema}`.loyalty l USING (customer_id)
        WHERE p.customer_id BETWEEN {SEED_LO} AND {SEED_HI}
        """
    )
    b = spark.sql(f"SELECT count(*) AS c FROM {bronze}").first()["c"]
    s = spark.sql(f"SELECT count(*) AS c FROM {silver}").first()["c"]
    present = spark.sql(f"SELECT count(*) AS c FROM {bronze} WHERE customer_id = {SUBJECT_ID}").first()["c"]
    print(f"[OK] Built {schema}.{BRONZE_TABLE} ({b} rows) and {schema}.{SILVER_TABLE} ({s} rows), deletion vectors on.")
    print(f"Subject customer_id {SUBJECT_ID} present in {BRONZE_TABLE}: {'yes' if present else 'NO (check seed range)'}.")


ensure_erasure_estate(my_catalog)

# COMMAND ----------

compute_validation(recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)

# COMMAND ----------

display_config_values([
    ("Your Catalog", my_catalog, my_catalog),
    ("Shared Demo Schema (read only in the find step)", DEMO_SCHEMA),
    ("Isolated erasure tables (mutated here)", f"{DEMO_SCHEMA}.{BRONZE_TABLE}, {DEMO_SCHEMA}.{SILVER_TABLE}"),
    ("Classification snapshot", f"{DEMO_SCHEMA}.{CLASS_SNAPSHOT}"),
    ("Lineage snapshots", f"{DEMO_SCHEMA}.{TABLE_LINEAGE_SNAP}, {DEMO_SCHEMA}.{COLUMN_LINEAGE_SNAP}"),
    ("Subject to erase (customer_id)", str(SUBJECT_ID)),
    ("File retention default", "168 hours (7 days), delta.deletedFileRetentionDuration"),
])
setup_complete_msg()