# Databricks notebook source
# Classroom-Setup-03
# DEMO 03: Discover and Classify Sensitive Data
# ---------------------------------------------------------------------
# Idempotent, serverless, safe to re-run.
#
# Steps
# - Common -> catalog -> Required-Data -> classification snapshot (deterministic) -> sensitivity_level tags (best effort) -> validate -> config.
#
# Creates
# - priv_demo (6 demo tables), snap_classification_results.
#
# Workspace-Setup creates the governed sensitivity_level tag at launch; this setup applies its column values.
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

# Deterministic classification snapshot (always rebuilt). Includes a false positive
# (created_at) and free-text detections (body_text).
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType,
)

SNAPSHOT_TABLE = "snap_classification_results"


def ensure_classification_snapshot(catalog: str, schema: str = DEMO_SCHEMA):
    detected = datetime.datetime(2026, 9, 2, 3, 14, 0) # a fixed scan time
    latest = datetime.datetime(2026, 9, 3, 3, 21, 0)

    # (table_name, column_name, data_type, class_tag, confidence, frequency, samples)
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
        # Region-gated identifier: only US-shaped values match, so frequency is low.
        ("customer_profile", "national_id", "string", "class.us_ssn", "HIGH", 0.10,
         ["123-45-6789", "987-65-4321", "456-78-9012"]),
        ("customer_profile", "ip_address", "string", "class.ip_address", "HIGH", 1.00,
         ["203.0.113.5", "198.51.100.23", "192.0.2.146"]),
        # FALSE POSITIVE: an account-creation timestamp read as a birth date.
        ("customer_profile", "created_at", "timestamp", "class.date_of_birth", "LOW", 0.28,
         ["2021-06-02T10:22:14", "2022-01-17T08:03:51", "2020-11-30T19:45:10"]),
        # The "unknown" column: free text that embeds contact details.
        ("support_records", "body_text", "string", "class.email_address", "HIGH", 0.50,
         ["My email is r.lopez@example.net.", "You can reach me at jane.doe@example.com or +12025550143."]),
        ("support_records", "body_text", "string", "class.phone_number", "LOW", 0.29,
         ["You can reach me at jane.doe@example.com or +12025550143.", "call me on +13105550187"]),
    ]

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
        spark.createDataFrame(rows, schema_def) # noqa: F821
        .withColumn("catalog_name", F.lit(catalog))
        .withColumn("schema_name", F.lit(schema))
        .withColumn("first_detected_time", F.lit(detected).cast("timestamp"))
        .withColumn("latest_detected_time", F.lit(latest).cast("timestamp"))
        .withColumn("catalog_id", F.lit("3f1a7d6e-9c59-4a2b-8e71-dfc0a4e12345"))
        .withColumn("table_id",
            F.when(F.col("table_name") == "customer_profile",
                   F.lit("7b2e8f3a-1d45-4c6e-9a0b-def123456789"))
             .otherwise(F.lit("9c4f0a2b-3e67-4d8f-b1c2-abc987654321")))
        .select(
            "catalog_name", "schema_name", "table_name", "column_name", "data_type",
            "class_tag", "samples", "confidence", "frequency",
            "first_detected_time", "latest_detected_time",
            "catalog_id", "table_id",
        )
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"`{catalog}`.`{schema}`.{SNAPSHOT_TABLE}"
    )
    n = spark.sql(f"SELECT count(*) AS c FROM `{catalog}`.`{schema}`.{SNAPSHOT_TABLE}").first()["c"] # noqa: F821
    print(f"[OK] Reviewable pre-scan snapshot ready: {n} detection(s) in {schema}.{SNAPSHOT_TABLE} (synthetic samples).")


ensure_classification_snapshot(my_catalog)

# COMMAND ----------

# The notebook-03 mapping. Workspace-Setup creates the governed tag and grants ASSIGN to account users;
# the learner owns these tables, so no APPLY TAG grant is needed. created_at (the LOW-confidence false
# positive) stays untagged and body_text takes the higher sensitivity of its two detections.
SENSITIVITY_TAG_PLAN = [
    ("customer_profile", "first_name", "internal"),
    ("customer_profile", "last_name", "internal"),
    ("customer_profile", "email", "restricted"),
    ("customer_profile", "phone", "confidential"),
    ("customer_profile", "street_address", "confidential"),
    ("customer_profile", "date_of_birth", "confidential"),
    ("customer_profile", "national_id", "restricted"),
    ("customer_profile", "ip_address", "confidential"),
    ("support_records", "body_text", "restricted"),
]


def ensure_sensitivity_tags(catalog: str, schema: str = DEMO_SCHEMA):
    applied = 0
    for tbl, col, val in SENSITIVITY_TAG_PLAN:
        try:
            spark.sql(
                f"ALTER TABLE `{catalog}`.`{schema}`.{tbl} "
                f"ALTER COLUMN {col} SET TAGS ('sensitivity_level' = '{val}')"
            )
            applied += 1
        except Exception:
            pass
    total = len(SENSITIVITY_TAG_PLAN)
    if applied == total:
        print(f"[OK] sensitivity_level applied to {applied}/{total} {schema} columns.")
    elif applied:
        print(f"[OK] sensitivity_level applied to {applied}/{total} {schema} columns; Section E reads back what is in place.")
    else:
        print("[SKIPPED] The sensitivity_level tag wasn't applied here (it needs ASSIGN on the governed tag); E2 stays the plan for what to tag.")


ensure_sensitivity_tags(my_catalog)

# COMMAND ----------

compute_validation(recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)

# COMMAND ----------

display_config_values([
    ("Your Catalog", my_catalog, my_catalog),
    ("Your Demo Schema", DEMO_SCHEMA),
    ("Reviewable snapshot", f"{DEMO_SCHEMA}.{SNAPSHOT_TABLE}"),
    ("Governed tag", "sensitivity_level (public, internal, confidential, restricted)"),
])
setup_complete_msg()