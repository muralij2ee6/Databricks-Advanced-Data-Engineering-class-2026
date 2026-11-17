# Databricks notebook source
# Classroom-Setup-10
# LAB 10: Fulfil and Verify an Erasure Request
# ---------------------------------------------------------------------
# Idempotent, serverless, safe to re-run.
#
# Half-finished erasure: subject soft-deleted from bronze only, silver still has it,
# gold counted it, no evidence trail. Completed during the lab.
#
# Steps
# - Common -> catalog -> ensure priv_lab -> orders medallion (fresh, subject soft-deleted from bronze only) -> lineage + classification snapshots -> empty register -> validate -> config.
#
# Creates
# - priv_lab.{orders_bronze, orders_silver, sales_by_country_gold} (fresh), privacy_requests (empty), orders_snap_* snapshots.
#
# Does NOT run Required-Data (isolated from priv_demo).
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

from pyspark.sql import functions as F

# Isolated lab estate. Distinct names to avoid collision with the notebook 05 lab.
LAB_SCHEMA = "priv_lab"

BRONZE_TABLE = "orders_bronze" # authoritative order-level table (PII), deletion vectors on
SILVER_TABLE = "orders_silver" # downstream enriched copy (PII), deletion vectors on
GOLD_TABLE = "sales_by_country_gold" # non-identifying aggregate: country totals, no identifiers
REGISTER_TABLE = "privacy_requests" # the request control plane (starts empty)

CLASS_SNAPSHOT = "orders_snap_classification_results"
TABLE_LINEAGE_SNAP = "orders_snap_table_lineage"
COLUMN_LINEAGE_SNAP = "orders_snap_column_lineage"

# The synthetic order estate. Small, deterministic, builds in seconds on serverless.
NUM_CUSTOMERS = 600 # customer_id in [CUST_ID_BASE, CUST_ID_BASE + NUM_CUSTOMERS - 1]
CUST_ID_BASE = 1000
N_ORDERS = 4000

# Subject of the erasure request (pseudonymous key resolved to a concrete customer_id).
SUBJECT_ID = 1462
SUBJECT_KEY = "SUBJ-9F3C7A"
REQUEST_ID = "DSR-2026-0731"
SUBJECT_ORDER_COUNT = 4
SUBJECT_COUNTRY = "United States"

# Fully qualified names for convenience.
BRONZE = f"`{my_catalog}`.`{LAB_SCHEMA}`.{BRONZE_TABLE}"
SILVER = f"`{my_catalog}`.`{LAB_SCHEMA}`.{SILVER_TABLE}"
GOLD = f"`{my_catalog}`.`{LAB_SCHEMA}`.{GOLD_TABLE}"
REGISTER = f"`{my_catalog}`.`{LAB_SCHEMA}`.{REGISTER_TABLE}"

# COMMAND ----------

create_schemas(in_catalog=my_catalog, schemas_to_create=[LAB_SCHEMA])

# COMMAND ----------

# Self-contained deterministic generators (this setup doesn't run Required-Data).
_FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "Aiko", "Mateus", "Priya", "Hiroshi", "Ingrid", "Camille",
    "Lucas", "Ananya", "Georg", "Yuki",
]
_LAST_NAMES = [
    "Smith", "Johnson", "Garcia", "Brown", "Nakamura", "Muller", "Patel", "Silva",
    "Rossi", "Dubois", "Nguyen", "Kaur", "Tanaka", "Schmidt", "Lopez", "Anderson",
    "Sato", "Rivera", "Flores", "King",
]
_COUNTRIES = ["United States", "Canada", "United Kingdom", "Australia", "Germany"]
_DOMAINS = ["example.com", "example.net", "mail.example.org", "shop.example.io"]


def _pick(arr, col, seed_offset=0):
    """Pick from a Python list using a deterministic hash. Returns a Column."""
    arr_expr = F.array(*[F.lit(x) for x in arr])
    idx = (F.pmod(F.hash(col + F.lit(seed_offset)), F.lit(len(arr)))).cast("int")
    return F.element_at(arr_expr, idx + 1)

# COMMAND ----------

# CREATE OR REPLACE resets the broken state. Deletion vectors on bronze/silver.
def build_orders_estate(catalog: str):
    # ---- generate the orders: the main population, with the subject held out ----
    main = (
        spark.range(0, N_ORDERS, numPartitions=8)
        .withColumn("order_id", F.lit(100000) + F.col("id"))
        .withColumn(
            "customer_id",
            F.lit(CUST_ID_BASE) + F.pmod(F.hash(F.col("id") + F.lit(3)), F.lit(NUM_CUSTOMERS)),
        )
        .where(F.col("customer_id") != F.lit(SUBJECT_ID))
    )
    cid = F.col("customer_id")
    fn = _pick(_FIRST_NAMES, cid, 0)
    ln = _pick(_LAST_NAMES, cid, 1)
    main = main.select(
        "order_id",
        "customer_id",
        F.concat(fn, F.lit(" "), ln).alias("customer_name"),
        F.concat(
            F.lower(fn), F.lit("."), F.lower(ln), cid.cast("string"),
            F.lit("@"), _pick(_DOMAINS, cid, 2),
        ).alias("customer_email"),
        _pick(_COUNTRIES, cid, 5).alias("country"),
        F.expr(
            "timestampadd(SECOND, cast(pmod(hash(order_id + 150), 63072000) as int), timestamp'2024-01-01')"
        ).alias("order_ts"),
        F.round((F.pmod(F.hash(F.col("order_id") + F.lit(7)), F.lit(49000)) + F.lit(1000)) / F.lit(100.0), 2).alias("amount"),
    )

    # ---- the subject's own orders (exactly SUBJECT_ORDER_COUNT), forced country ----
    s_fn = _pick(_FIRST_NAMES, F.lit(SUBJECT_ID), 0)
    s_ln = _pick(_LAST_NAMES, F.lit(SUBJECT_ID), 1)
    subject = (
        spark.range(0, SUBJECT_ORDER_COUNT)
        .withColumn("order_id", F.lit(990000) + F.col("id"))
        .withColumn("customer_id", F.lit(SUBJECT_ID))
        .select(
            "order_id",
            "customer_id",
            F.concat(s_fn, F.lit(" "), s_ln).alias("customer_name"),
            F.concat(
                F.lower(s_fn), F.lit("."), F.lower(s_ln), F.lit(str(SUBJECT_ID)),
                F.lit("@"), _pick(_DOMAINS, F.lit(SUBJECT_ID), 2),
            ).alias("customer_email"),
            F.lit(SUBJECT_COUNTRY).alias("country"),
            F.expr(
                "timestampadd(SECOND, cast(pmod(hash(order_id + 150), 63072000) as int), timestamp'2024-01-01')"
            ).alias("order_ts"),
            F.round((F.pmod(F.hash(F.col("order_id") + F.lit(7)), F.lit(49000)) + F.lit(1000)) / F.lit(100.0), 2).alias("amount"),
        )
    )

    # Repartition so the subject's orders share data files with other customers. Without it the
    # subject's rows land in files of their own, so deleting them drops whole files instead of
    # writing deletion vectors and the soft delete this lab teaches never happens.
    orders_all = main.unionByName(subject).repartition(4)
    orders_all.createOrReplaceTempView("_lab2_orders_all")

    # ---- bronze: authoritative, PII present, deletion vectors on ----
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {BRONZE}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true') AS
        SELECT order_id, customer_id, customer_name, customer_email, country, order_ts, amount
        FROM _lab2_orders_all
        """
    )
    # The prior partial deletion: a DELETE on the bronze table ONLY. Deletion vectors
    # are on, so this is a soft delete. The value stays in the file until a purge.
    spark.sql(f"DELETE FROM {BRONZE} WHERE customer_id = {SUBJECT_ID}")
    # Confirm the staged delete is the soft delete the lab teaches: no file removed, a deletion vector added.
    dm = (spark.sql(f"DESCRIBE HISTORY {BRONZE}").where("operation = 'DELETE'")
          .orderBy(F.col("version").desc()).first()["operationMetrics"])
    removed, dvs = dm.get("numRemovedFiles", "0"), dm.get("numDeletionVectorsAdded", "0")
    if removed == "0" and dvs != "0":
        print(f"[OK] The prior delete on {BRONZE_TABLE} is a soft delete: 0 files removed, {dvs} deletion vector(s) added.")
    else:
        print(f"[WARN] The prior delete on {BRONZE_TABLE} removed {removed} file(s) and added {dvs} deletion vector(s), "
              "so it isn't the soft delete the lab expects.")

    # ---- silver: downstream enriched copy, PII present, deletion vectors on ----
    # The prior job never propagated the delete, so the subject is still here.
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {SILVER}
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true') AS
        SELECT order_id, customer_id, customer_name, customer_email, country, amount,
               date_trunc('MONTH', order_ts) AS order_month
        FROM _lab2_orders_all
        """
    )

    # ---- gold: non-identifying aggregate, computed from silver (subject counted) ----
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {GOLD} AS
        SELECT country,
               count(*) AS order_count,
               round(sum(amount), 2) AS total_amount,
               count(DISTINCT customer_id) AS distinct_customers
        FROM {SILVER}
        GROUP BY country
        """
    )

    spark.sql("DROP VIEW IF EXISTS _lab2_orders_all")

    b = spark.sql(f"SELECT count(*) AS c FROM {BRONZE}").first()["c"]
    b_subj = spark.sql(f"SELECT count(*) AS c FROM {BRONZE} WHERE customer_id = {SUBJECT_ID}").first()["c"]
    s = spark.sql(f"SELECT count(*) AS c FROM {SILVER}").first()["c"]
    s_subj = spark.sql(f"SELECT count(*) AS c FROM {SILVER} WHERE customer_id = {SUBJECT_ID}").first()["c"]
    g = spark.sql(f"SELECT count(*) AS c FROM {GOLD}").first()["c"]
    print(f"[OK] Built {LAB_SCHEMA}.{BRONZE_TABLE} ({b} rows, deletion vectors on) and "
          f"{LAB_SCHEMA}.{SILVER_TABLE} ({s} rows, deletion vectors on).")
    print(f"[OK] Built {LAB_SCHEMA}.{GOLD_TABLE} ({g} country rows, no identifiers).")
    print(f"Subject customer_id {SUBJECT_ID}: {b_subj} rows in bronze current state "
          f"(soft-deleted by the prior job), {s_subj} rows still in silver.")


build_orders_estate(my_catalog)

# COMMAND ----------

# Deterministic lineage snapshots (same column names as system.access.*_lineage).
# Estate: orders_bronze -> orders_silver -> sales_by_country_gold.
import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, TimestampType, DateType,
)


def _has_rows(catalog: str, table: str) -> bool:
    try:
        n = spark.sql(f"SELECT count(*) AS c FROM `{catalog}`.`{LAB_SCHEMA}`.`{table}`").first()["c"]
        return n is not None and n > 0
    except Exception:
        return False


def ensure_lineage_snapshots(catalog: str):
    def full(tbl):
        return f"{catalog}.{LAB_SCHEMA}.{tbl}"

    event_time = datetime.datetime(2026, 9, 2, 3, 30, 0)
    event_date = datetime.date(2026, 9, 2)
    created_by = "workspace-setup@dbacademy.example"

    # --- table lineage: real column names from system.access.table_lineage ---
    t_edges = [
        (BRONZE_TABLE, SILVER_TABLE),
        (SILVER_TABLE, GOLD_TABLE),
    ]
    t_rows = [
        (full(s), LAB_SCHEMA, s, "TABLE", full(t), LAB_SCHEMA, t, "TABLE", "NOTEBOOK", created_by)
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
        .saveAsTable(f"`{catalog}`.`{LAB_SCHEMA}`.{TABLE_LINEAGE_SNAP}")
    )

    # --- column lineage: real column names from system.access.column_lineage ---
    # Identifiers propagate bronze -> silver; the gold aggregate keeps no identifier.
    c_edges = [
        (BRONZE_TABLE, "customer_id", SILVER_TABLE, "customer_id"),
        (BRONZE_TABLE, "customer_name", SILVER_TABLE, "customer_name"),
        (BRONZE_TABLE, "customer_email", SILVER_TABLE, "customer_email"),
        (BRONZE_TABLE, "country", SILVER_TABLE, "country"),
        (BRONZE_TABLE, "amount", SILVER_TABLE, "amount"),
        (SILVER_TABLE, "country", GOLD_TABLE, "country"),
        (SILVER_TABLE, "amount", GOLD_TABLE, "total_amount"),
        (SILVER_TABLE, "customer_id", GOLD_TABLE, "distinct_customers"),
    ]
    c_rows = [
        (full(s), LAB_SCHEMA, s, sc, "TABLE", full(t), LAB_SCHEMA, t, tc, "TABLE", "NOTEBOOK", created_by)
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
        .saveAsTable(f"`{catalog}`.`{LAB_SCHEMA}`.{COLUMN_LINEAGE_SNAP}")
    )
    print(f"[OK] Built {LAB_SCHEMA}.{TABLE_LINEAGE_SNAP} ({len(t_rows)} edges) and "
          f"{LAB_SCHEMA}.{COLUMN_LINEAGE_SNAP} ({len(c_rows)} column edges), matching the system-table columns.")


ensure_lineage_snapshots(my_catalog)

# COMMAND ----------

# Deterministic classification snapshot. Mirrors system.data_classification.results.
def ensure_classification_snapshot(catalog: str):
    detected = datetime.datetime(2026, 9, 2, 3, 14, 0)
    latest = datetime.datetime(2026, 9, 3, 3, 21, 0)
    # (table_name, column_name, data_type, class_tag, confidence, frequency, samples)
    rows = [
        (BRONZE_TABLE, "customer_email", "string", "class.email_address", "HIGH", 1.00,
         ["jordan.reyes1462@example.com", "mary.garcia1203@example.net", "aiko.nakamura1350@mail.example.org"]),
        (BRONZE_TABLE, "customer_name", "string", "class.name", "HIGH", 1.00,
         ["Jordan Reyes", "Mary Garcia", "Aiko Nakamura"]),
        (SILVER_TABLE, "customer_email", "string", "class.email_address", "HIGH", 1.00,
         ["jordan.reyes1462@example.com", "mary.garcia1203@example.net", "aiko.nakamura1350@mail.example.org"]),
        (SILVER_TABLE, "customer_name", "string", "class.name", "HIGH", 1.00,
         ["Jordan Reyes", "Mary Garcia", "Aiko Nakamura"]),
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
        spark.createDataFrame(rows, schema_def)
        .withColumn("catalog_name", F.lit(catalog))
        .withColumn("schema_name", F.lit(LAB_SCHEMA))
        .withColumn("first_detected_time", F.lit(detected).cast("timestamp"))
        .withColumn("latest_detected_time", F.lit(latest).cast("timestamp"))
        .select("catalog_name", "schema_name", "table_name", "column_name", "data_type",
                "class_tag", "samples", "confidence", "frequency",
                "first_detected_time", "latest_detected_time")
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"`{catalog}`.`{LAB_SCHEMA}`.{CLASS_SNAPSHOT}"
    )
    n = spark.sql(f"SELECT count(*) AS c FROM `{catalog}`.`{LAB_SCHEMA}`.{CLASS_SNAPSHOT}").first()["c"]
    print(f"[OK] Built {LAB_SCHEMA}.{CLASS_SNAPSHOT} ({n} detections naming the PII columns, synthetic samples).")


ensure_classification_snapshot(my_catalog)

# COMMAND ----------

# Empty request register. The learner populates it in Task C.
def ensure_request_register(catalog: str):
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {REGISTER} (
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
    print(f"[OK] Created {LAB_SCHEMA}.{REGISTER_TABLE} (empty: no request tracked yet, no evidence trail).")


ensure_request_register(my_catalog)

# COMMAND ----------

# The learner writes SQL to fulfil the request; these functions are how they SEE whether it worked.
# Each is called from the notebook as a one-liner (count_subject_rows(), show_delete_history(), ...).
def count_subject_rows():
    """Subject rows remaining in each identifiable table. Run it before and after a delete to watch
    it drop to 0 in both."""
    display(spark.sql(f"""
    SELECT '{BRONZE_TABLE}' AS table_name, count(*) AS subject_rows FROM {BRONZE} WHERE customer_id = {SUBJECT_ID}
    UNION ALL
    SELECT '{SILVER_TABLE}' AS table_name, count(*) AS subject_rows FROM {SILVER} WHERE customer_id = {SUBJECT_ID}
    ORDER BY table_name
    """))


def show_delete_history():
    """The DELETE and MERGE operations that deleted rows from the identifiable tables since the setup
    built them. files_removed = 0 with a deletion vector added means a soft delete: the rows are hidden
    by a deletion vector, but the value is still in the files."""
    rows_deleted = "coalesce(operationMetrics['numDeletedRows'], operationMetrics['numTargetRowsDeleted'])"
    for tbl, fq in [(BRONZE_TABLE, BRONZE), (SILVER_TABLE, SILVER)]:
        print(f"{tbl}: operations that deleted rows (files_removed = 0 with a deletion vector added means a soft delete):")
        hist = spark.sql(f"DESCRIBE HISTORY {fq}")
        # The setup's CREATE OR REPLACE keeps older history, so show only what happened after the latest build.
        built = hist.where("operation LIKE 'CREATE%' OR operation LIKE 'REPLACE%'").agg(F.max("version")).first()[0]
        display(
            hist.where("operation IN ('DELETE', 'MERGE')")
            .where(F.col("version") > F.lit(built if built is not None else -1))
            .where(f"cast({rows_deleted} AS BIGINT) > 0")
            .selectExpr(
                "version", "operation",
                f"{rows_deleted} AS rows_deleted",
                "coalesce(operationMetrics['numRemovedFiles'], operationMetrics['numTargetFilesRemoved']) AS files_removed",
                "coalesce(operationMetrics['numDeletionVectorsAdded'], operationMetrics['numTargetDeletionVectorsAdded']) AS deletion_vectors_added",
            ).orderBy(F.col("version").asc())
        )


def show_rewrite_history():
    """The most recent operations on the identifiable tables. After REORG ... APPLY (PURGE) the
    rewrite shows files removed and added: the soft-deleted values are gone from the current files."""
    for tbl, fq in [(BRONZE_TABLE, BRONZE), (SILVER_TABLE, SILVER)]:
        print(f"{tbl}: most recent operations (the REORG rewrite shows files removed and added):")
        hist = spark.sql(f"DESCRIBE HISTORY {fq}")
        display(
            hist.selectExpr(
                "version", "operation",
                "operationMetrics['numRemovedFiles'] AS numRemovedFiles",
                "operationMetrics['numAddedFiles'] AS numAddedFiles",
            ).orderBy(F.col("version").desc()).limit(3)
        )


def show_classification():
    """The HIGH-confidence classification detections that name the personal-data columns."""
    display(spark.sql(f"""
    SELECT table_name, column_name, class_tag, confidence
    FROM `{my_catalog}`.`{LAB_SCHEMA}`.{CLASS_SNAPSHOT}
    WHERE confidence = 'HIGH'
    ORDER BY table_name, column_name
    """))


def show_register():
    """The request register. subject_key is pseudonymous and there is no email or name column, so
    the register itself carries no raw PII."""
    display(spark.sql(f"SELECT * FROM {REGISTER} ORDER BY request_id"))


def show_gold():
    """The non-identifying gold aggregate: country totals, no identifier columns."""
    display(spark.sql(f"SELECT * FROM {GOLD} ORDER BY country"))

# COMMAND ----------

compute_validation(recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)

# COMMAND ----------

display_config_values([
    ("Your Catalog", my_catalog, my_catalog),
    ("Isolated Lab Schema", LAB_SCHEMA),
    ("Medallion (deletion vectors on bronze and silver)",
     f"{LAB_SCHEMA}.{BRONZE_TABLE}, {LAB_SCHEMA}.{SILVER_TABLE}, {LAB_SCHEMA}.{GOLD_TABLE}"),
    ("Request register (starts empty)", f"{LAB_SCHEMA}.{REGISTER_TABLE}"),
    ("Lineage snapshots", f"{LAB_SCHEMA}.{TABLE_LINEAGE_SNAP}, {LAB_SCHEMA}.{COLUMN_LINEAGE_SNAP}"),
    ("Classification snapshot", f"{LAB_SCHEMA}.{CLASS_SNAPSHOT}"),
    ("Subject (customer_id / subject_key / request)", f"{SUBJECT_ID} / {SUBJECT_KEY} / {REQUEST_ID}"),
    ("Broken on purpose", "prior DELETE on bronze only (soft, not purged); silver and gold still hold the subject; no evidence trail"),
    ("File retention default", "168 hours (7 days), delta.deletedFileRetentionDuration"),
])
setup_complete_msg()