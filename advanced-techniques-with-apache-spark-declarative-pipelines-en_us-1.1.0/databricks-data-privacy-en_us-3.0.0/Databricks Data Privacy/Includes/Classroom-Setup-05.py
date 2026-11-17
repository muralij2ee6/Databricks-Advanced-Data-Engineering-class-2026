# Databricks notebook source
# Classroom-Setup-05
# LAB 05: Discover, Tag and Protect PII
# ---------------------------------------------------------------------
# Idempotent, serverless, safe to re-run.
#
# Isolated, intentionally broken lab estate, repaired during the lab. Defects:
# (1) wrong tag on national_id, (2) brittle name-based mask, (3) missing tag on passport_number.
#
# Steps
# - Common -> catalog -> reset prior ABAC policy -> build priv_lab.customer_profile -> broken tags -> brittle mask -> identity grants -> classification snapshot -> validate -> config.
#
# Creates
# - priv_lab.customer_profile (~2k rows, broken), snap_classification_results.
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

# The ISOLATED lab estate. Distinct schema so nothing here can touch priv_demo.
LAB_SCHEMA = "priv_lab"
LAB_TABLE = f"`{my_catalog}`.`{LAB_SCHEMA}`.customer_profile"
SNAPSHOT_TABLE = f"`{my_catalog}`.`{LAB_SCHEMA}`.snap_classification_results"

# ~2,000 travelers is plenty for a lab and builds in seconds on serverless.
LAB_ROWS = 2_000

# The two identities the ABAC policy validates against. These match the Workspace-Setup
# naming contract and notebooks 03 and 04. The governed tag itself is created by
# Workspace-Setup (admin); here we only apply it.
RESTRICTED_GROUP = "account users" # built-in account-level group; the ABAC policy TO set (every user is in it)
PRIVILEGED_GROUP = "account privileged" # custom account group; the ABAC policy EXCEPT set (sees originals)

# The teammate's brittle manual mask UDF, and the policy the learner will drop from
# a prior run. The learner builds priv_lab.mask_restricted and mask_lab_restricted.
MANUAL_MASK_FN = f"`{my_catalog}`.`{LAB_SCHEMA}`.mask_by_name"
LAB_POLICY = "mask_lab_restricted"

# The broken tag state the teammate left. national_id is WRONG (internal, not
# restricted); passport_number is intentionally ABSENT so it's exposed. The other
# columns are tagged correctly, so the verify query flags exactly one WRONG TAG
# (national_id) and one MISSING TAG (passport_number).
BROKEN_COLUMN_TAGS = [
    ("first_name", "internal"), # correct (a name is internal)
    ("last_name", "internal"), # correct
    ("email", "restricted"), # correct
    ("national_id", "internal"), # WRONG: a national identifier is restricted
    ("phone", "confidential"), # correct (confidential, not restricted)
    ("date_of_birth", "confidential"), # correct
]


def _try(label: str, sql: str):
    """Run one optional statement, report the outcome, never raise. A step that can't apply
    is a normal outcome here, reported as a calm [SKIPPED] rather than a warning with a raw
    error line."""
    try:
        spark.sql(sql)
        print(f"[OK] {label}")
    except Exception:
        print(f"[SKIPPED] {label}")


def _apply_group_grants(grants):
    """Apply best-effort read grants. `account users` is built-in and always resolves;
    `account privileged` is provisioned externally, so its grant raises
    PRINCIPAL_DOES_NOT_EXIST when it isn't in place, which is the expected state on the
    deterministic-fallback path. Report it once and stop instead of warning on every
    statement. The notebook detects membership on its own and continues either way."""
    for sql in grants:
        try:
            spark.sql(sql)
        except Exception as e:
            low = (str(e).splitlines() or [""])[0].lower()
            if "principal_does_not_exist" in low or "could not find principal" in low:
                print("[SKIPPED] Read grant to account privileged: it isn't provisioned in this workspace.")
                return
            print("[INFO] A group access grant did not apply; the notebook still runs.")
            return
    print("[OK] Read grants applied to account users and account privileged.")


def _reset(label: str, sql: str):
    """Drop a leftover control before rebuilding. Nothing to drop is a normal first-run
    outcome, reported as [OK], not a warning."""
    try:
        spark.sql(sql)
        print(f"[OK] Reset {label}.")
    except Exception as e:
        msg = (str(e).splitlines() or [""])[0]
        low = msg.lower()
        if any(s in low for s in ("not_found", "cannot be found", "does not exist",
                                  "no row filter", "no column mask", "no mask",
                                  "does not have a")):
            print(f"[OK] No prior {label} to reset.")
        else:
            print(f"[INFO] Could not reset {label}: {msg}")

# COMMAND ----------

# Drop any schema-scoped ABAC policy from a prior run (survives table rebuild).
def reset_prior_control(catalog: str):
    create_schemas(in_catalog=catalog, schemas_to_create=[LAB_SCHEMA])
    _reset(
        f"{LAB_POLICY} policy on {LAB_SCHEMA}",
        f"DROP POLICY {LAB_POLICY} ON SCHEMA `{catalog}`.`{LAB_SCHEMA}`",
    )


reset_prior_control(my_catalog)

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
_PHONE_CC = ["1", "1", "44", "61", "49"]
_DOMAINS = ["example.com", "example.net", "mail.example.org", "trips.example.io"]
_TIERS = ["Blue", "Silver", "Gold", "Platinum"]
# Passport letters kept simple and readable (no I or O).
_PASSPORT_ALPHA = list("ABCDEFGHJKLMNPQRSTUVWXYZ")


def _pick(arr, col, seed_offset=0):
    """Pick from a Python list using a deterministic hash. Returns a Column."""
    arr_expr = F.array(*[F.lit(x) for x in arr])
    idx = (F.pmod(F.hash(col + F.lit(seed_offset)), F.lit(len(arr)))).cast("int")
    return F.element_at(arr_expr, idx + 1)


def _digits(col, seed, k):
    """Deterministic k-digit numeric string (leading zeros allowed)."""
    return F.lpad((F.pmod(F.hash(col + F.lit(seed)), F.lit(10 ** k))).cast("string"), k, "0")

# COMMAND ----------

# CREATE OR REPLACE resets the broken state (wipes prior tags and masks).
def build_lab_estate(catalog: str, n_rows: int = LAB_ROWS):
    _id = F.col("id")
    fn = _pick(_FIRST_NAMES, _id, 0)
    ln = _pick(_LAST_NAMES, _id, 1)
    loc_idx = (F.pmod(F.hash(_id + F.lit(5)), F.lit(len(_COUNTRIES)))).cast("int")
    country = F.element_at(F.array(*[F.lit(c) for c in _COUNTRIES]), loc_idx + 1)
    phone_cc = F.element_at(F.array(*[F.lit(c) for c in _PHONE_CC]), loc_idx + 1)
    passport_alpha = F.element_at(
        F.array(*[F.lit(c) for c in _PASSPORT_ALPHA]),
        (F.pmod(F.hash(_id + F.lit(80)), F.lit(len(_PASSPORT_ALPHA))) + 1).cast("int"),
    )

    df = (
        spark.range(0, n_rows, numPartitions=4)
        .select(
            (_id + F.lit(1)).alias("customer_id"),
            fn.alias("first_name"),
            ln.alias("last_name"),
            F.concat(F.lower(fn), F.lit("."), F.lower(ln), F.lit("@"), _pick(_DOMAINS, _id, 2)).alias("email"),
            F.concat(F.lit("+"), phone_cc, _digits(_id, 90, 10)).alias("phone"),
            country.alias("country"),
            # national_id: US SSN shape (NNN-NN-NNNN) so it reads as a national identifier.
            F.concat(_digits(_id, 41, 3), F.lit("-"), _digits(_id, 42, 2), F.lit("-"), _digits(_id, 43, 4)).alias("national_id"),
            # passport_number: one letter + 8 digits. The newly added, still-exposed column.
            F.concat(passport_alpha, _digits(_id, 70, 8)).alias("passport_number"),
            F.expr("date_add('1955-01-01', cast(pmod(hash(id + 110), 18250) as int))").alias("date_of_birth"),
            _pick(_TIERS, _id, 130).alias("membership_level"),
            F.expr("timestampadd(SECOND, cast(pmod(hash(id + 150), 126144000) as int), timestamp'2021-01-01')").alias("created_at"),
        )
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"`{catalog}`.`{LAB_SCHEMA}`.customer_profile"
    )
    n = spark.sql(f"SELECT count(*) AS c FROM {LAB_TABLE}").first()["c"]
    print(f"[OK] priv_lab.customer_profile rebuilt: {n:,} synthetic travelers (isolated lab estate).")


build_lab_estate(my_catalog)

# COMMAND ----------

# Defect 1: national_id tagged internal (should be restricted).
# Defect 3: passport_number intentionally left UNTAGGED.
def stage_broken_tags(catalog: str):
    for col, val in BROKEN_COLUMN_TAGS:
        _try(
            f"Tag priv_lab.customer_profile.{col} sensitivity_level={val}"
            + (" <- WRONG on purpose" if col == "national_id" else ""),
            f"ALTER TABLE {LAB_TABLE} ALTER COLUMN {col} SET TAGS ('sensitivity_level' = '{val}')",
        )
    print("[INFO] passport_number is intentionally left UNTAGGED (the newly added, exposed column).")


stage_broken_tags(my_catalog)

# COMMAND ----------

# Defect 2: brittle name-based mask on email and national_id only. passport_number exposed.
def stage_brittle_mask(catalog: str):
    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {MANUAL_MASK_FN}(val STRING)
        RETURNS STRING
        RETURN CASE
          WHEN is_account_group_member('{PRIVILEGED_GROUP}') THEN val
          WHEN val IS NULL THEN NULL
          ELSE concat(left(val, 2), '****')
        END
        """
    )
    print(f"[OK] Created the teammate's manual mask UDF {LAB_SCHEMA}.mask_by_name.")

    # Attach it by name to two columns only. The table was just rebuilt, so there's
    # no prior mask to drop, but guard anyway so a partial prior run is safe.
    for col in ("email", "national_id"):
        _try(
            f"Attach brittle name-based mask to priv_lab.customer_profile.{col}",
            f"ALTER TABLE {LAB_TABLE} ALTER COLUMN {col} SET MASK {MANUAL_MASK_FN}",
        )
    print("[INFO] passport_number is intentionally left UNMASKED (the name-based mask never covered it).")


stage_brittle_mask(my_catalog)

# COMMAND ----------

# Best-effort read grants: account users is built-in; account privileged is provisioned via SCIM/console.
def ensure_identity_context(catalog: str):
    grants = []
    for grp in (RESTRICTED_GROUP, PRIVILEGED_GROUP):
        grants += [
            f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{grp}`",
            f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{LAB_SCHEMA}` TO `{grp}`",
            f"GRANT SELECT ON SCHEMA `{catalog}`.`{LAB_SCHEMA}` TO `{grp}`",
        ]
    _apply_group_grants(grants)

    # Every user is in account users (the policy TO set), so the mask applies live unless you
    # are in the EXCEPT set, account privileged.
    try:
        row = spark.sql(
            f"SELECT current_user() AS u, "
            f"is_account_group_member('{PRIVILEGED_GROUP}') AS in_privileged"
        ).first()
        print(f"[INFO] Running as {row['u']}. In {PRIVILEGED_GROUP}: {row['in_privileged']} "
              f"(everyone is in account users, so the mask applies unless this is True).")
    except Exception as e:
        first = (str(e).splitlines() or [""])[0]
        print(f"[INFO] Could not read group membership here ({first}).")


ensure_identity_context(my_catalog)

# COMMAND ----------

# Deterministic classification snapshot. Includes a false positive (created_at).
import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType


def stage_classification_snapshot(catalog: str):
    detected = datetime.datetime(2026, 9, 2, 3, 14, 0)
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
         ["+14155550142", "+16045550198", "+442071838750"]),
        # national_id: a national identifier (restricted class). The applied tag says
        # internal, which is the wrong tag the learner corrects in Task D.
        ("customer_profile", "national_id", "string", "class.us_ssn", "HIGH", 0.95,
         ["123-45-6789", "987-65-4321", "456-78-9012"]),
        # passport_number: the same restricted class family, detected clearly, but the
        # teammate left it untagged and unmasked, so it's exposed (Task G).
        ("customer_profile", "passport_number", "string", "class.passport", "HIGH", 0.93,
         ["E12345678", "K90210034", "M55512890"]),
        ("customer_profile", "date_of_birth", "date", "class.date_of_birth", "HIGH", 1.00,
         ["1974-03-12", "1988-11-02", "1969-07-25"]),
        # FALSE POSITIVE: an account-creation timestamp read as a birth date, LOW.
        ("customer_profile", "created_at", "timestamp", "class.date_of_birth", "LOW", 0.24,
         ["2021-06-02T10:22:14", "2022-01-17T08:03:51", "2023-11-30T19:45:10"]),
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
        .withColumn("catalog_id", F.lit("3f1a7d6e-9c59-4a2b-8e71-dfc0a4e12345"))
        .withColumn("table_id", F.lit("7b2e8f3a-1d45-4c6e-9a0b-def123456789"))
        .select(
            "catalog_name", "schema_name", "table_name", "column_name", "data_type",
            "class_tag", "samples", "confidence", "frequency",
            "first_detected_time", "latest_detected_time",
            "catalog_id", "table_id",
        )
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"`{catalog}`.`{LAB_SCHEMA}`.snap_classification_results"
    )
    n = spark.sql(f"SELECT count(*) AS c FROM {SNAPSHOT_TABLE}").first()["c"]
    print(f"[OK] Reviewable classification snapshot ready: {n} detection(s) in {LAB_SCHEMA}.snap_classification_results (synthetic samples).")


stage_classification_snapshot(my_catalog)

# COMMAND ----------

# The learner writes SQL to fix the estate; these functions are how they SEE whether it worked.
# Each is called from the notebook as a one-liner (verify_tags(), validate_control(), ...).
def verify_tags():
    """Compare each HIGH-confidence classification detection to the sensitivity_level tag applied
    on the column. The finding column reads WRONG TAG, MISSING TAG or ok. Run it to see the defects,
    then again after you correct a tag."""
    try:
        display(spark.sql(f"""
        WITH detections AS (
          SELECT
            column_name,
            class_tag,
            CASE
              WHEN class_tag IN ('class.email_address','class.us_ssn','class.passport',
                                 'class.credit_card','class.iban_code','class.bank_number') THEN 'restricted'
              WHEN class_tag IN ('class.phone_number','class.date_of_birth','class.location') THEN 'confidential'
              WHEN class_tag = 'class.name' THEN 'internal'
              ELSE 'internal'
            END AS recommended_sensitivity
          FROM {SNAPSHOT_TABLE}
          WHERE confidence = 'HIGH'
        ),
        current_tags AS (
          SELECT column_name, tag_value AS current_sensitivity
          FROM `{my_catalog}`.information_schema.column_tags
          WHERE schema_name = '{LAB_SCHEMA}'
            AND table_name = 'customer_profile'
            AND tag_name = 'sensitivity_level'
        )
        SELECT
          d.column_name,
          d.class_tag,
          d.recommended_sensitivity,
          coalesce(t.current_sensitivity, '(none)') AS current_sensitivity,
          CASE
            WHEN t.current_sensitivity IS NULL THEN 'MISSING TAG'
            WHEN t.current_sensitivity <> d.recommended_sensitivity THEN 'WRONG TAG'
            ELSE 'ok'
          END AS finding
        FROM detections d
        LEFT JOIN current_tags t ON d.column_name = t.column_name
        ORDER BY finding, d.column_name
        """))
    except Exception as e:
        print(f"[INFO] Could not read information_schema.column_tags here: {(str(e).splitlines() or [''])[0]}")


def show_effective_policies():
    """Show the column-mask policies in force on customer_profile. After you create the ABAC policy
    the masked columns appear here; after you tag a new column it appears too, with no policy edit."""
    try:
        display(spark.sql(f"SHOW EFFECTIVE POLICIES ON TABLE {LAB_TABLE}"))
    except Exception as e:
        print(f"[INFO] SHOW EFFECTIVE POLICIES unavailable here: {(str(e).splitlines() or [''])[0]}")


def validate_control():
    """See the control from your own identity: who you are, what the table returns to you (email and
    national_id masked, phone visible) and the effective policy on the table."""
    display(spark.sql("""
    SELECT current_user() AS running_as,
           is_account_group_member('account users')      AS in_account_users,
           is_account_group_member('account privileged') AS in_account_privileged
    """))
    display(spark.sql(f"SELECT customer_id, email, national_id, phone FROM {LAB_TABLE} LIMIT 8"))
    show_effective_policies()

# COMMAND ----------

compute_validation(recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)

# COMMAND ----------

display_config_values([
    ("Your Catalog", my_catalog, my_catalog),
    ("Isolated Lab Schema", LAB_SCHEMA),
    ("Table to repair", f"{LAB_SCHEMA}.customer_profile"),
    ("Reviewable snapshot", f"{LAB_SCHEMA}.snap_classification_results"),
    ("Governed Tag", "sensitivity_level (public, internal, confidential, restricted)"),
    ("Restricted / Privileged groups", f"{RESTRICTED_GROUP} / {PRIVILEGED_GROUP}"),
    ("Broken on purpose", "national_id tag wrong, mask is name-based, passport_number exposed"),
])
setup_complete_msg()