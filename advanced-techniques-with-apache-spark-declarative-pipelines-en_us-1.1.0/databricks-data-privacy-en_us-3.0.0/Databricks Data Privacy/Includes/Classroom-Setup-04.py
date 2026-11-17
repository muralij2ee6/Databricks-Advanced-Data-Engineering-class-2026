# Databricks notebook source
# Classroom-Setup-04
# DEMO 04: Protect by Purpose and Risk
# ---------------------------------------------------------------------
# Idempotent, serverless, safe to re-run.
#
# Steps
# - Common -> catalog -> Required-Data -> isolated priv_secure estate (fresh copy, tagged) -> identity grants (best effort) -> validate -> config.
#
# Creates
# - priv_demo (6 demo tables), priv_secure.customer_profile (fresh, tagged).
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

# The shared estate this demo copies from, and the isolated schema it mutates.
DEMO_SCHEMA = "priv_demo"
SECURE_SCHEMA = "priv_secure"

# Governed tag values and identities the ABAC and mask demos validate against. The
# governed tag itself is created by Workspace-Setup (admin); here we only apply it.
SECURE_COLUMN_TAGS = [("email", "restricted"), ("national_id", "restricted"), ("phone", "confidential")]
RESTRICTED_GROUP = "account users" # built-in account-level group; the policy TO set (every user is in it)
PRIVILEGED_GROUP = "account privileged" # custom account group in the policy EXCEPT set; sees originals

# This demo's schema-scoped policy (distinct from priv_demo's mask_restricted).
SECURE_POLICY = "mask_secure_restricted"


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

# Rebuilt every run. REPLACE wipes tags/masks, resetting to a clean starting state.
def ensure_secure_estate(catalog: str):
    create_schemas(in_catalog=catalog, schemas_to_create=[SECURE_SCHEMA])
    tbl = f"`{catalog}`.`{SECURE_SCHEMA}`.customer_profile"

    # Drop leftover controls from a prior run (best effort).
    _reset(
        f"{SECURE_POLICY} policy on priv_secure",
        f"DROP POLICY {SECURE_POLICY} ON SCHEMA `{catalog}`.`{SECURE_SCHEMA}`",
    )
    _reset("row filter on customer_profile", f"ALTER TABLE {tbl} DROP ROW FILTER")
    _reset("mask on customer_profile.phone", f"ALTER TABLE {tbl} ALTER COLUMN phone DROP MASK")

    # Fresh copy of the identifier-bearing columns from the shared estate.
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {tbl} AS
        SELECT
          customer_id, first_name, last_name, email, phone,
          country, national_id, loyalty_tier
        FROM `{catalog}`.`{DEMO_SCHEMA}`.customer_profile
        """
    )
    n = spark.sql(f"SELECT count(*) AS c FROM {tbl}").first()["c"]
    print(f"[OK] priv_secure.customer_profile rebuilt: {n:,} rows (isolated copy).")

    # Tag columns for ABAC. last_name left UNTAGGED so the notebook can tag it live.
    for col, val in SECURE_COLUMN_TAGS:
        _try(
            f"Tag priv_secure.customer_profile.{col} sensitivity_level={val}",
            f"ALTER TABLE {tbl} ALTER COLUMN {col} SET TAGS ('sensitivity_level' = '{val}')",
        )


ensure_secure_estate(my_catalog)

# COMMAND ----------

# Best-effort read grants: account users is built-in; account privileged is provisioned via SCIM/console.
def ensure_identity_context(catalog: str):
    grants = []
    for grp in (RESTRICTED_GROUP, PRIVILEGED_GROUP):
        grants += [
            f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{grp}`",
            f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{SECURE_SCHEMA}` TO `{grp}`",
            f"GRANT SELECT ON SCHEMA `{catalog}`.`{SECURE_SCHEMA}` TO `{grp}`",
        ]
    _apply_group_grants(grants)

    # Report this session's own membership so the notebook's validation is grounded. Every user
    # is in account users (the policy TO set), so the mask applies live unless you are in the
    # EXCEPT set, account privileged.
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

compute_validation(recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)

# COMMAND ----------

display_config_values([
    ("Your Catalog", my_catalog, my_catalog),
    ("Shared Demo Schema", DEMO_SCHEMA),
    ("Isolated Secure Schema", SECURE_SCHEMA),
    ("Protected Table", f"{SECURE_SCHEMA}.customer_profile"),
    ("Governed Tag", "sensitivity_level (public, internal, confidential, restricted)"),
    ("Restricted / Privileged groups", f"{RESTRICTED_GROUP} / {PRIVILEGED_GROUP}"),
])
setup_complete_msg()