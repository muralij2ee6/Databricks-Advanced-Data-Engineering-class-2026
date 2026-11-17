# Databricks notebook source
# Classroom-Setup-06
# DEMO 06: De-identification and Key Management
# ---------------------------------------------------------------------
# Idempotent, serverless, safe to re-run.
#
# Steps
# - Common -> catalog -> Required-Data -> empty priv_deid schema -> token vault in priv_vault (rebuilt) -> probe secrets (scope provisioned by Workspace-Setup) -> governance grants (best effort) -> validate -> config.
#
# Creates
# - priv_demo (6 demo tables), priv_deid (empty), priv_vault.email_token_vault, dbacademy_privacy scope + keys (best effort).
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

# Schemas this demo uses.
DEMO_SCHEMA = "priv_demo" # shared retailer estate (read only here)
DEID_SCHEMA = "priv_deid" # this demo's isolated de-identified analytics estate
VAULT_SCHEMA = "priv_vault" # the separately governed token vault

# Secret scope and keys (Workspace-Setup provisions at launch; this setup self-heals).
SECRET_SCOPE = "dbacademy_privacy"
SALT_KEY = "pii_salt" # a random string used to salt sha2 pseudonyms
AES_KEY_NAME = "pii_aes_key" # a 16/24/32-byte AES key, stored base64-encoded

VAULT_TABLE = "email_token_vault"

# Identities for the governance split.
ANALYST_GROUP = "account users" # built-in account group; everyone may read the de-identified analytics estate
PRIVILEGED_GROUP = "account privileged" # custom account group; may read the governed vault

# Set by probe_secret_scope() below and read by the notebook after %run.
SECRETS_AVAILABLE = False


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
    PRINCIPAL_DOES_NOT_EXIST when it isn't in place, which is the expected state here.
    Report it once and stop instead of warning on every statement. The notebook detects
    membership on its own and continues either way."""
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

# COMMAND ----------

# Ensure the empty output schema exists. The notebook fills it.
def ensure_deid_estate(catalog: str):
    create_schemas(in_catalog=catalog, schemas_to_create=[DEID_SCHEMA])
    print(f"[OK] Isolated analytics schema ready: {catalog}.{DEID_SCHEMA} (the notebook fills it).")


ensure_deid_estate(my_catalog)

# COMMAND ----------

# Opaque token vault (random UUIDs), separately governed from the analytics estate.
def ensure_token_vault(catalog: str):
    create_schemas(in_catalog=catalog, schemas_to_create=[VAULT_SCHEMA])
    vault = f"`{catalog}`.`{VAULT_SCHEMA}`.{VAULT_TABLE}"
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {vault} AS
        SELECT
          concat('TKN-', upper(substr(replace(uuid(), '-', ''), 1, 16))) AS email_token,
          email
        FROM (SELECT DISTINCT email FROM `{catalog}`.`{DEMO_SCHEMA}`.customer_profile)
        """
    )
    n = spark.sql(f"SELECT count(*) AS c FROM {vault}").first()["c"]
    print(f"[OK] Token vault ready: {catalog}.{VAULT_SCHEMA}.{VAULT_TABLE} ({n:,} token/email rows).")
    # Document the intent on the schema so it reads as governed apart from the analytics estate.
    _try(
        "Comment priv_vault as a separately governed token vault",
        f"COMMENT ON SCHEMA `{catalog}`.`{VAULT_SCHEMA}` IS "
        "'Separately governed token vault. Holds the token to value reversal mapping only. "
        "Not part of the analytics estate; access is tightly restricted and audited.'",
    )


ensure_token_vault(my_catalog)

# COMMAND ----------

# Probe the scope with try_secret(). Reports whether keys resolve, never prints values.
def probe_secret_scope():
    global SECRETS_AVAILABLE
    try:
        row = spark.sql(
            f"SELECT try_secret('{SECRET_SCOPE}', '{SALT_KEY}') AS salt, "
            f"try_secret('{SECRET_SCOPE}', '{AES_KEY_NAME}') AS aeskey"
        ).first()
        have_salt = row["salt"] is not None
        have_key = row["aeskey"] is not None
    except Exception as e:
        first = (str(e).splitlines() or [""])[0]
        have_salt = have_key = False
        print(f"[INFO] try_secret() did not evaluate here ({first}).")

    SECRETS_AVAILABLE = bool(have_salt and have_key)
    if SECRETS_AVAILABLE:
        print(f"[OK] Secret scope reachable: '{SALT_KEY}' and '{AES_KEY_NAME}' resolved via secret() (values not shown).")
    else:
        print(f"[WARN] Secret scope '{SECRET_SCOPE}' isn't fully reachable in this workspace.")
        print(f"salt '{SALT_KEY}': {'found' if have_salt else 'missing'}; "
              f"key '{AES_KEY_NAME}': {'found' if have_key else 'missing'}.")
        print("On the training tenant Workspace-Setup provisions the scope at launch.")
        print("To set it up yourself ONCE from a terminal with the Databricks CLI:")
        print(f"databricks secrets create-scope {SECRET_SCOPE}")
        print(f"databricks secrets put-secret {SECRET_SCOPE} {SALT_KEY} --string-value \"<a long random salt string>\"")
        print(f"databricks secrets put-secret {SECRET_SCOPE} {AES_KEY_NAME} --string-value \"$(openssl rand -base64 32)\"")
        print("The AES key is stored base64-encoded; the notebook decodes it with unbase64() before aes_encrypt.")
        print("The de-identification cells guard for this: where a secret is missing they print guidance and continue.")


probe_secret_scope()

# COMMAND ----------

# Best-effort governance: account users read priv_deid, only account privileged reads priv_vault.
def ensure_governance(catalog: str):
    grants = [f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{grp}`"
              for grp in (ANALYST_GROUP, PRIVILEGED_GROUP)]
    # Analysts may read the de-identified analytics estate.
    grants += [
        f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{DEID_SCHEMA}` TO `{ANALYST_GROUP}`",
        f"GRANT SELECT ON SCHEMA `{catalog}`.`{DEID_SCHEMA}` TO `{ANALYST_GROUP}`",
    ]
    # Only the privileged group may read the vault. Analysts are deliberately NOT granted.
    grants += [
        f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{VAULT_SCHEMA}` TO `{PRIVILEGED_GROUP}`",
        f"GRANT SELECT ON SCHEMA `{catalog}`.`{VAULT_SCHEMA}` TO `{PRIVILEGED_GROUP}`",
    ]
    _apply_group_grants(grants)


ensure_governance(my_catalog)

# COMMAND ----------

compute_validation(recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)

# COMMAND ----------

display_config_values([
    ("Your Catalog", my_catalog, my_catalog),
    ("Shared Demo Schema (read only)", DEMO_SCHEMA),
    ("De-identified Analytics Schema", DEID_SCHEMA),
    ("Token Vault Schema (governed)", f"{VAULT_SCHEMA}.{VAULT_TABLE}"),
    ("Secret Scope", f"{SECRET_SCOPE} (keys: {SALT_KEY}, {AES_KEY_NAME})"),
    ("Secrets reachable here", "yes" if SECRETS_AVAILABLE else "no (see setup output for the one-time CLI step)"),
])
setup_complete_msg()