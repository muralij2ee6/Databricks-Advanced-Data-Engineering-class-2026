# Databricks notebook source
# Classroom-Setup-Common-Python
# ---------------------------------------------------------------------
# Shared helpers: Vocareum detection, username truncation, per-user catalog
# builder, warn-not-error compute check, config display. Serverless only (env version 6).
#
# Usage
# - %run ./Classroom-Setup-Common-Python
# - my_catalog = build_user_catalog(catalog_forced=None)

# COMMAND ----------

import re
from typing import Optional

# COMMAND ----------

# Compute defaults for the warn-not-error check. Serverless only.
RECOMMENDED_SERVERLESS_VERSION = 6 # serverless environment version the course targets
RECOMMENDED_DBR_CLASSIC_VERSION = None # serverless-only course: no classic DBR

# COMMAND ----------

def _safe_uc_name(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "user"


def _current_user_email() -> str:
    """Return the current user's email address."""
    return spark.sql("SELECT current_user()").first()[0]


def _get_workspace_catalogs():
    """Return the set of catalog names visible to the current user (lower-cased)."""
    return [row["catalog"].strip().lower() for row in spark.sql("SHOW CATALOGS").collect()]


def _catalog_exists(name: str, catalogs) -> bool:
    """True if a catalog with this name is visible to the current user."""
    return name.lower() in catalogs


def build_user_catalog(prefix: str = "labuser", catalog_forced=None) -> str:
    """
    Return a UC catalog name for the current user and make sure it exists.

    Vocareum:
      - The user already has a catalog equal to their 'labuserxxx' username.
        If it exists, use it.
    Other workspaces (incl. Free Edition / FEVM):
      - Use <prefix>_<user> and create it if the user has privileges.
      - If catalog_forced is set, use that existing catalog instead.

    Example:
        my_catalog = build_user_catalog(catalog_forced=None)
    """
    user_email = _current_user_email()
    user_name = user_email.split("@")[0]
    safe_user_name = _safe_uc_name(user_name)

    # VOCAREUM: catalog is just the username, already provisioned.
    vocareum_catalog_name = safe_user_name
    if user_email.lower().endswith("@vocareum.com"):
        print("[OK] Vocareum workspace detected.")
        if _catalog_exists(vocareum_catalog_name, _get_workspace_catalogs()):
            print(f"[OK] Using existing Vocareum catalog: '{vocareum_catalog_name}'.")
            return vocareum_catalog_name
        raise ValueError(
            f"❌ Catalog '{vocareum_catalog_name}' doesn't exist in this Vocareum workspace. "
            "Please create the catalog or verify the catalog name before continuing."
        )

    # OTHER WORKSPACES
    print("[INFO] Non-Vocareum workspace detected. Setting up catalog.")
    # Truncate to 19 chars: catalog.schema.object must stay under 64 chars, and a
    # long real name once broke setup. 19 matches typical Vocareum username length.
    safe_user_name_char_restrict = safe_user_name[:19]

    if catalog_forced is None:
        catalog_name = f"{prefix}_{safe_user_name_char_restrict}"
        print(f"[INFO] Using default catalog name: '{catalog_name}'.")
    else:
        catalog_name = catalog_forced
        print(f"[INFO] Using specified catalog name: '{catalog_name}'.")

    catalogs = _get_workspace_catalogs()
    if _catalog_exists(catalog_name, catalogs):
        print(f"[OK] Catalog '{catalog_name}' already exists. Using this catalog.")
        return catalog_name
    if catalog_forced is not None:
        raise RuntimeError(
            f"❌ Catalog '{catalog_name}' doesn't exist in this workspace. "
            "A forced catalog name must reference an existing catalog. "
            "Create it or reference an existing one, then rerun the notebook."
        )
    try:
        print(f"[INFO] Catalog '{catalog_name}' not found. Creating it now...")
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        print(f"[OK] Catalog '{catalog_name}' created successfully.")
        return catalog_name
    except Exception as e:
        print(
            f"[WARN] Could not create catalog '{catalog_name}'. "
            "You may not have privileges to create catalogs in this workspace.\n"
            f"Error: {e}"
        )
        raise

# COMMAND ----------

def create_schemas(in_catalog: str, schemas_to_create: list):
    """
    Create one or more schemas in a Unity Catalog catalog (idempotent).

    Example:
        create_schemas(my_catalog, ["priv_demo", "priv_lab"]) # isolate demo data from lab data
    """
    print(f"\n{'='*60}")
    print(f"STEP 1: Verifying catalog exists: {in_catalog}")
    print(f"{'='*60}")
    try:
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()] # noqa: F821
        if in_catalog not in catalogs:
            raise ValueError(
                f"Catalog '{in_catalog}' doesn't exist.\n"
                f"  Available catalogs: {', '.join(catalogs)}\n"
                f"  Make sure the catalog has been created before running this function."
            )
        print(f"Catalog '{in_catalog}' exists.")
    except ValueError:
        raise
    except Exception as e:
        raise RuntimeError(f"  Failed to verify catalog: {e}")

    print(f"\n{'='*60}")
    print(f"STEP 2: Setting up {len(schemas_to_create)} schema(s) in catalog: {in_catalog}")
    print(f"{'='*60}")

    existing_schemas = set(
        row.databaseName
        for row in spark.sql(f"SHOW SCHEMAS IN `{in_catalog}`").collect() # noqa: F821
    )

    created = 0
    for i, schema_name in enumerate(schemas_to_create, start=1):
        full_name = f"`{in_catalog}`.`{schema_name}`"
        print(f"[{i}/{len(schemas_to_create)}] Checking: {full_name}...", end=" ")
        if schema_name in existing_schemas:
            print("ALREADY EXISTS")
        else:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_name}") # noqa: F821
            created += 1
            print("CREATED")

    print(f"\n{'='*60}")
    print(f"COMPLETE: {created} schema(s) created, {len(schemas_to_create) - created} already existed.")
    print(f"{'='*60}\n")

# COMMAND ----------

# -----------------------------------------------------------------------------
# compute_validation() checks the current compute (classic All-Purpose or
# Serverless) and WARNS, never errors, when it doesn't match what the notebook was
# tested on. Learners move between serverless, classic and SQL warehouses.
#
# Examples (constants from the top of this file):
# # both: compute_validation(recommend_dbr_classic_version=RECOMMENDED_DBR_CLASSIC_VERSION,
# recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)
# # serverless: compute_validation(recommended_serverless_version=RECOMMENDED_SERVERLESS_VERSION)
# # classic: compute_validation(recommend_dbr_classic_version=RECOMMENDED_DBR_CLASSIC_VERSION)
# -----------------------------------------------------------------------------

import os


def _get_env():
    """
    Read the compute environment and extract classic (All-Purpose) or Serverless
    version. Serverless runtime looks like 'client.X.Y' (X is the version);
    classic looks like a plain DBR number such as '16.4'. IS_SERVERLESS may be
    'TRUE'/'true' or absent.
    """
    is_serverless = os.environ.get("IS_SERVERLESS", "FALSE").upper()
    runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")

    if is_serverless == "TRUE":
        try:
            current_serverless_version = int(runtime_version.split(".")[1])
        except (IndexError, ValueError):
            current_serverless_version = None
    else:
        current_serverless_version = None

    if is_serverless == "FALSE":
        try:
            current_dbr_version_all_purpose = float(runtime_version)
        except ValueError:
            current_dbr_version_all_purpose = None
    else:
        current_dbr_version_all_purpose = None

    return {
        "is_serverless": is_serverless,
        "current_serverless_version": current_serverless_version,
        "current_dbr_version_all_purpose": current_dbr_version_all_purpose,
    }


def _render_result(compute_type, recommended, current, match):
    if match:
        badge = '<span style="color:#00A972;font-weight:600">&#10003; Match</span>'
        detail = f"Version {current}"
        row_style = ""
    else:
        badge = '<span style="color:#98102A;font-weight:700">&#9888; Mismatch</span>'
        detail = f"Found {current}, recommended <strong>{recommended}</strong>"
        row_style = "background:#FDE0DC;"
    return f"""
    <tr style="{row_style}">
      <td style="padding:8px 12px;border-bottom:1px solid #EEEDE9">{compute_type}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #EEEDE9">{badge}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #EEEDE9">{detail}</td>
    </tr>"""


def _render_wrong_compute(expected_type, recommended):
    return f"""
    <tr style="background:#FDE0DC;">
      <td style="padding:8px 12px;border-bottom:1px solid #EEEDE9">{expected_type}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #EEEDE9"><span style="color:#98102A;font-weight:700">&#9888; Wrong compute type</span></td>
      <td style="padding:8px 12px;border-bottom:1px solid #EEEDE9">This notebook expects <strong>{expected_type}</strong> (version {recommended})</td>
    </tr>"""


def _has_mismatch(rows_html):
    return "#FDE0DC" in rows_html


def _display(title, rows):
    joined = "".join(rows)
    header_bg = "#FF5F46" if _has_mismatch(joined) else "#1B5162"
    html = f"""
    <div style="font-family:sans-serif;max-width:1100px;margin:12px 0;border:1px solid #EEEDE9;border-radius:8px;overflow:hidden">
      <div style="background:{header_bg};color:#fff;padding:10px 16px;font-size:16px;font-weight:600">{title}</div>
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#F9F7F4">
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #EEEDE9">Compute</th>
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #EEEDE9">Status</th>
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #EEEDE9">Details</th>
        </tr>
        {joined}
      </table>
      <div style="padding:8px 16px;font-size:13px;color:#618794;background:#F9F7F4">This is a warning only. Other compute may work but isn't guaranteed.</div>
    </div>"""
    displayHTML(html) # noqa: F821


def _check_serverless_only(current_serverless_version, recommended_serverless_version):
    rows = []
    if current_serverless_version is None:
        rows.append(_render_wrong_compute("Serverless", recommended_serverless_version))
    else:
        match = current_serverless_version == recommended_serverless_version
        rows.append(_render_result("Serverless", recommended_serverless_version, current_serverless_version, match))
    _display(f"Compute Check - Tested on Serverless v{recommended_serverless_version}", rows)


def _check_all_purpose_only(current_dbr_version_all_purpose, recommend_dbr_classic_version):
    rows = []
    if current_dbr_version_all_purpose is None:
        rows.append(_render_wrong_compute("All-Purpose (classic)", recommend_dbr_classic_version))
    else:
        match = current_dbr_version_all_purpose == recommend_dbr_classic_version
        rows.append(_render_result("All-Purpose (classic)", recommend_dbr_classic_version, current_dbr_version_all_purpose, match))
    _display(f"Compute Check - Tested on classic DBR {recommend_dbr_classic_version} + Photon", rows)


def _check_both(recommended_serverless_version, recommend_dbr_classic_version,
                current_serverless_version, current_dbr_version_all_purpose):
    rows = []
    if current_dbr_version_all_purpose is not None:
        match = current_dbr_version_all_purpose == recommend_dbr_classic_version
        rows.append(_render_result("All-Purpose (classic)", recommend_dbr_classic_version, current_dbr_version_all_purpose, match))
    if current_serverless_version is not None:
        match = current_serverless_version == recommended_serverless_version
        rows.append(_render_result("Serverless", recommended_serverless_version, current_serverless_version, match))
    if not rows:
        rows.append(_render_wrong_compute("Serverless or classic All-Purpose", f"{recommended_serverless_version} / {recommend_dbr_classic_version}"))
    _display(f"Compute Check - Tested on classic DBR {recommend_dbr_classic_version} + Photon / Serverless v{recommended_serverless_version}", rows)


def compute_validation(
    recommended_serverless_version: int = None,
    recommend_dbr_classic_version: float = None,
):
    """
    Warn (never error) when the current compute doesn't match what the notebook
    was validated on. Provide serverless only, classic only or both.
    """
    if recommended_serverless_version is None and recommend_dbr_classic_version is None:
        raise ValueError(
            "Specify a compute type to check (serverless version and/or classic DBR version)."
        )
    env_values = _get_env()
    cs = env_values["current_serverless_version"]
    cd = env_values["current_dbr_version_all_purpose"]

    if recommended_serverless_version is not None and recommend_dbr_classic_version is None:
        _check_serverless_only(cs, recommended_serverless_version)
    elif recommended_serverless_version is None and recommend_dbr_classic_version is not None:
        _check_all_purpose_only(cd, recommend_dbr_classic_version)
    else:
        _check_both(recommended_serverless_version, recommend_dbr_classic_version, cs, cd)

# COMMAND ----------

def setup_complete_msg():
    """Print a completion banner."""
    print('\n------------------------------------------------------------------------------')
    print('[OK] SETUP COMPLETE!')
    print('------------------------------------------------------------------------------')

# COMMAND ----------

def display_config_values(config_values, copy_values=False):
    """
    Display a list of (key, value[, copy_value]) tuples as an HTML config table
    with optional copy buttons. Used at the end of each setup script to show the
    learner their catalog and schema.
    """
    rows = ""
    for name, value, *rest in config_values:
        copy_value = rest[0] if rest else (copy_values or "")
        copy_btn = ""
        if copy_value:
            if isinstance(copy_value, str):
                copy_value = f'data-copy="{copy_value}"'
            else:
                copy_value = ""
            copy_btn = """
                <button type="button" onclick="copyFromSibling(this)"
                    style="margin-left:auto;padding:2px 10px;border:1px solid #EEEDE9;border-radius:4px;background:#F9F7F4;cursor:pointer;font-size:13px;flex:0 0 auto;min-width:68px;text-align:center;box-sizing:border-box;">
                    Copy
                </button>"""
        rows += f"""
        <tr>
          <td style="padding:6px 12px;white-space:nowrap;border-bottom:1px solid #EEEDE9;font-weight:600">{name}:</td>
          <td style="padding:6px 12px;border-bottom:1px solid #EEEDE9">
            <div style="display:flex;align-items:center;gap:12px;">
                <span class="copy-source" {copy_value} style="display:inline-block;padding:4px 8px;font-size:15px;min-width:0;">{value}</span>
                {copy_btn}
            </div>
          </td>
        </tr>"""

    html = """
    <div style="font-family:sans-serif;max-width:1100px;margin:12px 0;border:1px solid #EEEDE9;border-radius:8px;overflow:hidden">
      <div style="background:#1B5162;color:#fff;padding:10px 16px;font-size:16px;font-weight:600">Configuration Values</div>
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#F9F7F4">
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #EEEDE9">Information</th>
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #EEEDE9">Value</th>
        </tr>""" + rows + """
      </table>
    </div>
    <script>
        if (!window.copyFromSibling) {
            window.copyFromSibling = function(btn) {
            var source = btn.parentElement.querySelector('.copy-source');
            if (!source) return;
            var text = source.dataset.copy || source.innerText;
            var t = document.createElement('textarea');
            t.value = text;
            t.style.position = 'fixed';
            t.style.opacity = '0';
            t.style.pointerEvents = 'none';
            document.body.appendChild(t);
            t.select();
            document.execCommand('copy');
            document.body.removeChild(t);
            var original = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(function() { btn.textContent = original; }, 1500);
            };
        }
    </script>"""
    displayHTML(html) # noqa: F821

# COMMAND ----------

import os as _os


def find_folder(folder_name: str) -> str:
    """
    Locate a folder in the current working directory. Raises FileNotFoundError
    if it doesn't exist. Used to find bundled data/image folders.
    """
    cwd = _os.getcwd()
    folder_path = _os.path.join(cwd, folder_name)
    print(f"\n{'='*60}")
    print(f"Searching for '{folder_name}' folder...")
    print(f"{'='*60}")
    print(f"Current directory: {cwd}")
    print(f"Checking: {folder_path}...", end=" ")
    if _os.path.isdir(folder_path):
        print("FOUND")
        print(f"{'='*60}\n")
        return folder_path
    print("NOT FOUND")
    print(f"{'='*60}\n")
    raise FileNotFoundError(
        f"Could not find '{folder_name}' folder in: {cwd}\n"
        f"  Make sure the folder exists in your current working directory."
    )