# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace-Setup
# MAGIC
# MAGIC This notebook provisions the workspace for the course. The manifest points
# MAGIC `config.workspace_setup.entry` here and runs it on a serverless job cluster. Two runners run it the
# MAGIC same way and get the same result: Vocareum runs it non-interactively when it creates the user
# MAGIC workspace, and you can Run All on it by hand on an existing build workspace. Every step is
# MAGIC programmatic SQL or SDK, idempotent and safe to re-run, and it finishes in a few minutes.
# MAGIC
# MAGIC Nothing waits on this notebook. Each notebook's own `Classroom-Setup-NN` rebuilds what
# MAGIC it needs, so a user who starts before this finishes, or on a workspace where it was skipped,
# MAGIC isn't blocked.
# MAGIC
# MAGIC **What this notebook provisions**
# MAGIC
# MAGIC - Creates the governed `sensitivity_level` tag with its values (public, internal, confidential,
# MAGIC   restricted) and grants `account users` `ASSIGN` on it, so each notebook's own setup can apply it to
# MAGIC   the learner's tables. Creating a governed tag is an account-level action a lesson notebook cannot do.
# MAGIC - Verifies the `account privileged` account group (the ABAC policy `EXCEPT` set). The policy's `TO`
# MAGIC   set is the built-in `account users`, so the
# MAGIC   mask applies to every user's own live query with no provisioning. Only `account privileged` (who see
# MAGIC   originals) is a custom account group a workspace job cannot create. The ABAC demo keeps a
# MAGIC   deterministic fallback if it is not in place.
# MAGIC - Provisions the `dbacademy_privacy` secret scope with a demo salt and AES key (via the Databricks
# MAGIC   SDK) that the de-identification demo reads with `secret()`.
# MAGIC
# MAGIC It doesn't build any data. This notebook runs as the provisioning principal and never as the
# MAGIC learner, so the estate and the classification, lineage, audit and query-history snapshots are built
# MAGIC by each notebook's own `Classroom-Setup-NN` in the learner's catalog.
# MAGIC
# MAGIC **Privileges.** Runs as the workspace provisioning principal (Vocareum) or as you. Some steps need
# MAGIC account-level privileges (the `CREATE` privilege for governed tags and permission to create a secret
# MAGIC scope). Any step this principal can't perform is recorded and reported at the end rather than aborting
# MAGIC the run, and the matching per-notebook setup covers it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Naming contract (later setups and demos rely on these)
# MAGIC
# MAGIC None of these live in a catalog. The governed tag and the groups are account-level and the secret
# MAGIC scope belongs to the workspace:
# MAGIC
# MAGIC | Object | Name | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | Governed tag | `sensitivity_level` VALUES (`public`, `internal`, `confidential`, `restricted`) | The attribute ABAC policies match on |
# MAGIC | Restricted set | `account users` (built-in account group) | The policy `TO` set. Every user is a member, so the mask applies to their own query live with no provisioning |
# MAGIC | Privileged group | `account privileged` (custom account group, provisioned externally) | The policy `EXCEPT` set, who see originals. Keep users out of it, or the mask will not apply |
# MAGIC | Secret scope | `dbacademy_privacy` (keys `pii_salt`, `pii_aes_key`) | Salt and base64 AES key the de-identification demo (notebook 06) reads with `secret()` |
# MAGIC
# MAGIC **Deployment note.** Because nothing here is catalog-scoped, one run as the provisioning principal
# MAGIC covers every learner in the workspace. Each learner's `Classroom-Setup-NN` builds the estate in their
# MAGIC own catalog and applies the governed tag there. The names above are the contract.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A resilient runner for the account-level steps
# MAGIC
# MAGIC The steps below depend on account-level privileges, the tenant's serverless release channel or newer
# MAGIC platform features:
# MAGIC
# MAGIC - Governed-tag SQL DDL (`CREATE GOVERNED TAG`) needs the `CREATE` privilege and, for the SQL form,
# MAGIC   Databricks Runtime 18.1 or above. If the serverless channel here doesn't support it, create the
# MAGIC   tag in the UI (**Catalog > Govern > Governed Tags**) instead.
# MAGIC
# MAGIC Rather than abort on the first step a tenant can't do, each step records its outcome and keeps
# MAGIC going, and a report prints at the end.

# COMMAND ----------

_provision_report = []

def run_sql_safely(label, sql, ok_if_exists=False):
    # Run one statement, record the outcome, never raise. When ok_if_exists is set, an
    # "already exists" failure (a re-run of a CREATE that has no IF NOT EXISTS form) is
    # recorded as OK rather than a follow-up.
    try:
        spark.sql(sql)
        _provision_report.append(("OK", label))
        print(f"[OK] {label}")
    except Exception as e:
        first = (str(e).splitlines() or [repr(e)])[0]
        if ok_if_exists and "already" in first.lower() and "exists" in first.lower():
            _provision_report.append(("OK", f"{label} (already exists)"))
            print(f"[OK] {label} (already exists)")
            return
        _provision_report.append(("SKIPPED", f"{label} -> {first}"))
        print(f"[SKIPPED] {label}\n{first}")

PRIVILEGED_GROUP = "account privileged" # custom account group; the ABAC policy EXCEPT set (sees originals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Governed `sensitivity_level` tag and controlled values
# MAGIC
# MAGIC The syntax is `CREATE GOVERNED TAG key [DESCRIPTION d] [VALUES (...)]`. Governed
# MAGIC tags are account-level and GA. Creating one needs the `CREATE` privilege; the SQL form needs DBR
# MAGIC 18.1+. There's no documented `IF NOT EXISTS`, so a re-run (or an account that already has the tag)
# MAGIC hits "already exists"; this step treats that as success rather than a follow-up. Keep PII and secrets
# MAGIC out of tag keys and values.

# COMMAND ----------

run_sql_safely(
    "CREATE GOVERNED TAG sensitivity_level (public, internal, confidential, restricted)",
    "CREATE GOVERNED TAG sensitivity_level "
    "DESCRIPTION 'Course sensitivity_level taxonomy that ABAC policies match on' "
    "VALUES ('public', 'internal', 'confidential', 'restricted')",
    ok_if_exists=True,
)

# Let users apply the tag to their own columns. If it's rejected, grant
# ASSIGN in the UI (Catalog > Govern > Governed Tags > sensitivity_level > Permissions).
run_sql_safely(
    "GRANT ASSIGN on governed tag sensitivity_level to account users",
    "GRANT ASSIGN ON GOVERNED TAG sensitivity_level TO `account users`",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify the account group the ABAC policy excepts
# MAGIC
# MAGIC The ABAC demo (notebook 04) and the first lab (notebook 05) mask restricted columns
# MAGIC `TO account users EXCEPT account privileged`.
# MAGIC
# MAGIC - `account users` (restricted): the policy `TO` set. This is the built-in account-level group that
# MAGIC   every user belongs to, so the mask applies to a learner's own live query with no provisioning.
# MAGIC - `account privileged` (privileged): the policy `EXCEPT` set, who see originals. No built-in group
# MAGIC   works here, so it is a custom account group.
# MAGIC
# MAGIC `account privileged` is an **account group**, and a workspace or serverless job cannot create it or
# MAGIC manage its membership. Provision it once at the account level (account console under **User management
# MAGIC > Groups**, Terraform, or your LMS/SCIM) with only the reviewers who should see originals. The cell
# MAGIC below only reads and reports: it confirms whether the current principal is in `account privileged`.
# MAGIC It never blocks. The demo keeps a deterministic
# MAGIC fallback, so the contrast still shows if `account privileged` is not in place.

# COMMAND ----------

# Report membership only. `account users` is built-in (every user is a member), so the policy's
# TO set covers everyone with no provisioning and the mask applies to a learner's own query live.
# `account privileged` (the EXCEPT set) is a custom account group provisioned at the account level;
# a workspace job cannot create it. is_account_group_member returns false for a group that does not
# exist or that the principal is not in, so this never raises.
try:
    _m = spark.sql(
        f"SELECT current_user() AS who, "
        f"is_account_group_member('{PRIVILEGED_GROUP}') AS in_privileged"
    ).first()
    if not _m["in_privileged"]:
        _provision_report.append(("OK", f"{_m['who']} not in {PRIVILEGED_GROUP} (mask applies live)"))
        print(f"[OK] {_m['who']} is in account users and not in {PRIVILEGED_GROUP}: the ABAC mask applies to their own query live.")
    else:
        _provision_report.append(("SKIPPED", f"{_m['who']} is in {PRIVILEGED_GROUP} (reads originals)"))
        print(f"[SKIPPED] {_m['who']} is in {PRIVILEGED_GROUP}, so they read originals rather than the mask. The demo assumes an analyst who is not in {PRIVILEGED_GROUP}.")
    print(f"To demonstrate the privileged (EXCEPT) side, provision {PRIVILEGED_GROUP} at the ACCOUNT level (account console > User management > Groups, Terraform, or SCIM) with the reviewers who should see originals.")
    print("account users needs no provisioning, and the ABAC demo keeps a deterministic fallback, so the contrast still shows without this.")
except Exception as e:
    first = (str(e).splitlines() or [repr(e)])[0]
    _provision_report.append(("SKIPPED", f"Verify {PRIVILEGED_GROUP} membership -> {first}"))
    print(f"[SKIPPED] Could not verify group membership: {first}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Secret scope and demo secrets for de-identification (notebook 06)
# MAGIC
# MAGIC Notebook 06 reads a salt and an AES key with `secret()` and `try_secret()`. Creating a secret scope
# MAGIC isn't plain SQL, so it's done here via the Databricks SDK as part of provisioning. This notebook runs
# MAGIC as the provisioning principal, and a secret scope is private to its creator, so it also grants all
# MAGIC workspace users READ on the scope. That read grant is what lets a user (a different principal)
# MAGIC resolve the secrets in notebook 06; the user never creates or owns the scope.
# MAGIC
# MAGIC - Scope `dbacademy_privacy`, keys `pii_salt` (a salt for `sha2`) and `pii_aes_key` (a 32-byte AES
# MAGIC   key for `aes_encrypt`, stored base64-encoded, so the notebook decodes it with `unbase64()`).
# MAGIC - The values here are synthetic demo secrets, not real keys, so all-users READ is acceptable.
# MAGIC - If the SDK call isn't permitted for this principal, the step is recorded as skipped and the cell
# MAGIC   prints the one-time Databricks CLI commands to create the scope and grant read by hand.

# COMMAND ----------

# Secret scope + demo salt/key for the de-identification demo (notebook 06).
# Creating a secret scope isn't plain SQL, so it's done here via the Databricks SDK,
# best effort like the SQL steps above. Synthetic values only.
import base64 as _b64

SECRET_SCOPE = "dbacademy_privacy"
_DEMO_SALT = "dp-demo-salt-2026"
_DEMO_AES_KEY_B64 = _b64.b64encode(b"0123456789abcdef0123456789abcdef").decode()  # 32-byte key, base64

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import AclPermission
    _w = WorkspaceClient()
    _scopes = [s.name for s in _w.secrets.list_scopes()]
    if SECRET_SCOPE not in _scopes:
        _w.secrets.create_scope(scope=SECRET_SCOPE)
    _w.secrets.put_secret(scope=SECRET_SCOPE, key="pii_salt", string_value=_DEMO_SALT)
    _w.secrets.put_secret(scope=SECRET_SCOPE, key="pii_aes_key", string_value=_DEMO_AES_KEY_B64)
    # This notebook runs as the provisioning principal, not the user, and a scope is private
    # to its creator. Grant all workspace users READ so a user can resolve the secrets with
    # secret()/try_secret() in notebook 06 (synthetic values, read only).
    _w.secrets.put_acl(scope=SECRET_SCOPE, principal="users", permission=AclPermission.READ)
    _provision_report.append(("OK", f"Secret scope {SECRET_SCOPE} with pii_salt and pii_aes_key (users READ)"))
    print(f"[OK] Secret scope {SECRET_SCOPE} with pii_salt and pii_aes_key (users READ)")
except Exception as e:
    first = (str(e).splitlines() or [repr(e)])[0]
    _provision_report.append(("SKIPPED", f"Secret scope {SECRET_SCOPE} -> {first}"))
    print(f"[SKIPPED] Secret scope {SECRET_SCOPE}\n{first}")
    print("Create it once with the Databricks CLI, then re-run:")
    print(f"databricks secrets create-scope {SECRET_SCOPE}")
    print(f"databricks secrets put-secret {SECRET_SCOPE} pii_salt --string-value '<salt>'")
    print(f"databricks secrets put-secret {SECRET_SCOPE} pii_aes_key --string-value '<base64-32-byte-key>'")
    print(f"databricks secrets put-acl {SECRET_SCOPE} users READ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Provisioning report
# MAGIC
# MAGIC What completed and what needs a manual follow-up on this tenant.

# COMMAND ----------

print("=" * 78)
print("Workspace-Setup provisioning report")
print("=" * 78)
ok = [x for s, x in _provision_report if s == "OK"]
skipped = [x for s, x in _provision_report if s == "SKIPPED"]
print(f"Completed: {len(ok)} Needs follow-up: {len(skipped)}\n")
if skipped:
    print("FOLLOW UP ON THESE (permissions, compute channel or not populated yet):")
    for x in skipped:
        print(f"- {x}")
else:
    print("All provisioning steps completed.")
print("=" * 78)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. After provisioning, confirm
# MAGIC
# MAGIC - The `dbacademy_privacy` secret scope exists with `pii_salt` and `pii_aes_key`. The de-identification
# MAGIC   demo reads them with `secret()`.
# MAGIC - The `sensitivity_level` governed tag exists with its four values and `account users` has `ASSIGN`
# MAGIC   on it. Each notebook's own setup applies the values to the learner's tables.
# MAGIC - The current principal is not in `account privileged`, so the notebook-04 and notebook-05 masks apply to
# MAGIC   their own query live (every user is in the built-in `account users`, the policy `TO` set). To show
# MAGIC   the privileged side, provision the `account privileged` account group at the account level. The demo
# MAGIC   keeps a deterministic fallback either way.
# MAGIC - The provisioning report above lists no unexpected follow-ups.