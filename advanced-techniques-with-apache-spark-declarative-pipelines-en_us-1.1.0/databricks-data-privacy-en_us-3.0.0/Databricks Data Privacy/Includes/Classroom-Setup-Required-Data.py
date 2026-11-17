# Databricks notebook source
# Classroom-Setup-Required-Data
# ---------------------------------------------------------------------
# Shared priv_demo estate: a multinational retailer with synthetic, PII-shaped
# data. Pure Spark generators. Idempotent by skip-if-exists (preserves tags).
#
# Creates
# - customer_profile, clickstream, loyalty, support_records, customer_enriched, customer_metrics_gold.
#
# Usage
# - %run ./Classroom-Setup-Common-Python, then %run ./Classroom-Setup-Required-Data.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Scale: builds in seconds on serverless, realistic for all demos.
DATA_SCALE = {
    "customers": 50_000, # customer_profile and loyalty (1 row per customer)
    "clickstream": 500_000, # clickstream events
    "support_records": 20_000, # support tickets
}

DEMO_SCHEMA = "priv_demo"

# COMMAND ----------

# Deterministic generators: pick from lists by hashing a column. Runs in Spark/Photon.

_FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Sofia", "Anthony", "Margaret", "Mark", "Sandra", "Aiko", "Ashley",
    "Mateus", "Priya", "Paul", "Emily", "Andrew", "Ananya", "Hiroshi", "Michelle",
    "Kenneth", "Carol", "Lucas", "Amanda", "Rahul", "Ingrid", "Georg", "Camille",
    "Timothy", "Yuki",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Silva", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Nakamura",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Muller", "Schmidt", "King", "Wright", "Sato", "Dubois", "Nguyen", "Patel", "Flores",
    "Kaur", "Adams", "Nelson", "Baker", "Rossi", "Rivera", "Tanaka", "Mitchell",
    "Carter", "Suzuki",
]

_STREETS = [
    "Main St", "Oak Ave", "Pine Rd", "Elm Blvd", "Cedar Ln", "Maple Dr",
    "Washington Ave", "Park Pl", "Lake Dr", "Hill Rd", "River St", "Forest Ave",
    "Sunset Blvd", "Broadway", "Highland Ave", "Valley Rd", "Spring St",
    "Church St", "School Rd", "Market St", "Union Ave", "Academy Dr",
    "Kings Rd", "Liberty St", "Franklin Rd", "Center St", "Prospect Ave",
    "Mill Rd", "Court St", "Bridge Ln",
]

# Parallel location arrays: one loc index picks a consistent city, country,
# and phone country code for a row. National id and postal code are then
# derived from the country, so each synthetic person's geography is coherent.
_CITIES = [
    "New York", "London", "Berlin", "Paris", "Sydney",
    "Sao Paulo", "Mumbai", "Tokyo", "Toronto", "Madrid",
]
_COUNTRIES = [
    "United States", "United Kingdom", "Germany", "France", "Australia",
    "Brazil", "India", "Japan", "Canada", "Spain",
]
_PHONE_CC = [
    "1", "44", "49", "33", "61",
    "55", "91", "81", "1", "34",
]

_DOMAINS = ["example.com", "example.net", "mail.example.org", "shop.example.com", "post.example.io"]

# Letters without I and O to keep generated ids easy to read.
_LETTERS = list("ABCDEFGHJKLMNPQRSTUVWXYZ")

_URL_PATHS = [
    "home", "search?q=headphones", "category/electronics", "product/SKU-00421",
    "cart", "checkout", "account/orders", "wishlist", "support/contact", "deals",
]

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Safari/17.5",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) Chrome/126.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) Mobile/15E148",
]

_TICKET_SUBJECTS = [
    "Order not delivered", "Refund request", "Wrong item received",
    "Update my account details", "Loyalty points missing", "Cancel my subscription",
    "Damaged package", "Delivery address change", "Payment failed", "Data deletion request",
]

_TICKET_BODIES = [
    "The order still has not arrived and the tracking has not updated in days.",
    "I would like a full refund for this purchase as it did not match the description.",
    "I received the wrong item and need to arrange a return and replacement.",
    "Please update the details on my account, some information is out of date.",
    "My loyalty points did not post after my last purchase, please look into it.",
    "I want to cancel my subscription and stop all further marketing contact.",
    "The package arrived damaged and I would like a replacement shipped.",
    "I have moved and need my delivery address corrected before the next order.",
    "My payment keeps failing at checkout even though the card details are correct.",
    "Under privacy rules I am requesting that you delete my personal data.",
]


def _pick(arr, col, seed_offset=0):
    """Pick from a Python list using a deterministic hash. Returns a Column."""
    arr_expr = F.array(*[F.lit(x) for x in arr])
    idx = (F.pmod(F.hash(col + F.lit(seed_offset)), F.lit(len(arr)))).cast("int")
    return F.element_at(arr_expr, idx + 1)


def _digits(col, seed, k):
    """Deterministic k-digit numeric string (leading zeros allowed)."""
    return F.lpad((F.pmod(F.hash(col + F.lit(seed)), F.lit(10 ** k))).cast("string"), k, "0")


def _letters(col, seed, k):
    """Deterministic string of k uppercase letters."""
    letters_arr = F.array(*[F.lit(c) for c in _LETTERS])
    parts = []
    for j in range(k):
        idx = (F.pmod(F.hash(col + F.lit(seed + j)), F.lit(len(_LETTERS)))).cast("int")
        parts.append(F.element_at(letters_arr, idx + 1))
    return F.concat(*parts)


def _ipv4(col, seed=100):
    """Deterministic, valid-looking IPv4 address string."""
    return F.concat(
        (F.pmod(F.hash(col + F.lit(seed)), F.lit(223)) + 1).cast("string"), F.lit("."),
        (F.pmod(F.hash(col + F.lit(seed + 1)), F.lit(256))).cast("string"), F.lit("."),
        (F.pmod(F.hash(col + F.lit(seed + 2)), F.lit(256))).cast("string"), F.lit("."),
        (F.pmod(F.hash(col + F.lit(seed + 3)), F.lit(254)) + 1).cast("string"),
    )


def _national_id(col, country):
    """Per-country national identifier, shaped for regional classification classes."""
    return (
        F.when(country == "United States", F.concat(_digits(col, 41, 3), F.lit("-"), _digits(col, 42, 2), F.lit("-"), _digits(col, 43, 4)))
         .when(country == "United Kingdom", F.concat(_letters(col, 44, 2), _digits(col, 45, 6), _letters(col, 46, 1)))
         .when(country == "Germany", _digits(col, 47, 11))
         .when(country == "France", _digits(col, 48, 13))
         .when(country == "Australia", F.concat(_digits(col, 49, 3), F.lit(" "), _digits(col, 50, 3), F.lit(" "), _digits(col, 51, 3)))
         .when(country == "Brazil", F.concat(_digits(col, 52, 3), F.lit("."), _digits(col, 53, 3), F.lit("."), _digits(col, 54, 3), F.lit("-"), _digits(col, 55, 2)))
         .when(country == "India", F.concat(_digits(col, 56, 4), F.lit(" "), _digits(col, 57, 4), F.lit(" "), _digits(col, 58, 4)))
         .when(country == "Japan", _digits(col, 59, 12))
         .when(country == "Canada", F.concat(_digits(col, 60, 3), F.lit("-"), _digits(col, 61, 3), F.lit("-"), _digits(col, 62, 3)))
         .otherwise(F.concat(_digits(col, 63, 3), F.lit(" "), _digits(col, 64, 3), F.lit(" "), _digits(col, 65, 3)))
    )


def _postal_code(col, country):
    """Per-country postal / ZIP code, kept simple but country-appropriate."""
    return (
        F.when(country == "United States", _digits(col, 71, 5))
         .when(country == "Canada", F.concat(_letters(col, 72, 1), _digits(col, 73, 1), _letters(col, 74, 1), F.lit(" "), _digits(col, 75, 1), _letters(col, 76, 1), _digits(col, 77, 1)))
         .when(country == "United Kingdom", F.concat(_letters(col, 78, 2), _digits(col, 79, 1), F.lit(" "), _digits(col, 80, 1), _letters(col, 81, 2)))
         .otherwise(_digits(col, 82, 5))
    )

# COMMAND ----------

def _table_ready(catalog: str, schema: str, table: str, min_rows: int) -> bool:
    """True if the table exists with at least min_rows rows. Skip guard so we don't
    drop and rebuild, which would wipe classification and governed tags."""
    full = f"`{catalog}`.`{schema}`.`{table}`"
    try:
        n = spark.sql(f"SELECT COUNT(*) AS c FROM {full}").first()["c"] # noqa: F821
        return n is not None and n >= min_rows
    except Exception:
        return False


def _print_table_info(table: str):
    """Print row count, size in MB and file count for a table."""
    detail = spark.sql(f"DESCRIBE DETAIL `{table}`").collect()[0] # noqa: F821
    rows = spark.sql(f"SELECT COUNT(*) AS c FROM `{table}`").first()["c"] # noqa: F821
    print(f"{table}: {rows:,} rows, {detail['sizeInBytes'] / 1e6:.1f} MB, {detail['numFiles']} files")


def _analyze(table: str):
    """Light ANALYZE pass. Guarded because some serverless configs restrict it."""
    try:
        spark.sql(f"ANALYZE TABLE `{table}` COMPUTE STATISTICS") # noqa: F821
    except Exception as e:
        print(f"(skipped ANALYZE on {table}: {e})")

# COMMAND ----------

def build_demo_data(catalog: str, schema: str = DEMO_SCHEMA, scale: dict = None):
    """
    Build the shared demo estate in `catalog`.`schema`. Idempotent by
    SKIP-IF-EXISTS: an existing table at expected size is left untouched so its
    classification and governed tags survive. To force a clean rebuild, DROP the
    tables first.
    """
    scale = scale or DATA_SCALE
    create_schemas(in_catalog=catalog, schemas_to_create=[schema]) # noqa: F821
    spark.sql(f"USE CATALOG `{catalog}`") # noqa: F821
    spark.sql(f"USE SCHEMA `{schema}`") # noqa: F821

    n_customers = scale["customers"]
    n_events = scale["clickstream"]
    n_support = scale["support_records"]

    n_countries = len(_COUNTRIES)
    countries_arr = F.array(*[F.lit(c) for c in _COUNTRIES])
    cities_arr = F.array(*[F.lit(c) for c in _CITIES])
    phonecc_arr = F.array(*[F.lit(c) for c in _PHONE_CC])

    # ---- customer_profile (the main PII table) -------------------------------
    if _table_ready(catalog, schema, "customer_profile", n_customers):
        print("[OK] customer_profile already present. Skipping (tags preserved).")
    else:
        print("[INFO] Building customer_profile ...")
        _id = F.col("id")
        fn = _pick(_FIRST_NAMES, _id, 0)
        ln = _pick(_LAST_NAMES, _id, 1)
        loc_idx = (F.pmod(F.hash(_id + F.lit(5)), F.lit(n_countries))).cast("int")
        country = F.element_at(countries_arr, loc_idx + 1)
        city = F.element_at(cities_arr, loc_idx + 1)
        phone_cc = F.element_at(phonecc_arr, loc_idx + 1)

        customer_profile = (
            spark.range(0, n_customers, numPartitions=8) # noqa: F821
            .select(
                (_id + F.lit(1)).alias("customer_id"),
                fn.alias("first_name"),
                ln.alias("last_name"),
                F.concat(F.lower(fn), F.lit("."), F.lower(ln), F.lit("@"), _pick(_DOMAINS, _id, 2)).alias("email"),
                F.concat(F.lit("+"), phone_cc, _digits(_id, 90, 10)).alias("phone"),
                F.concat((F.pmod(F.hash(_id + F.lit(6)), F.lit(9900)) + 100).cast("string"), F.lit(" "), _pick(_STREETS, _id, 7)).alias("street_address"),
                city.alias("city"),
                country.alias("country"),
                _postal_code(_id, country).alias("postal_code"),
                F.expr("date_add('1950-01-01', cast(pmod(hash(id + 110), 20440) as int))").alias("date_of_birth"),
                _national_id(_id, country).alias("national_id"),
                _ipv4(_id, 120).alias("ip_address"),
                F.element_at(
                    F.array(F.lit("Bronze"), F.lit("Silver"), F.lit("Gold"), F.lit("Platinum")),
                    (F.pmod(F.hash(_id + F.lit(130)), F.lit(4)) + 1).cast("int"),
                ).alias("loyalty_tier"),
                (F.pmod(F.hash(_id + F.lit(140)), F.lit(100)) < 60).alias("marketing_opt_in"),
                F.expr("timestampadd(SECOND, cast(pmod(hash(id + 150), 189216000) as int), timestamp'2020-01-01')").alias("created_at"),
            )
        )
        customer_profile.write.mode("overwrite").saveAsTable("customer_profile")
        _print_table_info("customer_profile")
        _analyze("customer_profile")

    # ---- loyalty (1 row per customer) ----------------------------------------
    if _table_ready(catalog, schema, "loyalty", n_customers):
        print("[OK] loyalty already present. Skipping.")
    else:
        print("[INFO] Building loyalty ...")
        _id = F.col("id")
        loyalty = (
            spark.range(0, n_customers, numPartitions=8) # noqa: F821
            .select(
                (_id + F.lit(1)).alias("customer_id"),
                (F.pmod(F.hash(_id + F.lit(400)), F.lit(50000))).cast("bigint").alias("points_balance"),
                F.element_at(
                    F.array(F.lit("Bronze"), F.lit("Silver"), F.lit("Gold"), F.lit("Platinum")),
                    (F.pmod(F.hash(_id + F.lit(410)), F.lit(4)) + 1).cast("int"),
                ).alias("tier"),
                F.expr("timestampadd(SECOND, cast(pmod(hash(id + 420), 157680000) as int), timestamp'2019-01-01')").alias("enrolled_at"),
            )
        )
        loyalty.write.mode("overwrite").saveAsTable("loyalty")
        _print_table_info("loyalty")
        _analyze("loyalty")

    # ---- clickstream ---------------------------------------------------------
    if _table_ready(catalog, schema, "clickstream", n_events):
        print("[OK] clickstream already present. Skipping.")
    else:
        print(f"[INFO] Building clickstream ({n_events:,} rows) ...")
        _id = F.col("id")
        loc_idx = (F.pmod(F.hash(_id + F.lit(230)), F.lit(n_countries))).cast("int")
        clickstream = (
            spark.range(0, n_events, numPartitions=16) # noqa: F821
            .select(
                (_id + F.lit(1)).alias("event_id"),
                (F.pmod(F.hash(_id + F.lit(200)), F.lit(n_customers)) + 1).alias("customer_id"),
                F.concat(F.lit("SESS-"), _digits(_id, 205, 12)).alias("session_id"),
                F.expr("timestampadd(SECOND, cast(pmod(hash(id + 210), 63072000) as int), timestamp'2024-01-01')").alias("event_ts"),
                F.concat(F.lit("https://shop.example.com/"), _pick(_URL_PATHS, _id, 220)).alias("page_url"),
                _ipv4(_id, 240).alias("ip_address"),
                _pick(_USER_AGENTS, _id, 250).alias("user_agent"),
                F.element_at(
                    F.array(F.lit("mobile"), F.lit("desktop"), F.lit("tablet")),
                    (F.pmod(F.hash(_id + F.lit(260)), F.lit(3)) + 1).cast("int"),
                ).alias("device_type"),
                F.element_at(countries_arr, loc_idx + 1).alias("country"),
            )
        )
        clickstream.write.mode("overwrite").saveAsTable("clickstream")
        _print_table_info("clickstream")
        _analyze("clickstream")

    # ---- support_records (free text sometimes embeds an email or phone) ------
    if _table_ready(catalog, schema, "support_records", n_support):
        print("[OK] support_records already present. Skipping.")
    else:
        print("[INFO] Building support_records ...")
        _id = F.col("id")
        # A synthetic contact string embedded in the free-text body for ~50% of
        # tickets, so classification and the masking/erasure demos have a target.
        t_email = F.concat(
            F.lower(_pick(_FIRST_NAMES, _id, 500)), F.lit("."),
            F.lower(_pick(_LAST_NAMES, _id, 501)), F.lit("@"), _pick(_DOMAINS, _id, 502),
        )
        t_phone = F.concat(F.lit("+1"), _digits(_id, 503, 10))
        contact = (
            F.when(F.pmod(F.hash(_id + F.lit(504)), F.lit(10)) < 3,
                   F.concat(F.lit(" You can reach me at "), t_email, F.lit(" or "), t_phone, F.lit(".")))
             .when(F.pmod(F.hash(_id + F.lit(504)), F.lit(10)) < 5,
                   F.concat(F.lit(" My email is "), t_email, F.lit(".")))
             .otherwise(F.lit(""))
        )
        support_records = (
            spark.range(0, n_support, numPartitions=8) # noqa: F821
            .select(
                (_id + F.lit(1)).alias("ticket_id"),
                (F.pmod(F.hash(_id + F.lit(300)), F.lit(n_customers)) + 1).alias("customer_id"),
                F.expr("timestampadd(SECOND, cast(pmod(hash(id + 305), 63072000) as int), timestamp'2024-01-01')").alias("created_at"),
                F.element_at(
                    F.array(F.lit("email"), F.lit("chat"), F.lit("phone"), F.lit("web"), F.lit("social")),
                    (F.pmod(F.hash(_id + F.lit(308)), F.lit(5)) + 1).cast("int"),
                ).alias("channel"),
                _pick(_TICKET_SUBJECTS, _id, 309).alias("subject"),
                F.concat(_pick(_TICKET_BODIES, _id, 310), contact).alias("body_text"),
                F.concat(F.lit("AGT-"), _digits(_id, 320, 4)).alias("agent_id"),
                F.element_at(
                    F.array(F.lit("open"), F.lit("pending"), F.lit("resolved"), F.lit("closed"), F.lit("escalated")),
                    (F.pmod(F.hash(_id + F.lit(330)), F.lit(5)) + 1).cast("int"),
                ).alias("status"),
            )
        )
        support_records.write.mode("overwrite").saveAsTable("support_records")
        _print_table_info("support_records")
        _analyze("support_records")

    # ---- customer_enriched (SILVER: join -> column lineage for evidence demo)-
    if _table_ready(catalog, schema, "customer_enriched", 1):
        print("[OK] customer_enriched already present. Skipping.")
    else:
        print("[INFO] Building customer_enriched (silver, joins customer_profile + loyalty) ...")
        # CTAS from Unity Catalog tables so column lineage is captured. est_revenue
        # is a synthetic per-customer measure used only by the gold aggregate.
        spark.sql( # noqa: F821
            """
            CREATE TABLE customer_enriched AS
            SELECT
              p.customer_id,
              p.first_name,
              p.last_name,
              p.email,
              p.phone,
              p.country,
              p.date_of_birth,
              p.national_id,
              p.ip_address,
              p.loyalty_tier,
              l.points_balance,
              l.tier AS loyalty_program_tier,
              l.enrolled_at,
              round(l.points_balance * 0.012 + pmod(hash(p.customer_id + 600), 500), 2) AS est_revenue
            FROM customer_profile p
            LEFT JOIN loyalty l USING (customer_id)
            """
        )
        _print_table_info("customer_enriched")
        _analyze("customer_enriched")

    # ---- customer_metrics_gold (GOLD: non-identifying aggregate) -------------
    if _table_ready(catalog, schema, "customer_metrics_gold", 1):
        print("[OK] customer_metrics_gold already present. Skipping.")
    else:
        print("[INFO] Building customer_metrics_gold (gold, aggregate over customer_enriched) ...")
        spark.sql( # noqa: F821
            """
            CREATE TABLE customer_metrics_gold AS
            SELECT
              country,
              loyalty_tier,
              count(*) AS customer_count,
              round(sum(est_revenue), 2) AS total_est_revenue,
              round(avg(est_revenue), 2) AS avg_est_revenue,
              sum(points_balance) AS total_points
            FROM customer_enriched
            GROUP BY country, loyalty_tier
            """
        )
        _print_table_info("customer_metrics_gold")
        _analyze("customer_metrics_gold")

    print(
        f"\nDemo estate ready in {catalog}.{schema}: customer_profile, loyalty, "
        "clickstream, support_records, customer_enriched, customer_metrics_gold"
    )

# COMMAND ----------

try:
    my_catalog
except NameError:
    my_catalog = build_user_catalog(catalog_forced=None) # noqa: F821

build_demo_data(catalog=my_catalog, schema=DEMO_SCHEMA)

display_config_values([ # noqa: F821
    ("Your Catalog", my_catalog),
    ("Your Demo Schema", DEMO_SCHEMA),
    ("Demo Tables", "customer_profile, loyalty, clickstream, support_records, customer_enriched, customer_metrics_gold"),
])
setup_complete_msg() # noqa: F821