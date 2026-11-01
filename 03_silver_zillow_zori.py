# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver_zillow_zori
# MAGIC **Bronze -> Silver.** Turns the landed Zillow ZORI county file into two tables that join onto the FIPS spine by `geoid`.
# MAGIC
# MAGIC | Output table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | `silver_zori_county_month` | one row per county x month **that Zillow actually reported** | the observations |
# MAGIC | `silver_zori_coverage` | one row per **spine** county (all 3,235) | who has data, who doesn't, and why |
# MAGIC
# MAGIC **Input:** the newest `landed` file for `zillow_zori_county` in `bronze_ingest_manifest` (override with the `zori_file` widget).
# MAGIC
# MAGIC ## Decisions (written down so they survive until week 3)
# MAGIC 1. **The spine is the authority.** Coverage is built *from the spine*, left-joined to Zillow. A county Zillow does not publish appears as `not_in_zillow_file`, not as a missing row.
# MAGIC 2. **No silent backfill.** The long table holds only months Zillow reported. Blanks are never filled, interpolated, or borrowed from a neighbouring county. Interior gaps are counted in `n_gap_months`.
# MAGIC 3. **A Zillow county that is not in the spine stops the run.** Dropping it quietly would be silent shaping.
# MAGIC 4. **Method is labelled.** This is Zillow's own county-level file, not a ZIP-code rollup (the two give different Virginia coverage). The label travels with the data in `zori_method`.
# MAGIC 5. **Connecticut.** Zillow uses the 8 legacy county codes, which match the spine directly. A check asserts this, so if Zillow ever switches to planning regions the run fails loudly instead of losing CT silently.
# MAGIC 6. **The series variant is whatever the file name says** (`..._sfrcondomfr_sm_month`). I have not verified Zillow's description of that variant (for example whether `sm` also means seasonally adjusted), so no such claim is made here.

# COMMAND ----------

import re
import calendar
from datetime import datetime, timezone

from pyspark.sql import functions as F

CATALOG = "bootcamp_students"
SCHEMA = "dataexpert_student_viknim"

SPINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_fips_spine"
MANIFEST_TABLE = f"{CATALOG}.{SCHEMA}.bronze_ingest_manifest"
LONG_TABLE = f"{CATALOG}.{SCHEMA}.silver_zori_county_month"
COVERAGE_TABLE = f"{CATALOG}.{SCHEMA}.silver_zori_coverage"

SOURCE_NAME = "zillow_zori_county"          # must match config/sources.yml
EXPECTED_SPINE_ROWS = 3235
ZORI_METHOD = "Zillow county-level ZORI file (not a ZIP-code rollup)"

REQUIRED_COLUMNS = [
    "RegionID", "SizeRank", "RegionName", "RegionType", "StateName", "State",
    "Metro", "StateCodeFIPS", "MunicipalCodeFIPS",
]
MIN_WIDE_ROWS, MAX_WIDE_ROWS = 1000, EXPECTED_SPINE_ROWS
STALE_AFTER_DAYS = 120                      # warn (not fail) if the newest month is older than this
NAME_MISMATCH_FAIL_SHARE = 0.02             # more than 2% name disagreement on matched FIPS = something is wrong

dbutils.widgets.text("zori_file", "latest", "zori_file (latest = newest landed in manifest)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pick the bronze file
# MAGIC `latest` reads the manifest, so this notebook uses exactly the file the ingestion notebook recorded (with its dated name), not a guessed filename. To rebuild from an older landing, paste its full Volume path into the widget.

# COMMAND ----------

requested = dbutils.widgets.get("zori_file").strip()

if requested and requested.lower() != "latest":
    BRONZE_PATH = requested
    print(f"Using file from widget: {BRONZE_PATH}")
else:
    latest = (
        spark.table(MANIFEST_TABLE)
        .filter((F.col("source") == SOURCE_NAME) & (F.col("status") == "landed") & F.col("landed_path").isNotNull())
        .orderBy(F.col("fetched_at").desc())
        .select("landed_path", "fetched_at", "bytes", "sha256")
        .limit(1)
        .collect()
    )
    assert latest, (
        f"No 'landed' row for {SOURCE_NAME} in {MANIFEST_TABLE}. Run 02_ingest_sources first "
        "(or paste a Volume path into the zori_file widget)."
    )
    BRONZE_PATH = latest[0]["landed_path"]
    print(f"Manifest says newest landing: {BRONZE_PATH}")
    print(f"  fetched_at={latest[0]['fetched_at']}  bytes={latest[0]['bytes']}  sha256={str(latest[0]['sha256'])[:12]}...")

try:
    dbutils.fs.ls(BRONZE_PATH)
except Exception as e:
    raise AssertionError(f"Bronze file not found at {BRONZE_PATH}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read bronze (everything as string) and validate the shape
# MAGIC No type inference: `StateCodeFIPS` `01` must stay `01`. The monthly columns are the ones named like `2026-08-31`; there is a new one every month, so they are discovered from the header, never hard-coded.

# COMMAND ----------

def validate_date_columns(columns):
    """Return (date_cols, problems). Date columns must be real month-end dates in one contiguous monthly run."""
    date_cols = [c for c in columns if re.fullmatch(r"\d{4}-\d{2}-\d{2}", c)]
    problems = []
    if len(date_cols) < 12:
        problems.append(f"only {len(date_cols)} monthly columns")
    if len(set(date_cols)) != len(date_cols):
        problems.append("duplicate monthly column names")
    parsed = []
    for c in date_cols:
        try:
            d = datetime.strptime(c, "%Y-%m-%d").date()
        except ValueError:
            problems.append(f"{c} is not a real date")
            continue
        if d.day != calendar.monthrange(d.year, d.month)[1]:
            problems.append(f"{c} is not a month-end")
        parsed.append(d)
    ordered = sorted(parsed)
    if parsed != ordered:
        problems.append("monthly columns are not in ascending order")
    months = [d.year * 12 + d.month for d in ordered]
    gaps = [(a, b) for a, b in zip(months, months[1:]) if b - a != 1]
    if gaps:
        problems.append(f"{len(gaps)} gap(s) in the monthly header sequence")
    return date_cols, problems


wide = (
    spark.read
    .option("header", "true")
    .option("mode", "FAILFAST")
    .csv(BRONZE_PATH)          # no inferSchema -> every column is string
)

missing = [c for c in REQUIRED_COLUMNS if c not in wide.columns]
assert not missing, f"Zillow file is missing columns {missing}. Header starts: {wide.columns[:12]}"

DATE_COLS, header_problems = validate_date_columns(wide.columns)
assert not header_problems, f"Monthly header problems: {header_problems}"
FILE_FIRST_MONTH, FILE_LAST_HEADER_MONTH = DATE_COLS[0], DATE_COLS[-1]

n_wide = wide.count()
print(f"rows: {n_wide}   monthly columns: {len(DATE_COLS)}   header range: {FILE_FIRST_MONTH} .. {FILE_LAST_HEADER_MONTH}")
assert MIN_WIDE_ROWS <= n_wide <= MAX_WIDE_ROWS, f"{n_wide} rows is outside the plausible range {MIN_WIDE_ROWS}-{MAX_WIDE_ROWS}"

region_types = [r[0] for r in wide.select("RegionType").distinct().collect()]
assert region_types == ["county"], f"Expected only RegionType 'county', got {region_types}"

# `lpad` TRUNCATES values longer than the target width, so prove the parts are short enough BEFORE padding.
def bad_part(col, width):
    return ~F.coalesce(F.trim(F.col(col)).rlike(f"^[0-9]{{1,{width}}}$"), F.lit(False))

bad_parts = wide.filter(bad_part("StateCodeFIPS", 2) | bad_part("MunicipalCodeFIPS", 3)).count()
assert bad_parts == 0, f"{bad_parts} rows have a non-numeric, empty, or over-long FIPS part"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build `geoid` and check it against the spine
# MAGIC Zillow splits the code into `StateCodeFIPS` (2) and `MunicipalCodeFIPS` (3). We left-pad each and join them into the same 5-character `geoid` the spine uses. Any Zillow county the spine does not know about stops the run.

# COMMAND ----------

base = wide.select(
    F.concat(
        F.lpad(F.trim(F.col("StateCodeFIPS")), 2, "0"),
        F.lpad(F.trim(F.col("MunicipalCodeFIPS")), 3, "0"),
    ).alias("geoid"),
    F.col("RegionID").alias("zillow_region_id"),
    F.col("RegionName").alias("zillow_region_name"),
    F.col("Metro").alias("zillow_metro"),
    F.col("SizeRank").cast("int").alias("zillow_size_rank"),
    *[F.col(f"`{c}`") for c in DATE_COLS],
)

dupe_geoids = base.groupBy("geoid").count().filter("count > 1")
n_dupes = dupe_geoids.count()
assert n_dupes == 0, f"{n_dupes} geoids appear more than once in the Zillow file: {[r.geoid for r in dupe_geoids.limit(10).collect()]}"

spine = spark.table(SPINE_TABLE)
n_spine = spine.count()
assert n_spine == EXPECTED_SPINE_ROWS, f"Spine has {n_spine} rows, expected {EXPECTED_SPINE_ROWS}. Rebuild 01_build_fips_spine first."

orphans = base.join(spine.select("geoid"), "geoid", "left_anti")
n_orphans = orphans.count()
if n_orphans:
    sample = orphans.select("geoid", "zillow_region_name").limit(15).collect()
    ct_orphans = [r.geoid for r in orphans.filter(F.col("geoid").startswith("09")).collect()]
    hint = (
        " Connecticut codes in this list mean Zillow moved to the planning regions (09110-09190); "
        "the spine still has the 8 legacy counties, so a crosswalk decision is needed before this can run."
        if ct_orphans else ""
    )
    raise AssertionError(f"{n_orphans} Zillow counties are not in the spine: {[(r.geoid, r.zillow_region_name) for r in sample]}.{hint}")

print(f"All {n_wide} Zillow counties exist in the spine ({n_spine} rows).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wide -> long, keeping only months Zillow actually reported
# MAGIC Each county-month becomes a row. Blank cells are dropped, not filled. Two reconciliations run afterwards: the number of long rows must equal the number of non-blank cells in the wide file, and no value may fail to parse.

# COMMAND ----------

exploded = base.select(
    "geoid",
    "zillow_region_id",
    F.explode(
        F.array(*[
            F.struct(F.lit(c).alias("month_raw"), F.col(f"`{c}`").alias("zori_raw"))
            for c in DATE_COLS
        ])
    ).alias("kv"),
).select("geoid", "zillow_region_id", F.col("kv.month_raw").alias("month_raw"), F.col("kv.zori_raw").alias("zori_raw"))

observed = exploded.filter(F.col("zori_raw").isNotNull() & (F.trim(F.col("zori_raw")) != ""))

long_df = (
    observed
    .select(
        "geoid",
        F.to_date("month_raw", "yyyy-MM-dd").alias("month_end"),
        F.expr("try_cast(trim(zori_raw) as double)").alias("zori"),
        "zillow_region_id",
        "zori_raw",
    )
)

# --- reconciliation ------------------------------------------------------------
nonblank_cells = (
    base.select(*[
        (F.col(f"`{c}`").isNotNull() & (F.trim(F.col(f"`{c}`")) != "")).cast("int").alias(f"n{i}")
        for i, c in enumerate(DATE_COLS)
    ])
    .agg(F.sum(sum((F.col(f"n{i}") for i in range(len(DATE_COLS))), F.lit(0))).alias("n"))
    .collect()[0]["n"]
)
n_long = long_df.count()
n_unparsed = long_df.filter(F.col("zori").isNull()).count()
n_bad_value = long_df.filter(F.col("zori").isNotNull() & ((F.col("zori") <= 0) | F.col("zori").isin(float("inf"), float("nan")))).count()
n_bad_month = long_df.filter(F.col("month_end").isNull()).count()

print(f"non-blank cells in wide file : {nonblank_cells}")
print(f"rows in long table           : {n_long}")
print(f"unparseable values           : {n_unparsed}")
print(f"zero / negative / non-finite : {n_bad_value}")
print(f"unparseable month            : {n_bad_month}")

assert n_long == nonblank_cells, f"Long rows ({n_long}) != non-blank wide cells ({nonblank_cells}): something was dropped or invented"
assert n_unparsed == 0, "Some ZORI values are not numbers"
assert n_bad_value == 0, "Some ZORI values are <= 0 or non-finite"
assert n_bad_month == 0, "Some month labels failed to parse as dates"

FILE_LATEST_MONTH = long_df.agg(F.max("month_end")).collect()[0][0]     # newest month that has at least one value
print(f"as-of month (newest month with any data): {FILE_LATEST_MONTH}")

long_out = (
    long_df.select("geoid", "month_end", "zori", "zillow_region_id")
    .withColumn("_source_file", F.lit(BRONZE_PATH))
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Coverage: one row per spine county
# MAGIC Start from the spine, left-join what Zillow has. `coverage_status`:
# MAGIC - `current`: has a value in the as-of month
# MAGIC - `series_ended`: Zillow reported this county before, but not in the as-of month
# MAGIC - `in_file_no_observations`: row exists in the file with every month blank
# MAGIC - `not_in_zillow_file`: Zillow publishes nothing for this county. **Nothing is imputed.**

# COMMAND ----------

per_county = (
    long_df.groupBy("geoid").agg(
        F.count("*").alias("n_months_observed"),
        F.min("month_end").alias("first_month"),
        F.max("month_end").alias("last_month"),
        F.expr("max_by(zori, month_end)").alias("latest_zori"),
        F.sum(F.when(F.col("month_end") > F.add_months(F.lit(FILE_LATEST_MONTH), -12), 1).otherwise(0)).alias("n_months_last_12"),
    )
)

zillow_meta = base.select("geoid", "zillow_region_id", "zillow_region_name", "zillow_metro", "zillow_size_rank")

cov = (
    spine.select(
        "geoid", "state_abbr", "county_name", "class_fp",
        "is_state_or_dc", "is_independent_city", "is_consolidated_city_county",
        "is_census_area", "is_inactive_county", "known_hazard",
    )
    .join(zillow_meta, "geoid", "left")
    .join(per_county, "geoid", "left")
    .withColumn("zillow_in_file", F.col("zillow_region_id").isNotNull())
    .withColumn("n_months_observed", F.coalesce(F.col("n_months_observed"), F.lit(0)))
    .withColumn("n_months_last_12", F.coalesce(F.col("n_months_last_12"), F.lit(0)))
    .withColumn(
        "n_gap_months",
        F.when(
            F.col("n_months_observed") > 0,
            F.round(F.months_between(F.col("last_month"), F.col("first_month"))).cast("int") + 1 - F.col("n_months_observed"),
        ),
    )
    .withColumn(
        "coverage_status",
        F.when(~F.col("zillow_in_file"), "not_in_zillow_file")
        .when(F.col("n_months_observed") == 0, "in_file_no_observations")
        .when(F.col("last_month") < F.lit(FILE_LATEST_MONTH), "series_ended")
        .otherwise("current"),
    )
    .withColumn(
        "coverage_note",
        F.when(F.col("coverage_status") == "not_in_zillow_file",
               F.lit("Zillow publishes no county-level ZORI for this county. No value is imputed and no neighbouring county is substituted."))
        .when(F.col("coverage_status") == "in_file_no_observations",
              F.lit("Zillow lists this county but every month is blank. No value is imputed."))
        .when(F.col("coverage_status") == "series_ended",
              F.format_string("Zillow's series for this county ends %s, before the as-of month %s.",
                              F.date_format("last_month", "yyyy-MM"), F.lit(str(FILE_LATEST_MONTH)[:7])))
        .when(F.col("n_gap_months") > 0,
              F.format_string("Current, but %d month(s) inside its reporting window are blank and were left blank.", F.col("n_gap_months"))),
    )
    .withColumn("zori_as_of", F.lit(FILE_LATEST_MONTH))
    .withColumn("zori_method", F.lit(ZORI_METHOD))
    .withColumn("_source_file", F.lit(BRONZE_PATH))
    .withColumn("_ingested_at", F.current_timestamp())
    .select(
        "geoid", "state_abbr", "county_name", "class_fp",
        "is_state_or_dc", "is_independent_city", "is_consolidated_city_county", "is_census_area", "is_inactive_county",
        "zillow_in_file", "zillow_region_id", "zillow_region_name", "zillow_metro", "zillow_size_rank",
        "coverage_status", "coverage_note",
        "n_months_observed", "n_months_last_12", "n_gap_months", "first_month", "last_month", "latest_zori",
        "zori_as_of", "zori_method", "known_hazard",
        "_source_file", "_ingested_at",
    )
)

display(cov.filter("state_abbr = 'VA'").orderBy("geoid").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write the managed Delta tables

# COMMAND ----------

(long_out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LONG_TABLE))
(cov.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(COVERAGE_TABLE))

spark.sql(
    f"COMMENT ON TABLE {LONG_TABLE} IS "
    "'Zillow ZORI (county, monthly). One row per county-month Zillow actually reported; blanks are never filled. "
    "Join to silver_fips_spine on geoid. Source: Zillow Research county ZORI file, latest landing in bronze_ingest_manifest.'"
)
spark.sql(
    f"COMMENT ON TABLE {COVERAGE_TABLE} IS "
    "'One row per silver_fips_spine county saying whether Zillow ZORI exists for it, how complete the series is, and why not. "
    "coverage_status not_in_zillow_file means no data, not zero. Method: Zillow county file, not a ZIP rollup.'"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Constraints that Unity Catalog actually enforces
# MAGIC `NOT NULL` and `CHECK` are enforced on write. `PRIMARY KEY` is informational only, so uniqueness is tested in section 8.

# COMMAND ----------

for col_name in ["geoid", "month_end", "zori"]:
    spark.sql(f"ALTER TABLE {LONG_TABLE} ALTER COLUMN {col_name} SET NOT NULL")
for col_name in ["geoid", "coverage_status", "zillow_in_file", "n_months_observed"]:
    spark.sql(f"ALTER TABLE {COVERAGE_TABLE} ALTER COLUMN {col_name} SET NOT NULL")

for table, name, expr in [
    (LONG_TABLE, "geoid_len_5", "length(geoid) = 5"),
    (LONG_TABLE, "zori_positive", "zori > 0"),
    (COVERAGE_TABLE, "geoid_len_5", "length(geoid) = 5"),
    (COVERAGE_TABLE, "coverage_status_known",
     "coverage_status IN ('current','series_ended','in_file_no_observations','not_in_zillow_file')"),
]:
    spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")      # so this cell can be re-run
    spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Repeatable validation
# MAGIC Re-run after every rebuild. All checks run, then the notebook fails once at the end with every problem listed.

# COMMAND ----------

long_t = spark.table(LONG_TABLE)
cov_t = spark.table(COVERAGE_TABLE)
spine_t = spark.table(SPINE_TABLE)

results, warnings = [], []

def check(name, passed, detail=""):
    results.append((name, bool(passed), str(detail)))

def warn(name, detail):
    warnings.append((name, str(detail)))

# --- grain and keys --------------------------------------------------------------
n_cov = cov_t.count()
n_cov_distinct = cov_t.select("geoid").distinct().count()
n_long_t = long_t.count()
long_dupes = long_t.groupBy("geoid", "month_end").count().filter("count > 1").count()
long_orphans = long_t.join(spine_t.select("geoid"), "geoid", "left_anti").count()
cov_not_in_spine = cov_t.join(spine_t.select("geoid"), "geoid", "left_anti").count()
spine_not_in_cov = spine_t.join(cov_t.select("geoid"), "geoid", "left_anti").count()

check("coverage has one row per spine county", n_cov == EXPECTED_SPINE_ROWS and n_cov_distinct == n_cov,
      f"rows={n_cov}, distinct geoid={n_cov_distinct}, expected={EXPECTED_SPINE_ROWS}")
check("coverage and spine hold the same geoids", cov_not_in_spine == 0 and spine_not_in_cov == 0,
      f"coverage-only={cov_not_in_spine}, spine-only={spine_not_in_cov}")
check("long table has no duplicate (geoid, month_end)", long_dupes == 0, f"duplicate groups={long_dupes}")
check("every long-table geoid exists in the spine", long_orphans == 0, f"orphans={long_orphans}")

# --- nothing dropped, nothing invented ------------------------------------------
n_in_file = cov_t.filter("zillow_in_file").count()
check("every Zillow county survived the join", n_in_file == n_wide, f"in coverage={n_in_file}, in file={n_wide}")
sum_obs = cov_t.agg(F.sum("n_months_observed")).collect()[0][0]
check("coverage month counts add up to the long table", sum_obs == n_long_t, f"sum(n_months_observed)={sum_obs}, long rows={n_long_t}")
check("long table row count equals non-blank cells in the file", n_long_t == nonblank_cells, f"long={n_long_t}, file cells={nonblank_cells}")

status_rows = {r["coverage_status"]: r["count"] for r in cov_t.groupBy("coverage_status").count().collect()}
check("every spine county has exactly one known status", sum(status_rows.values()) == EXPECTED_SPINE_ROWS, status_rows)
check("not_in_zillow_file rows carry no data",
      cov_t.filter((F.col("coverage_status") == "not_in_zillow_file") & (F.col("n_months_observed") > 0)).count() == 0)
check("first_month <= last_month everywhere",
      cov_t.filter(F.col("first_month") > F.col("last_month")).count() == 0)
check("gap months are never negative",
      cov_t.filter(F.col("n_gap_months") < 0).count() == 0)

# --- Connecticut: Zillow must still use the 8 legacy county codes -----------------
ct = cov_t.filter("state_abbr = 'CT'")
ct_in_file = ct.filter("zillow_in_file").count()
check("CT: all 8 legacy counties are in Zillow's file", ct.count() == 8 and ct_in_file == 8, f"CT spine rows={ct.count()}, in Zillow={ct_in_file}")

# --- Virginia ---------------------------------------------------------------------
va = cov_t.filter("state_abbr = 'VA'")
check("VA has 133 coverage rows (95 counties + 38 independent cities)", va.count() == 133, va.count())

# --- name sanity: FIPS is authoritative, but names disagreeing wholesale means a bad join ----
def name_key(c):
    s = F.regexp_replace(F.lower(F.col(c)), "ñ", "n")
    s = F.regexp_replace(s, r"\bsaint\b", "st")
    s = F.regexp_replace(s, r"\b(city and borough|census area|municipality|county|parish|borough|city)\b", "")
    return F.regexp_replace(s, r"[^a-z0-9]", "")

name_mismatch = (
    cov_t.filter("zillow_in_file")
    .filter(name_key("zillow_region_name") != name_key("county_name"))
    .select("geoid", "county_name", "zillow_region_name")
)
n_name_mismatch = name_mismatch.count()
share = n_name_mismatch / max(n_in_file, 1)
check(f"Zillow and Census county names agree for >= {100 - NAME_MISMATCH_FAIL_SHARE * 100:.0f}% of matched FIPS",
      share <= NAME_MISMATCH_FAIL_SHARE, f"{n_name_mismatch} of {n_in_file} differ ({share:.1%})")
if n_name_mismatch:
    warn("county names differ for some matched FIPS (FIPS wins; eyeball these)",
         [(r.geoid, r.county_name, r.zillow_region_name) for r in name_mismatch.limit(20).collect()])

# --- freshness (warning only) -------------------------------------------------------
age_days = (datetime.now(timezone.utc).date() - FILE_LATEST_MONTH).days
if age_days > STALE_AFTER_DAYS:
    warn("as-of month is old", f"{FILE_LATEST_MONTH} is {age_days} days ago; re-run 02_ingest_sources to check for a newer file")

# --- report ------------------------------------------------------------------------------
width = max(len(n) for n, _, _ in results)
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name.ljust(width)}  {detail}")
for name, detail in warnings:
    print(f"WARN  {name}: {detail}")

failed = [n for n, ok, _ in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
assert not failed, f"Failed checks: {failed}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. What the coverage actually looks like
# MAGIC These are the numbers for the write-up. Nothing here is estimated; every figure is a count from the coverage table.

# COMMAND ----------

print(f"ZORI as of {FILE_LATEST_MONTH}   (file: {BRONZE_PATH.split('/')[-1]})\n")

def summarize(label, df):
    total = df.count()
    rows = {r["coverage_status"]: r["count"] for r in df.groupBy("coverage_status").count().collect()}
    current = rows.get("current", 0)
    any_data = total - rows.get("not_in_zillow_file", 0) - rows.get("in_file_no_observations", 0)
    print(f"{label}: {total} counties | current {current} ({current / total:.1%}) | any data {any_data} ({any_data / total:.1%}) | {rows}")

summarize("All spine rows           ", cov_t)
summarize("50 states + DC           ", cov_t.filter("is_state_or_dc"))
summarize("Virginia (all 133)       ", va)
summarize("  VA counties (H1)       ", va.filter("class_fp = 'H1'"))
summarize("  VA independent cities  ", va.filter("is_independent_city"))
summarize("Connecticut (legacy)     ", ct)

# COMMAND ----------

display(
    cov_t.filter("is_state_or_dc")
    .groupBy("state_abbr")
    .agg(
        F.count("*").alias("counties"),
        F.sum(F.col("coverage_status").isin("current").cast("int")).alias("current"),
        F.round(F.avg(F.col("coverage_status").isin("current").cast("int")) * 100, 1).alias("pct_current"),
    )
    .orderBy("pct_current", "state_abbr")
)

# COMMAND ----------

display(
    va.select("geoid", "county_name", "class_fp", "coverage_status", "n_months_observed", "n_months_last_12",
              "first_month", "last_month", "latest_zori", "coverage_note")
    .orderBy("coverage_status", "geoid")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for later layers
# MAGIC - **Gold join:** `silver_fips_spine` LEFT JOIN `silver_zori_coverage` ON `geoid`, then LEFT JOIN `silver_zori_county_month` for the time series. Never inner-join, or counties without Zillow data vanish from the national view.
# MAGIC - **Agent:** `coverage_status`, `coverage_note`, `zori_as_of` and `zori_method` are exactly what `explain_gap(location)` should quote. "No data" and "zero" must never be conflated.
# MAGIC - **Connecticut:** this notebook proves Zillow aligns with the 8 legacy codes. ACS may not; that is a separate decision.