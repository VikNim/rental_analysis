# Databricks notebook source
# MAGIC %md
# MAGIC # 02_ingest_sources
# MAGIC **Source websites -> bronze Volume**, with a provenance manifest.
# MAGIC
# MAGIC What this notebook does, in order:
# MAGIC 1. **Probes** each source host from *this* compute. Free Edition serverless restricts outbound traffic to a trusted-domain list, so reachability is something to measure, not assume.
# MAGIC 2. **Downloads** reachable sources with retries, streaming to local disk while hashing.
# MAGIC 3. **Validates** the content (header, columns, row counts) *before* it touches bronze, so an HTML error page never gets landed as if it were data.
# MAGIC 4. **Lands** the file in the Volume under a dated name. Bronze is append-only: nothing is overwritten.
# MAGIC 5. **Logs every attempt** (including failures and blocked hosts) to `bronze_ingest_manifest`. A blocked source is a finding, not something to hide.
# MAGIC
# MAGIC If a host is blocked, the fallback is: download in your browser, upload to the Volume folder, then call `register_manual_file(...)` (section 6) so the manual file gets the same provenance record.
# MAGIC
# MAGIC ## What is and isn't verified
# MAGIC - The Census county file URL and its header are known (you already hold a copy).
# MAGIC - The Zillow county URL follows the pattern of the one Metro link Zillow's page shows. I could not confirm the county URL from the page, so the validator checks the real header and prints it if it differs.
# MAGIC - The ACS call uses Census's documented Data API. The `2024` vintage is a parameter (`ACS_YEAR`); if the probe returns 404, drop to `2023`.
# MAGIC - HUD CHAS has **no direct URL** on its page (it is served through a dropdown), so it is registered as manual.

# COMMAND ----------

# --- CONFIG START ---
import csv
import hashlib
import json
import os
import re
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

CATALOG = "bootcamp_students"
SCHEMA = "dataexpert_student_viknim"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/bronze_landing"
MANIFEST_TABLE = f"{CATALOG}.{SCHEMA}.bronze_ingest_manifest"

# Identify the project honestly; some CDNs reject the default python-requests agent.
USER_AGENT = "rent-affordability-capstone/1.0 (Databricks bootcamp project)"

ACS_YEAR = 2024   # ACS 5-year vintage (2020-2024). If the probe says 404, try 2023.
ACS_BASE = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"

# B25064 median gross rent | B25071 median gross rent as % of income | B19013 median household income
# B25070 gross rent as % of income distribution (001 = total ... 011 = not computed)
ACS_TABLE_VARS = (
    ["B25064_001", "B25071_001", "B19013_001"]
    + [f"B25070_{i:03d}" for i in range(1, 12)]
)
ACS_VARS = [f"{v}{suffix}" for v in ACS_TABLE_VARS for suffix in ("E", "M")]   # estimate + margin of error
ACS_COUNTY_URL = f"{ACS_BASE}?get=NAME,{','.join(ACS_VARS)}&for=county:*"
ACS_PROBE_URL = f"{ACS_BASE}?get=NAME,B25064_001E&for=state:51"

FIPS_HEADER = "STATE|STATEFP|COUNTYFP|COUNTYNS|COUNTYNAME|CLASSFP|FUNCSTAT"
ZORI_REQUIRED_COLUMNS = {"RegionID", "RegionName", "StateCodeFIPS", "MunicipalCodeFIPS"}

SOURCES = [
    dict(
        name="census_county_reference", subdir="fips_reference",
        stem="national_county2020", ext=".txt", validator="fips_reference",
        url="https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt",
        probe_url="https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt",
    ),
    dict(
        name="zillow_zori_county", subdir="zori",
        stem="County_zori_uc_sfrcondomfr_sm_month", ext=".csv", validator="zillow_zori",
        url="https://files.zillowstatic.com/research/public_csvs/zori/County_zori_uc_sfrcondomfr_sm_month.csv",
        probe_url="https://files.zillowstatic.com/research/public_csvs/zori/County_zori_uc_sfrcondomfr_sm_month.csv",
    ),
    dict(
        name="acs5_county", subdir="acs",
        stem=f"acs5_{ACS_YEAR}_county_rent_income", ext=".json", validator="acs_api",
        url=ACS_COUNTY_URL, probe_url=ACS_PROBE_URL,
    ),
    dict(
        # HUD serves CHAS through a download dropdown; no direct URL was visible on the page.
        # To automate: click download in your browser, copy the zip URL from the Network tab,
        # paste it into `url` below, and add a validator. Until then it is manual.
        name="hud_chas_county", subdir="chas",
        stem="chas_county", ext=".zip", validator=None, url=None, probe_url=None,
    ),
]
# --- CONFIG END ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validators
# MAGIC Each takes a local file path and returns `(ok, detail)`. They check *shape*, not just "the download worked".

# COMMAND ----------

# --- VALIDATORS START ---
def validate_fips_reference(path):
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    if not lines:
        return False, "empty file"
    if lines[0].strip() != FIPS_HEADER:
        return False, f"unexpected header: {lines[0][:200]!r}"
    n = len(lines) - 1
    if n < 3000:
        return False, f"only {n} data rows (expected 3,000+)"
    return True, f"{n} rows"


def validate_zillow_zori(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return False, "empty file"
        missing = ZORI_REQUIRED_COLUMNS - set(header)
        if missing:
            return False, f"missing columns {sorted(missing)}; header starts {header[:12]}"
        date_cols = [c for c in header if re.fullmatch(r"\d{4}-\d{2}-\d{2}", c)]
        if len(date_cols) < 12:
            return False, f"only {len(date_cols)} monthly columns"
        n = sum(1 for _ in reader)
    if n < 500:
        return False, f"only {n} data rows"
    return True, f"{n} rows, {len(date_cols)} monthly columns, latest {max(date_cols)}"


def validate_acs_api(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or len(data) < 2:
        return False, "not a header+rows JSON array"
    header, rows = data[0], data[1:]
    needed = ["NAME", "state", "county"] + ACS_VARS
    missing = [c for c in needed if c not in header]
    if missing:
        return False, f"missing columns {missing}"
    s, c = header.index("state"), header.index("county")
    geoids = [r[s] + r[c] for r in rows]
    if len(set(geoids)) != len(geoids):
        return False, f"duplicate geoids: {len(geoids) - len(set(geoids))}"
    if len(rows) < 3100:
        return False, f"only {len(rows)} rows (expected 3,100+)"
    bad_len = [g for g in geoids if len(g) != 5]
    if bad_len:
        return False, f"{len(bad_len)} geoids not 5 chars, e.g. {bad_len[:3]}"
    return True, f"{len(rows)} counties, {len(ACS_VARS)} variables"


VALIDATORS = {
    "fips_reference": validate_fips_reference,
    "zillow_zori": validate_zillow_zori,
    "acs_api": validate_acs_api,
}
# --- VALIDATORS END ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Manifest table
# MAGIC One row per attempt. `status` is one of: `landed`, `unchanged` (same hash as a prior landing, nothing written), `blocked`, `failed_fetch`, `failed_validation`, `skipped_manual`.

# COMMAND ----------

MANIFEST_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("fetched_at", TimestampType()),
    StructField("source", StringType()),
    StructField("method", StringType()),        # http | manual_upload
    StructField("url", StringType()),
    StructField("http_status", IntegerType()),
    StructField("status", StringType()),
    StructField("bytes", LongType()),
    StructField("sha256", StringType()),
    StructField("landed_path", StringType()),
    StructField("detail", StringType()),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
        run_id STRING, fetched_at TIMESTAMP, source STRING, method STRING, url STRING,
        http_status INT, status STRING, bytes BIGINT, sha256 STRING, landed_path STRING, detail STRING
    ) COMMENT 'One row per ingestion attempt (including failures and blocked hosts). Bronze provenance.'
""")

RUN_ID = uuid.uuid4().hex[:12]


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def record(source, method, url, status, http_status=None, nbytes=None, sha=None, path=None, detail=None):
    row = (RUN_ID, utcnow(), source, method, url, http_status, status, nbytes, sha, path, detail)
    spark.createDataFrame([row], MANIFEST_SCHEMA).write.mode("append").saveAsTable(MANIFEST_TABLE)


def already_landed(source, sha):
    return (
        spark.table(MANIFEST_TABLE)
        .filter((F.col("source") == source) & (F.col("sha256") == sha)
                & F.col("status").isin("landed", "unchanged"))
        .limit(1).count() > 0
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. HTTP helpers
# MAGIC Retries cover transient failures (429/5xx, timeouts) with backoff. **403 is deliberately not retried**: a 403 is a policy answer, not a glitch.

# COMMAND ----------

def make_session():
    s = requests.Session()
    retry = Retry(
        total=4, connect=4, read=4, backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"), respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def probe(session, src):
    """Tiny request to see whether this compute can reach the host at all."""
    host = urlparse(src["probe_url"]).hostname
    try:
        r = session.get(src["probe_url"], headers={"Range": "bytes=0-1023"}, stream=True, timeout=(10, 30))
        code = r.status_code
        r.close()
        return dict(source=src["name"], host=host, reachable=code in (200, 206),
                    http_status=code, error=None)
    except requests.exceptions.RequestException as e:
        return dict(source=src["name"], host=host, reachable=False,
                    http_status=None, error=f"{type(e).__name__}: {str(e)[:200]}")


def fetch_to_local(session, url, tmp_dir):
    """Stream to local disk while hashing. Returns (http_status, local_path, n_bytes, sha256)."""
    h, n = hashlib.sha256(), 0
    local = os.path.join(tmp_dir, uuid.uuid4().hex)
    with session.get(url, stream=True, timeout=(10, 120)) as r:
        if r.status_code != 200:
            return r.status_code, None, 0, None
        with open(local, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                h.update(chunk)
                n += len(chunk)
        return r.status_code, local, n, h.hexdigest()


def land_file(local_path, subdir, stem, ext):
    """Copy into the Volume under a dated name. Never overwrites an existing file."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    dest_dir = f"{VOLUME_ROOT}/{subdir}"
    os.makedirs(dest_dir, exist_ok=True)
    dest = f"{dest_dir}/{stem}_{stamp}{ext}"
    if os.path.exists(dest):
        dest = f"{dest_dir}/{stem}_{stamp}_{uuid.uuid4().hex[:6]}{ext}"
    shutil.copyfile(local_path, dest)
    return dest

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Probe every source from this compute
# MAGIC Read this table before anything downloads. `reachable = False` with a 403 or a connection error means the host is outside this workspace's allowed domains.

# COMMAND ----------

session = make_session()

probe_results = [probe(session, s) for s in SOURCES if s["probe_url"]]
display(spark.createDataFrame(probe_results))

blocked_hosts = sorted({p["host"] for p in probe_results if not p["reachable"]})
if blocked_hosts:
    print("NOT reachable from this compute:", blocked_hosts)
    print("Those sources will be logged as 'blocked'. Use the manual fallback in section 6 for them.")
else:
    print("All probed hosts reachable.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Download, validate, land, log

# COMMAND ----------

reachable = {p["source"]: p for p in probe_results}
outcomes = []

with tempfile.TemporaryDirectory() as tmp_dir:
    for src in SOURCES:
        name, url = src["name"], src["url"]

        if url is None:
            record(name, "http", None, "skipped_manual",
                   detail="No direct URL configured; land the file manually and call register_manual_file().")
            outcomes.append((name, "skipped_manual", "no URL configured"))
            continue

        p = reachable[name]
        if not p["reachable"]:
            detail = f"probe failed: http_status={p['http_status']} error={p['error']}"
            record(name, "http", url, "blocked", http_status=p["http_status"], detail=detail)
            outcomes.append((name, "blocked", detail))
            continue

        try:
            status, local, n, sha = fetch_to_local(session, url, tmp_dir)
        except requests.exceptions.RequestException as e:
            detail = f"{type(e).__name__}: {str(e)[:200]}"
            record(name, "http", url, "failed_fetch", detail=detail)
            outcomes.append((name, "failed_fetch", detail))
            continue

        if local is None:
            detail = f"HTTP {status}"
            record(name, "http", url, "failed_fetch", http_status=status, detail=detail)
            outcomes.append((name, "failed_fetch", detail))
            continue

        try:
            ok, detail = VALIDATORS[src["validator"]](local)
        except Exception as e:   # unparseable content (e.g. an HTML error page) counts as a validation failure
            ok, detail = False, f"{type(e).__name__}: {str(e)[:200]}"

        if not ok:
            record(name, "http", url, "failed_validation", http_status=status, nbytes=n, sha=sha, detail=detail)
            outcomes.append((name, "failed_validation", detail))
            continue

        if already_landed(name, sha):
            record(name, "http", url, "unchanged", http_status=status, nbytes=n, sha=sha,
                   detail=f"identical to a previous landing; {detail}")
            outcomes.append((name, "unchanged", detail))
            continue

        dest = land_file(local, src["subdir"], src["stem"], src["ext"])
        record(name, "http", url, "landed", http_status=status, nbytes=n, sha=sha, path=dest, detail=detail)
        outcomes.append((name, "landed", f"{detail} -> {dest}"))

display(spark.createDataFrame(outcomes, ["source", "status", "detail"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Manual fallback: register a file you uploaded by hand
# MAGIC Upload the file into the matching Volume folder (`fips_reference/`, `zori/`, `acs/`, `chas/`), then register it so it gets the same hash + validation + manifest record as a downloaded file. Run this only for sources that came back `blocked` or `skipped_manual`.

# COMMAND ----------

def register_manual_file(source_name, volume_path):
    src = next(s for s in SOURCES if s["name"] == source_name)
    h, n = hashlib.sha256(), 0
    with open(volume_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
            n += len(chunk)
    sha = h.hexdigest()

    if src["validator"]:
        try:
            ok, detail = VALIDATORS[src["validator"]](volume_path)
        except Exception as e:
            ok, detail = False, f"{type(e).__name__}: {str(e)[:200]}"
    else:
        ok, detail = True, "no validator configured for this source"

    status = "landed" if ok else "failed_validation"
    record(source_name, "manual_upload", None, status, nbytes=n, sha=sha, path=volume_path, detail=detail)
    print(f"{source_name}: {status} - {detail}")

# Example (uncomment and edit). This would also retro-register the FIPS file you uploaded earlier:
# register_manual_file(
#     "census_county_reference",
#     f"{VOLUME_ROOT}/fips_reference/canonical_national_county2020.txt",
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fail loudly if anything needs attention
# MAGIC `landed`, `unchanged` and `skipped_manual` are fine. Anything else stops the run, so a scheduled job can never look green while a source silently went stale.

# COMMAND ----------

display(
    spark.table(MANIFEST_TABLE)
    .filter(F.col("run_id") == RUN_ID)
    .select("source", "method", "status", "http_status", "bytes", "landed_path", "detail")
)

needs_attention = [(n, s, d) for n, s, d in outcomes if s not in ("landed", "unchanged", "skipped_manual")]
if needs_attention:
    lines = "\n".join(f"  - {n}: {s} ({d})" for n, s, d in needs_attention)
    raise RuntimeError(f"{len(needs_attention)} source(s) need attention:\n{lines}")
print("Ingestion finished with no failures.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for silver (don't forget these)
# MAGIC - **ACS sentinel values:** the Census API returns large negative numbers (e.g. `-666666666`) where an estimate or margin is unavailable. Bronze keeps them raw on purpose. In silver, convert them to NULL **and** keep the reason in a separate field. Leaving them in would put a fake rent of -$666 million in your average.
# MAGIC - **Leading zeros:** check whether Zillow's `StateCodeFIPS` / `MunicipalCodeFIPS` arrive without zero padding (state `1` instead of `01`). Read every FIPS-like column as string and left-pad to 2 and 3 characters before building the geoid.
# MAGIC - **Zillow variant:** confirm on Zillow's ZORI methodology page what the `sm_month` file is (smoothed, or smoothed and seasonally adjusted) and put that in the source label the agent shows users.
# MAGIC - **Connecticut:** after the first ACS landing, look at which Connecticut GEOIDs appear (`09001-09015` or `09110-09190`). That is your answer to the planning-region question.
