# Databricks notebook source
# MAGIC %md
# MAGIC # Sentinel AI — 01 · Bronze ingestion
# MAGIC
# MAGIC Loads the CFPB credit-reporting sample (uploaded Parquet) into the **managed Delta table**
# MAGIC `workspace.sentinel.bronze_complaints`.
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Source | `/Volumes/workspace/sentinel/raw_files/cfpb_credit_reporting_sample.parquet` |
# MAGIC | Produced by | `PYTHONPATH=src python -m ingestion bulk-sample` (see `docs/data_sampling.md`) |
# MAGIC | Rows / strata | 50,006 complaints = 22 months × 2,273 |
# MAGIC | Target | `workspace.sentinel.bronze_complaints` (managed Delta, Unity Catalog) |
# MAGIC | Compute | Databricks Free Edition **serverless** (no cluster configuration needed) |
# MAGIC
# MAGIC **Bronze principles applied here**
# MAGIC 1. *No business transformation* — the 24 source columns are stored exactly as received (names, types, values).
# MAGIC 2. *Lineage* — three audit columns are **added** (prefixed `_bronze_`): source file path, content hash, load timestamp.
# MAGIC 3. *Idempotent* — a `MERGE` keyed on `complaint_id` inserts new complaints, updates only rows whose content changed,
# MAGIC    and does nothing for unchanged rows. Re-running the notebook never duplicates records.
# MAGIC 4. *Fail loudly* — validation runs **before** the write (source) and **after** it (target); any failure raises an error.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Parameters
# MAGIC Widgets let you point the notebook at another catalog/schema/file without editing code.
# MAGIC Identifiers are validated so a typo cannot inject SQL.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sentinel")
dbutils.widgets.text("table", "bronze_complaints")
dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/sentinel/raw_files/cfpb_credit_reporting_sample.parquet",
)

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
TABLE = dbutils.widgets.get("table").strip()
SOURCE_PATH = dbutils.widgets.get("source_path").strip()

for name, value in {"catalog": CATALOG, "schema": SCHEMA, "table": TABLE}.items():
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {name} identifier: {value!r}")
if not SOURCE_PATH.startswith("/Volumes/"):
    raise ValueError(f"source_path must be a Unity Catalog volume path, got {SOURCE_PATH!r}")

TARGET = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"
print(f"Source: {SOURCE_PATH}\nTarget: {TARGET}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Expected contract
# MAGIC The 24 columns and Spark types produced by the local sampler (`SAMPLE_SCHEMA` in
# MAGIC `src/ingestion/bulk_sample.py`; a unit test keeps the two in sync).
# MAGIC Expected counts come from the sample manifest.

# COMMAND ----------

EXPECTED_COLUMNS = [
    ("complaint_id", "string"),
    ("date_received", "string"),
    ("date_sent_to_company", "string"),
    ("product", "string"),
    ("sub_product", "string"),
    ("issue", "string"),
    ("sub_issue", "string"),
    ("company", "string"),
    ("company_response", "string"),
    ("company_public_response", "string"),
    ("timely", "string"),
    ("submitted_via", "string"),
    ("state", "string"),
    ("zip_code", "string"),
    ("tags", "string"),
    ("_raw_json", "string"),
    ("_window_start", "string"),
    ("_window_end", "string"),
    ("_ingested_at", "string"),
    ("_source_system", "string"),
    ("_stratum", "string"),
    ("_sample_key", "string"),
    ("_stratum_population", "bigint"),
    ("_sample_weight", "double"),
]
AUDIT_COLUMNS = [
    ("_bronze_source_file", "string"),
    ("_bronze_record_hash", "string"),
    ("_bronze_loaded_at", "timestamp"),
]
# Columns excluded from the content hash: _ingested_at changes on every local
# re-sample even when the data is identical, so it must not trigger updates.
HASH_EXCLUDED = {"_ingested_at"}

EXPECTED_ROWS = 50_006
EXPECTED_STRATA = 22
EXPECTED_PER_STRATUM = 2_273
EXPECTED_POPULATION = 10_500_160  # sum of exact monthly totals (API = bulk frame)

SOURCE_NAMES = [name for name, _ in EXPECTED_COLUMNS]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Read the source file
# MAGIC `spark.read.parquet` reads the uploaded file from the Unity Catalog volume.
# MAGIC `_metadata.file_path` is Spark's built-in file metadata column — it records exactly which
# MAGIC file each row came from (lineage), without modifying source columns.

# COMMAND ----------

from pyspark.sql import functions as F

raw_df = spark.read.parquet(SOURCE_PATH)
source_df = raw_df.select(*SOURCE_NAMES, F.col("_metadata.file_path").alias("_bronze_source_file"))

print(f"Columns read: {len(raw_df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Validate the source (before writing anything)
# MAGIC | Check | Why |
# MAGIC |---|---|
# MAGIC | Exact 24-column schema (names **and** types) | Detects a wrong/changed file before it reaches Bronze |
# MAGIC | Row count = 50,006 | Matches the sample manifest |
# MAGIC | No null / blank `complaint_id` | The MERGE key must always be present |
# MAGIC | `complaint_id` unique | A duplicate key would make the MERGE ambiguous (Delta raises an error) |
# MAGIC | 22 strata × 2,273 rows | Confirms the equal-allocation design |
# MAGIC | Σ stratum populations = 10,500,160 | Confirms the weights refer to the correct population |
# MAGIC | Every `_stratum` equals the month of `date_received` | Rows sit in the right stratum |
# MAGIC
# MAGIC All failures are collected and raised together so one run shows every problem.

# COMMAND ----------

def check(failures, condition, message):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {message}")
    if not condition:
        failures.append(message)


source_failures = []

actual_types = dict(raw_df.dtypes)
missing = [c for c in SOURCE_NAMES if c not in actual_types]
unexpected = [c for c in actual_types if c not in SOURCE_NAMES]
wrong_types = {c: (actual_types[c], t) for c, t in EXPECTED_COLUMNS
               if c in actual_types and actual_types[c] != t}
check(source_failures, not missing, f"no missing columns (missing={missing})")
check(source_failures, not unexpected, f"no unexpected columns (unexpected={unexpected})")
check(source_failures, not wrong_types, f"column types match (mismatches={wrong_types})")

stats = source_df.agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct("complaint_id").alias("unique_ids"),
    F.sum(F.when(F.col("complaint_id").isNull() | (F.trim("complaint_id") == ""), 1).otherwise(0)).alias("null_ids"),
    F.countDistinct("_stratum").alias("strata"),
    F.sum(F.when(F.substring("date_received", 1, 7) != F.col("_stratum"), 1).otherwise(0)).alias("misplaced"),
).first()

per_stratum = source_df.groupBy("_stratum").agg(
    F.count(F.lit(1)).alias("rows"),
    F.max("_stratum_population").alias("population"),
    F.countDistinct("_stratum_population").alias("population_values"),
)
strata = per_stratum.collect()
bad_strata = [r["_stratum"] for r in strata if r["rows"] != EXPECTED_PER_STRATUM or r["population_values"] != 1]
population_total = sum(r["population"] for r in strata)

check(source_failures, stats["rows"] == EXPECTED_ROWS, f"row count {stats['rows']} == {EXPECTED_ROWS}")
check(source_failures, stats["null_ids"] == 0, f"null/blank complaint_id = {stats['null_ids']}")
check(source_failures, stats["unique_ids"] == stats["rows"], f"unique complaint_id {stats['unique_ids']} == rows {stats['rows']}")
check(source_failures, stats["strata"] == EXPECTED_STRATA, f"strata {stats['strata']} == {EXPECTED_STRATA}")
check(source_failures, not bad_strata, f"every stratum has {EXPECTED_PER_STRATUM} rows and one population value (bad={bad_strata})")
check(source_failures, population_total == EXPECTED_POPULATION, f"population total {population_total:,} == {EXPECTED_POPULATION:,}")
check(source_failures, stats["misplaced"] == 0, f"rows whose date_received month != _stratum: {stats['misplaced']}")

if source_failures:
    raise AssertionError(f"Source validation failed ({len(source_failures)}): {source_failures}")

display(per_stratum.orderBy("_stratum"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Add Bronze audit columns
# MAGIC * `_bronze_record_hash` — SHA-256 of all source columns except `_ingested_at`, serialised as JSON.
# MAGIC   Used by the MERGE to update a row **only if its content actually changed**.
# MAGIC * `_bronze_loaded_at` — when this version of the row was written to Bronze.
# MAGIC
# MAGIC The 24 source columns are passed through untouched.

# COMMAND ----------

hash_columns = [c for c in SOURCE_NAMES if c not in HASH_EXCLUDED]

staged_df = (
    source_df
    .withColumn("_bronze_record_hash",
                F.sha2(F.to_json(F.struct(*[F.col(c) for c in hash_columns])), 256))
    .withColumn("_bronze_loaded_at", F.current_timestamp())
    .select(*SOURCE_NAMES, *[name for name, _ in AUDIT_COLUMNS])
)
staged_df.createOrReplaceTempView("bronze_complaints_staged")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Create the managed Delta table (first run only)
# MAGIC `CREATE TABLE IF NOT EXISTS` without a `LOCATION` creates a **managed** Unity Catalog table
# MAGIC (Databricks manages its storage). The explicit schema means the table never silently
# MAGIC changes shape. Change Data Feed is enabled so Silver can later read only changed rows.
# MAGIC
# MAGIC If the table already exists, its schema is checked against the contract.

# COMMAND ----------

column_ddl = ",\n  ".join(f"`{name}` {dtype.upper()}" for name, dtype in EXPECTED_COLUMNS + AUDIT_COLUMNS)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  {column_ddl}
)
USING DELTA
COMMENT 'Sentinel AI Bronze: CFPB credit-reporting complaints, stratified sample (Jan 2025 - Oct 2026). 24 source columns as received + 3 _bronze_ audit columns.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

target_types = dict(spark.table(TARGET).dtypes)
expected_target = dict(EXPECTED_COLUMNS + AUDIT_COLUMNS)
if target_types != expected_target:
    raise AssertionError(
        f"Existing table {TARGET} schema differs from the contract.\n"
        f"Only in table: { {k: v for k, v in target_types.items() if expected_target.get(k) != v} }\n"
        f"Expected: { {k: v for k, v in expected_target.items() if target_types.get(k) != v} }"
    )
print(f"{TARGET} ready ({len(target_types)} columns)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Idempotent MERGE on `complaint_id`
# MAGIC | Case | Action |
# MAGIC |---|---|
# MAGIC | `complaint_id` not in Bronze | **INSERT** |
# MAGIC | In Bronze, same content hash | **no-op** (this is what makes reruns safe) |
# MAGIC | In Bronze, different content hash | **UPDATE** the row (e.g. CFPB corrected a field) |
# MAGIC | In Bronze but not in this file | **kept** — Bronze never deletes; reported in step 8 |
# MAGIC
# MAGIC Expected metrics: first run `inserted = 50,006`; any rerun with the same file `inserted = 0, updated = 0`.

# COMMAND ----------

merge_metrics = spark.sql(f"""
MERGE INTO {TARGET} AS t
USING bronze_complaints_staged AS s
ON t.complaint_id = s.complaint_id
WHEN MATCHED AND t._bronze_record_hash <> s._bronze_record_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
display(merge_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Validate the Bronze table (after writing)
# MAGIC Re-checks the same rules against the **table**, then reconciles table ↔ source:
# MAGIC * every source `complaint_id` is in the table and vice versa (anti-joins = 0);
# MAGIC * every matched row has the same content hash (nothing altered in transit);
# MAGIC * the 24 source columns exist with their original types.

# COMMAND ----------

bronze = spark.table(TARGET)
target_failures = []

t = bronze.agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct("complaint_id").alias("unique_ids"),
    F.sum(F.when(F.col("complaint_id").isNull(), 1).otherwise(0)).alias("null_ids"),
    F.countDistinct("_stratum").alias("strata"),
).first()

missing_in_target = staged_df.join(bronze, "complaint_id", "left_anti").count()
extra_in_target = bronze.join(staged_df, "complaint_id", "left_anti").count()
hash_mismatches = (
    bronze.alias("b").join(staged_df.alias("s"), "complaint_id")
    .where(F.col("b._bronze_record_hash") != F.col("s._bronze_record_hash")).count()
)
bronze_types = dict(bronze.dtypes)

check(target_failures, t["rows"] == EXPECTED_ROWS, f"table rows {t['rows']} == {EXPECTED_ROWS}")
check(target_failures, t["unique_ids"] == t["rows"], f"table unique complaint_id {t['unique_ids']} == rows {t['rows']}")
check(target_failures, t["null_ids"] == 0, f"table null complaint_id = {t['null_ids']}")
check(target_failures, t["strata"] == EXPECTED_STRATA, f"table strata {t['strata']} == {EXPECTED_STRATA}")
check(target_failures, missing_in_target == 0, f"source rows missing from table = {missing_in_target}")
check(target_failures, extra_in_target == 0, f"table rows not in this source file = {extra_in_target}")
check(target_failures, hash_mismatches == 0, f"content hash mismatches = {hash_mismatches}")
check(target_failures, all(bronze_types.get(c) == d for c, d in EXPECTED_COLUMNS),
      "all 24 source columns present with original types")

if target_failures:
    raise AssertionError(f"Bronze validation failed ({len(target_failures)}): {target_failures}")
print(f"\nBronze table {TARGET} validated: {t['rows']:,} rows, {t['strata']} strata.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Audit trail
# MAGIC Delta keeps a version history of every write. After the first run you should see `CREATE TABLE`
# MAGIC and one `MERGE`; each rerun adds a `MERGE` whose metrics show 0 inserted / 0 updated rows.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TARGET}")
        .select("version", "timestamp", "operation", "operationMetrics")
        .orderBy(F.col("version").desc())
        .limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next (not implemented in this notebook)
# MAGIC * **Silver** will read `bronze_complaints` (or its Change Data Feed), parse `date_received` into `DATE`,
# MAGIC   standardise categorical values, and keep `_sample_weight` for weighted estimates.
# MAGIC * **Gold** will aggregate Silver with weights; complaint **volumes** will come from the exact API monthly
# MAGIC   totals, never from sample counts.
