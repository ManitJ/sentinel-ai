# Databricks notebook source
# MAGIC %md
# MAGIC # Sentinel AI — 03 · Load exact CFPB population reference
# MAGIC
# MAGIC Loads the **exact** monthly population counts fetched from the CFPB search API into two managed
# MAGIC Delta tables. These — not the equal-size sample — are the source of truth for complaint volumes
# MAGIC and for every one-way breakdown (company, issue, state, response, timeliness, channel, tags).
# MAGIC
# MAGIC | Table | Grain | Content |
# MAGIC |---|---|---|
# MAGIC | `ref_cfpb_monthly_totals` | month | exact credit-reporting complaints received |
# MAGIC | `ref_cfpb_monthly_facets` | month × dimension × category | exact count; `category = NULL` + `is_residual = true` holds complaints with no value for that dimension |
# MAGIC
# MAGIC Source files (produced locally by `PYTHONPATH=src python -m lakehouse.reference_export`) are uploaded to
# MAGIC the `raw_files` volume. Loads are idempotent (`MERGE`, including deletion of rows no longer in the
# MAGIC file), and validated before and after writing.

# COMMAND ----------

import re

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sentinel")
dbutils.widgets.text("totals_path", "/Volumes/workspace/sentinel/raw_files/cfpb_api_monthly_totals.parquet")
dbutils.widgets.text("facets_path", "/Volumes/workspace/sentinel/raw_files/cfpb_api_monthly_facets.parquet")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
TOTALS_PATH = dbutils.widgets.get("totals_path").strip()
FACETS_PATH = dbutils.widgets.get("facets_path").strip()
for label, value in {"catalog": CATALOG, "schema": SCHEMA}.items():
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
for path in (TOTALS_PATH, FACETS_PATH):
    if not path.startswith("/Volumes/"):
        raise ValueError(f"Expected a Unity Catalog volume path, got {path!r}")


def fq(name):
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


SILVER = fq("silver_complaints")
EXPECTED_MONTHS = 22
EXPECTED_POPULATION = 10_500_160
EXPECTED_DIMENSIONS = ["company", "company_public_response", "company_response", "issue",
                       "state", "submitted_via", "tags", "timely"]
TOTALS_TYPES = {"month": "string", "population_complaints": "bigint", "product": "string",
                "fetched_at": "string"}
FACETS_TYPES = {"month": "string", "dimension": "string", "category": "string",
                "exact_count": "bigint", "is_residual": "boolean", "product": "string",
                "fetched_at": "string"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Read and validate the files (before writing)
# MAGIC * exact schemas; 22 contiguous months; totals sum to 10,500,160
# MAGIC * all 8 dimensions present every month; no negative counts; one residual at most per month × dimension;
# MAGIC   `category` is NULL **only** on residual rows; no duplicate keys
# MAGIC * **every month × dimension sums exactly to that month's total** (facets are complete)
# MAGIC * **reconciliation with Silver**: each month's total equals `stratum_population` in Silver
# MAGIC   (the sampling frame and the API agree exactly)

# COMMAND ----------

def check(failures, condition, message):
    print(f"[{'PASS' if condition else 'FAIL'}] {message}")
    if not condition:
        failures.append(message)


totals = spark.read.parquet(TOTALS_PATH)
facets = spark.read.parquet(FACETS_PATH)
failures = []

check(failures, dict(totals.dtypes) == TOTALS_TYPES, f"totals schema {dict(totals.dtypes)}")
check(failures, dict(facets.dtypes) == FACETS_TYPES, f"facets schema {dict(facets.dtypes)}")

months = [r["month"] for r in totals.select("month").orderBy("month").collect()]
expected_seq = []
if months:
    y, m = int(months[0][:4]), int(months[0][5:7])
    for _ in range(len(months)):
        expected_seq.append(f"{y}-{m:02d}")
        y, m = y + (m == 12), m % 12 + 1
check(failures, len(months) == EXPECTED_MONTHS, f"months {len(months)} == {EXPECTED_MONTHS}")
check(failures, months == expected_seq, "months are contiguous and unique")
total_sum = totals.agg(F.sum("population_complaints")).first()[0]
check(failures, total_sum == EXPECTED_POPULATION, f"population total {total_sum:,} == {EXPECTED_POPULATION:,}")

f = facets.agg(
    F.sum(F.when(F.col("exact_count") < 0, 1).otherwise(0)).alias("negative"),
    F.sum(F.when(F.col("category").isNull() != F.col("is_residual"), 1).otherwise(0)).alias("bad_null"),
    F.count(F.lit(1)).alias("rows"),
).first()
dupes = facets.groupBy("month", "dimension", "category").count().where("count > 1").count()
dims_per_month = facets.groupBy("month").agg(F.sort_array(F.collect_set("dimension")).alias("d")).collect()
mismatch = (
    facets.groupBy("month", "dimension").agg(F.sum("exact_count").alias("s"))
    .join(totals, "month").where(F.col("s") != F.col("population_complaints")).count()
)
silver_pop = spark.table(SILVER).groupBy("sample_stratum").agg(F.max("stratum_population").alias("pop"))
recon = (totals.join(silver_pop, totals.month == silver_pop.sample_stratum, "full_outer")
         .where(F.col("population_complaints").isNull() | F.col("pop").isNull()
                | (F.col("population_complaints") != F.col("pop"))).count())

check(failures, f["negative"] == 0, f"negative counts = {f['negative']}")
check(failures, f["bad_null"] == 0, f"NULL category only on residual rows (violations = {f['bad_null']})")
check(failures, dupes == 0, f"duplicate (month, dimension, category) keys = {dupes}")
check(failures, all(r["d"] == EXPECTED_DIMENSIONS for r in dims_per_month), "all 8 dimensions in every month")
check(failures, mismatch == 0, f"month x dimension sums != monthly total: {mismatch}")
check(failures, recon == 0, f"months where API total != Silver stratum_population: {recon}")
if failures:
    raise AssertionError(f"Reference validation failed: {failures}")
print(f"\nfacets: {f['rows']:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Idempotent load
# MAGIC `CREATE TABLE IF NOT EXISTS … AS SELECT … WHERE 1 = 0` creates an empty managed table with the
# MAGIC staged schema on the first run. `MERGE` then inserts new keys, updates changed rows (row hash),
# MAGIC and deletes keys no longer present (`WHEN NOT MATCHED BY SOURCE`). Keys use null-safe `<=>`
# MAGIC because residual rows have `category = NULL`. A rerun with the same files changes nothing.

# COMMAND ----------

def merge_sync(df, name, keys, comment):
    target = fq(name)
    staged = df.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in df.columns])), 256))
    view = f"staged_{name}"
    staged.createOrReplaceTempView(view)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} USING DELTA "
              f"TBLPROPERTIES (delta.enableChangeDataFeed = true) AS SELECT * FROM {view} WHERE 1 = 0")
    if dict(spark.table(target).dtypes) != dict(staged.dtypes):
        raise AssertionError(f"{target} schema differs from staged data; drop the table to rebuild it")
    spark.sql(f"COMMENT ON TABLE {target} IS '{comment}'")
    on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    metrics = spark.sql(f"""
        MERGE INTO {target} t USING {view} s ON {on}
        WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE""").first().asDict()
    print(f"{name}: {metrics}")
    return metrics


merge_sync(totals, "ref_cfpb_monthly_totals", ["month"],
           "EXACT monthly credit-reporting complaint totals from the CFPB search API (reference for all volume metrics).")
merge_sync(facets, "ref_cfpb_monthly_facets", ["month", "dimension", "category"],
           "EXACT monthly counts by dimension/category from CFPB API facets; category NULL with is_residual = no value.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Post-load reconciliation

# COMMAND ----------

post = []
for name, src in [("ref_cfpb_monthly_totals", totals), ("ref_cfpb_monthly_facets", facets)]:
    table = spark.table(fq(name)).drop("_row_hash")
    keys = ["month"] if name.endswith("totals") else ["month", "dimension", "category"]
    cond = [table[k].eqNullSafe(src[k]) for k in keys]
    only_table = table.join(src, cond, "left_anti").count()
    only_src = src.join(table, cond, "left_anti").count()
    check(post, table.count() == src.count(), f"{name}: rows {table.count():,} == file rows {src.count():,}")
    check(post, only_table == 0 and only_src == 0, f"{name}: key differences table/file = {only_table}/{only_src}")
if post:
    raise AssertionError(f"Post-load reconciliation failed: {post}")
print("\nReference tables loaded and reconciled.")
