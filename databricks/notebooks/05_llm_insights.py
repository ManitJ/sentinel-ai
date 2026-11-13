# Databricks notebook source
# MAGIC %md
# MAGIC # Sentinel AI — 05 · Evidence-grounded monthly insights
# MAGIC
# MAGIC Generates short monthly claims from `gold_insight_facts` and publishes **only claims that pass
# MAGIC deterministic validation** (`lib_insights.py`). Two generators write to the same tables:
# MAGIC
# MAGIC | generator | Model calls | Purpose |
# MAGIC |---|---|---|
# MAGIC | `template` | **0** | Deterministic fallback: the project works without LLM access |
# MAGIC | `llm` | ≤ `max_calls` per run | `system.ai.gpt-oss-120b` via `ai_query()` |
# MAGIC
# MAGIC **Safety defaults:** `dry_run = true` (preview only: **no model call and no table writes**),
# MAGIC `max_calls = 1`, one call per month at most, and months whose evidence is unchanged are served from
# MAGIC the cache (0 calls). Template-only run that writes tables: `dry_run = false`, `max_calls = 0`.
# MAGIC
# MAGIC **What the model sees:** only aggregate facts for one month — no complaint records, IDs, ZIP codes
# MAGIC or narratives; counts below 10 and their related facts are removed.
# MAGIC
# MAGIC **What is published:** accepted claims, each with the `fact_id`s it cites. Rejected claims are kept
# MAGIC for audit as a SHA-256 hash, length and rejection reasons only (their text is not stored unless
# MAGIC `store_rejected_text = true`). Raw model responses are never stored (hash and length only).
# MAGIC
# MAGIC Databricks Free Edition is quota-limited; exceeding the quota can stop workspace compute for the
# MAGIC rest of the day. Keep `max_calls` small.

# COMMAND ----------

# MAGIC %run ./lib_insights

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Parameters

# COMMAND ----------

import hashlib
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sentinel")
dbutils.widgets.text("months", "latest")            # "latest", or comma-separated YYYY-MM
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.text("max_calls", "1")
dbutils.widgets.dropdown("force_refresh", "false", ["true", "false"])
dbutils.widgets.dropdown("store_rejected_text", "false", ["true", "false"])

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
for label, value in {"catalog": CATALOG, "schema": SCHEMA}.items():
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
MAX_CALLS = int(dbutils.widgets.get("max_calls"))
FORCE_REFRESH = dbutils.widgets.get("force_refresh") == "true"
STORE_REJECTED_TEXT = dbutils.widgets.get("store_rejected_text") == "true"
MAX_CALLS_HARD_LIMIT = 22            # one per month in the dataset; never exceeded
MODEL_PARAMETERS = "named_struct('max_tokens', 6000, 'temperature', 0.0)"
if not 0 <= MAX_CALLS <= MAX_CALLS_HARD_LIMIT:
    raise ValueError(f"max_calls must be between 0 and {MAX_CALLS_HARD_LIMIT}")


def fq(name):
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


INSIGHTS = fq("gold_llm_insights")
CLAIMS = fq("gold_llm_insight_claims")
print(f"dry_run={DRY_RUN} max_calls={MAX_CALLS} force_refresh={FORCE_REFRESH} model={MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Load facts (aggregates only)

# COMMAND ----------

facts_df = spark.table(fq("gold_insight_facts"))
all_months = [r["month_label"] for r in spark.table(fq("gold_monthly_volume"))
              .select("month_label").orderBy("month_label").collect()]
requested = dbutils.widgets.get("months").strip()
MONTHS = [all_months[-1]] if requested == "latest" else [m.strip() for m in requested.split(",") if m.strip()]
unknown = [m for m in MONTHS if m not in all_months]
if unknown:
    raise ValueError(f"Unknown months: {unknown}")

FACT_FIELDS = ["fact_id", "fact_type", "period", "entity_type", "entity", "metric", "value", "unit",
               "measure_type", "ci95_low", "ci95_high", "is_reliable", "sample_n"]
facts = [r.asDict() for r in facts_df.select(*FACT_FIELDS).where(F.col("period").isin(MONTHS)).collect()]
# Entity names from every period, so mentions of uncited companies/states/issues are caught.
entity_pool = [r.asDict() for r in facts_df.select("entity", "entity_type").distinct().collect()]
print(f"months={MONTHS}  facts loaded={len(facts)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Evidence packs, cache check and plan
# MAGIC `evidence_hash` = SHA-256 of (month, evidence, model, prompt version). If a stored `llm` row has
# MAGIC the same hash (whatever its status), the month is skipped — no call. `force_refresh = true` overrides.

# COMMAND ----------

TABLE_DDL = [f"""CREATE TABLE IF NOT EXISTS {INSIGHTS} (
  month_label STRING, generator STRING, model STRING, prompt_version STRING, validator_version STRING,
  evidence_hash STRING, evidence_fact_count INT, excluded_small_count INT, excluded_suspicious INT,
  status STRING, claims_total INT, claims_accepted INT, claims_rejected INT, summary_markdown STRING,
  response_sha256 STRING, response_chars INT, error_message STRING, generated_at TIMESTAMP, _row_hash STRING)
USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""",
             f"""CREATE TABLE IF NOT EXISTS {CLAIMS} (
  month_label STRING, generator STRING, claim_index INT, status STRING, claim_text STRING,
  claim_text_sha256 STRING, claim_chars INT, fact_ids ARRAY<STRING>, rejection_reasons ARRAY<STRING>,
  evidence_hash STRING, model STRING, prompt_version STRING, validator_version STRING, _row_hash STRING)
USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""",
             f"COMMENT ON TABLE {INSIGHTS} IS 'Monthly insight runs (template and LLM): status, validation counts, model metadata and accepted-claim summary.'",
             f"COMMENT ON TABLE {CLAIMS} IS 'Per-claim validation results. Accepted claims store text and fact_ids; rejected claims store hash, length and reasons only by default.'"]

insights_exists = spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.gold_llm_insights")
cached = ({r["month_label"]: r["evidence_hash"] for r in spark.table(INSIGHTS)
           .where("generator = 'llm'").select("month_label", "evidence_hash").collect()}
          if insights_exists else {})

plan = {}
for month in MONTHS:
    evidence, excluded = select_evidence(facts, month)
    prompt = build_prompt(month, evidence)
    if len(prompt) > MAX_PROMPT_CHARS:
        raise ValueError(f"{month}: prompt {len(prompt):,} chars exceeds {MAX_PROMPT_CHARS:,}")
    ehash = evidence_hash(month, evidence)
    is_cached = cached.get(month) == ehash and not FORCE_REFRESH
    plan[month] = {"evidence": evidence, "excluded": excluded, "prompt": prompt, "hash": ehash,
                   "cached": is_cached}
    print(f"{month}: evidence={len(evidence)} excluded={excluded} prompt_chars={len(prompt):,} "
          f"(~{len(prompt) // 4:,} tokens) cached={is_cached}")

pending = [m for m in MONTHS if not plan[m]["cached"]]
to_call = [] if DRY_RUN else pending[:MAX_CALLS]
skipped_by_cap = [m for m in pending if m not in to_call and not DRY_RUN]
print(f"\nLLM calls this run: {len(to_call)}  months={to_call}  "
      f"(pending={pending}, over cap={skipped_by_cap}, dry_run={DRY_RUN})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Model call (skipped when `dry_run = true`)
# MAGIC One `ai_query` row per month to call, in a single query; `failOnError => false` returns errors per
# MAGIC row instead of failing the run. `temperature = 0`; output capped by `max_tokens`.

# COMMAND ----------

responses = {}
if to_call:
    assert len(to_call) <= MAX_CALLS <= MAX_CALLS_HARD_LIMIT
    spark.createDataFrame([(m, plan[m]["prompt"]) for m in to_call], "month_label string, prompt string") \
        .createOrReplaceTempView("sentinel_llm_requests")
    rows = spark.sql(f"""
        SELECT month_label,
               ai_query('{MODEL}', prompt, modelParameters => {MODEL_PARAMETERS}, failOnError => false) AS r
        FROM sentinel_llm_requests""").collect()          # single action = one call per row
    for row in rows:
        result = row["r"].asDict()
        error = result.pop("errorMessage", None)
        text = next(iter(result.values()), None)
        responses[row["month_label"]] = {"text": text if isinstance(text, str) else None, "error": error}
        print(f"{row['month_label']}: response_chars={len(text or '')} error={'yes' if error else 'no'}")
else:
    print("No model call made.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Validate and assemble rows
# MAGIC Template claims go through the **same validator** as model claims.

# COMMAND ----------

now = datetime.now(timezone.utc)
insight_rows, claim_rows, regenerated = [], [], []


def add_run(month, generator, status, error, results, response_text=None):
    p = plan[month]
    records = claim_records(results, keep_rejected_text=STORE_REJECTED_TEXT)
    accepted = sum(r["status"] == "accepted" for r in records)
    insight_rows.append((
        month, generator, MODEL if generator == "llm" else None, PROMPT_VERSION, VALIDATOR_VERSION,
        p["hash"], len(p["evidence"]), p["excluded"]["small_count"], p["excluded"]["suspicious_entity"],
        status, len(records), accepted, len(records) - accepted, summary_markdown(month, results),
        hashlib.sha256(response_text.encode()).hexdigest() if response_text else None,
        len(response_text) if response_text else None, (error or None) and str(error)[:300], now))
    for r in records:
        claim_rows.append((month, generator, r["claim_index"], r["status"], r["claim_text"],
                           r["claim_text_sha256"], r["claim_chars"], r["fact_ids"], r["rejection_reasons"],
                           p["hash"], MODEL if generator == "llm" else None, PROMPT_VERSION, VALIDATOR_VERSION))
    regenerated.append((month, generator))


for month in MONTHS:
    results = validate_claims(template_claims(month, plan[month]["evidence"]), plan[month]["evidence"], entity_pool)
    add_run(month, "template", month_status(None, results), None, results)

for month, response in responses.items():
    if response["error"]:
        add_run(month, "llm", "failed", f"ai_query error: {response['error']}", [], response["text"])
        continue
    status, error, results = evaluate_response(month, response["text"], plan[month]["evidence"], entity_pool)
    add_run(month, "llm", status, error, results, response["text"])

# insight_rows tuple positions: 0 month, 1 generator, 9 status, 10 total, 11 accepted, 12 rejected, 13 summary
display(spark.createDataFrame(
    [(r[0], r[1], r[9], r[10], r[11], r[12], r[13]) for r in insight_rows],
    "month string, generator string, status string, claims int, accepted int, rejected int, summary string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Idempotent writes
# MAGIC `MERGE` keyed on (month, generator) and (month, generator, claim_index). The row hash excludes
# MAGIC `generated_at`, so re-running unchanged template output changes nothing. Stale claim rows are
# MAGIC deleted only for the (month, generator) pairs regenerated in this run; cached LLM months are untouched.

# COMMAND ----------

INSIGHT_SCHEMA = ("month_label string, generator string, model string, prompt_version string, "
                  "validator_version string, evidence_hash string, evidence_fact_count int, "
                  "excluded_small_count int, excluded_suspicious int, status string, claims_total int, "
                  "claims_accepted int, claims_rejected int, summary_markdown string, response_sha256 string, "
                  "response_chars int, error_message string, generated_at timestamp")
CLAIM_SCHEMA = ("month_label string, generator string, claim_index int, status string, claim_text string, "
                "claim_text_sha256 string, claim_chars int, fact_ids array<string>, "
                "rejection_reasons array<string>, evidence_hash string, model string, prompt_version string, "
                "validator_version string")


def merge_rows(table, rows, schema, keys, hash_exclude=()):
    df = spark.createDataFrame(rows, schema)
    hashed = [c for c in df.columns if c not in hash_exclude]
    staged = df.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in hashed])), 256))
    staged.createOrReplaceTempView("sentinel_staged")
    scope = " OR ".join(f"(t.month_label = '{m}' AND t.generator = '{g}')" for m, g in regenerated)
    on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    metrics = spark.sql(f"""
        MERGE INTO {table} t USING sentinel_staged s ON {on}
        WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND ({scope}) THEN DELETE""").first().asDict()
    print(f"{table}: {metrics}")


if DRY_RUN:
    print("dry_run = true: nothing written. Set dry_run = false to write (max_calls = 0 for template only).")
else:
    for statement in TABLE_DDL:
        spark.sql(statement)
    merge_rows(INSIGHTS, insight_rows, INSIGHT_SCHEMA, ["month_label", "generator"], hash_exclude=("generated_at",))
    if claim_rows:
        merge_rows(CLAIMS, claim_rows, CLAIM_SCHEMA, ["month_label", "generator", "claim_index"])
    else:
        scope = " OR ".join(f"(month_label = '{m}' AND generator = '{g}')" for m, g in regenerated)
        spark.sql(f"DELETE FROM {CLAIMS} WHERE {scope}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Post-write checks

# COMMAND ----------

if DRY_RUN:
    dbutils.notebook.exit("dry_run: preview complete, no model call, no writes")

failures = []


def check(condition, message):
    print(f"[{'PASS' if condition else 'FAIL'}] {message}")
    if not condition:
        failures.append(message)


claims_tbl = spark.table(CLAIMS)
accepted = claims_tbl.where("status = 'accepted'")
orphan_ids = (accepted.select(F.explode("fact_ids").alias("fact_id")).distinct()
              .join(facts_df.select("fact_id"), "fact_id", "left_anti").count())
over_limit = accepted.groupBy("month_label", "generator").count().where(f"count > {MAX_CLAIMS}").count()
leaked = 0 if STORE_REJECTED_TEXT else claims_tbl.where("status = 'rejected' AND claim_text IS NOT NULL").count()
mismatch = (spark.table(INSIGHTS).alias("i")
            .join(claims_tbl.groupBy("month_label", "generator")
                  .agg(F.sum(F.when(F.col("status") == "accepted", 1).otherwise(0)).alias("a")).alias("c"),
                  ["month_label", "generator"], "left")
            .where(F.coalesce("a", F.lit(0)) != F.col("claims_accepted")).count())
check(orphan_ids == 0, f"every accepted claim cites existing gold_insight_facts ids (orphans={orphan_ids})")
check(over_limit == 0, f"at most {MAX_CLAIMS} accepted claims per month and generator")
check(leaked == 0, "rejected claim text not stored")
check(mismatch == 0, "insight claim counts match the claims table")
check(spark.table(INSIGHTS).where("generator = 'template' AND status <> 'verified'")
      .where(F.col("month_label").isin(MONTHS)).count() == 0, "template claims all validate")
if failures:
    raise AssertionError(f"Post-write checks failed: {failures}")

display(spark.table(INSIGHTS).where(F.col("month_label").isin(MONTHS))
        .select("month_label", "generator", "status", "claims_accepted", "claims_rejected", "summary_markdown")
        .orderBy("month_label", "generator"))
