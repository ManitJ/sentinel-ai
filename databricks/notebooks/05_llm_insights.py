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
# MAGIC **Safety defaults:** `dry_run = true` (preview only: **no model call, no schema change, no writes,
# MAGIC no view creation**), `max_calls = 1`, at most one call per month, and months whose evidence is
# MAGIC unchanged are served from the cache (0 calls). Template-only run that writes tables:
# MAGIC `dry_run = false`, `max_calls = 0` (use `months = all` to refresh every month).
# MAGIC
# MAGIC **What the model sees:** only aggregate facts for one month — no complaint records, IDs, ZIP codes
# MAGIC or narratives; counts below 10 and their related facts are removed; percentage changes on a base
# MAGIC below 100 are removed. **Provisional months** (≥ 1% of complaints still "In progress") also lose
# MAGIC response, public-response, timeliness and channel metrics, and their summaries carry a disclaimer.
# MAGIC
# MAGIC **History and publication.** The base tables `gold_llm_insights` and `gold_llm_insight_claims`
# MAGIC are an **append-only audit history**: rows are never deleted and their content is never rewritten.
# MAGIC Each row is keyed by (month, generator, prompt version, validator version, evidence hash); only
# MAGIC `publication_status`, `superseded_at` and `superseded_reason` change, when a newer version or newer
# MAGIC evidence supersedes a row. **Consumers must read the views** `gold_llm_insights_published` and
# MAGIC `gold_llm_insight_claims_published`, which expose only current, verified insights and their accepted
# MAGIC claims. Rejected claims are stored as a hash, length and reasons only (text only if
# MAGIC `store_rejected_text = true`); raw model responses are never stored.
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
dbutils.widgets.text("months", "latest")            # "latest", "all", or comma-separated YYYY-MM
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.text("max_calls", "1")
dbutils.widgets.dropdown("store_rejected_text", "false", ["true", "false"])

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
for label, value in {"catalog": CATALOG, "schema": SCHEMA}.items():
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
MAX_CALLS = int(dbutils.widgets.get("max_calls"))
STORE_REJECTED_TEXT = dbutils.widgets.get("store_rejected_text") == "true"
MAX_CALLS_HARD_LIMIT = 22            # one per month in the dataset; never exceeded
MODEL_PARAMETERS = "named_struct('max_tokens', 6000, 'temperature', 0.0)"
if not 0 <= MAX_CALLS <= MAX_CALLS_HARD_LIMIT:
    raise ValueError(f"max_calls must be between 0 and {MAX_CALLS_HARD_LIMIT}")


def fq(name):
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


INSIGHTS, CLAIMS = fq("gold_llm_insights"), fq("gold_llm_insight_claims")
INSIGHTS_VIEW, CLAIMS_VIEW = fq("gold_llm_insights_published"), fq("gold_llm_insight_claims_published")
RUN_AT = datetime.now(timezone.utc).replace(microsecond=0)
RUN_AT_SQL = f"timestamp_seconds({int(RUN_AT.timestamp())})"   # UTC epoch; independent of session time zone
print(f"dry_run={DRY_RUN} max_calls={MAX_CALLS} model={MODEL} "
      f"prompt={PROMPT_VERSION} validator={VALIDATOR_VERSION} run_at={RUN_AT.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Load facts (aggregates only)

# COMMAND ----------

facts_df = spark.table(fq("gold_insight_facts"))
all_months = [r["month_label"] for r in spark.table(fq("gold_monthly_volume"))
              .select("month_label").orderBy("month_label").collect()]
requested = dbutils.widgets.get("months").strip()
if requested == "latest":
    MONTHS = [all_months[-1]]
elif requested == "all":
    MONTHS = list(all_months)
else:
    MONTHS = [m.strip() for m in requested.split(",") if m.strip()]
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
# MAGIC ## 3 · Existing history (read-only), evidence packs and cache
# MAGIC An LLM month is cached when a row with the **current versions and the same evidence hash** exists
# MAGIC (any status). Regenerating under identical evidence and versions is not possible: it would need to
# MAGIC overwrite the stored audit row (see docs/llm_insights.md, "Same-version regeneration").

# COMMAND ----------

def table_exists(name):
    return spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{name}")


def read_existing(table, name, extra):
    """Stored rows (key, status, hash, lifecycle) — tolerant of tables without the new columns."""
    if not table_exists(name):
        return []
    df = spark.table(table)
    cols = [F.col(c) if c in df.columns else F.lit(None).cast("string").alias(c)
            for c in list(KEY_FIELDS) + extra + ["publication_status", "_row_hash"]]
    return [r.asDict() for r in df.select(*cols).collect()]


existing = read_existing(INSIGHTS, "gold_llm_insights", ["status"])
existing_keys = {row_key(r) for r in existing}
print(f"stored insight rows: {len(existing)}")

plan = {}
for month in MONTHS:
    context = month_context(facts, month)
    evidence, excluded = select_evidence(facts, month)
    prompt = build_prompt(month, evidence, context["provisional"])
    if len(prompt) > MAX_PROMPT_CHARS:
        raise ValueError(f"{month}: prompt {len(prompt):,} chars exceeds {MAX_PROMPT_CHARS:,}")
    ehash = evidence_hash(month, evidence)
    cached = llm_cache_hit(existing_keys, month, ehash)
    plan[month] = {"evidence": evidence, "excluded": excluded, "prompt": prompt, "hash": ehash,
                   "cached": cached, "provisional": context["provisional"]}
    share = context["in_progress_share"]
    print(f"{month}: provisional={context['provisional']} (in progress "
          f"{'n/a' if share is None else f'{share:.1%}'}) evidence={len(evidence)} excluded={excluded} "
          f"prompt_chars={len(prompt):,} (~{len(prompt) // 4:,} tokens) llm_cached={cached}")

pending = [m for m in MONTHS if not plan[m]["cached"]]
to_call = [] if DRY_RUN else pending[:MAX_CALLS]
print(f"\nLLM calls this run: {len(to_call)}  months={to_call}  (pending={pending}, dry_run={DRY_RUN})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Model call (skipped when `dry_run = true` or `max_calls = 0`)
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
# MAGIC ## 5 · Validate and assemble candidate rows
# MAGIC Template claims go through the **same validator** as model claims.

# COMMAND ----------

candidates, candidate_claims = [], {}


def add_run(month, generator, status, error, results, response_text=None):
    p = plan[month]
    records = claim_records(results, keep_rejected_text=STORE_REJECTED_TEXT)
    accepted = sum(r["status"] == "accepted" for r in records)
    row = {
        "month_label": month, "generator": generator, "model": MODEL if generator == "llm" else None,
        "prompt_version": PROMPT_VERSION, "validator_version": VALIDATOR_VERSION, "evidence_hash": p["hash"],
        "evidence_fact_count": len(p["evidence"]), "excluded_small_count": p["excluded"]["small_count"],
        "excluded_suspicious": p["excluded"]["suspicious_entity"], "status": status,
        "claims_total": len(records), "claims_accepted": accepted, "claims_rejected": len(records) - accepted,
        "summary_markdown": summary_markdown(month, results, p["provisional"]),
        "response_sha256": hashlib.sha256(response_text.encode()).hexdigest() if response_text else None,
        "response_chars": len(response_text) if response_text else None,
        "error_message": (error or None) and str(error)[:300], "generated_at": RUN_AT,
    }
    row["_row_hash"] = content_hash(row)
    candidates.append(row)
    claims = []
    for r in records:
        claim = {"month_label": month, "generator": generator, "claim_index": r["claim_index"],
                 "status": r["status"], "claim_text": r["claim_text"], "claim_text_sha256": r["claim_text_sha256"],
                 "claim_chars": r["claim_chars"], "fact_ids": r["fact_ids"],
                 "rejection_reasons": r["rejection_reasons"], "evidence_hash": p["hash"],
                 "model": row["model"], "prompt_version": PROMPT_VERSION, "validator_version": VALIDATOR_VERSION}
        claim["_row_hash"] = content_hash(claim)
        claims.append(claim)
    candidate_claims[row_key(row)] = claims


for month in MONTHS:
    results = validate_claims(template_claims(month, plan[month]["evidence"]), plan[month]["evidence"], entity_pool)
    add_run(month, "template", month_status(None, results), None, results)

for month, response in responses.items():
    if response["error"]:
        add_run(month, "llm", "failed", f"ai_query error: {response['error']}", [], response["text"])
        continue
    status, error, results = evaluate_response(month, response["text"], plan[month]["evidence"], entity_pool)
    add_run(month, "llm", status, error, results, response["text"])

display(spark.createDataFrame(
    [(c["month_label"], c["generator"], c["status"], c["claims_total"], c["claims_accepted"],
      c["claims_rejected"], c["summary_markdown"]) for c in candidates],
    "month string, generator string, status string, claims int, accepted int, rejected int, summary string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Publication plan (read-only)
# MAGIC Every stored row whose prompt/validator version is outdated — or whose evidence is older than the
# MAGIC evidence computed now for a processed month — is marked **superseded**. New rows are inserted;
# MAGIC identical rows are left alone; a row with the same key but different content is a **conflict** and
# MAGIC stops the run before any write (history is never overwritten).

# COMMAND ----------

current_hashes = {(m, g): plan[m]["hash"] for m in MONTHS for g in ("template", "llm")}
publication = plan_publication(existing, candidates, current_hashes)
after = current_rows_after(existing, publication)
print({k: len(v) for k, v in publication.items()})
for item in publication["supersede"]:
    print(f"  supersede {item['month_label']} {item['generator']} "
          f"{item['prompt_version']}/{item['validator_version']}: {item['superseded_reason']}")
multiple = {k: v for k, v in after.items() if len(v) > 1}
months_without_template = sorted(set(all_months) - {m for (m, g) in after if g == "template"})
if publication["conflicts"]:
    raise AssertionError(f"Refusing to overwrite stored rows with different content: {publication['conflicts']}. "
                         "Bump PROMPT_VERSION/VALIDATOR_VERSION or add generation-level history first.")
if multiple:
    raise AssertionError(f"Plan would leave several current rows for: {multiple}")
if months_without_template:
    print(f"NOTE: no current template insight after this run for {months_without_template} "
          f"(run with months = all to refresh them).")

if DRY_RUN:
    dbutils.notebook.exit(f"dry_run: no model call, no schema change, no writes. Plan: "
                          f"{ {k: len(v) for k, v in publication.items()} }")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Writes (only when `dry_run = false`)
# MAGIC Order is chosen so that stopping at any point is safe and a rerun completes the work:
# MAGIC 1. Create tables if missing; **add** missing lifecycle columns (never drop or rewrite).
# MAGIC 2. Create/replace the published views (they filter on `publication_status = 'published'`, so
# MAGIC    rows without a status — e.g. pre-migration rows — are never exposed).
# MAGIC 3. Supersede outdated insight rows, then their claims.
# MAGIC 4. Recover any current rows still missing a `publication_status`.
# MAGIC 5. Insert new claims, then new insights (insert-only `MERGE`; existing keys untouched).
# MAGIC No `DELETE`, no `UPDATE SET *`, no overwrite.

# COMMAND ----------

KEY_ON = lambda t, s: " AND ".join(f"{t}.`{k}` <=> {s}.`{k}`" for k in KEY_FIELDS)  # noqa: E731
INSIGHT_COLUMNS = [
    ("month_label", "string"), ("generator", "string"), ("model", "string"), ("prompt_version", "string"),
    ("validator_version", "string"), ("evidence_hash", "string"), ("evidence_fact_count", "int"),
    ("excluded_small_count", "int"), ("excluded_suspicious", "int"), ("status", "string"),
    ("claims_total", "int"), ("claims_accepted", "int"), ("claims_rejected", "int"),
    ("summary_markdown", "string"), ("response_sha256", "string"), ("response_chars", "int"),
    ("error_message", "string"), ("generated_at", "timestamp"), ("_row_hash", "string"),
    ("publication_status", "string"), ("superseded_at", "timestamp"), ("superseded_reason", "string")]
CLAIM_COLUMNS = [
    ("month_label", "string"), ("generator", "string"), ("claim_index", "int"), ("status", "string"),
    ("claim_text", "string"), ("claim_text_sha256", "string"), ("claim_chars", "int"),
    ("fact_ids", "array<string>"), ("rejection_reasons", "array<string>"), ("evidence_hash", "string"),
    ("model", "string"), ("prompt_version", "string"), ("validator_version", "string"), ("_row_hash", "string"),
    ("publication_status", "string"), ("superseded_at", "timestamp")]
NEW_INSIGHT_COLUMNS = {"publication_status": "STRING", "superseded_at": "TIMESTAMP", "superseded_reason": "STRING"}
NEW_CLAIM_COLUMNS = {"publication_status": "STRING", "superseded_at": "TIMESTAMP"}


def ddl(columns):
    return ", ".join(f"`{n}` {t.upper()}" for n, t in columns)


def schema(columns):
    return ", ".join(f"{n} {t}" for n, t in columns)


rows_before = {name: spark.table(t).count() if table_exists(name) else 0
               for name, t in (("gold_llm_insights", INSIGHTS), ("gold_llm_insight_claims", CLAIMS))}

# 7.1 Tables and additive migration (idempotent).
spark.sql(f"CREATE TABLE IF NOT EXISTS {INSIGHTS} ({ddl(INSIGHT_COLUMNS)}) USING DELTA "
          "TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {CLAIMS} ({ddl(CLAIM_COLUMNS)}) USING DELTA "
          "TBLPROPERTIES (delta.enableChangeDataFeed = true)")
for table, new_columns in ((INSIGHTS, NEW_INSIGHT_COLUMNS), (CLAIMS, NEW_CLAIM_COLUMNS)):
    missing = {c: t for c, t in new_columns.items() if c not in spark.table(table).columns}
    if missing:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(f'`{c}` {t}' for c, t in missing.items())})")
        print(f"{table}: added columns {sorted(missing)}")
spark.sql(f"COMMENT ON TABLE {INSIGHTS} IS 'Append-only audit history of insight runs (template and LLM). "
          f"Read gold_llm_insights_published for current insights; superseded rows are kept for audit.'")
spark.sql(f"COMMENT ON TABLE {CLAIMS} IS 'Append-only audit history of claim validation. Read "
          f"gold_llm_insight_claims_published for current accepted claims; rejected claim text is not stored by default.'")

# 7.2 Published views (safe to create early: they only expose publication_status = published).
spark.sql(f"""CREATE OR REPLACE VIEW {INSIGHTS_VIEW}
  COMMENT 'Current, verified insights only (one per month and generator).' AS
  SELECT * EXCEPT (_row_hash, superseded_at, superseded_reason) FROM {INSIGHTS}
  WHERE publication_status = 'published' AND status = 'verified'""")
spark.sql(f"""CREATE OR REPLACE VIEW {CLAIMS_VIEW}
  COMMENT 'Accepted claims of currently published insights (same month, generator, versions and evidence).' AS
  SELECT c.* EXCEPT (_row_hash, superseded_at) FROM {CLAIMS} c
  JOIN {INSIGHTS} i ON {KEY_ON('c', 'i')}
  WHERE i.publication_status = 'published' AND i.status = 'verified'
    AND c.publication_status = 'published' AND c.status = 'accepted'""")


def merge(sql):
    return spark.sql(sql).first().asDict()


# 7.3 Supersede outdated insight rows (only rows not yet superseded; content untouched).
if publication["supersede"]:
    spark.createDataFrame(
        [(*[r[k] for k in KEY_FIELDS], r["superseded_reason"]) for r in publication["supersede"]],
        f"{schema([(k, 'string') for k in KEY_FIELDS])}, superseded_reason string",
    ).createOrReplaceTempView("sentinel_supersede")
    print("supersede insights:", merge(f"""
        MERGE INTO {INSIGHTS} t USING sentinel_supersede s ON {KEY_ON('t', 's')}
        WHEN MATCHED AND (t.publication_status IS NULL OR t.publication_status <> 'superseded') THEN UPDATE SET
          publication_status = 'superseded', superseded_at = {RUN_AT_SQL},
          superseded_reason = s.superseded_reason"""))

# Claims follow their parent: every claim of a superseded insight becomes superseded (idempotent).
# Sources are staged as temp views (one row per parent key, so each claim matches one source row).
spark.table(INSIGHTS).where("publication_status = 'superseded'").select(*KEY_FIELDS).distinct() \
    .createOrReplaceTempView("sentinel_superseded_parents")
print("supersede claims:", merge(f"""
    MERGE INTO {CLAIMS} c USING sentinel_superseded_parents s ON {KEY_ON('c', 's')}
    WHEN MATCHED AND (c.publication_status IS NULL OR c.publication_status <> 'superseded') THEN UPDATE SET
      publication_status = 'superseded', superseded_at = {RUN_AT_SQL}"""))

# 7.4 Recovery: current rows (and their claims) that still have no publication_status.
if publication["set_status"]:
    spark.createDataFrame(
        [(*[r[k] for k in KEY_FIELDS], r["publication_status"]) for r in publication["set_status"]],
        f"{schema([(k, 'string') for k in KEY_FIELDS])}, publication_status string",
    ).createOrReplaceTempView("sentinel_set_status")
    print("set status:", merge(f"""
        MERGE INTO {INSIGHTS} t USING sentinel_set_status s ON {KEY_ON('t', 's')}
        WHEN MATCHED AND t.publication_status IS NULL THEN UPDATE SET publication_status = s.publication_status"""))
spark.table(INSIGHTS).where("publication_status IN ('published', 'not_published')") \
    .select(*KEY_FIELDS, F.col("publication_status").alias("parent_status")) \
    .createOrReplaceTempView("sentinel_current_parents")
print("claim status recovery:", merge(f"""
    MERGE INTO {CLAIMS} c USING sentinel_current_parents s ON {KEY_ON('c', 's')}
    WHEN MATCHED AND c.publication_status IS NULL THEN UPDATE SET publication_status =
      CASE WHEN s.parent_status = 'published' AND c.status = 'accepted' THEN 'published' ELSE 'not_published' END"""))

# 7.5 Insert new claims first, then their insight rows (so claims never appear without a parent).
new_claims = []
for row in publication["insert"]:
    for claim in candidate_claims[row_key(row)]:
        new_claims.append({**claim, "superseded_at": None,
                           "publication_status": claim_publication_status(claim["status"], row["publication_status"])})
if new_claims:
    spark.createDataFrame([tuple(c[n] for n, _ in CLAIM_COLUMNS) for c in new_claims], schema(CLAIM_COLUMNS)) \
        .createOrReplaceTempView("sentinel_new_claims")
    print("insert claims:", merge(f"""
        MERGE INTO {CLAIMS} t USING sentinel_new_claims s ON {KEY_ON('t', 's')} AND t.claim_index = s.claim_index
        WHEN NOT MATCHED THEN INSERT *"""))
if publication["insert"]:
    spark.createDataFrame([tuple(r[n] for n, _ in INSIGHT_COLUMNS) for r in publication["insert"]],
                          schema(INSIGHT_COLUMNS)).createOrReplaceTempView("sentinel_new_insights")
    print("insert insights:", merge(f"""
        MERGE INTO {INSIGHTS} t USING sentinel_new_insights s ON {KEY_ON('t', 's')}
        WHEN NOT MATCHED THEN INSERT *"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Post-write checks

# COMMAND ----------

failures = []


def check(condition, message):
    print(f"[{'PASS' if condition else 'FAIL'}] {message}")
    if not condition:
        failures.append(message)


ins, cl = spark.table(INSIGHTS), spark.table(CLAIMS)
pub_ins, pub_cl = spark.table(INSIGHTS_VIEW), spark.table(CLAIMS_VIEW)
key_cols = list(KEY_FIELDS)

check(ins.count() >= rows_before["gold_llm_insights"] and cl.count() >= rows_before["gold_llm_insight_claims"],
      "no rows deleted (row counts never decrease)")
check(ins.where("publication_status IS NULL").count() == 0, "every insight row has a publication_status")
check(cl.where("publication_status IS NULL").count() == 0, "every claim row has a publication_status")
check(ins.groupBy(*key_cols).count().where("count > 1").count() == 0, "insight keys unique (no duplicates)")
check(cl.groupBy(*key_cols, "claim_index").count().where("count > 1").count() == 0, "claim keys unique")
check(ins.where("publication_status <> 'superseded'").groupBy("month_label", "generator").count()
      .where("count > 1").count() == 0, "at most one current row per month and generator")
check(ins.where(f"publication_status <> 'superseded' AND (prompt_version <> '{PROMPT_VERSION}' "
                f"OR validator_version <> '{VALIDATOR_VERSION}')").count() == 0,
      "no current row with an outdated prompt/validator version")
check(ins.where("publication_status = 'superseded' AND status = 'verified' AND summary_markdown IS NULL").count() == 0,
      "superseded verified rows keep their historical summaries")
orphan_published = (cl.where("publication_status = 'published'").join(
    ins.where("publication_status = 'published'").select(*key_cols), key_cols, "left_anti").count())
check(orphan_published == 0, "every published claim belongs to a published insight")
check(cl.where("publication_status = 'published' AND status <> 'accepted'").count() == 0,
      "only accepted claims are published")
stale_claims = (cl.where("publication_status <> 'superseded'").join(
    ins.where("publication_status = 'superseded'").select(*key_cols), key_cols, "left_semi").count())
check(stale_claims == 0, "claims of superseded insights are superseded")
check(pub_cl.count() == (pub_ins.agg(F.coalesce(F.sum("claims_accepted"), F.lit(0))).first()[0]),
      "published claims = accepted claims of published insights")
orphan_ids = (pub_cl.select(F.explode("fact_ids").alias("fact_id")).distinct()
              .join(facts_df.select("fact_id"), "fact_id", "left_anti").count())
check(orphan_ids == 0, f"published claims cite existing gold_insight_facts ids (orphans={orphan_ids})")
check(pub_cl.groupBy("month_label", "generator").count().where(f"count > {MAX_CLAIMS}").count() == 0,
      f"at most {MAX_CLAIMS} published claims per month and generator")
if not STORE_REJECTED_TEXT:
    check(cl.where("status = 'rejected' AND claim_text IS NOT NULL").count() == 0, "rejected claim text not stored")
check(pub_ins.where(F.col("generator") == "template").where(F.col("month_label").isin(MONTHS)).count() == len(MONTHS),
      "every processed month has a published template insight")
if failures:
    raise AssertionError(f"Post-write checks failed: {failures}")

display(ins.groupBy("generator", "prompt_version", "validator_version", "publication_status").count()
        .orderBy("generator", "prompt_version", "publication_status"))
display(pub_ins.where(F.col("month_label").isin(MONTHS))
        .select("month_label", "generator", "status", "claims_accepted", "summary_markdown")
        .orderBy("month_label", "generator"))
