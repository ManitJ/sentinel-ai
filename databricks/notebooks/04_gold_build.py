# Databricks notebook source
# MAGIC %md
# MAGIC # Sentinel AI — 04 · Gold analytics
# MAGIC
# MAGIC Builds analytics-ready managed Delta tables from `silver_complaints` (stratified sample) and the
# MAGIC exact API reference tables (`ref_cfpb_monthly_totals`, `ref_cfpb_monthly_facets`, notebook 03).
# MAGIC
# MAGIC ## Two kinds of numbers — never mixed
# MAGIC | `measure_type` | Source | Used for |
# MAGIC |---|---|---|
# MAGIC | **exact** | CFPB API population counts (complete facets) | volumes, growth, and every one-way breakdown: company, issue, state, response, timeliness, channel, public response, tags |
# MAGIC | **estimated** | weighted stratified sample (`sample_weight`) | only what the API cannot give exactly: company × response type, company × timeliness, sub-issues — always with SE, 95% CI, sample size and a reliability flag |
# MAGIC
# MAGIC **Why this matters:** every month has the same sample size (2,273) by design, so raw sample counts say
# MAGIC nothing about volume, and unweighted pooled shares over-represent low-volume months. Volumes therefore
# MAGIC come only from the API; pooled estimates use weights `N_h / n_h`.
# MAGIC
# MAGIC ## Estimator (stratified SRS, strata = months)
# MAGIC `R = Σ_h w_h a_h / Σ_h w_h b_h` (a = domain-and-category sample count, b = domain sample count).
# MAGIC Variance by Taylor linearisation with finite-population correction
# MAGIC `Var = Σ_h N_h² (1 − n_h/N_h) s²_h / n_h`; 95% CI = Wilson interval with effective n = R(1−R)/Var.
# MAGIC **Reliable** (project rule) = ≥ 30 domain sample records **and** CI width ≤ 0.20.
# MAGIC `gold_estimator_calibration` checks the estimator against exact API shares (expected ≈ 95% coverage).
# MAGIC
# MAGIC The same maths is implemented and unit-tested in `src/lakehouse/gold_metrics.py`; section 9 fails the
# MAGIC run if this notebook's results differ from that reference on any table fingerprint.

# COMMAND ----------

import re
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sentinel")
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
for label, value in {"catalog": CATALOG, "schema": SCHEMA}.items():
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")


def fq(name):
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Constants (identical to `gold_metrics.py` / `silver_rules.py`; enforced by unit tests)

# COMMAND ----------

Z95 = 1.959963984540054
MIN_DOMAIN_N = 30
MAX_CI_WIDTH = 0.20
COVERAGE_MIN_EXPECTED = 10
COVERAGE_FAIL_BELOW = 0.90

EXACT_DIMENSIONS = ["issue", "timely", "company_response", "submitted_via", "company",
                    "state", "company_public_response", "tags"]
RESPONSE_DIMENSIONS = ["company_response", "timely", "submitted_via",
                       "company_public_response", "tags"]
COMPANY_ESTIMATE_DIMENSIONS = ["company_response", "timely"]
YEAR_PERIODS = {"2025": "2025", "2026": "2026-YTD"}
PERIOD_BOUNDS = {"ALL": ("2025-01", "2026-11"), "2025": ("2025-01", "2026-01"),
                 "2026-YTD": ("2026-01", "2026-11")}
TOP_N = {"company": 10, "state": 10}
SUB_ISSUE_NONE = "(no sub-issue)"

ISSUE_GROUPS = {
    "Problem with a company's investigation into an existing problem":
        "Problem with a company's investigation into an existing problem or issue",
    "Problem with a company's investigation into an existing issue":
        "Problem with a company's investigation into an existing problem or issue",
}
ISSUE_GROUP_NOTE = ("Grouped for analysis only: two CFPB issue labels with near-identical "
                    "wording that co-occur in every month. Original labels are preserved "
                    "in the issue column.")

US_STATES = [
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA", "ID", "IL",
    "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND",
    "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY",
]
DISTRICT = ["DC"]
TERRITORIES = ["AS", "FM", "GU", "MH", "MP", "PR", "PW", "UM", "VI"]
MILITARY = ["AA", "AE", "AP"]
STATE_NAME_TO_CODE = {"UNITED STATES MINOR OUTLYING ISLANDS": "UM"}

EXPECTED_GOLD = {
    "gold_monthly_volume": {"rows": 22, "sum_population": 10500160, "sum_mom_pct": 0.767065},
    "gold_company_monthly": {"rows": 17594, "sum_complaints": 10500160},
    "gold_issue_monthly": {"rows": 182, "sum_complaints": 10500160},
    "gold_state_monthly": {"rows": 1326, "sum_complaints": 10500160},
    "gold_response_monthly": {"rows": 538, "sum_complaints": 52500800},
    "gold_issue_category_map": {"rows": 13, "combined": 2},
    "gold_company_response_estimates": {"rows": 5453, "reliable": 212, "sum_est_share": 1558.0,
                                        "sum_std_error": 37.575572},
    "gold_sub_issue_estimates": {"rows": 713, "reliable": 713, "sum_est_share": 23.0,
                                 "sum_std_error": 1.357213},
    "gold_estimator_calibration": {"rows": 19640, "qc_comparisons": 974, "inside_ci95": 924},
    "gold_insight_facts": {"rows": 4284, "unique_fact_ids": 4284, "estimated": 925,
                           "sum_value": 85940772.88},
}
FLOAT_TOLERANCE = {"sum_mom_pct": 1e-6, "sum_est_share": 1e-6, "sum_std_error": 1e-6, "sum_value": 0.02}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Inputs

# COMMAND ----------

silver = spark.table(fq("silver_complaints"))
totals = (spark.table(fq("ref_cfpb_monthly_totals")).drop("_row_hash")
          .withColumnRenamed("month", "month_label"))
facets = (spark.table(fq("ref_cfpb_monthly_facets")).drop("_row_hash")
          .withColumnRenamed("month", "month_label"))
FETCHED_AT = totals.agg(F.max("fetched_at")).first()[0]
MONTHS = [r["month_label"] for r in totals.select("month_label").orderBy("month_label").collect()]
FIRST_MONTH = MONTHS[0]

# n_h = sample records in month h, N_h = population in month h. Spark column names are
# case-insensitive, so these MUST NOT be called "n_h" and "N_h" (AMBIGUOUS_REFERENCE).
strata = silver.groupBy(F.col("sample_stratum").alias("stratum")).agg(
    F.count(F.lit(1)).alias("sample_n_h"), F.max("stratum_population").alias("population_n_h"))


def month_date(col):
    return F.to_date(F.concat(col, F.lit("-01")))


def label_to_date_map(mapping, index):
    """PERIOD_BOUNDS -> CASE expression giving period start (index 0) or end (index 1)."""
    expr = None
    for period, bounds in mapping.items():
        value = month_date(F.lit(bounds[index]))
        expr = F.when(F.col("period") == period, value) if expr is None else expr.when(F.col("period") == period, value)
    month_value = month_date(F.col("period"))
    return expr.otherwise(month_value if index == 0 else F.add_months(month_value, 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · `gold_monthly_volume` — exact volumes and growth
# MAGIC MoM / YoY = change vs previous month / same month a year earlier (null when unavailable);
# MAGIC 3-month rolling average needs three months. `sample_size` is shown only for transparency.

# COMMAND ----------

w = Window.orderBy("month_label")
sample_sizes = silver.groupBy(F.col("sample_stratum").alias("month_label")).agg(F.count(F.lit(1)).alias("sample_size"))
n, prev, prev_year = F.col("population_complaints"), F.lag("population_complaints", 1).over(w), F.lag("population_complaints", 12).over(w)
gold_volume = (
    totals.join(sample_sizes, "month_label", "left")
    .withColumn("prev", prev).withColumn("prev_year", prev_year)
    .withColumn("win_n", F.count("population_complaints").over(w.rowsBetween(-2, 0)))
    .withColumn("win_avg", F.avg("population_complaints").over(w.rowsBetween(-2, 0)))
    .select(
        month_date(F.col("month_label")).alias("month"), "month_label", "population_complaints",
        (n - F.col("prev")).alias("mom_change"),
        F.when(F.col("prev") != 0, (n - F.col("prev")) / F.col("prev")).alias("mom_pct_change"),
        (n - F.col("prev_year")).alias("yoy_change"),
        F.when(F.col("prev_year") != 0, (n - F.col("prev_year")) / F.col("prev_year")).alias("yoy_pct_change"),
        F.when(F.col("win_n") == 3, F.col("win_avg")).alias("rolling_3m_avg"),
        F.coalesce("sample_size", F.lit(0)).cast("bigint").alias("sample_size"),
        (F.coalesce("sample_size", F.lit(0)) / n).alias("sample_fraction"),
        F.lit("exact").alias("measure_type"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Exact one-way breakdowns (company, issue, state, response dimensions)
# MAGIC From API facets: exact count, share of the month's total, rank (residual "no value" rows unranked),
# MAGIC previous-month count (0 if the category had no complaints that month), MoM change.

# COMMAND ----------

def exact_breakdown(dimension):
    f = (facets.where(F.col("dimension") == dimension)
         .select("month_label", "category", "is_residual", F.col("exact_count").alias("complaints")))
    p = f.select(F.date_format(F.add_months(month_date(F.col("month_label")), 1), "yyyy-MM").alias("p_month"),
                 F.col("category").alias("p_category"), F.col("complaints").alias("p_count"))
    rank_w = Window.partitionBy("month_label", "is_residual").orderBy(F.col("complaints").desc(), F.col("category").asc())
    joined = (f.join(totals.select("month_label", "population_complaints"), "month_label")
              .join(p, (F.col("month_label") == F.col("p_month")) & F.col("category").eqNullSafe(F.col("p_category")), "left"))
    prev_count = F.coalesce(F.col("p_count"), F.when(F.col("month_label") != FIRST_MONTH, F.lit(0).cast("bigint")))
    return joined.select(
        month_date(F.col("month_label")).alias("month"), "month_label", "category", "is_residual", "complaints",
        (F.col("complaints") / F.col("population_complaints")).alias("share_of_month"),
        F.when(~F.col("is_residual"), F.row_number().over(rank_w)).alias("rank_in_month"),
        prev_count.alias("prev_month_complaints"),
        (F.col("complaints") - prev_count).alias("mom_change"),
        F.when(prev_count != 0, (F.col("complaints") - prev_count) / prev_count).alias("mom_pct_change"),
        F.lit("exact").alias("measure_type"),
    )


breakdowns = {d: exact_breakdown(d) for d in EXACT_DIMENSIONS}

issue_map = (facets.where((F.col("dimension") == "issue") & ~F.col("is_residual"))
             .select(F.col("category").alias("issue")).distinct())
group_expr = reduce(lambda acc, kv: acc.when(F.col("issue") == kv[0], F.lit(kv[1])),
                    list(ISSUE_GROUPS.items())[1:],
                    F.when(F.col("issue") == list(ISSUE_GROUPS)[0], F.lit(list(ISSUE_GROUPS.values())[0])))
combined = F.col("issue").isin(list(ISSUE_GROUPS))
gold_issue_map = issue_map.select(
    "issue", F.coalesce(group_expr, F.col("issue")).alias("issue_group"),
    combined.alias("is_combined_group"),
    F.when(combined, F.lit(ISSUE_GROUP_NOTE)).alias("note"))

gold_company = breakdowns["company"].withColumnRenamed("category", "company")
gold_issue = (breakdowns["issue"].withColumnRenamed("category", "issue")
              .join(gold_issue_map.select("issue", "issue_group", "is_combined_group"), "issue", "left"))

state_code = reduce(lambda acc, kv: F.when(F.col("state") == kv[0], F.lit(kv[1])).otherwise(acc),
                    STATE_NAME_TO_CODE.items(), F.col("state"))
gold_state = (breakdowns["state"].withColumnRenamed("category", "state")
              .withColumn("state_code", state_code)
              .withColumn("state_category",
                          F.when(F.col("state_code").isNull(), F.lit(None).cast("string"))
                          .when(F.col("state_code").isin(US_STATES), "state")
                          .when(F.col("state_code").isin(DISTRICT), "district")
                          .when(F.col("state_code").isin(TERRITORIES), "territory")
                          .when(F.col("state_code").isin(MILITARY), "military").otherwise("unknown")))
gold_response = reduce(lambda a, b: a.unionByName(b),
                       [breakdowns[d].withColumn("dimension", F.lit(d)) for d in RESPONSE_DIMENSIONS])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Stratified estimator (shared by sections 6 and 7)
# MAGIC Works purely on counts per stratum (weights are constant within a month), so it is exact,
# MAGIC scalable and identical to the Python reference.

# COMMAND ----------

def estimate(obs):
    """obs columns: period, dimension, domain, category, stratum (one row per sample record)."""
    keys = ["period", "dimension", "domain", "category"]
    cats = obs.select("dimension", "category").distinct()
    b = obs.groupBy("period", "dimension", "domain", "stratum").agg(F.count(F.lit(1)).alias("b"))
    a = obs.groupBy(*keys, "stratum").agg(F.count(F.lit(1)).alias("a"))
    grid = (b.join(cats, "dimension").join(a, keys + ["stratum"], "left").fillna(0, subset=["a"])
            .join(strata, "stratum").withColumn("w", F.col("population_n_h") / F.col("sample_n_h")))
    agg = (grid.groupBy(*keys).agg(F.sum(F.col("w") * F.col("b")).alias("X"),
                                   F.sum(F.col("w") * F.col("a")).alias("Y"),
                                   F.sum("b").alias("n_domain_sample"),
                                   F.sum("a").alias("n_category_sample"))
           .withColumn("R", F.col("Y") / F.col("X")))
    a_, b_, r, x = F.col("a"), F.col("b"), F.col("R"), F.col("X")
    s1 = (a_ - r * b_) / x
    s2 = (a_ * (1 - r) * (1 - r) + (b_ - a_) * r * r) / (x * x)
    n_h, big_n = F.col("sample_n_h"), F.col("population_n_h")
    term = (big_n * big_n * (1 - n_h / big_n)
            * ((s2 - s1 * s1 / n_h) / (n_h - 1)) / n_h)
    var = grid.join(agg.select(*keys, "X", "R"), keys).groupBy(*keys).agg(F.sum(term).alias("var_raw"))
    out = (agg.join(var, keys).withColumn("variance", F.greatest(F.col("var_raw"), F.lit(0.0)))
           .withColumn("effective_n", F.when(F.col("variance") > 0, r * (1 - r) / F.col("variance"))
                       .otherwise(F.col("n_domain_sample").cast("double"))))
    return add_wilson(out, "R", "effective_n").select(
        *keys, F.col("R").alias("est_share"), F.sqrt("variance").alias("std_error"),
        "ci95_low", "ci95_high", "n_domain_sample", "n_category_sample", "effective_n",
        ((F.col("n_domain_sample") >= MIN_DOMAIN_N)
         & ((F.col("ci95_high") - F.col("ci95_low")) <= MAX_CI_WIDTH)).alias("is_reliable"),
        F.lit("estimated").alias("measure_type"))


def add_wilson(df, p_col, n_col):
    p, n = F.col(p_col), F.col(n_col)
    z = F.lit(Z95)
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * F.sqrt(F.greatest(p * (1 - p) / n + z * z / (4 * n * n), F.lit(0.0))) / denom
    # Clamp so floating-point noise can never exclude the estimate (low <= p <= high).
    return (df.withColumn("ci95_low", F.least(p, F.greatest(F.lit(0.0), centre - half)))
              .withColumn("ci95_high", F.greatest(p, F.least(F.lit(1.0), centre + half))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · `gold_company_response_estimates` — ESTIMATED company × response / timeliness
# MAGIC Domain = company; periods = ALL (Jan 2025–Oct 2026), 2025, 2026-YTD (Jan–Oct). Every company gets
# MAGIC every category (zeros included). Most companies have too few sample records → `is_reliable = false`;
# MAGIC those rows are kept for transparency but must not be used as findings.

# COMMAND ----------

year_period = reduce(lambda acc, kv: acc.when(F.substring("stratum", 1, 4) == kv[0], F.lit(kv[1])),
                     list(YEAR_PERIODS.items())[1:],
                     F.when(F.substring("stratum", 1, 4) == list(YEAR_PERIODS)[0], F.lit(list(YEAR_PERIODS.values())[0])))
base = (silver.select(F.col("sample_stratum").alias("stratum"), F.col("company").alias("domain"),
                      *COMPANY_ESTIMATE_DIMENSIONS)
        .withColumn("period", F.explode(F.array(F.lit("ALL"), year_period))))
company_obs = reduce(lambda a, b: a.unionByName(b), [
    base.select("period", F.lit(d).alias("dimension"), "domain", F.col(d).alias("category"), "stratum")
    for d in COMPANY_ESTIMATE_DIMENSIONS])
gold_company_est = (estimate(company_obs).withColumnRenamed("domain", "company")
                    .withColumn("period_start", label_to_date_map(PERIOD_BOUNDS, 0))
                    .withColumn("period_end", label_to_date_map(PERIOD_BOUNDS, 1)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · `gold_sub_issue_estimates` — ESTIMATED share of all complaints by issue › sub-issue
# MAGIC Periods = ALL and each month. Original `issue` and `sub_issue` values are kept as columns
# MAGIC (`sub_issue` NULL = none recorded).

# COMMAND ----------

key = F.concat(F.col("issue"), F.lit(" > "), F.coalesce(F.col("sub_issue"), F.lit(SUB_ISSUE_NONE)))
sub_obs = (silver.select(F.col("sample_stratum").alias("stratum"), key.alias("category"))
           .withColumn("period", F.explode(F.array(F.lit("ALL"), F.col("stratum"))))
           .select("period", F.lit("issue_sub_issue").alias("dimension"), F.lit("ALL").alias("domain"),
                   "category", "stratum"))
sub_lookup = silver.select(key.alias("category"), "issue", "sub_issue").distinct()
gold_sub_issue_est = (estimate(sub_obs).drop("domain", "dimension")
                      .join(sub_lookup, "category").withColumnRenamed("category", "issue_sub_issue")
                      .withColumn("period_start", label_to_date_map(PERIOD_BOUNDS, 0))
                      .withColumn("period_end", label_to_date_map(PERIOD_BOUNDS, 1)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · `gold_estimator_calibration` — does the sample reproduce the exact shares?
# MAGIC For every month × exact dimension × category: single-month sample estimate (with 95% CI) vs the exact
# MAGIC API share. Coverage is assessed on categories with ≥ 10 expected sample records; a correct
# MAGIC estimator gives ≈ 95%. The build **fails** if coverage < 90% or the sample contains a category the
# MAGIC population does not.

# COMMAND ----------

sample_counts = reduce(lambda a, b: a.unionByName(b), [
    silver.select(F.col("sample_stratum").alias("month_label"), F.lit(d).alias("dimension"),
                  F.col(d).alias("category")) for d in EXACT_DIMENSIONS]
).groupBy("month_label", "dimension", "category").agg(F.count(F.lit(1)).alias("a"))

sample_only_count = sample_counts.alias("s").join(
    facets.alias("f"),
    (F.col("s.month_label") == F.col("f.month_label")) & (F.col("s.dimension") == F.col("f.dimension"))
    & F.col("s.category").eqNullSafe(F.col("f.category")), "left_anti").count()

cal = (facets.alias("f")
       .join(sample_counts.alias("s"),
             (F.col("f.month_label") == F.col("s.month_label")) & (F.col("f.dimension") == F.col("s.dimension"))
             & F.col("f.category").eqNullSafe(F.col("s.category")), "left")
       .select("f.month_label", "f.dimension", "f.category", "f.exact_count", F.coalesce("s.a", F.lit(0)).alias("a"))
       .join(totals.select("month_label", "population_complaints"), "month_label")
       .join(strata.withColumnRenamed("stratum", "month_label"), "month_label"))
n_h, big_n = F.col("sample_n_h"), F.col("population_n_h")
p = F.col("a") / n_h
cal = (cal.withColumn("est_share", p)
          .withColumn("variance", (1 - n_h / big_n) * F.col("est_share") * (1 - F.col("est_share")) / (n_h - 1))
          .withColumn("exact_share", F.col("exact_count") / F.col("population_complaints"))
          .withColumn("n_eff", F.when(F.col("variance") > 0,
                                      F.col("est_share") * (1 - F.col("est_share")) / F.col("variance"))
                      .otherwise(n_h.cast("double"))))
cal = add_wilson(cal, "est_share", "n_eff")
gold_calibration = cal.select(
    month_date(F.col("month_label")).alias("month"), "month_label", "dimension", "category",
    "exact_share", "est_share", F.sqrt("variance").alias("std_error"), "ci95_low", "ci95_high",
    ((F.col("ci95_low") <= F.col("exact_share")) & (F.col("exact_share") <= F.col("ci95_high"))).alias("inside_ci95"),
    F.when(F.col("variance") > 0, (F.col("est_share") - F.col("exact_share")) / F.sqrt("variance")).alias("z_score"),
    (n_h * F.col("exact_share")).alias("expected_sample_count"),
    ((n_h * F.col("exact_share")) >= COVERAGE_MIN_EXPECTED).alias("in_coverage_qc"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · `gold_insight_facts` — LLM-ready, evidence-grounded facts
# MAGIC One row per atomic, citable number. `fact_id` is a stable hash of (fact_type, period, entity_type,
# MAGIC entity, metric); `source_table` + `source_key` point to the exact Gold row the value came from, so an
# MAGIC LLM answer can cite fact IDs and every number can be verified automatically. Estimated facts are
# MAGIC included **only if reliable** and always carry their 95% CI and sample size.

# COMMAND ----------

def fact_id_col(*parts):
    return F.substring(F.sha2(F.concat_ws("|", *[F.coalesce(c, F.lit("")) for c in parts]), 256), 1, 16)


def facts_from(df, fact_type, period, start, end, entity_type, entity, metric, value, unit, measure,
               table, source_key, ci=(None, None), reliable=F.lit(True), sample_n=None):
    return df.select(
        fact_id_col(F.lit(fact_type) if isinstance(fact_type, str) else fact_type, period,
                    F.lit(entity_type) if isinstance(entity_type, str) else entity_type, entity,
                    F.lit(metric) if isinstance(metric, str) else metric).alias("fact_id"),
        (F.lit(fact_type) if isinstance(fact_type, str) else fact_type).alias("fact_type"),
        period.alias("period"), start.alias("period_start"), end.alias("period_end"),
        (F.lit(entity_type) if isinstance(entity_type, str) else entity_type).alias("entity_type"),
        entity.alias("entity"),
        (F.lit(metric) if isinstance(metric, str) else metric).alias("metric"),
        value.cast("double").alias("value"), F.lit(unit).alias("unit"), F.lit(measure).alias("measure_type"),
        (ci[0] if ci[0] is not None else F.lit(None).cast("double")).alias("ci95_low"),
        (ci[1] if ci[1] is not None else F.lit(None).cast("double")).alias("ci95_high"),
        reliable.alias("is_reliable"),
        (sample_n if sample_n is not None else F.lit(None).cast("bigint")).alias("sample_n"),
        F.lit(table).alias("source_table"), source_key.alias("source_key"))


parts = []
vol_key = F.to_json(F.struct(F.col("month_label")))
for metric, unit in [("population_complaints", "complaints"), ("mom_pct_change", "ratio"),
                     ("yoy_pct_change", "ratio"), ("rolling_3m_avg", "complaints")]:
    parts.append(facts_from(gold_volume.where(F.col(metric).isNotNull()), "monthly_volume", F.col("month_label"),
                            F.col("month"), F.add_months("month", 1), "product", F.lit("credit_reporting"),
                            metric, F.col(metric), unit, "exact", "gold_monthly_volume", vol_key))

breakdown_tables = {"company": "gold_company_monthly", "issue": "gold_issue_monthly", "state": "gold_state_monthly",
                    **{d: "gold_response_monthly" for d in RESPONSE_DIMENSIONS}}
for dim, table in breakdown_tables.items():
    df = breakdowns[dim].where(~F.col("is_residual"))
    if dim in TOP_N:
        df = df.where(F.col("rank_in_month") <= TOP_N[dim])
    bkey = F.to_json(F.struct(F.col("category"), F.lit(dim).alias("dimension"), F.col("month_label")))
    for metric, unit in [("complaints", "complaints"), ("share_of_month", "proportion"), ("mom_pct_change", "ratio")]:
        parts.append(facts_from(df.where(F.col(metric).isNotNull()), f"{dim}_monthly", F.col("month_label"),
                                F.col("month"), F.add_months("month", 1), dim, F.col("category"), metric,
                                F.col(metric), unit, "exact", table, bkey))

ce = gold_company_est.where("is_reliable")
parts.append(facts_from(
    ce, F.concat(F.col("dimension"), F.lit("_estimate")), F.col("period"), F.col("period_start"), F.col("period_end"),
    "company", F.col("company"), F.concat(F.lit("share_"), F.col("dimension"), F.lit("="), F.col("category")),
    F.col("est_share"), "proportion", "estimated", "gold_company_response_estimates",
    F.to_json(F.struct("category", "dimension", F.col("company").alias("domain"), "period")),
    (F.col("ci95_low"), F.col("ci95_high")), F.lit(True), F.col("n_domain_sample")))
se = gold_sub_issue_est.where("is_reliable")
parts.append(facts_from(
    se, "issue_sub_issue_estimate", F.col("period"), F.col("period_start"), F.col("period_end"),
    "issue_sub_issue", F.col("issue_sub_issue"), "share_of_complaints", F.col("est_share"), "proportion",
    "estimated", "gold_sub_issue_estimates",
    F.to_json(F.struct(F.col("issue_sub_issue").alias("category"), F.lit("issue_sub_issue").alias("dimension"),
                       F.lit("ALL").alias("domain"), "period")),
    (F.col("ci95_low"), F.col("ci95_high")), F.lit(True), F.col("n_domain_sample")))
gold_facts = reduce(lambda a, b: a.unionByName(b), parts).withColumn("reference_fetched_at", F.lit(FETCHED_AT))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Data-quality checks and reconciliation (before writing)

# COMMAND ----------

def check(failures, condition, message):
    print(f"[{'PASS' if condition else 'FAIL'}] {message}")
    if not condition:
        failures.append(message)


failures = []

# Volume: exact totals reconcile with the sampling frame recorded in Silver.
vol_recon = (gold_volume.join(silver.groupBy(F.col("sample_stratum").alias("month_label"))
                              .agg(F.max("stratum_population").alias("frame")), "month_label", "full_outer")
             .where(F.col("frame").isNull() | (F.col("frame") != F.col("population_complaints"))).count())
check(failures, vol_recon == 0, f"monthly API totals == Silver stratum_population (mismatches = {vol_recon})")
check(failures, gold_volume.where("sample_size <> 2273").count() == 0, "every month has 2,273 sample records")

# Exact breakdowns: each month (x dimension) sums to the monthly total; shares sum to 1; unique keys.
for name, df, keys in [("company", gold_company, ["month_label", "company"]),
                       ("issue", gold_issue, ["month_label", "issue"]),
                       ("state", gold_state, ["month_label", "state"]),
                       ("response", gold_response, ["month_label", "dimension", "category"])]:
    group = ["month_label"] + (["dimension"] if name == "response" else [])
    sums = (df.groupBy(*group).agg(F.sum("complaints").alias("s"), F.sum("share_of_month").alias("sh"))
            .join(totals.select("month_label", "population_complaints"), "month_label"))
    bad = sums.where((F.col("s") != F.col("population_complaints")) | (F.abs(F.col("sh") - 1) > 1e-9)).count()
    dup = df.groupBy(*keys).count().where("count > 1").count()
    check(failures, bad == 0 and dup == 0, f"{name}: month totals reconcile, shares sum to 1, unique keys (bad={bad}, dup={dup})")
check(failures, gold_state.where("state_category = 'unknown'").count() == 0, "all state values recognised")
check(failures, gold_issue.where("issue_group IS NULL").count() == 0, "every issue has an issue_group")

# Estimates: valid ranges; shares sum to 1 per domain; sample sizes reconcile.
for name, df, group in [("company estimates", gold_company_est, ["period", "dimension", "company"]),
                        ("sub-issue estimates", gold_sub_issue_est, ["period"])]:
    rng = df.where((F.col("est_share") < 0) | (F.col("est_share") > 1) | (F.col("ci95_low") > F.col("est_share"))
                   | (F.col("ci95_high") < F.col("est_share")) | (F.col("n_category_sample") > F.col("n_domain_sample"))).count()
    sums = df.groupBy(*group).agg(F.sum("est_share").alias("s")).where(F.abs(F.col("s") - 1) > 1e-9).count()
    check(failures, rng == 0 and sums == 0, f"{name}: ranges valid and shares sum to 1 (range={rng}, sums={sums})")
period_n = {r["period"]: r["n"] for r in gold_company_est.where("dimension = 'timely' AND category = 'Yes'")
            .groupBy("period").agg(F.sum("n_domain_sample").alias("n")).collect()}
check(failures, period_n == {"ALL": 50006, "2025": 27276, "2026-YTD": 22730},
      f"company domains partition each period's sample: {period_n}")
check(failures, gold_company_est.where("period IS NULL").count() == 0, "every stratum maps to a period")

# Calibration.
check(failures, sample_only_count == 0, f"sample categories absent from exact population = {sample_only_count}")
cov = gold_calibration.where("in_coverage_qc").agg(F.count(F.lit(1)).alias("n"),
                                                   F.sum(F.col("inside_ci95").cast("int")).alias("inside")).first()
coverage = cov["inside"] / cov["n"]
check(failures, coverage >= COVERAGE_FAIL_BELOW,
      f"estimator 95% CI coverage {coverage:.3f} ({cov['inside']}/{cov['n']}) >= {COVERAGE_FAIL_BELOW}")

# Facts.
fq_ = gold_facts.agg(F.count(F.lit(1)).alias("n"), F.countDistinct("fact_id").alias("ids"),
                     F.sum(F.when((F.col("measure_type") == "estimated")
                                  & (~F.col("is_reliable") | F.col("ci95_low").isNull()), 1).otherwise(0)).alias("bad_est"),
                     F.sum(F.when((F.col("measure_type") == "exact") & F.col("ci95_low").isNotNull(), 1).otherwise(0)).alias("bad_exact")).first()
check(failures, fq_["n"] == fq_["ids"], f"fact_id unique ({fq_['ids']}/{fq_['n']})")
check(failures, fq_["bad_est"] == 0 and fq_["bad_exact"] == 0,
      "estimated facts are reliable with CI; exact facts carry no CI")

if failures:
    raise AssertionError(f"Gold validation failed ({len(failures)}): {failures}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · Parity with the tested Python reference
# MAGIC Recomputes the fingerprints produced by `gold_metrics.expected_digests()` for the same inputs.

# COMMAND ----------

def agg_first(df, **exprs):
    return df.agg(*[e.alias(k) for k, e in exprs.items()]).first().asDict()


cnt = F.count(F.lit(1))
actual = {
    "gold_monthly_volume": agg_first(gold_volume, rows=cnt, sum_population=F.sum("population_complaints"),
                                     sum_mom_pct=F.sum(F.coalesce("mom_pct_change", F.lit(0.0)))),
    "gold_company_monthly": agg_first(gold_company, rows=cnt, sum_complaints=F.sum("complaints")),
    "gold_issue_monthly": agg_first(gold_issue, rows=cnt, sum_complaints=F.sum("complaints")),
    "gold_state_monthly": agg_first(gold_state, rows=cnt, sum_complaints=F.sum("complaints")),
    "gold_response_monthly": agg_first(gold_response, rows=cnt, sum_complaints=F.sum("complaints")),
    "gold_issue_category_map": agg_first(gold_issue_map, rows=cnt, combined=F.sum(F.col("is_combined_group").cast("int"))),
    "gold_company_response_estimates": agg_first(gold_company_est, rows=cnt,
                                                 reliable=F.sum(F.col("is_reliable").cast("int")),
                                                 sum_est_share=F.sum("est_share"), sum_std_error=F.sum("std_error")),
    "gold_sub_issue_estimates": agg_first(gold_sub_issue_est, rows=cnt, reliable=F.sum(F.col("is_reliable").cast("int")),
                                          sum_est_share=F.sum("est_share"), sum_std_error=F.sum("std_error")),
    "gold_estimator_calibration": agg_first(gold_calibration, rows=cnt,
                                            qc_comparisons=F.sum(F.col("in_coverage_qc").cast("int")),
                                            inside_ci95=F.sum(F.when(F.col("in_coverage_qc") & F.col("inside_ci95"), 1).otherwise(0))),
    "gold_insight_facts": agg_first(gold_facts, rows=cnt, unique_fact_ids=F.countDistinct("fact_id"),
                                    estimated=F.sum(F.when(F.col("measure_type") == "estimated", 1).otherwise(0)),
                                    sum_value=F.sum("value")),
}
differences = {}
for table, expected in EXPECTED_GOLD.items():
    for metric, value in expected.items():
        got = actual[table][metric]
        tol = FLOAT_TOLERANCE.get(metric)
        ok = abs(got - value) <= tol if tol is not None else got == value
        print(f"[{'PASS' if ok else 'FAIL'}] {table}.{metric}: {got} (expected {value})")
        if not ok:
            differences[f"{table}.{metric}"] = (got, value)
if differences:
    raise AssertionError(f"Spark results differ from the Python reference: {differences}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · Idempotent writes
# MAGIC Each table: created empty from the staged schema on first run (managed Delta, Change Data Feed on),
# MAGIC then synchronised with `MERGE` (insert new keys, update changed rows by row hash, delete keys no
# MAGIC longer produced). Rerunning with unchanged inputs reports 0 affected rows for every table.

# COMMAND ----------

TABLES = [
    ("gold_monthly_volume", gold_volume, ["month_label"],
     "EXACT monthly credit-reporting complaint volumes and growth (CFPB API totals). Use for all volume trends."),
    ("gold_company_monthly", gold_company, ["month_label", "company"],
     "EXACT monthly complaints by company (CFPB API facets): count, share, rank, MoM."),
    ("gold_issue_monthly", gold_issue, ["month_label", "issue"],
     "EXACT monthly complaints by original CFPB issue; issue_group is a documented analysis grouping."),
    ("gold_issue_category_map", gold_issue_map, ["issue"],
     "Documented mapping of original CFPB issues to analysis groups; is_combined_group marks grouped labels."),
    ("gold_state_monthly", gold_state, ["month_label", "state"],
     "EXACT monthly complaints by state (CFPB API facets); state NULL = not provided."),
    ("gold_response_monthly", gold_response, ["month_label", "dimension", "category"],
     "EXACT monthly complaints by company response, timeliness, channel, public response and tags."),
    ("gold_company_response_estimates", gold_company_est, ["period", "dimension", "company", "category"],
     "ESTIMATED company x response/timeliness shares from the weighted stratified sample, with SE, 95% CI and reliability flag."),
    ("gold_sub_issue_estimates", gold_sub_issue_est, ["period", "issue_sub_issue"],
     "ESTIMATED share of complaints by issue > sub-issue from the weighted stratified sample, with SE and 95% CI."),
    ("gold_estimator_calibration", gold_calibration, ["month_label", "dimension", "category"],
     "QC: single-month sample estimates vs exact API shares; inside_ci95 measures estimator calibration."),
    ("gold_insight_facts", gold_facts, ["fact_id"],
     "LLM-ready atomic facts with stable fact_id, measure_type (exact/estimated), CI, reliability and source_table/source_key."),
]


def merge_sync(name, df, keys, comment):
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
    return {"table": name, **{k: int(v) for k, v in metrics.items()}}


results = [merge_sync(*t) for t in TABLES]
display(spark.createDataFrame(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 · Post-write reconciliation

# COMMAND ----------

post = []
for name, df, keys, _ in TABLES:
    stored = spark.table(fq(name))
    cond = [stored[k].eqNullSafe(df[k]) for k in keys]
    rows, staged_rows = stored.count(), df.count()
    diff = stored.join(df, cond, "left_anti").count() + df.join(stored, cond, "left_anti").count()
    check(post, rows == staged_rows and diff == 0, f"{name}: {rows:,} rows, key differences = {diff}")
if post:
    raise AssertionError(f"Post-write reconciliation failed: {post}")
print("\nGold build complete.")
