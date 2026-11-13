"""Gold metrics — pure-Python reference implementation.

Two kinds of numbers, never mixed:

* EXACT — population counts from the CFPB search API (monthly totals and
  complete facet counts, ``sum_other_doc_count == 0``). Used for volumes,
  growth, and every one-way breakdown the API provides (company, issue,
  state, response, timeliness, channel, public response, tags).
* ESTIMATED — from the stratified sample (strata = months, equal allocation,
  SRS within month) only where the API has no exact answer: cross-tabs such
  as company x response type / timeliness, and sub-issues.

Estimator (ratio / domain proportion under stratified SRS):
    w_h = N_h / n_h
    R   = sum_h w_h a_h / sum_h w_h b_h      a = domain-and-category count,
                                             b = domain count, in stratum h
    Var(R) via Taylor linearisation, u_i = (y_i - R x_i) / X_hat:
    Var = sum_h N_h^2 (1 - n_h/N_h) s2_h / n_h,
    s2_h = (S2_h - S1_h^2 / n_h) / (n_h - 1),
    S1_h = (a_h - R b_h) / X_hat,  S2_h = (a_h (1-R)^2 + (b_h - a_h) R^2) / X_hat^2
    95% CI: Wilson score interval with effective n = R(1-R)/Var
    (n_eff = domain sample size when Var == 0, i.e. R in {0, 1}).

Reliability (project rule, documented): estimate is "reliable" when the
domain has >= 30 sample records AND the 95% CI is no wider than 0.20.

The Databricks notebook ``04_gold_build.py`` implements the same maths in
Spark and checks its output against ``expected_digests()`` from this module.
"""

import hashlib
import json
import math
from collections import defaultdict
from datetime import date

Z95 = 1.959963984540054
MIN_DOMAIN_N = 30
MAX_CI_WIDTH = 0.20
COVERAGE_MIN_EXPECTED = 10     # calibration QC only uses categories with n*p >= 10
COVERAGE_FAIL_BELOW = 0.90     # nominal 95% CIs; fail Gold QC if coverage < 90%

EXACT_DIMENSIONS = ["issue", "timely", "company_response", "submitted_via", "company",
                    "state", "company_public_response", "tags"]
RESPONSE_DIMENSIONS = ["company_response", "timely", "submitted_via",
                       "company_public_response", "tags"]
COMPANY_ESTIMATE_DIMENSIONS = ["company_response", "timely"]

# Documented grouping. Originals are always kept; issue_group is an optional
# analysis column. Both wordings occur in every month (verified in the API
# reference), so they are distinct CFPB categories, grouped only for analysis.
ISSUE_GROUPS = {
    "Problem with a company's investigation into an existing problem":
        "Problem with a company's investigation into an existing problem or issue",
    "Problem with a company's investigation into an existing issue":
        "Problem with a company's investigation into an existing problem or issue",
}
ISSUE_GROUP_NOTE = ("Grouped for analysis only: two CFPB issue labels with near-identical "
                    "wording that co-occur in every month. Original labels are preserved "
                    "in the issue column.")

YEAR_PERIODS = {"2025": "2025", "2026": "2026-YTD"}  # 2026 = Jan-Oct (data ends Oct)


# --- helpers ------------------------------------------------------------------

def month_start(label):
    return date(int(label[:4]), int(label[5:7]), 1)


def next_month(label):
    y, m = int(label[:4]), int(label[5:7])
    return f"{y + (m == 12)}-{m % 12 + 1:02d}"


def shift_month(label, months):
    y, m = int(label[:4]), int(label[5:7])
    total = y * 12 + (m - 1) + months
    return f"{total // 12}-{total % 12 + 1:02d}"


def periods_for(month):
    """Estimation periods a stratum belongs to."""
    return ["ALL", YEAR_PERIODS[month[:4]]]


def wilson(p, n, z=Z95):
    if n <= 0:
        return 0.0, 1.0
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(max(p * (1 - p) / n + z * z / (4 * n * n), 0.0)) / denom
    # Mathematically low <= p <= high; clamp so floating-point noise (e.g. a
    # lower bound of 3e-18 when p == 0) can never exclude the estimate itself.
    return min(p, max(0.0, centre - half)), max(p, min(1.0, centre + half))


def is_reliable(n_domain, ci_low, ci_high):
    return n_domain >= MIN_DOMAIN_N and (ci_high - ci_low) <= MAX_CI_WIDTH


# --- reference data -----------------------------------------------------------

def reference_tables(ref_json):
    """API benchmark JSON -> (totals rows, facet rows incl. exact residuals)."""
    totals, facets = [], []
    for month in ref_json["months"]:
        label, total = month["month"], month["total"]
        totals.append({"month": label, "population_complaints": total,
                       "product": ref_json["product"], "fetched_at": ref_json["fetched_at"]})
        for dim in EXACT_DIMENSIONS:
            buckets = month["aggregations"].get(dim, {})
            for category, count in sorted(buckets.items()):
                facets.append({"month": label, "dimension": dim, "category": category,
                               "exact_count": count, "is_residual": False})
            residual = total - sum(buckets.values())
            if residual < 0:
                raise ValueError(f"{label} {dim}: facet sum exceeds total")
            if residual:
                facets.append({"month": label, "dimension": dim, "category": None,
                               "exact_count": residual, "is_residual": True})
    return totals, facets


# --- exact tables -------------------------------------------------------------

def monthly_volume(totals, sample_sizes):
    by_month = {t["month"]: t["population_complaints"] for t in totals}
    rows = []
    for label in sorted(by_month):
        n = by_month[label]
        prev, prev_year = by_month.get(shift_month(label, -1)), by_month.get(shift_month(label, -12))
        window = [by_month.get(shift_month(label, -k)) for k in range(3)]
        rows.append({
            "month": month_start(label), "month_label": label,
            "population_complaints": n,
            "mom_change": n - prev if prev is not None else None,
            "mom_pct_change": (n - prev) / prev if prev else None,
            "yoy_change": n - prev_year if prev_year is not None else None,
            "yoy_pct_change": (n - prev_year) / prev_year if prev_year else None,
            "rolling_3m_avg": sum(window) / 3 if None not in window else None,
            "sample_size": sample_sizes.get(label, 0),
            "sample_fraction": sample_sizes.get(label, 0) / n,
            "measure_type": "exact",
        })
    return rows


def exact_breakdown(facets, totals, dimension):
    """month x category exact counts, share of month, rank, MoM for one dimension."""
    total = {t["month"]: t["population_complaints"] for t in totals}
    counts = {(f["month"], f["category"]): f for f in facets if f["dimension"] == dimension}
    rows = []
    for month in sorted(total):
        month_rows = [f for (m, _c), f in counts.items() if m == month]
        ranked = sorted([f for f in month_rows if not f["is_residual"]],
                        key=lambda f: (-f["exact_count"], f["category"]))
        rank = {f["category"]: i + 1 for i, f in enumerate(ranked)}
        for f in sorted(month_rows, key=lambda f: (f["category"] is None, f["category"] or "")):
            prev = counts.get((shift_month(month, -1), f["category"]))
            has_prev_month = shift_month(month, -1) in total
            prev_count = prev["exact_count"] if prev else (0 if has_prev_month else None)
            rows.append({
                "month": month_start(month), "month_label": month,
                "category": f["category"], "is_residual": f["is_residual"],
                "complaints": f["exact_count"],
                "share_of_month": f["exact_count"] / total[month],
                "rank_in_month": rank.get(f["category"]),
                "prev_month_complaints": prev_count,
                "mom_change": f["exact_count"] - prev_count if prev_count is not None else None,
                "mom_pct_change": ((f["exact_count"] - prev_count) / prev_count
                                   if prev_count else None),
                "measure_type": "exact",
            })
    return rows


def issue_category_map(facets):
    issues = sorted({f["category"] for f in facets if f["dimension"] == "issue" and not f["is_residual"]})
    return [{"issue": i, "issue_group": ISSUE_GROUPS.get(i, i),
             "is_combined_group": i in ISSUE_GROUPS,
             "note": ISSUE_GROUP_NOTE if i in ISSUE_GROUPS else None} for i in issues]


# --- stratified estimator -------------------------------------------------------

def estimate(observations, strata, categories):
    """Domain proportions under stratified SRS.

    observations: iterable of (period, dimension, domain, category, stratum)
    strata: {stratum: (N_h, n_h)}
    categories: {dimension: [category, ...]} — every domain gets every category
    (zero counts included).
    Returns rows keyed by (period, dimension, domain, category).
    """
    b = defaultdict(int)   # (period, dim, domain, stratum) -> domain count
    a = defaultdict(int)   # (period, dim, domain, category, stratum) -> count
    for period, dim, domain, category, stratum in observations:
        b[(period, dim, domain, stratum)] += 1
        a[(period, dim, domain, category, stratum)] += 1

    domains = defaultdict(list)  # (period, dim, domain) -> [stratum, ...]
    for (period, dim, domain, stratum) in b:
        domains[(period, dim, domain)].append(stratum)

    rows = []
    for (period, dim, domain), stratum_list in sorted(domains.items(), key=lambda kv: tuple(map(str, kv[0]))):
        weight = {h: strata[h][0] / strata[h][1] for h in stratum_list}
        x_hat = sum(weight[h] * b[(period, dim, domain, h)] for h in stratum_list)
        n_domain = sum(b[(period, dim, domain, h)] for h in stratum_list)
        for category in categories[dim]:
            counts = {h: a.get((period, dim, domain, category, h), 0) for h in stratum_list}
            y_hat = sum(weight[h] * counts[h] for h in stratum_list)
            r = y_hat / x_hat
            variance = 0.0
            for h in stratum_list:
                big_n, n_h = strata[h]
                ah, bh = counts[h], b[(period, dim, domain, h)]
                s1 = (ah - r * bh) / x_hat
                s2 = (ah * (1 - r) ** 2 + (bh - ah) * r ** 2) / x_hat ** 2
                variance += big_n ** 2 * (1 - n_h / big_n) * ((s2 - s1 * s1 / n_h) / (n_h - 1)) / n_h
            variance = max(variance, 0.0)
            n_eff = r * (1 - r) / variance if variance > 0 else float(n_domain)
            low, high = wilson(r, n_eff)
            rows.append({
                "period": period, "dimension": dim, "domain": domain, "category": category,
                "est_share": r, "std_error": math.sqrt(variance),
                "ci95_low": low, "ci95_high": high,
                "n_domain_sample": n_domain, "n_category_sample": sum(counts.values()),
                "effective_n": n_eff, "is_reliable": is_reliable(n_domain, low, high),
                "measure_type": "estimated",
            })
    return rows


def sample_strata(silver):
    strata = {}
    for r in silver:
        n = strata.get(r["sample_stratum"], (r["stratum_population"], 0))[1]
        strata[r["sample_stratum"]] = (r["stratum_population"], n + 1)
    return strata


def company_response_estimates(silver):
    obs, cats = [], {d: set() for d in COMPANY_ESTIMATE_DIMENSIONS}
    for r in silver:
        for dim in COMPANY_ESTIMATE_DIMENSIONS:
            cats[dim].add(r[dim])
            for period in periods_for(r["sample_stratum"]):
                obs.append((period, dim, r["company"], r[dim], r["sample_stratum"]))
    return estimate(obs, sample_strata(silver), {d: sorted(v) for d, v in cats.items()})


SUB_ISSUE_NONE = "(no sub-issue)"


def sub_issue_key(r):
    return f"{r['issue']} > {r['sub_issue'] or SUB_ISSUE_NONE}"


def sub_issue_estimates(silver):
    obs, cats = [], set()
    for r in silver:
        key = sub_issue_key(r)
        cats.add(key)
        for period in ["ALL", r["sample_stratum"]]:
            obs.append((period, "issue_sub_issue", "ALL", key, r["sample_stratum"]))
    return estimate(obs, sample_strata(silver), {"issue_sub_issue": sorted(cats)})


def single_stratum_estimate(a, n, big_n):
    """Closed form of ``estimate`` for one stratum and domain = all records.

    Var = (1 - n/N) p (1 - p) / (n - 1). Used for calibration (same formula in
    Spark, so both engines perform identical floating-point operations).
    """
    p = a / n
    variance = (1 - n / big_n) * p * (1 - p) / (n - 1)
    n_eff = p * (1 - p) / variance if variance > 0 else float(n)
    low, high = wilson(p, n_eff)
    return p, math.sqrt(variance), low, high


def calibration(silver, facets, totals):
    """Per month x exact dimension x category: sample estimate vs exact share."""
    total = {t["month"]: t["population_complaints"] for t in totals}
    strata = sample_strata(silver)
    counts = defaultdict(int)
    for r in silver:
        for dim in EXACT_DIMENSIONS:
            counts[(r["sample_stratum"], dim, r[dim])] += 1
    rows = []
    for dim in EXACT_DIMENSIONS:
        cats = {f["category"] for f in facets if f["dimension"] == dim}
        for f in facets:
            if f["dimension"] != dim:
                continue
            big_n, n = strata[f["month"]]
            p, se, low, high = single_stratum_estimate(counts.get((f["month"], dim, f["category"]), 0), n, big_n)
            exact_share = f["exact_count"] / total[f["month"]]
            rows.append({
                "month": month_start(f["month"]), "month_label": f["month"], "dimension": dim,
                "category": f["category"], "exact_share": exact_share,
                "est_share": p, "std_error": se, "ci95_low": low, "ci95_high": high,
                "inside_ci95": low <= exact_share <= high,
                "z_score": (p - exact_share) / se if se > 0 else None,
                "expected_sample_count": n * exact_share,
                "in_coverage_qc": n * exact_share >= COVERAGE_MIN_EXPECTED,
            })
        sample_only = {r[dim] for r in silver} - set(cats)
        if sample_only:
            raise ValueError(f"{dim}: sample categories absent from exact facets: {sample_only}")
    return rows


def coverage_summary(calibration_rows):
    qc = [r for r in calibration_rows if r["in_coverage_qc"]]
    inside = sum(r["inside_ci95"] for r in qc)
    return {"qc_comparisons": len(qc), "inside_ci95": inside,
            "coverage": inside / len(qc) if qc else None}


# --- LLM-ready facts --------------------------------------------------------------

def fact_id(*parts):
    return hashlib.sha256("|".join("" if p is None else str(p) for p in parts).encode()).hexdigest()[:16]


def _fact(fact_type, period, period_start, period_end, entity_type, entity, metric, value, unit,
          measure_type, source_table, source_key, ci=(None, None), reliable=True, sample_n=None):
    return {
        "fact_id": fact_id(fact_type, period, entity_type, entity, metric),
        "fact_type": fact_type, "period": period, "period_start": period_start,
        "period_end": period_end, "entity_type": entity_type, "entity": entity,
        "metric": metric, "value": value, "unit": unit, "measure_type": measure_type,
        "ci95_low": ci[0], "ci95_high": ci[1], "is_reliable": reliable,
        "sample_n": sample_n, "source_table": source_table,
        "source_key": json.dumps(source_key, sort_keys=True, separators=(",", ":")),
    }


PERIOD_BOUNDS = {"ALL": ("2025-01", "2026-11"), "2025": ("2025-01", "2026-01"),
                 "2026-YTD": ("2026-01", "2026-11")}
TOP_N = {"company": 10, "state": 10}


def insight_facts(volume, breakdowns, company_est, sub_issue_est):
    facts = []
    for v in volume:
        m, start, end = v["month_label"], v["month"], month_start(next_month(v["month_label"]))
        key = {"month_label": m}
        for metric, unit in [("population_complaints", "complaints"), ("mom_pct_change", "ratio"),
                             ("yoy_pct_change", "ratio"), ("rolling_3m_avg", "complaints")]:
            if v[metric] is not None:
                facts.append(_fact("monthly_volume", m, start, end, "product", "credit_reporting",
                                   metric, float(v[metric]), unit, "exact",
                                   "gold_monthly_volume", key))
    for dim, table in [("company", "gold_company_monthly"), ("issue", "gold_issue_monthly"),
                       ("state", "gold_state_monthly")] + [(d, "gold_response_monthly") for d in RESPONSE_DIMENSIONS]:
        for r in breakdowns[dim]:
            if r["is_residual"] or (dim in TOP_N and r["rank_in_month"] > TOP_N[dim]):
                continue
            m = r["month_label"]
            key = {"month_label": m, "dimension": dim, "category": r["category"]}
            for metric, unit in [("complaints", "complaints"), ("share_of_month", "proportion"),
                                 ("mom_pct_change", "ratio")]:
                if r[metric] is not None:
                    facts.append(_fact(f"{dim}_monthly", m, r["month"], month_start(next_month(m)),
                                       dim, r["category"], metric, float(r[metric]), unit, "exact",
                                       table, key))
    for e in company_est + sub_issue_est:
        if not e["is_reliable"]:
            continue
        lo, hi = (PERIOD_BOUNDS[e["period"]] if e["period"] in PERIOD_BOUNDS
                  else (e["period"], next_month(e["period"])))
        table = ("gold_company_response_estimates" if e["dimension"] in COMPANY_ESTIMATE_DIMENSIONS
                 else "gold_sub_issue_estimates")
        entity_type = "company" if table == "gold_company_response_estimates" else "issue_sub_issue"
        entity = e["domain"] if entity_type == "company" else e["category"]
        metric = f"share_{e['dimension']}={e['category']}" if entity_type == "company" else "share_of_complaints"
        key = {"period": e["period"], "dimension": e["dimension"], "domain": e["domain"],
               "category": e["category"]}
        facts.append(_fact(f"{e['dimension']}_estimate", e["period"], month_start(lo), month_start(hi),
                           entity_type, entity, metric, e["est_share"], "proportion", "estimated",
                           table, key, (e["ci95_low"], e["ci95_high"]), True, e["n_domain_sample"]))
    return facts


# --- build everything + digests for the notebook ---------------------------------

def build_gold(silver, ref_json):
    totals, facets = reference_tables(ref_json)
    sample_sizes = defaultdict(int)
    for r in silver:
        sample_sizes[r["sample_stratum"]] += 1
    volume = monthly_volume(totals, sample_sizes)
    breakdowns = {d: exact_breakdown(facets, totals, d) for d in EXACT_DIMENSIONS}
    company_est = company_response_estimates(silver)
    sub_est = sub_issue_estimates(silver)
    calib = calibration(silver, facets, totals)
    return {
        "ref_totals": totals, "ref_facets": facets, "volume": volume, "breakdowns": breakdowns,
        "issue_map": issue_category_map(facets), "company_estimates": company_est,
        "sub_issue_estimates": sub_est, "calibration": calib,
        "coverage": coverage_summary(calib),
        "facts": insight_facts(volume, breakdowns, company_est, sub_est),
    }


def _round(x, digits=6):
    return round(x, digits)


def expected_digests(gold):
    """Compact fingerprints the notebook recomputes in Spark (rows + sums)."""
    b = gold["breakdowns"]
    return {
        "gold_monthly_volume": {"rows": len(gold["volume"]),
                                "sum_population": sum(v["population_complaints"] for v in gold["volume"]),
                                "sum_mom_pct": _round(sum(v["mom_pct_change"] or 0 for v in gold["volume"]))},
        "gold_company_monthly": {"rows": len(b["company"]),
                                 "sum_complaints": sum(r["complaints"] for r in b["company"])},
        "gold_issue_monthly": {"rows": len(b["issue"]),
                               "sum_complaints": sum(r["complaints"] for r in b["issue"])},
        "gold_state_monthly": {"rows": len(b["state"]),
                               "sum_complaints": sum(r["complaints"] for r in b["state"])},
        "gold_response_monthly": {"rows": sum(len(b[d]) for d in RESPONSE_DIMENSIONS),
                                  "sum_complaints": sum(r["complaints"] for d in RESPONSE_DIMENSIONS for r in b[d])},
        "gold_issue_category_map": {"rows": len(gold["issue_map"]),
                                    "combined": sum(m["is_combined_group"] for m in gold["issue_map"])},
        "gold_company_response_estimates": {
            "rows": len(gold["company_estimates"]),
            "reliable": sum(e["is_reliable"] for e in gold["company_estimates"]),
            "sum_est_share": _round(sum(e["est_share"] for e in gold["company_estimates"])),
            "sum_std_error": _round(sum(e["std_error"] for e in gold["company_estimates"]))},
        "gold_sub_issue_estimates": {
            "rows": len(gold["sub_issue_estimates"]),
            "reliable": sum(e["is_reliable"] for e in gold["sub_issue_estimates"]),
            "sum_est_share": _round(sum(e["est_share"] for e in gold["sub_issue_estimates"])),
            "sum_std_error": _round(sum(e["std_error"] for e in gold["sub_issue_estimates"]))},
        "gold_estimator_calibration": {"rows": len(gold["calibration"]),
                                       "qc_comparisons": gold["coverage"]["qc_comparisons"],
                                       "inside_ci95": gold["coverage"]["inside_ci95"]},
        "gold_insight_facts": {"rows": len(gold["facts"]),
                               "unique_fact_ids": len({f["fact_id"] for f in gold["facts"]}),
                               "estimated": sum(f["measure_type"] == "estimated" for f in gold["facts"]),
                               "sum_value": _round(sum(f["value"] for f in gold["facts"]), 2)},
    }
