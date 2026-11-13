"""Exact monthly API totals/aggregations, and sample-vs-population checks.

The search API returns exact facet counts per month (``sum_other_doc_count``
was 0 for every facet when verified on 2026-11-12). These are used as:

* the authoritative monthly complaint volume (never the equal-size sample);
* the population benchmark for validating sample estimates.
"""

import json
import logging
import math
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
import requests

from ingestion.bulk_sample import month_bounds
from ingestion.client import extract_hits, fetch_page, total_hits
from ingestion.logging_utils import log_event

logger = logging.getLogger(__name__)
DIMENSIONS = ["issue", "timely", "company_response", "submitted_via", "company",
              "state", "company_public_response", "tags"]
Z_THRESHOLD = 3.0
MIN_EXPECTED = 5  # skip z-tests where n*p or n*(1-p) < 5 (normal approx invalid)


def months_between(start, end):
    months, current = [], date(start.year, start.month, 1)
    while current < end:
        months.append(current.strftime("%Y-%m"))
        current = month_bounds(months[-1])[1]
    return months


def parse_aggregations(payload):
    """Return {dimension: {bucket_key: doc_count}} for the known facets."""
    aggregations = payload.get("aggregations") or {}
    parsed = {}
    for dim in DIMENSIONS:
        node = (aggregations.get(dim) or {}).get(dim) or {}
        if node.get("sum_other_doc_count"):
            log_event(logger, logging.WARNING, "aggregation_truncated", dimension=dim,
                      other=node["sum_other_doc_count"])
        parsed[dim] = {b["key"]: b["doc_count"] for b in node.get("buckets", [])}
    return parsed


def fetch_monthly_benchmarks(start, end, product, get=requests.get, sleep=time.sleep,
                             request_delay=1.0):
    """One request per month: exact total + facet counts for ``product``."""
    results = []
    for index, month in enumerate(months_between(start, end)):
        if index and request_delay:
            sleep(request_delay)
        first, nxt = month_bounds(month)
        filters = {"product": [product], "date_received_min": first.isoformat(),
                   "date_received_max": (nxt - timedelta(days=1)).isoformat(),
                   "no_aggs": "false"}
        payload = fetch_page(0, 1, filters=filters, get=get, sleep=sleep)
        extract_hits(payload)  # validates shape
        results.append({"month": month, "total": total_hits(payload),
                        "aggregations": parse_aggregations(payload)})
        log_event(logger, logging.INFO, "benchmark_month", month=month,
                  total=results[-1]["total"])
    return {"fetched_at": datetime.now(timezone.utc).isoformat(), "product": product,
            "source": "CFPB search API v1 (exact counts)", "months": results}


def _z(p_hat, p, se):
    return (p_hat - p) / se if se > 0 else None


def compare_sample(sample_path, benchmarks, manifest=None):
    """Compare sample category shares against exact API population shares.

    Per month: p_hat = n_cat / n_h vs p = count / N_h (API); SE = sqrt(p(1-p)/n_h).
    Overall: weighted p_hat = sum(N_h * p_hat_h) / sum(N_h); SE from strata.
    Flags |z| > 3. Also reconciles bulk-frame month counts with API totals.
    """
    table = pq.read_table(sample_path, columns=["_stratum"] + DIMENSIONS)
    rows = table.to_pylist()
    by_month = {}
    for row in rows:
        by_month.setdefault(row["_stratum"], []).append(row)

    report = {"tests": 0, "flags": [], "overall": [], "reconciliation": [],
              "categories_missing_from_population": []}
    bench = {m["month"]: m for m in benchmarks["months"]}

    if manifest:
        for month, info in manifest["months"].items():
            api_total = bench.get(month, {}).get("total")
            if api_total:
                diff = info["population"] - api_total
                report["reconciliation"].append({
                    "month": month, "bulk_frame": info["population"], "api_total": api_total,
                    "difference": diff, "pct": round(100 * diff / api_total, 4)})

    for dim in DIMENSIONS:
        overall = {}  # key -> [sum N_h*p_hat, sum N_h*p_api, variance, sum N_h]
        for month, sample_rows in sorted(by_month.items()):
            if month not in bench:
                continue
            n = len(sample_rows)
            N = bench[month]["total"]
            pop = bench[month]["aggregations"].get(dim, {})
            counts = {}
            for row in sample_rows:
                counts[row[dim]] = counts.get(row[dim], 0) + 1
            for key in set(pop) | {k for k in counts if k is not None}:
                p = pop.get(key, 0) / N
                p_hat = counts.get(key, 0) / n
                if p == 0 and p_hat > 0:
                    report["categories_missing_from_population"].append(
                        {"dimension": dim, "month": month, "key": key, "sample_count": counts[key]})
                    continue
                acc = overall.setdefault(key, [0.0, 0.0, 0.0])
                acc[0] += N * p_hat
                acc[1] += N * p
                acc[2] += (N ** 2) * p * (1 - p) / n
                if n * p < MIN_EXPECTED or n * (1 - p) < MIN_EXPECTED:
                    continue
                report["tests"] += 1
                se = math.sqrt(p * (1 - p) / n)
                z = _z(p_hat, p, se)
                if z is not None and abs(z) > Z_THRESHOLD:
                    report["flags"].append({"dimension": dim, "month": month, "key": key,
                                            "p_api": round(p, 5), "p_sample": round(p_hat, 5),
                                            "z": round(z, 2)})
        total_N = sum(bench[m]["total"] for m in by_month if m in bench)
        for key, (np_hat, np_api, var) in overall.items():
            p, p_hat = np_api / total_N, np_hat / total_N
            se = math.sqrt(var) / total_N
            if p < 0.005:  # keep the overall table readable
                continue
            z = _z(p_hat, p, se)
            report["overall"].append({"dimension": dim, "key": key, "p_api": round(p, 5),
                                      "p_sample_weighted": round(p_hat, 5),
                                      "diff_pp": round(100 * (p_hat - p), 3),
                                      "z": round(z, 2) if z is not None else None})
    report["expected_false_flags"] = round(report["tests"] * math.erfc(Z_THRESHOLD / math.sqrt(2)), 1)
    report["overall_flags"] = [o for o in report["overall"]
                               if o["z"] is not None and abs(o["z"]) > Z_THRESHOLD]
    return report


def save_json(obj, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, default=str))
    tmp.replace(path)
    return path
