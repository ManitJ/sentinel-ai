"""Offline tests for Gold metrics, the stratified estimator and notebook parity."""

import ast
import json
import math
import random
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lakehouse import gold_metrics as gm
from lakehouse import silver_rules as sr
from lakehouse.reference_export import export

ROOT = Path(__file__).resolve().parents[1]
GOLD_NB = ROOT / "databricks/notebooks/04_gold_build.py"
REF_NB = ROOT / "databricks/notebooks/03_gold_reference_load.py"
SAMPLE = ROOT / "data/bronze/cfpb_credit_reporting_sample/_upload/cfpb_credit_reporting_sample.parquet"
REF_JSON = ROOT / "data/reference/cfpb_api_monthly_credit_reporting.json"


def notebook_constant(path, name):
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not in {path.name}")


def ref_json(months):
    """months: {label: (total, {dim: {category: count}})}"""
    return {"product": "Credit reporting", "fetched_at": "2026-11-12T00:00:00+00:00",
            "months": [{"month": m, "total": t,
                        "aggregations": {d: aggs.get(d, {}) for d in gm.EXACT_DIMENSIONS}}
                       for m, (t, aggs) in months.items()]}


# --- reference tables ----------------------------------------------------------

def test_reference_tables_add_exact_residuals():
    totals, facets = gm.reference_tables(ref_json({
        "2025-01": (100, {"state": {"CA": 60, "TX": 30}, "issue": {"A": 100}})}))
    assert totals == [{"month": "2025-01", "population_complaints": 100,
                       "product": "Credit reporting", "fetched_at": "2026-11-12T00:00:00+00:00"}]
    state = [f for f in facets if f["dimension"] == "state"]
    assert {(f["category"], f["exact_count"], f["is_residual"]) for f in state} == {
        ("CA", 60, False), ("TX", 30, False), (None, 10, True)}
    assert not [f for f in facets if f["dimension"] == "issue" and f["is_residual"]]


def test_reference_tables_reject_facets_exceeding_total():
    with pytest.raises(ValueError, match="exceeds"):
        gm.reference_tables(ref_json({"2025-01": (10, {"state": {"CA": 11}})}))


def test_reference_export_roundtrip(tmp_path):
    source = tmp_path / "ref.json"
    source.write_text(json.dumps(ref_json({"2025-01": (100, {"state": {"CA": 60}}),
                                           "2025-02": (50, {"state": {"CA": 50}})})))
    info = export(source, tmp_path / "out")
    totals = pq.read_table(info["totals"]["path"]).to_pylist()
    facets = pq.read_table(info["facets"]["path"]).to_pylist()
    assert [t["population_complaints"] for t in totals] == [100, 50]
    for month, total in [("2025-01", 100), ("2025-02", 50)]:
        for dim in gm.EXACT_DIMENSIONS:
            assert sum(f["exact_count"] for f in facets
                       if f["month"] == month and f["dimension"] == dim) == total
    assert {f["fetched_at"] for f in facets} == {"2026-11-12T00:00:00+00:00"}


# --- exact tables --------------------------------------------------------------

def test_monthly_volume_growth_metrics():
    totals = [{"month": f"2025-{m:02d}", "population_complaints": 100 + 10 * m} for m in range(1, 13)]
    totals.append({"month": "2026-01", "population_complaints": 330})
    rows = {r["month_label"]: r for r in gm.monthly_volume(totals, {"2026-01": 33})}
    assert rows["2025-01"]["mom_change"] is None and rows["2025-01"]["rolling_3m_avg"] is None
    assert rows["2025-02"]["mom_change"] == 10 and rows["2025-02"]["mom_pct_change"] == pytest.approx(10 / 110)
    assert rows["2025-03"]["rolling_3m_avg"] == pytest.approx((110 + 120 + 130) / 3)
    assert rows["2026-01"]["yoy_change"] == 330 - 110
    assert rows["2026-01"]["yoy_pct_change"] == pytest.approx(220 / 110)
    assert rows["2026-01"]["sample_fraction"] == pytest.approx(0.1)
    assert rows["2025-12"]["yoy_change"] is None
    assert all(r["measure_type"] == "exact" for r in rows.values())


def test_exact_breakdown_shares_ranks_and_mom():
    totals, facets = gm.reference_tables(ref_json({
        "2025-01": (100, {"company": {"B": 50, "A": 50}}),
        "2025-02": (80, {"company": {"A": 60, "C": 20}}),
    }))
    rows = {(r["month_label"], r["category"]): r for r in gm.exact_breakdown(facets, totals, "company")}
    assert rows[("2025-01", "A")]["rank_in_month"] == 1  # tie broken alphabetically
    assert rows[("2025-01", "B")]["rank_in_month"] == 2
    assert rows[("2025-01", "A")]["prev_month_complaints"] is None  # first month
    assert rows[("2025-02", "C")]["prev_month_complaints"] == 0     # absent last month = exact 0
    assert rows[("2025-02", "C")]["mom_pct_change"] is None
    assert rows[("2025-02", "A")]["mom_pct_change"] == pytest.approx(0.2)
    assert sum(r["share_of_month"] for (m, _), r in rows.items() if m == "2025-02") == pytest.approx(1)


def test_residual_rows_are_unranked():
    totals, facets = gm.reference_tables(ref_json({"2025-01": (100, {"state": {"CA": 90}})}))
    residual = [r for r in gm.exact_breakdown(facets, totals, "state") if r["is_residual"]]
    assert residual[0]["rank_in_month"] is None and residual[0]["complaints"] == 10


def test_issue_map_preserves_originals_and_documents_grouping():
    _, facets = gm.reference_tables(ref_json({"2025-01": (10, {"issue": {
        "Problem with a company's investigation into an existing problem": 6,
        "Problem with a company's investigation into an existing issue": 1, "Other": 3}})}))
    mapping = {m["issue"]: m for m in gm.issue_category_map(facets)}
    assert set(mapping) == {"Other", "Problem with a company's investigation into an existing problem",
                            "Problem with a company's investigation into an existing issue"}
    assert mapping["Other"] == {"issue": "Other", "issue_group": "Other", "is_combined_group": False, "note": None}
    grouped = [m for m in mapping.values() if m["is_combined_group"]]
    assert len(grouped) == 2 and len({m["issue_group"] for m in grouped}) == 1
    assert all(m["note"] for m in grouped)


# --- estimator -------------------------------------------------------------------

def test_wilson_interval_properties():
    low, high = gm.wilson(0.0, 100)
    assert low == 0.0 and 0 < high < 0.05
    low, high = gm.wilson(1.0, 100)
    assert high == 1.0 and 0.95 < low < 1
    low, high = gm.wilson(0.5, 1000)
    assert low < 0.5 < high and high - low == pytest.approx(2 * 1.96 * math.sqrt(0.25 / 1000), rel=0.01)


def test_reliability_rule():
    assert gm.is_reliable(30, 0.40, 0.60)
    assert not gm.is_reliable(29, 0.40, 0.41)
    assert not gm.is_reliable(500, 0.30, 0.51)


def test_single_stratum_matches_textbook_formula_and_general_estimator():
    n, big_n, a = 100, 1000, 30
    p, se, low, high = gm.single_stratum_estimate(a, n, big_n)
    assert p == 0.3
    assert se == pytest.approx(math.sqrt((1 - n / big_n) * 0.3 * 0.7 / (n - 1)))
    obs = [("P", "d", "ALL", "x" if i < a else "y", "h") for i in range(n)]
    general = next(e for e in gm.estimate(obs, {"h": (big_n, n)}, {"d": ["x", "y"]}) if e["category"] == "x")
    assert general["est_share"] == pytest.approx(p)
    assert general["std_error"] == pytest.approx(se, rel=1e-12)
    assert (general["ci95_low"], general["ci95_high"]) == pytest.approx((low, high), rel=1e-12)


def test_weights_correct_for_equal_allocation():
    # Stratum A: N=900, sample 100, 10% "x". Stratum B: N=100, sample 100, 90% "x".
    obs = [("P", "d", "ALL", "x" if i < 10 else "y", "A") for i in range(100)]
    obs += [("P", "d", "ALL", "x" if i < 90 else "y", "B") for i in range(100)]
    est = next(e for e in gm.estimate(obs, {"A": (900, 100), "B": (100, 100)}, {"d": ["x", "y"]})
               if e["category"] == "x")
    assert est["est_share"] == pytest.approx((900 * 0.1 + 100 * 0.9) / 1000)  # 0.18, not the naive 0.50
    # Stratum B is fully enumerated (fpc = 0): only stratum A contributes variance.
    expected_var = (0.9 ** 2) * (1 - 100 / 900) * 0.1 * 0.9 / 99
    assert est["std_error"] == pytest.approx(math.sqrt(expected_var), rel=1e-9)


def test_domain_estimates_cover_every_category_and_sum_to_one():
    obs = [("ALL", "timely", "Co1", "Yes", "h1")] * 40 + [("ALL", "timely", "Co2", "No", "h1")] * 5
    obs += [("ALL", "timely", "Co2", "Yes", "h2")] * 15
    rows = gm.estimate(obs, {"h1": (4500, 45), "h2": (1500, 15)}, {"timely": ["No", "Yes"]})
    by = {(r["domain"], r["category"]): r for r in rows}
    assert set(by) == {("Co1", "No"), ("Co1", "Yes"), ("Co2", "No"), ("Co2", "Yes")}
    assert by[("Co1", "No")]["est_share"] == 0 and by[("Co1", "No")]["ci95_high"] > 0
    assert by[("Co1", "Yes")]["is_reliable"] and not by[("Co2", "Yes")]["is_reliable"]
    for co in ("Co1", "Co2"):
        assert by[(co, "No")]["est_share"] + by[(co, "Yes")]["est_share"] == pytest.approx(1)
    # Co2: weighted (5*100) / (5*100 + 15*100) = 0.25
    assert by[("Co2", "No")]["est_share"] == pytest.approx(0.25)


def test_confidence_intervals_achieve_nominal_coverage_by_simulation():
    """Draw repeated stratified samples from a known population; ~95% of CIs must cover the truth."""
    rng = random.Random(20261112)
    population = {"h1": [1] * 300 + [0] * 2700, "h2": [1] * 1200 + [0] * 800, "h3": [1] * 50 + [0] * 950}
    sizes = {"h1": 150, "h2": 150, "h3": 150}
    truth = sum(sum(v) for v in population.values()) / sum(len(v) for v in population.values())
    strata = {h: (len(v), sizes[h]) for h, v in population.items()}
    covered, reps = 0, 400
    for _ in range(reps):
        obs = [("P", "d", "ALL", "x" if y else "y", h)
               for h, values in population.items() for y in rng.sample(values, sizes[h])]
        est = next(e for e in gm.estimate(obs, strata, {"d": ["x", "y"]}) if e["category"] == "x")
        covered += est["ci95_low"] <= truth <= est["ci95_high"]
    assert 0.92 <= covered / reps <= 0.98, covered / reps


# --- facts -----------------------------------------------------------------------

def test_fact_ids_are_stable_and_source_keys_are_compact_json():
    f = gm._fact("monthly_volume", "2025-01", None, None, "product", "credit_reporting",
                 "population_complaints", 1.0, "complaints", "exact", "gold_monthly_volume",
                 {"month_label": "2025-01"})
    assert f["fact_id"] == gm.fact_id("monthly_volume", "2025-01", "product", "credit_reporting",
                                      "population_complaints")
    assert len(f["fact_id"]) == 16
    assert f["source_key"] == '{"month_label":"2025-01"}'  # same format as Spark to_json


# --- notebooks ----------------------------------------------------------------------

GOLD_SHARED = ["Z95", "MIN_DOMAIN_N", "MAX_CI_WIDTH", "COVERAGE_MIN_EXPECTED", "COVERAGE_FAIL_BELOW",
               "EXACT_DIMENSIONS", "RESPONSE_DIMENSIONS", "COMPANY_ESTIMATE_DIMENSIONS", "YEAR_PERIODS",
               "PERIOD_BOUNDS", "TOP_N", "SUB_ISSUE_NONE", "ISSUE_GROUPS", "ISSUE_GROUP_NOTE"]
STATE_SHARED = ["US_STATES", "DISTRICT", "TERRITORIES", "MILITARY", "STATE_NAME_TO_CODE"]


@pytest.mark.parametrize("name", GOLD_SHARED)
def test_gold_notebook_constants_match_reference(name):
    assert notebook_constant(GOLD_NB, name) == getattr(gm, name)


@pytest.mark.parametrize("name", STATE_SHARED)
def test_gold_notebook_state_lists_match_silver_rules(name):
    assert notebook_constant(GOLD_NB, name) == getattr(sr, name)


def test_reference_notebook_dimensions_match():
    assert notebook_constant(REF_NB, "EXPECTED_DIMENSIONS") == sorted(gm.EXACT_DIMENSIONS)


@pytest.mark.parametrize("path", [GOLD_NB, REF_NB])
def test_gold_notebooks_compile(path):
    text = path.read_text()
    assert text.startswith("# Databricks notebook source\n")
    compile(text, str(path), "exec")


def test_table_comments_have_no_quotes_that_break_sql():
    tables = ast.literal_eval(
        GOLD_NB.read_text().split("TABLES = ", 1)[1].split("\n]\n", 1)[0]
        .replace("gold_volume", "None").replace("gold_company_est", "None")
        .replace("gold_sub_issue_est", "None").replace("gold_calibration", "None")
        .replace("gold_company", "None").replace("gold_issue_map", "None").replace("gold_issue", "None")
        .replace("gold_state", "None").replace("gold_response", "None").replace("gold_facts", "None") + "\n]")
    assert len(tables) == 10
    assert all("'" not in comment for _n, _df, _k, comment in tables)


@pytest.mark.skipif(not (SAMPLE.exists() and REF_JSON.exists()), reason="local data not present")
def test_expected_gold_digests_match_reference_on_real_data():
    silver = [sr.transform_record(r) for r in pq.read_table(SAMPLE).to_pylist()]
    gold = gm.build_gold(silver, json.loads(REF_JSON.read_text()))
    assert notebook_constant(GOLD_NB, "EXPECTED_GOLD") == gm.expected_digests(gold)
    assert gold["coverage"]["coverage"] >= gm.COVERAGE_FAIL_BELOW
    facts = gold["facts"]
    assert len({f["fact_id"] for f in facts}) == len(facts)
    assert all(f["is_reliable"] and f["ci95_low"] is not None for f in facts if f["measure_type"] == "estimated")
