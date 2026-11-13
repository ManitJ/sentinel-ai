"""Offline tests for the SQL showcase validation runner and notebook generator."""

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from lakehouse import build_sql_validation_notebook as builder
from lakehouse import sql_validation as sv
from lakehouse.save_sql_validation_report import save_report

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"
NOTEBOOK = ROOT / "databricks/notebooks/06_sql_showcase_validation.py"
REF_JSON = ROOT / "data/reference/cfpb_api_monthly_credit_reporting.json"
EXPECTED_IDS = ["Q2.1", "Q2.2", "Q2.3", "Q2.4", "Q3.1", "Q3.2", "Q3.3", "Q3.4", "Q4.1", "Q4.2", "Q4.3",
                "Q5.1", "Q5.2", "Q5.3", "Q5.4", "Q6.1", "Q6.2"]


# --- parsing ----------------------------------------------------------------------------

def test_all_17_queries_parsed_in_order():
    assert [q["id"] for q in sv.load_queries(SQL_DIR)] == EXPECTED_IDS


def test_parsed_sql_matches_the_files_exactly():
    for name in sv.ANALYTICS_FILES:
        text = (SQL_DIR / name).read_text()
        code = sv.strip_comments(text)
        statements = [s.strip() for s in code.split(";") if s.strip()]
        parsed = [q["sql"] for q in sv.parse_queries(text, name)]
        assert [re.sub(r"\s+", " ", s) for s in parsed] == [re.sub(r"\s+", " ", s) for s in statements]


def test_semicolons_and_apostrophes_in_comments_or_strings_do_not_split():
    text = ("-- Q9.1 first; with a semicolon and the company's apostrophe\n"
            "SELECT 'a;b' AS x, '--not a comment' AS y FROM t;\n"
            "-- Q9.2\nWITH c AS (SELECT 1 AS n) SELECT n FROM c;\n")
    queries = sv.parse_queries(text, "f.sql")
    assert [q["id"] for q in queries] == ["Q9.1", "Q9.2"]
    assert "'a;b'" in queries[0]["sql"] and "'--not a comment'" in queries[0]["sql"]


@pytest.mark.parametrize("text, message", [
    ("SELECT 1;", "without a '-- Qn.m' label"),
    ("-- Q9.1\nSELECT 1", "trailing statement"),
])
def test_malformed_files_are_rejected(text, message):
    with pytest.raises(ValueError, match=message):
        sv.parse_queries(text, "f.sql")


# --- read-only guard ----------------------------------------------------------------------

@pytest.mark.parametrize("sql", [q["sql"] for q in sv.load_queries(SQL_DIR)], ids=EXPECTED_IDS)
def test_every_showcase_query_passes_the_read_only_guard(sql):
    assert sv.read_only_problem(sql) is None


@pytest.mark.parametrize("sql", [
    "DELETE FROM workspace.sentinel.gold_monthly_volume",
    "CREATE TABLE x AS SELECT 1",
    "INSERT INTO t SELECT 1",
    "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE",
    "WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a",
    "SELECT 1; DROP TABLE t",
    "UPDATE t SET a = 1",
    "USE CATALOG other",
    "SET spark.sql.ansi.enabled = false",
    "CREATE OR REPLACE VIEW v AS SELECT 1",
    "OPTIMIZE t",
    "",
])
def test_guard_blocks_writes_session_changes_and_multiple_statements(sql):
    assert sv.read_only_problem(sql) is not None


# --- benchmarks and checks ------------------------------------------------------------------

def test_every_query_has_benchmarks():
    assert sorted(sv.BENCHMARKS) == sorted(EXPECTED_IDS)
    assert all(any(kind == "row_count" for kind, _ in sv.BENCHMARKS[q]) for q in EXPECTED_IDS)


def test_check_kinds_pass_and_fail_correctly():
    rows = [{"k": "a", "n": Decimal("10.004"), "m": "estimated"}, {"k": "b", "n": 5, "m": "estimated"}]
    checks = [("row_count", 2), sv._value({"k": "a"}, "n", 10.0, 0.011), sv._sum("n", 15, 0.01),
              sv._all_equal("m", "estimated")]
    assert all(c["passed"] for c in sv.check_result("X", ["k", "n", "m"], rows, 2, checks))
    bad = [("row_count", 3), sv._value({"k": "a"}, "n", 11, 0.011), sv._sum("n", 16), sv._all_equal("m", "exact"),
           sv._value({"k": "zzz"}, "n", 1), sv._value({}, "missing", 1)]
    results = sv.check_result("X", ["k", "n", "m"], rows, 2, bad)
    assert not any(c["passed"] for c in results)
    assert results[-1]["actual"] == "column missing" and results[-2]["actual"] == "0 matching rows"


def fake_rows_for(query_id):
    """A result that satisfies every benchmark of a query (built from the benchmarks)."""
    specs = sv.BENCHMARKS[query_id]
    n = next(v for k, v in specs if k == "row_count")
    rows = [dict() for _ in range(n)]
    for kind, spec in specs:
        if kind == "value":
            target = next(r for r in rows if all(r.get(k, v) == v for k, v in spec["where"].items()))
            target.update(spec["where"])
            target[spec["column"]] = spec["equals"]
        elif kind == "sum":
            for r in rows:
                r[spec["column"]] = 0
            rows[0][spec["column"]] = spec["equals"]
        elif kind == "all_equal":
            for r in rows:
                r[spec["column"]] = spec["equals"]
    if query_id == "Q2.1":                                   # three distinct ranked rows
        for rank, r in enumerate(rows, 1):
            r.setdefault("company_rank", rank)
    columns = sorted({c for r in rows for c in r}) or ["x"]
    return columns, [{c: r.get(c) for c in columns} for r in rows], n


def test_runner_passes_failures_and_not_run_states():
    queries = sv.load_queries(SQL_DIR)
    calls = []

    def execute(sql):
        query_id = next(q["id"] for q in queries if q["sql"] == sql)
        calls.append(query_id)
        if query_id == "Q3.1":
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND gold_issue_monthly")
        columns, rows, n = fake_rows_for(query_id)
        if query_id == "Q5.1":
            rows[0]["pct_growth"] = 12.0                      # wrong value -> FAIL
        return columns, rows, n

    blocked = dict(queries[0], sql="DELETE FROM workspace.sentinel.gold_monthly_volume")
    report = sv.run_validation([blocked] + queries[1:], execute, selected=set(EXPECTED_IDS) - {"Q6.2"},
                               preview_rows=3)
    status = {e["id"]: e["status"] for e in report}
    assert status["Q2.1"] == sv.NOT_RUN and "read-only guard" in report[0]["error"]
    assert "Q2.1" not in calls                                # blocked query never executed
    assert status["Q6.2"] == sv.NOT_RUN and "Q6.2" not in calls
    assert status["Q3.1"] == sv.FAIL and "TABLE_OR_VIEW_NOT_FOUND" in next(e for e in report if e["id"] == "Q3.1")["error"]
    assert status["Q5.1"] == sv.FAIL
    assert [q for q, s in status.items() if s == sv.PASS] == [q for q in EXPECTED_IDS
                                                             if q not in {"Q2.1", "Q3.1", "Q5.1", "Q6.2"}]
    assert all(len(e["preview"]) <= 3 for e in report)
    summary = {r["query"]: r for r in sv.summary_rows(report)}
    assert "pct_growth" in summary["Q5.1"]["failed_checks"]
    parsed = json.loads(sv.report_json(report, generated_at="t"))
    assert parsed["summary"] == {"PASS": 13, "FAIL": 2, "NOT RUN": 2}


# --- notebook --------------------------------------------------------------------------------

def test_committed_notebook_is_in_sync_with_sql_files_and_module():
    assert NOTEBOOK.read_text() == builder.build(), \
        "regenerate: PYTHONPATH=src python -m lakehouse.build_sql_validation_notebook"


def test_notebook_is_read_only_and_credential_free():
    text = NOTEBOOK.read_text()
    assert text.startswith("# Databricks notebook source\n")
    compile(text, str(NOTEBOOK), "exec")
    code = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))
    tail = code[code.index('dbutils.widgets.text("queries", "all")'):]
    assert tail.count("spark.sql(") == 1                       # only inside execute(), after the guard
    assert not re.search(r"\.write\b|saveAsTable|insertInto|createOrReplace|\.save\(|dbutils\.fs|open\(",
                         tail)
    assert not re.search(r"(token|password|secret|DATABRICKS_HOST|https://)", tail, re.IGNORECASE)
    assert "run_validation(QUERIES, execute" in tail          # guard applied inside run_validation


# --- saving results locally -----------------------------------------------------------------

def test_save_report_writes_summary_and_limited_previews(tmp_path):
    queries = sv.load_queries(SQL_DIR)[:3]
    report = sv.run_validation(queries, lambda sql: (["a"], [{"a": i} for i in range(25)], 25), preview_rows=25)
    info = save_report(sv.report_json(report, generated_at="2026-11-13T00:00:00+00:00"), tmp_path)
    assert info["counts"]["FAIL"] == 3 and info["previews_written"] == 3
    preview = (tmp_path / "previews" / "Q2.1.md").read_text()
    assert preview.count("\n| ") == 1 + 10                    # header + 10 rows (limited)
    summary = (tmp_path / "validation_summary.md").read_text()
    assert "0 PASS · 3 FAIL · 0 NOT RUN" in summary and "row_count" in summary
    saved = json.loads((tmp_path / "validation_report.json").read_text())
    assert all(len(e["preview"]) <= 10 for e in saved["queries"])


def test_save_report_rejects_unrelated_json(tmp_path):
    with pytest.raises((ValueError, KeyError)):
        save_report(json.dumps({"queries": [{"foo": 1}]}), tmp_path)


# --- benchmarks trace back to the reference data -----------------------------------------------

@pytest.mark.skipif(not REF_JSON.exists(), reason="local reference data not present")
def test_benchmarks_match_reference_data():
    ref = json.loads(REF_JSON.read_text())
    months = {m["month"]: m for m in ref["months"]}
    total = {k: v["total"] for k, v in months.items()}
    current = [m for m in total if m.startswith("2026")]
    prior = [m.replace("2026", "2025") for m in current]
    q51 = {spec["column"]: spec["equals"] for kind, spec in sv.BENCHMARKS["Q5.1"] if kind == "value"}
    assert q51["prior_complaints"] == sum(total[m] for m in prior)
    assert q51["current_complaints"] == sum(total[m] for m in current)
    assert sum(total.values()) == 10500160
    companies = {}
    for m in months.values():
        for name, n in m["aggregations"]["company"].items():
            companies[name] = companies.get(name, 0) + n
    top3 = sorted(companies.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
    assert top3 == sv.TOP3
    october = months["2026-10"]["aggregations"]["company"]
    shares = sorted((n / total["2026-10"] for n in october.values()), reverse=True)
    q61 = {spec["column"]: spec["equals"] for kind, spec in sv.BENCHMARKS["Q6.1"] if kind == "value"}
    assert q61["complaint_concentration_hhi"] == round(10000 * sum(s * s for s in shares))
    assert q61["top3_company_pct"] == round(100 * sum(shares[:3]), 2)
