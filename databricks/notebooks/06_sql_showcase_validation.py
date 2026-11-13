# Databricks notebook source
# MAGIC %md
# MAGIC # Sentinel AI — 06 · SQL showcase validation (read-only)
# MAGIC
# MAGIC **Generated file — do not edit.** Rebuild with
# MAGIC `PYTHONPATH=src python -m lakehouse.build_sql_validation_notebook`.
# MAGIC
# MAGIC Runs queries **Q2.1–Q6.2** from `sql/02`–`06` in this workspace session and checks the results
# MAGIC against benchmarks computed from the same reference data the Gold layer was built from.
# MAGIC
# MAGIC * **Read-only.** Every query must be a single `SELECT`/`WITH` statement; anything else is
# MAGIC   blocked before execution (status `NOT RUN`). The notebook writes no tables, files or views.
# MAGIC * **No credentials.** It uses the notebook's own Databricks session.
# MAGIC * **Output.** A summary table (PASS / FAIL / NOT RUN per query) and a JSON report with
# MAGIC   column names, row counts, check details, errors and up to `preview_rows` rows per query.
# MAGIC   Copy the JSON report back to the repository for `sql/results/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Validation code (from `src/lakehouse/sql_validation.py`)

# COMMAND ----------

"""Validation of the SQL analytics showcase (pure Python, no Spark).

Parses the labelled queries (``-- Qn.m``) from ``sql/02``–``06``, refuses anything that is not a
single read-only ``SELECT``/``WITH`` statement, runs them through an injected ``execute(sql)``
callable, and checks results against benchmarks.

The Databricks notebook ``databricks/notebooks/06_sql_showcase_validation.py`` is GENERATED from
this module and the SQL files by ``python -m lakehouse.build_sql_validation_notebook``, so the
notebook always runs exactly the code tested offline.

Benchmarks were computed locally from the same reference data the Gold layer was built from
(CFPB API monthly totals and facets fetched 2026-11-12, and the 50,006-record sample).
"""

import json
import re
from decimal import Decimal

ANALYTICS_FILES = ["02_company_benchmarking.sql", "03_issue_trends.sql", "04_geographic_distribution.sql",
                   "05_growth_drivers.sql", "06_executive_scorecard.sql"]
QUERY_ID_RE = re.compile(r"--\s*(Q\d+\.\d+)\b")
WRITE_RE = re.compile(r"\b(CREATE|INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|REPLACE|OPTIMIZE|VACUUM|"
                      r"GRANT|REVOKE|COPY|REFRESH|CACHE|UNCACHE|MSCK|RESTORE|SET|RESET|USE|CALL|EXECUTE)\b",
                      re.IGNORECASE)
PASS, FAIL, NOT_RUN = "PASS", "FAIL", "NOT RUN"
MAX_ROWS = 1000          # every showcase query returns far fewer rows; more is reported, not fetched
PCT2, PCT1 = 0.011, 0.051  # tolerances for values rounded to 2 / 1 decimals


# --- parsing and read-only guard ------------------------------------------------------

def strip_comments(sql):
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _split_code(line):
    """(code, comment) for one line; '--' and ';' inside single-quoted strings are code."""
    in_string = False
    for i, ch in enumerate(line):
        if ch == "'":
            in_string = not in_string
        elif not in_string and line.startswith("--", i):
            return line[:i], line[i:]
    return line, ""


def parse_queries(text, file_name):
    """Split a SQL file into labelled queries: [{'id', 'file', 'sql'}].

    Labels come only from comments; statements end only at a ';' in code outside a string,
    so semicolons or apostrophes inside comments cannot split a query.
    """
    queries, label, buffer = [], None, []
    for line in text.splitlines():
        code, comment = _split_code(line)
        found = QUERY_ID_RE.findall(comment)
        if found:
            label = found[-1]
        in_string, start = False, 0
        for i, ch in enumerate(code):
            if ch == "'":
                in_string = not in_string
            elif ch == ";" and not in_string:
                buffer.append(code[start:i])
                statement = "\n".join(buffer).strip()
                if statement:
                    if not label:
                        raise ValueError(f"{file_name}: statement without a '-- Qn.m' label: {statement[:60]!r}")
                    queries.append({"id": label, "file": file_name, "sql": statement})
                buffer, start = [], i + 1
        buffer.append(code[start:])
    if "\n".join(buffer).strip():
        raise ValueError(f"{file_name}: trailing statement without ';'")
    return queries


def load_queries(sql_dir, files=ANALYTICS_FILES):
    from pathlib import Path
    queries = []
    for name in files:
        queries += parse_queries((Path(sql_dir) / name).read_text(), name)
    ids = [q["id"] for q in queries]
    if len(ids) != len(set(ids)):
        raise ValueError(f"duplicate query ids: {ids}")
    return queries


def read_only_problem(sql):
    """None if the statement is a single read-only SELECT/WITH; otherwise the reason."""
    code = strip_comments(sql).strip().rstrip(";").strip()
    if ";" in code:
        return "more than one statement"
    first = code.split(None, 1)[0].upper() if code else ""
    if first not in ("SELECT", "WITH"):
        return f"statement starts with {first or 'nothing'}, not SELECT/WITH"
    match = WRITE_RE.search(code)
    if match:
        return f"write/session keyword {match.group(1).upper()}"
    return None


# --- benchmarks -------------------------------------------------------------------------

def _value(where, column, equals, tol=0):
    return ("value", {"where": where, "column": column, "equals": equals, "tol": tol})


def _sum(column, equals, tol=0):
    return ("sum", {"column": column, "equals": equals, "tol": tol})


def _rows(n):
    return ("row_count", n)


def _all_equal(column, equals):
    return ("all_equal", {"column": column, "equals": equals})


TOP3 = [("TRANSUNION INTERMEDIATE HOLDINGS, INC.", 3567685), ("EQUIFAX, INC.", 3368770),
        ("Experian Information Solutions Inc.", 3128932)]
OCT = {"month_label": "2026-10"}
ONE = {}  # single-row result

BENCHMARKS = {
    "Q2.1": [_rows(10)] + [c for rank, (name, n) in enumerate(TOP3, 1)
                           for c in (_value({"company_rank": rank}, "company", name),
                                     _value({"company_rank": rank}, "complaints", n))],
    "Q2.2": [_rows(22), _value(OCT, "top1_complaint_pct", 36.77, PCT2),
             _value(OCT, "top3_complaint_pct", 97.68, PCT2), _value(OCT, "complaint_concentration_hhi", 3219, 0.5)],
    "Q2.3": [_rows(10)],
    "Q2.4": [_rows(47), _all_equal("measure_type", "estimated")],
    "Q3.1": [_rows(13), _sum("complaints", 10500160)],
    "Q3.2": [_rows(160), _sum("complaints", 10500160)],
    "Q3.3": [_rows(12), _sum("prior_complaints", 3906784), _sum("current_complaints", 5689871)],
    "Q3.4": [_rows(15), _all_equal("measure_type", "estimated")],
    "Q4.1": [_rows(5), _sum("complaints", 10500160)],
    "Q4.2": [_rows(51), _sum("pct_of_state_and_dc", 100, 0.3)],
    "Q4.3": [_rows(51), _sum("prior_complaints", 3881011), _sum("current_complaints", 5660738)],
    "Q5.1": [_rows(1), _value(ONE, "prior_complaints", 3906784), _value(ONE, "current_complaints", 5689871),
             _value(ONE, "net_change_complaints", 1783087), _value(ONE, "pct_growth", 45.6, PCT1)],
    "Q5.2": [_rows(15)],
    "Q5.3": [_rows(12), _sum("net_change_complaints", 1783087), _sum("contribution_to_net_change_pct", 100, 0.07)],
    "Q5.4": [_rows(15)],
    "Q6.1": [_rows(1)] + [_value(ONE, column, expected, tol) for column, expected, tol in [
        ("latest_month", "2026-10", 0), ("latest_month_complaints", 711218, 0), ("mom_pct", 16.9, PCT1),
        ("yoy_pct", 51.6, PCT1), ("ytd_complaints", 5689871, 0), ("prior_ytd_complaints", 3906784, 0),
        ("ytd_pct_growth", 45.6, PCT1), ("top_company", "Experian Information Solutions Inc.", 0),
        ("top_company_pct", 36.77, PCT2), ("top3_company_pct", 97.68, PCT2),
        ("complaint_concentration_hhi", 3219, 0.5), ("top_issue", "Incorrect information on your report", 0),
        ("top_issue_pct", 61.09, PCT2), ("top_state", "TX", 0), ("top_state_pct", 16.92, PCT2),
        ("response_reference_month", "2026-08", 0), ("timely_response_pct", 99.92, PCT2),
        ("monetary_relief_pct", 0.01, PCT2), ("non_monetary_relief_pct", 36.15, PCT2),
        ("closed_with_explanation_pct", 63.75, PCT2), ("provisional_response_months", "2026-09, 2026-10", 0)]],
    "Q6.2": [_rows(20)],
}


# --- checking ------------------------------------------------------------------------------

def _norm(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def _equal(actual, expected, tol):
    actual = _norm(actual)
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)) and not isinstance(actual, bool):
        return abs(actual - expected) <= tol
    return str(actual) == str(expected)


def check_result(query_id, columns, rows, row_count, benchmarks=None):
    """Apply benchmarks to a result. Returns [{'check', 'expected', 'actual', 'passed'}]."""
    results = []
    for kind, spec in (benchmarks if benchmarks is not None else BENCHMARKS.get(query_id, [])):
        if kind == "row_count":
            results.append({"check": "row_count", "expected": spec, "actual": row_count,
                            "passed": row_count == spec})
            continue
        column = spec["column"]
        if column not in columns:
            results.append({"check": f"{kind}:{column}", "expected": spec.get("equals"),
                            "actual": "column missing", "passed": False})
            continue
        if kind == "value":
            matches = [r for r in rows if all(_equal(r.get(k), v, 0) for k, v in spec["where"].items())]
            actual = _norm(matches[0][column]) if len(matches) == 1 else f"{len(matches)} matching rows"
            passed = len(matches) == 1 and _equal(actual, spec["equals"], spec["tol"])
            label = f"{column}" + (f" where {spec['where']}" if spec["where"] else "")
        elif kind == "sum":
            values = [_norm(r[column]) for r in rows if r.get(column) is not None]
            actual = round(sum(values), 6)
            passed = _equal(actual, spec["equals"], spec["tol"])
            label = f"sum({column})"
        elif kind == "all_equal":
            distinct = sorted({str(r.get(column)) for r in rows})
            actual = ", ".join(distinct)
            passed = distinct == [str(spec["equals"])]
            label = f"all {column}"
        else:
            raise ValueError(f"unknown check {kind}")
        results.append({"check": label, "expected": spec["equals"], "actual": actual, "passed": passed})
    return results


def run_validation(queries, execute, selected=None, preview_rows=10):
    """Run each query through ``execute(sql) -> (columns, rows, row_count)``.

    Every query is checked by ``read_only_problem`` BEFORE execution; a query that fails the
    guard is NOT RUN. Errors are caught per query (FAIL) so one failure does not stop the rest.
    """
    report = []
    for query in queries:
        entry = {"id": query["id"], "file": query["file"], "status": NOT_RUN, "row_count": None,
                 "columns": [], "checks": [], "error": None, "preview": []}
        if selected is not None and query["id"] not in selected:
            entry["error"] = "not selected"
            report.append(entry)
            continue
        problem = read_only_problem(query["sql"])
        if problem:
            entry["error"] = f"blocked by read-only guard: {problem}"
            report.append(entry)
            continue
        try:
            columns, rows, row_count = execute(query["sql"])
        except Exception as error:  # noqa: BLE001 - every failure is reported per query
            entry.update(status=FAIL, error=f"{type(error).__name__}: {str(error)[:500]}")
            report.append(entry)
            continue
        checks = check_result(query["id"], columns, rows, row_count)
        entry.update(columns=list(columns), row_count=row_count, checks=checks,
                     preview=[{k: _norm(v) for k, v in r.items()} for r in rows[:preview_rows]],
                     status=PASS if checks and all(c["passed"] for c in checks) else FAIL)
        if not checks:
            entry["error"] = "no benchmarks defined"
        report.append(entry)
    return report


def summary_rows(report):
    return [{"query": e["id"], "file": e["file"], "status": e["status"], "rows": e["row_count"],
             "checks_passed": sum(c["passed"] for c in e["checks"]), "checks_total": len(e["checks"]),
             "failed_checks": "; ".join(f"{c['check']} expected {c['expected']} got {c['actual']}"
                                        for c in e["checks"] if not c["passed"]) or None,
             "error": e["error"]} for e in report]


def report_json(report, generated_at=None):
    counts = {s: sum(e["status"] == s for e in report) for s in (PASS, FAIL, NOT_RUN)}
    return json.dumps({"generated_at": generated_at, "summary": counts, "queries": report},
                      indent=2, default=str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Queries (parsed from `sql/02`–`06`)

# COMMAND ----------

QUERIES = json.loads(r'''[
 {
  "id": "Q2.1",
  "file": "02_company_benchmarking.sql",
  "sql": "WITH all_complaints AS (\n  SELECT SUM(population_complaints) AS total FROM workspace.sentinel.gold_monthly_volume\n),\ncompany_totals AS (\n  SELECT company,\n         SUM(complaints) AS complaints,\n         COUNT(*)        AS months_with_complaints\n  FROM workspace.sentinel.gold_company_monthly\n  WHERE NOT is_residual\n  GROUP BY company\n),\nranked AS (\n  SELECT c.*,\n         ROW_NUMBER() OVER (ORDER BY c.complaints DESC, c.company)                        AS company_rank,\n         SUM(c.complaints) OVER (ORDER BY c.complaints DESC, c.company\n                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS cumulative_complaints\n  FROM company_totals AS c\n)\nSELECT r.company_rank,\n       r.company,\n       r.complaints,\n       r.months_with_complaints,\n       ROUND(100 * r.complaints / a.total, 2)            AS pct_of_all_complaints,\n       ROUND(100 * r.cumulative_complaints / a.total, 2) AS cumulative_pct_of_all_complaints\nFROM ranked AS r\nCROSS JOIN all_complaints AS a\nWHERE r.company_rank <= 10\nORDER BY r.company_rank"
 },
 {
  "id": "Q2.2",
  "file": "02_company_benchmarking.sql",
  "sql": "SELECT month_label,\n       COUNT(*)                                                                AS companies_with_complaints,\n       ROUND(100 * MAX(share_of_month), 2)                                     AS top1_complaint_pct,\n       ROUND(100 * SUM(CASE WHEN rank_in_month <= 3 THEN share_of_month ELSE 0 END), 2)\n                                                                               AS top3_complaint_pct,\n       ROUND(10000 * SUM(share_of_month * share_of_month), 0)                  AS complaint_concentration_hhi\nFROM workspace.sentinel.gold_company_monthly\nWHERE NOT is_residual\nGROUP BY month_label\nORDER BY month_label"
 },
 {
  "id": "Q2.3",
  "file": "02_company_benchmarking.sql",
  "sql": "WITH latest AS (\n  SELECT MAX(month) AS latest_month FROM workspace.sentinel.gold_monthly_volume\n),\nmonths AS (\n  SELECT DATE_FORMAT(latest_month, 'yyyy-MM')                 AS current_label,\n         DATE_FORMAT(ADD_MONTHS(latest_month, -12), 'yyyy-MM') AS prior_label\n  FROM latest\n),\ntop_companies AS (\n  SELECT company\n  FROM workspace.sentinel.gold_company_monthly\n  WHERE NOT is_residual\n  GROUP BY company\n  ORDER BY SUM(complaints) DESC, company\n  LIMIT 10\n)\nSELECT t.company,\n       m.prior_label                                            AS prior_month,\n       p.rank_in_month                                          AS prior_rank,\n       p.complaints                                             AS prior_complaints,\n       ROUND(100 * p.share_of_month, 2)                         AS prior_pct_of_complaints,\n       m.current_label                                          AS current_month,\n       c.rank_in_month                                          AS current_rank,\n       c.complaints                                             AS current_complaints,\n       ROUND(100 * c.share_of_month, 2)                         AS current_pct_of_complaints,\n       COALESCE(c.complaints, 0) - COALESCE(p.complaints, 0)    AS net_change_complaints,\n       ROUND(100 * (COALESCE(c.share_of_month, 0) - COALESCE(p.share_of_month, 0)), 2)\n                                                                AS share_change_pct_points\nFROM top_companies AS t\nCROSS JOIN months AS m\nLEFT JOIN workspace.sentinel.gold_company_monthly AS p\n  ON p.company = t.company AND p.month_label = m.prior_label\nLEFT JOIN workspace.sentinel.gold_company_monthly AS c\n  ON c.company = t.company AND c.month_label = m.current_label\nORDER BY c.rank_in_month NULLS LAST, t.company"
 },
 {
  "id": "Q2.4",
  "file": "02_company_benchmarking.sql",
  "sql": "WITH top_companies AS (\n  SELECT company\n  FROM workspace.sentinel.gold_company_monthly\n  WHERE NOT is_residual\n  GROUP BY company\n  ORDER BY SUM(complaints) DESC, company\n  LIMIT 10\n)\nSELECT e.company,\n       e.dimension,\n       e.category,\n       ROUND(100 * e.est_share, 2)  AS est_pct_of_company_complaints,\n       ROUND(100 * e.ci95_low, 2)   AS ci95_low_pct,\n       ROUND(100 * e.ci95_high, 2)  AS ci95_high_pct,\n       e.n_domain_sample            AS company_sample_records,\n       e.measure_type\nFROM workspace.sentinel.gold_company_response_estimates AS e\nJOIN top_companies AS t ON t.company = e.company\nWHERE e.period = '2025'\n  AND e.is_reliable\nORDER BY e.company, e.dimension, e.est_share DESC"
 },
 {
  "id": "Q3.1",
  "file": "03_issue_trends.sql",
  "sql": "WITH all_complaints AS (\n  SELECT SUM(population_complaints) AS total FROM workspace.sentinel.gold_monthly_volume\n)\nSELECT RANK() OVER (ORDER BY SUM(i.complaints) DESC) AS issue_rank,\n       i.issue,\n       i.issue_group,\n       i.is_combined_group,\n       SUM(i.complaints)                              AS complaints,\n       ROUND(100 * SUM(i.complaints) / MAX(a.total), 2) AS pct_of_complaints\nFROM workspace.sentinel.gold_issue_monthly AS i\nCROSS JOIN all_complaints AS a\nWHERE NOT i.is_residual\nGROUP BY i.issue, i.issue_group, i.is_combined_group\nORDER BY complaints DESC"
 },
 {
  "id": "Q3.2",
  "file": "03_issue_trends.sql",
  "sql": "SELECT month_label,\n       issue_group,\n       SUM(complaints)                                                       AS complaints,\n       ROUND(100 * SUM(share_of_month), 2)                                   AS pct_of_month,\n       RANK() OVER (PARTITION BY month_label ORDER BY SUM(complaints) DESC)  AS rank_in_month\nFROM workspace.sentinel.gold_issue_monthly\nWHERE NOT is_residual\nGROUP BY month_label, issue_group\nORDER BY month_label, rank_in_month"
 },
 {
  "id": "Q3.3",
  "file": "03_issue_trends.sql",
  "sql": "WITH bounds AS (\n  SELECT MAX(month)                         AS latest_month,\n         DATE_TRUNC('YEAR', MAX(month))     AS current_start\n  FROM workspace.sentinel.gold_monthly_volume\n),\nperiods AS (\n  SELECT 'current' AS period, CAST(current_start AS DATE) AS start_month, latest_month AS end_month FROM bounds\n  UNION ALL\n  SELECT 'prior', ADD_MONTHS(CAST(current_start AS DATE), -12), ADD_MONTHS(latest_month, -12) FROM bounds\n),\nissue_period AS (\n  SELECT p.period, i.issue_group, SUM(i.complaints) AS complaints\n  FROM workspace.sentinel.gold_issue_monthly AS i\n  JOIN periods AS p ON i.month BETWEEN p.start_month AND p.end_month\n  WHERE NOT i.is_residual\n  GROUP BY p.period, i.issue_group\n),\nperiod_totals AS (\n  SELECT p.period, SUM(v.population_complaints) AS total\n  FROM workspace.sentinel.gold_monthly_volume AS v\n  JOIN periods AS p ON v.month BETWEEN p.start_month AND p.end_month\n  GROUP BY p.period\n)\nSELECT g.issue_group,\n       COALESCE(pr.complaints, 0)                                          AS prior_complaints,\n       COALESCE(cu.complaints, 0)                                          AS current_complaints,\n       ROUND(100 * COALESCE(pr.complaints, 0) / tp.total, 2)               AS prior_pct_of_complaints,\n       ROUND(100 * COALESCE(cu.complaints, 0) / tc.total, 2)               AS current_pct_of_complaints,\n       ROUND(100 * (COALESCE(cu.complaints, 0) / tc.total\n                    - COALESCE(pr.complaints, 0) / tp.total), 2)           AS share_change_pct_points\nFROM (SELECT DISTINCT issue_group FROM issue_period) AS g\nLEFT JOIN issue_period AS pr ON pr.issue_group = g.issue_group AND pr.period = 'prior'\nLEFT JOIN issue_period AS cu ON cu.issue_group = g.issue_group AND cu.period = 'current'\nCROSS JOIN (SELECT total FROM period_totals WHERE period = 'prior')   AS tp\nCROSS JOIN (SELECT total FROM period_totals WHERE period = 'current') AS tc\nORDER BY current_complaints DESC"
 },
 {
  "id": "Q3.4",
  "file": "03_issue_trends.sql",
  "sql": "SELECT issue,\n       COALESCE(sub_issue, '(no sub-issue)')  AS sub_issue,\n       ROUND(100 * est_share, 2)              AS est_pct_of_complaints,\n       ROUND(100 * ci95_low, 2)               AS ci95_low_pct,\n       ROUND(100 * ci95_high, 2)              AS ci95_high_pct,\n       n_category_sample                      AS sample_records,\n       measure_type\nFROM workspace.sentinel.gold_sub_issue_estimates\nWHERE period = 'ALL'\n  AND is_reliable\nORDER BY est_share DESC\nLIMIT 15"
 },
 {
  "id": "Q4.1",
  "file": "04_geographic_distribution.sql",
  "sql": "WITH all_complaints AS (\n  SELECT SUM(population_complaints) AS total FROM workspace.sentinel.gold_monthly_volume\n)\nSELECT COALESCE(s.state_category, 'no state given')        AS location_type,\n       COUNT(DISTINCT s.state)                             AS distinct_locations,\n       SUM(s.complaints)                                   AS complaints,\n       ROUND(100 * SUM(s.complaints) / MAX(a.total), 3)    AS pct_of_all_complaints\nFROM workspace.sentinel.gold_state_monthly AS s\nCROSS JOIN all_complaints AS a\nGROUP BY s.state_category\nORDER BY complaints DESC"
 },
 {
  "id": "Q4.2",
  "file": "04_geographic_distribution.sql",
  "sql": "WITH all_complaints AS (\n  SELECT SUM(population_complaints) AS total FROM workspace.sentinel.gold_monthly_volume\n),\nstate_totals AS (\n  SELECT state_code, state_category, SUM(complaints) AS complaints\n  FROM workspace.sentinel.gold_state_monthly\n  WHERE state_category IN ('state', 'district')\n  GROUP BY state_code, state_category\n)\nSELECT RANK() OVER (ORDER BY st.complaints DESC)                        AS state_rank,\n       st.state_code,\n       st.state_category,\n       st.complaints,\n       ROUND(100 * st.complaints / a.total, 2)                         AS pct_of_all_complaints,\n       ROUND(100 * st.complaints / SUM(st.complaints) OVER (), 2)      AS pct_of_state_and_dc\nFROM state_totals AS st\nCROSS JOIN all_complaints AS a\nORDER BY state_rank, st.state_code"
 },
 {
  "id": "Q4.3",
  "file": "04_geographic_distribution.sql",
  "sql": "WITH bounds AS (\n  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start\n  FROM workspace.sentinel.gold_monthly_volume\n),\nby_period AS (\n  SELECT s.state_code,\n         SUM(CASE WHEN s.month BETWEEN b.current_start AND b.latest_month THEN s.complaints ELSE 0 END)\n           AS current_complaints,\n         SUM(CASE WHEN s.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)\n                  THEN s.complaints ELSE 0 END)\n           AS prior_complaints\n  FROM workspace.sentinel.gold_state_monthly AS s\n  CROSS JOIN bounds AS b\n  WHERE s.state_category IN ('state', 'district')\n  GROUP BY s.state_code\n)\nSELECT state_code,\n       prior_complaints,\n       current_complaints,\n       current_complaints - prior_complaints                                   AS net_change_complaints,\n       CASE WHEN prior_complaints >= 100\n            THEN ROUND(100 * (current_complaints - prior_complaints) / prior_complaints, 1) END\n                                                                               AS pct_growth,\n       RANK() OVER (ORDER BY current_complaints - prior_complaints DESC)       AS net_change_rank\nFROM by_period\nORDER BY net_change_complaints DESC"
 },
 {
  "id": "Q5.1",
  "file": "05_growth_drivers.sql",
  "sql": "WITH bounds AS (\n  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start\n  FROM workspace.sentinel.gold_monthly_volume\n)\nSELECT DATE_FORMAT(ADD_MONTHS(b.current_start, -12), 'yyyy-MM') || ' to ' ||\n         DATE_FORMAT(ADD_MONTHS(b.latest_month, -12), 'yyyy-MM')                     AS prior_period,\n       DATE_FORMAT(b.current_start, 'yyyy-MM') || ' to ' ||\n         DATE_FORMAT(b.latest_month, 'yyyy-MM')                                      AS current_period,\n       SUM(CASE WHEN v.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)\n                THEN v.population_complaints ELSE 0 END)                             AS prior_complaints,\n       SUM(CASE WHEN v.month BETWEEN b.current_start AND b.latest_month\n                THEN v.population_complaints ELSE 0 END)                             AS current_complaints,\n       SUM(CASE WHEN v.month BETWEEN b.current_start AND b.latest_month\n                THEN v.population_complaints ELSE 0 END)\n       - SUM(CASE WHEN v.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)\n                  THEN v.population_complaints ELSE 0 END)                           AS net_change_complaints,\n       ROUND(100 * (SUM(CASE WHEN v.month BETWEEN b.current_start AND b.latest_month\n                             THEN v.population_complaints ELSE 0 END)\n                    / SUM(CASE WHEN v.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)\n                               THEN v.population_complaints ELSE 0 END) - 1), 1)     AS pct_growth\nFROM workspace.sentinel.gold_monthly_volume AS v\nCROSS JOIN bounds AS b\nGROUP BY b.current_start, b.latest_month"
 },
 {
  "id": "Q5.2",
  "file": "05_growth_drivers.sql",
  "sql": "WITH bounds AS (\n  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start\n  FROM workspace.sentinel.gold_monthly_volume\n),\nsegments AS (\n  SELECT c.company AS segment,\n         SUM(CASE WHEN c.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)\n                  THEN c.complaints ELSE 0 END) AS prior_complaints,\n         SUM(CASE WHEN c.month BETWEEN b.current_start AND b.latest_month\n                  THEN c.complaints ELSE 0 END) AS current_complaints\n  FROM workspace.sentinel.gold_company_monthly AS c\n  CROSS JOIN bounds AS b\n  GROUP BY c.company\n),\nscored AS (\n  SELECT segment, prior_complaints, current_complaints,\n         current_complaints - prior_complaints                              AS net_change_complaints,\n         SUM(current_complaints - prior_complaints) OVER ()                 AS total_net_change\n  FROM segments\n)\nSELECT segment                                                              AS company,\n       prior_complaints,\n       current_complaints,\n       net_change_complaints,\n       CASE WHEN prior_complaints >= 100\n            THEN ROUND(100 * net_change_complaints / prior_complaints, 1) END AS pct_growth,\n       ROUND(100 * net_change_complaints / NULLIF(total_net_change, 0), 2) AS contribution_to_net_change_pct\nFROM scored\nORDER BY ABS(net_change_complaints) DESC, segment\nLIMIT 15"
 },
 {
  "id": "Q5.3",
  "file": "05_growth_drivers.sql",
  "sql": "WITH bounds AS (\n  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start\n  FROM workspace.sentinel.gold_monthly_volume\n),\nsegments AS (\n  SELECT COALESCE(i.issue_group, 'no issue given') AS segment,\n         SUM(CASE WHEN i.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)\n                  THEN i.complaints ELSE 0 END) AS prior_complaints,\n         SUM(CASE WHEN i.month BETWEEN b.current_start AND b.latest_month\n                  THEN i.complaints ELSE 0 END) AS current_complaints\n  FROM workspace.sentinel.gold_issue_monthly AS i\n  CROSS JOIN bounds AS b\n  GROUP BY COALESCE(i.issue_group, 'no issue given')\n),\nscored AS (\n  SELECT segment, prior_complaints, current_complaints,\n         current_complaints - prior_complaints              AS net_change_complaints,\n         SUM(current_complaints - prior_complaints) OVER () AS total_net_change\n  FROM segments\n)\nSELECT segment                                                              AS issue_group,\n       prior_complaints,\n       current_complaints,\n       net_change_complaints,\n       CASE WHEN prior_complaints >= 100\n            THEN ROUND(100 * net_change_complaints / prior_complaints, 1) END AS pct_growth,\n       ROUND(100 * net_change_complaints / NULLIF(total_net_change, 0), 2) AS contribution_to_net_change_pct\nFROM scored\nORDER BY net_change_complaints DESC"
 },
 {
  "id": "Q5.4",
  "file": "05_growth_drivers.sql",
  "sql": "WITH bounds AS (\n  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start\n  FROM workspace.sentinel.gold_monthly_volume\n),\nsegments AS (\n  SELECT COALESCE(s.state_code, 'no state given') AS segment,\n         MAX(COALESCE(s.state_category, 'none'))   AS state_category,\n         SUM(CASE WHEN s.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)\n                  THEN s.complaints ELSE 0 END) AS prior_complaints,\n         SUM(CASE WHEN s.month BETWEEN b.current_start AND b.latest_month\n                  THEN s.complaints ELSE 0 END) AS current_complaints\n  FROM workspace.sentinel.gold_state_monthly AS s\n  CROSS JOIN bounds AS b\n  GROUP BY COALESCE(s.state_code, 'no state given')\n),\nscored AS (\n  SELECT segment, state_category, prior_complaints, current_complaints,\n         current_complaints - prior_complaints              AS net_change_complaints,\n         SUM(current_complaints - prior_complaints) OVER () AS total_net_change\n  FROM segments\n)\nSELECT segment                                                              AS state_code,\n       state_category,\n       prior_complaints,\n       current_complaints,\n       net_change_complaints,\n       CASE WHEN prior_complaints >= 100\n            THEN ROUND(100 * net_change_complaints / prior_complaints, 1) END AS pct_growth,\n       ROUND(100 * net_change_complaints / NULLIF(total_net_change, 0), 2) AS contribution_to_net_change_pct\nFROM scored\nORDER BY ABS(net_change_complaints) DESC, segment\nLIMIT 15"
 },
 {
  "id": "Q6.1",
  "file": "06_executive_scorecard.sql",
  "sql": "WITH response_status AS (\n  SELECT v.month_label,\n         COALESCE(ip.complaints, 0) / v.population_complaints >= 0.01 AS is_provisional\n  FROM workspace.sentinel.gold_monthly_volume AS v\n  LEFT JOIN workspace.sentinel.gold_response_monthly AS ip\n    ON ip.month_label = v.month_label\n   AND ip.dimension = 'company_response'\n   AND ip.category = 'In progress'\n),\nlatest AS (\n  SELECT * FROM workspace.sentinel.gold_monthly_volume\n  WHERE month = (SELECT MAX(month) FROM workspace.sentinel.gold_monthly_volume)\n),\nytd AS (\n  SELECT SUM(CASE WHEN v.month BETWEEN CAST(DATE_TRUNC('YEAR', l.month) AS DATE) AND l.month\n                  THEN v.population_complaints ELSE 0 END) AS current_ytd,\n         SUM(CASE WHEN v.month BETWEEN ADD_MONTHS(CAST(DATE_TRUNC('YEAR', l.month) AS DATE), -12)\n                                   AND ADD_MONTHS(l.month, -12)\n                  THEN v.population_complaints ELSE 0 END) AS prior_ytd\n  FROM workspace.sentinel.gold_monthly_volume AS v\n  CROSS JOIN latest AS l\n),\ncompany_latest AS (\n  SELECT MAX_BY(company, complaints)                                              AS top_company,\n         ROUND(100 * MAX(share_of_month), 2)                                      AS top_company_pct,\n         ROUND(100 * SUM(CASE WHEN rank_in_month <= 3 THEN share_of_month ELSE 0 END), 2) AS top3_company_pct,\n         ROUND(10000 * SUM(share_of_month * share_of_month), 0)                   AS complaint_concentration_hhi\n  FROM workspace.sentinel.gold_company_monthly\n  WHERE NOT is_residual AND month_label = (SELECT month_label FROM latest)\n),\nissue_latest AS (\n  SELECT MAX_BY(issue, complaints) AS top_issue, ROUND(100 * MAX(share_of_month), 2) AS top_issue_pct\n  FROM workspace.sentinel.gold_issue_monthly\n  WHERE NOT is_residual AND month_label = (SELECT month_label FROM latest)\n),\nstate_latest AS (\n  SELECT MAX_BY(state_code, complaints) AS top_state, ROUND(100 * MAX(share_of_month), 2) AS top_state_pct\n  FROM workspace.sentinel.gold_state_monthly\n  WHERE NOT is_residual AND month_label = (SELECT month_label FROM latest)\n),\nreference_month AS (\n  SELECT MAX(month_label) AS month_label FROM response_status WHERE NOT is_provisional\n),\nresponse_kpis AS (\n  SELECT\n    ROUND(100 * SUM(CASE WHEN dimension = 'timely' AND category = 'Yes' THEN share_of_month ELSE 0 END), 2)\n      AS timely_response_pct,\n    ROUND(100 * SUM(CASE WHEN dimension = 'company_response' AND category = 'Closed with monetary relief'\n                         THEN share_of_month ELSE 0 END), 2)     AS monetary_relief_pct,\n    ROUND(100 * SUM(CASE WHEN dimension = 'company_response' AND category = 'Closed with non-monetary relief'\n                         THEN share_of_month ELSE 0 END), 2)     AS non_monetary_relief_pct,\n    ROUND(100 * SUM(CASE WHEN dimension = 'company_response' AND category = 'Closed with explanation'\n                         THEN share_of_month ELSE 0 END), 2)     AS closed_with_explanation_pct\n  FROM workspace.sentinel.gold_response_monthly\n  WHERE month_label = (SELECT month_label FROM reference_month)\n)\nSELECT l.month_label                                               AS latest_month,\n       l.population_complaints                                     AS latest_month_complaints,\n       ROUND(100 * l.mom_pct_change, 1)                            AS mom_pct,\n       ROUND(100 * l.yoy_pct_change, 1)                            AS yoy_pct,\n       ROUND(l.rolling_3m_avg, 0)                                  AS rolling_3m_avg,\n       y.current_ytd                                               AS ytd_complaints,\n       y.prior_ytd                                                 AS prior_ytd_complaints,\n       ROUND(100 * (y.current_ytd / y.prior_ytd - 1), 1)           AS ytd_pct_growth,\n       c.top_company, c.top_company_pct, c.top3_company_pct, c.complaint_concentration_hhi,\n       i.top_issue, i.top_issue_pct,\n       s.top_state, s.top_state_pct,\n       r.month_label                                               AS response_reference_month,\n       k.timely_response_pct, k.monetary_relief_pct, k.non_monetary_relief_pct, k.closed_with_explanation_pct,\n       (SELECT ARRAY_JOIN(ARRAY_SORT(COLLECT_LIST(month_label)), ', ')\n          FROM response_status WHERE is_provisional)\n                                                                   AS provisional_response_months\nFROM latest AS l\nCROSS JOIN ytd AS y\nCROSS JOIN company_latest AS c\nCROSS JOIN issue_latest AS i\nCROSS JOIN state_latest AS s\nCROSS JOIN reference_month AS r\nCROSS JOIN response_kpis AS k"
 },
 {
  "id": "Q6.2",
  "file": "06_executive_scorecard.sql",
  "sql": "WITH response_status AS (\n  SELECT v.month_label,\n         COALESCE(ip.complaints, 0) / v.population_complaints >= 0.01 AS is_provisional\n  FROM workspace.sentinel.gold_monthly_volume AS v\n  LEFT JOIN workspace.sentinel.gold_response_monthly AS ip\n    ON ip.month_label = v.month_label\n   AND ip.dimension = 'company_response'\n   AND ip.category = 'In progress'\n)\nSELECT r.month_label,\n       ROUND(100 * SUM(CASE WHEN r.dimension = 'timely' AND r.category = 'Yes' THEN r.share_of_month ELSE 0 END), 2)\n         AS timely_response_pct,\n       ROUND(100 * SUM(CASE WHEN r.dimension = 'company_response' AND r.category = 'Closed with monetary relief'\n                            THEN r.share_of_month ELSE 0 END), 3) AS monetary_relief_pct,\n       ROUND(100 * SUM(CASE WHEN r.dimension = 'company_response' AND r.category = 'Closed with non-monetary relief'\n                            THEN r.share_of_month ELSE 0 END), 2) AS non_monetary_relief_pct,\n       ROUND(100 * SUM(CASE WHEN r.dimension = 'company_response' AND r.category = 'Closed with explanation'\n                            THEN r.share_of_month ELSE 0 END), 2) AS closed_with_explanation_pct,\n       ROUND(100 * SUM(CASE WHEN r.dimension = 'company_response' AND r.category = 'Untimely response'\n                            THEN r.share_of_month ELSE 0 END), 3) AS untimely_response_pct\nFROM workspace.sentinel.gold_response_monthly AS r\nJOIN response_status AS s ON s.month_label = r.month_label\nWHERE NOT s.is_provisional\nGROUP BY r.month_label\nORDER BY r.month_label"
 }
]''')
print(f"{len(QUERIES)} queries: {[q['id'] for q in QUERIES]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Run (read-only)
# MAGIC `queries`: `all` or comma-separated ids such as `Q2.1,Q6.1`. `preview_rows`: rows kept per query in the report.

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("queries", "all")
dbutils.widgets.text("preview_rows", "10")
requested = dbutils.widgets.get("queries").strip()
SELECTED = None if requested.lower() == "all" else {q.strip() for q in requested.split(",") if q.strip()}
PREVIEW_ROWS = max(0, min(int(dbutils.widgets.get("preview_rows")), 50))


def execute(sql):
    """Run one read-only query; fetch at most MAX_ROWS + 1 rows to detect larger results."""
    df = spark.sql(sql)
    rows = [r.asDict(recursive=True) for r in df.limit(MAX_ROWS + 1).collect()]
    row_count = len(rows) if len(rows) <= MAX_ROWS else df.count()
    return df.columns, rows[:MAX_ROWS], row_count


report = run_validation(QUERIES, execute, selected=SELECTED, preview_rows=PREVIEW_ROWS)
SUMMARY_COLUMNS = ["query", "file", "status", "rows", "checks_passed", "checks_total", "failed_checks", "error"]
display(spark.createDataFrame(
    [tuple(r[c] for c in SUMMARY_COLUMNS) for r in summary_rows(report)],
    "query string, file string, status string, rows bigint, checks_passed int, checks_total int, "
    "failed_checks string, error string"))  # explicit schema: columns can be entirely NULL
counts = {s: sum(e["status"] == s for e in report) for s in (PASS, FAIL, NOT_RUN)}
print(counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · JSON report
# MAGIC Copy the full output of the next cell and send it back for `sql/results/`.

# COMMAND ----------

print(report_json(report, generated_at=datetime.now(timezone.utc).isoformat()))
