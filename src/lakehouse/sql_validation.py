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
