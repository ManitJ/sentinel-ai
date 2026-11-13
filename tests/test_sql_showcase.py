"""Offline static checks for the SQL analytics showcase (no database required)."""

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
ANALYTICS = sorted(SQL_DIR.glob("0[1-6]_*.sql"))
ALL_SQL = sorted(SQL_DIR.glob("*.sql"))

# Gold columns (verified in Databricks with 00_verify_schema.sql, section A).
BREAKDOWN = {"month", "month_label", "is_residual", "complaints", "share_of_month", "rank_in_month",
             "prev_month_complaints", "mom_change", "mom_pct_change", "measure_type", "_row_hash"}
ESTIMATE = {"period", "est_share", "std_error", "ci95_low", "ci95_high", "n_domain_sample",
            "n_category_sample", "effective_n", "is_reliable", "measure_type", "period_start",
            "period_end", "_row_hash"}
SCHEMAS = {
    "gold_monthly_volume": {"month", "month_label", "population_complaints", "mom_change", "mom_pct_change",
                            "yoy_change", "yoy_pct_change", "rolling_3m_avg", "sample_size",
                            "sample_fraction", "measure_type", "_row_hash"},
    "gold_company_monthly": BREAKDOWN | {"company"},
    "gold_issue_monthly": BREAKDOWN | {"issue", "issue_group", "is_combined_group"},
    "gold_issue_category_map": {"issue", "issue_group", "is_combined_group", "note", "_row_hash"},
    "gold_state_monthly": BREAKDOWN | {"state", "state_code", "state_category"},
    "gold_response_monthly": BREAKDOWN | {"category", "dimension"},
    "gold_company_response_estimates": ESTIMATE | {"dimension", "company", "category"},
    "gold_sub_issue_estimates": ESTIMATE | {"issue_sub_issue", "issue", "sub_issue"},
    "gold_estimator_calibration": {"month", "month_label", "dimension", "category", "exact_share", "est_share",
                                   "std_error", "ci95_low", "ci95_high", "inside_ci95", "z_score",
                                   "expected_sample_count", "in_coverage_qc", "_row_hash"},
    "gold_insight_facts": {"fact_id", "fact_type", "period", "period_start", "period_end", "entity_type",
                           "entity", "metric", "value", "unit", "measure_type", "ci95_low", "ci95_high",
                           "is_reliable", "sample_n", "source_table", "source_key", "reference_fetched_at",
                           "_row_hash"},
}
# Category values verified in 00_verify_schema.sql section F (56 rows).
VERIFIED_LITERALS = {
    "company_response", "timely", "submitted_via", "company_public_response", "tags",
    "Closed with explanation", "Closed with monetary relief", "Closed with non-monetary relief",
    "In progress", "Untimely response", "Yes", "No",
    "state", "district", "territory", "military",
    "ALL", "2025", "2026-YTD",
}
# Labels created inside the queries themselves (not data values).
QUERY_LABELS = {"current", "prior"}
WRITE_KEYWORDS = r"\b(CREATE|INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|REPLACE|OPTIMIZE|VACUUM|GRANT|REVOKE|COPY)\b"


def code(path):
    """SQL with comments removed."""
    return "\n".join(line.split("--")[0] for line in path.read_text().splitlines())


def statements(path):
    return [s.strip() for s in code(path).split(";") if s.strip()]


def test_all_six_analytics_files_and_readme_exist():
    assert [p.name for p in ANALYTICS] == [
        "01_monthly_trends.sql", "02_company_benchmarking.sql", "03_issue_trends.sql",
        "04_geographic_distribution.sql", "05_growth_drivers.sql", "06_executive_scorecard.sql"]
    assert (SQL_DIR / "README.md").exists()


@pytest.mark.parametrize("path", ALL_SQL, ids=lambda p: p.name)
def test_read_only(path):
    assert not re.search(WRITE_KEYWORDS, code(path), re.IGNORECASE)
    for stmt in statements(path):
        assert stmt.split()[0].upper() in {"SELECT", "WITH", "DESCRIBE"}, stmt[:60]


@pytest.mark.parametrize("path", ALL_SQL, ids=lambda p: p.name)
def test_balanced_parentheses_and_fully_qualified_tables(path):
    sql = code(path)
    assert sql.count("(") == sql.count(")")
    for table in re.findall(r"\b(?:FROM|JOIN)\s+([\w.]+)", sql, re.IGNORECASE):
        if table.startswith("gold_"):
            pytest.fail(f"unqualified Gold table {table}")


@pytest.mark.parametrize("path", ANALYTICS, ids=lambda p: p.name)
def test_only_known_tables_and_columns(path):
    sql = code(path)
    tables = set(re.findall(r"workspace\.sentinel\.(\w+)", sql))
    assert tables and tables <= set(SCHEMAS) - {"gold_estimator_calibration", "gold_insight_facts"}
    aliases = dict(re.findall(r"workspace\.sentinel\.(\w+)\s+AS\s+(\w+)", sql))
    alias_to_table = {alias: table for table, alias in aliases.items()}
    for alias, column in re.findall(r"\b([a-z]\w*)\.(\w+)\b", sql):
        if alias in alias_to_table:
            assert column in SCHEMAS[alias_to_table[alias]], f"{alias}.{column} not in {alias_to_table[alias]}"


@pytest.mark.parametrize("path", ANALYTICS, ids=lambda p: p.name)
def test_category_literals_are_verified_values(path):
    literals = set(re.findall(r"=\s*'([^']+)'|IN\s*\(([^)]*)\)", code(path)))
    found = set()
    for eq, in_list in literals:
        found |= {eq} if eq else set(re.findall(r"'([^']+)'", in_list))
    assert found <= VERIFIED_LITERALS | QUERY_LABELS, found - VERIFIED_LITERALS - QUERY_LABELS


@pytest.mark.parametrize("path", ANALYTICS, ids=lambda p: p.name)
def test_headers_document_questions_definitions_and_caveats(path):
    text = path.read_text()
    for section in ("Business questions", "Metric definitions", "Caveats"):
        assert section in text, section
    assert "EXACT" in text


def test_hhi_is_complaint_concentration_never_market_share():
    for path in ANALYTICS + [SQL_DIR / "README.md"]:
        text = path.read_text()
        if "hhi" not in text.lower():
            continue
        assert "complaint_concentration_hhi" in text or "Complaint concentration" in text
        for match in re.finditer(r"market share", text, re.IGNORECASE):
            context = text[max(0, match.start() - 40):match.start()].lower()
            assert re.search(r"\b(not|never)\b", context), f"{path.name}: '{text[match.start()-40:match.end()]}'"
        assert not re.search(r"\bAS\s+\w*market\w*", code(path) if path.suffix == ".sql" else "", re.IGNORECASE)


def test_estimates_are_reliable_only_with_confidence_intervals():
    for path in ANALYTICS:
        sql = code(path)
        for stmt in statements(path):
            if re.search(r"gold_(company_response|sub_issue)_estimates", stmt):
                assert re.search(r"\bis_reliable\b", stmt.split("WHERE", 1)[-1]), path.name
                assert "ci95_low" in stmt and "ci95_high" in stmt, path.name
                assert "measure_type" in stmt, path.name


def test_company_outcome_estimates_exclude_provisional_periods():
    stmt = next(s for s in statements(SQL_DIR / "02_company_benchmarking.sql")
                if "gold_company_response_estimates" in s)
    assert "e.period = '2025'" in stmt
    assert "'ALL'" not in stmt and "'2026-YTD'" not in stmt


def test_response_metrics_exclude_provisional_months():
    for path in ANALYTICS:
        for stmt in statements(path):
            uses_response_metric = re.search(r"dimension\s*=\s*'(timely|company_response)'", stmt) and \
                re.search(r"'(Yes|Closed with[^']*|Untimely response)'", stmt)
            if uses_response_metric:
                assert ">= 0.01" in stmt, f"{path.name}: response metric without provisional rule"
                assert re.search(r"NOT\s+(s\.)?is_provisional", stmt), f"{path.name}: provisional months not excluded"


def test_growth_queries_separate_net_contribution_from_percentage_growth():
    text = (SQL_DIR / "05_growth_drivers.sql").read_text()
    assert "pct_growth" in text and "contribution_to_net_change_pct" in text
    assert "never mixed" in text
    for stmt in statements(SQL_DIR / "05_growth_drivers.sql")[1:]:
        assert "contribution_to_net_change_pct" in stmt and "pct_growth" in stmt
        assert "NULLIF(total_net_change, 0)" in stmt                    # no division-by-zero error
        assert "prior_complaints >= 100" in stmt                         # unstable bases suppressed


def test_provisional_rule_is_described_as_a_reporting_rule():
    for path in [SQL_DIR / "01_monthly_trends.sql", SQL_DIR / "06_executive_scorecard.sql", SQL_DIR / "README.md"]:
        text = path.read_text().lower()
        assert "reporting rule" in text and ("not proof" in text)
