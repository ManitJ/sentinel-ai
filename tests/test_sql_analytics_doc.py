"""Checks that docs/sql_analytics.md is grounded in the verified Databricks results."""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "sql_analytics.md"
REPORT = ROOT / "sql" / "results" / "validation_report.json"
METHOD_CONSTANTS = {1.0, 95.0, 100.0}          # 1% reporting rule, 95% CIs, base of 100 complaints


def report():
    return json.loads(REPORT.read_text())


def report_numbers():
    values = set()

    def add(value):
        if isinstance(value, bool):
            return
        if isinstance(value, (int, float)):
            values.add(round(abs(float(value)), 6))
        elif isinstance(value, str):
            for token in re.findall(r"\d+(?:\.\d+)?", value):
                values.add(round(float(token), 6))

    r = report()
    for entry in r["queries"]:
        add(entry["row_count"])
        for check in entry["checks"]:
            add(check["actual"])
        for row in entry["preview"]:
            for value in row.values():
                add(value)
    add(len(r["queries"]))
    add(sum(len(e["checks"]) for e in r["queries"]))
    return values


def derived_numbers():
    """Values the document derives, recomputed here from the report."""
    by_id = {e["id"]: e for e in report()["queries"]}
    q52 = by_id["Q5.2"]["preview"]
    top3_contribution = round(sum(r["contribution_to_net_change_pct"] for r in q52[:3]), 2)
    jan = by_id["Q6.2"]["preview"][0]
    aug = by_id["Q6.1"]["preview"][0]
    relief_gap = round(jan["non_monetary_relief_pct"] - aug["non_monetary_relief_pct"], 2)
    assert jan["month_label"] == "2025-01" and aug["response_reference_month"] == "2026-08"
    return {top3_contribution, relief_gap}


def doc_lines():
    """(line, cleaned text) with code spans, link targets, query ids, dates and years removed."""
    for line in DOC.read_text().splitlines():
        text = re.sub(r"`[^`]*`", " ", line)
        text = re.sub(r"\]\([^)]*\)", "] ", text)
        text = re.sub(r"\bQ\d\.\d\b", " ", text)
        text = re.sub(r"\b20\d{2}-\d{2}\b", " ", text)
        text = re.sub(r"\b20\d{2}\b", " ", text)
        text = re.sub(r"^#+\s*\d+\.", " ", text)
        yield line, text


def numbers_in(text):
    return [round(float(n.replace(",", "")), 6) for n in re.findall(r"\d[\d,]*(?:\.\d+)?", text)]


def test_every_number_is_from_the_verified_report_or_documented():
    allowed = report_numbers() | derived_numbers() | METHOD_CONSTANTS
    unknown = [(n, line.strip()[:90]) for line, text in doc_lines() for n in numbers_in(text) if n not in allowed]
    assert not unknown, unknown


def test_derived_values_match_their_stated_arithmetic():
    text = DOC.read_text()
    assert "99.49%" in text and "40.67% + 36.81% + 22.01%" in text
    assert "12.08 percentage points" in text and "48.23% − 36.15%" in text
    assert {99.49, 12.08} == derived_numbers()


def blocks():
    """Paragraphs, list items and table rows as units: (raw text, cleaned text)."""
    current = []
    for line, text in doc_lines():
        starts_new = not line.strip() or line.lstrip().startswith(("- ", "|", "#"))
        if starts_new and current:
            yield " ".join(r for r, _ in current), " ".join(c for _, c in current)
            current = []
        if line.strip():
            current.append((line, text))
            if line.lstrip().startswith(("|", "#")):
                yield line, text
                current = []
    if current:
        yield " ".join(r for r, _ in current), " ".join(c for _, c in current)


SENTENCE_END = re.compile(r"(?<=\.)\s+(?=[A-Z*])")   # full stops only; a colon introduces cited lists


def test_numerical_findings_cite_query_ids():
    """Every sentence (or table row) containing a finding cites a query id."""
    finding_numbers = report_numbers() | derived_numbers()
    uncited = []
    for raw, _ in blocks():
        for sentence in ([raw] if raw.lstrip().startswith("|") else SENTENCE_END.split(raw)):
            cleaned = sentence
            for pattern in (r"`[^`]*`", r"\]\([^)]*\)", r"\bQ\d\.\d\b", r"\b20\d{2}-\d{2}\b", r"\b20\d{2}\b",
                            r"^#+\s*\d+\."):
                cleaned = re.sub(pattern, " ", cleaned)
            if any(n in finding_numbers and n not in METHOD_CONSTANTS for n in numbers_in(cleaned)):
                if not re.search(r"\bQ\d\.\d\b", sentence) and "validation" not in sentence.lower():
                    uncited.append(sentence.strip()[:110])
    assert not uncited, uncited


def test_cited_query_ids_exist_and_passed():
    statuses = {e["id"]: e["status"] for e in report()["queries"]}
    cited = set(re.findall(r"\bQ([2-6]\.\d)\b", DOC.read_text()))
    assert cited and all(statuses.get(f"Q{c}") == "PASS" for c in cited), cited


def test_key_findings_present():
    text = DOC.read_text()
    for fact in ["45.6%", "1,783,087", "3,906,784", "5,689,871", "TransUnion", "Equifax", "Experian",
                 "Incorrect information on your report", "Texas", "September and October 2026"]:
        assert fact in text, fact


def test_relative_links_resolve():
    for target in re.findall(r"\]\(([^)#]+)\)", DOC.read_text()):
        assert (DOC.parent / target).resolve().exists(), target


def test_wording_safeguards():
    text = DOC.read_text()
    lowered = text.lower()
    for match in re.finditer(r"market share", lowered):
        assert re.search(r"\bnot\b", lowered[max(0, match.start() - 40):match.start()]), "market share"
    assert not re.search(r"\b(fraud\w*|illegal|unlawful|violat\w*|misconduct|wrongdoing)\b", lowered)
    assert "unverified" in lowered and "not adjusted" in lowered
    assert "reporting rule, not proof" in lowered
    assert "estimated" in lowered and "95% ci" in lowered


def test_previews_are_not_presented_as_complete_results():
    text = DOC.read_text()
    assert "first three rows" in text
    charts = text.split("## Charts", 1)[1].split("## Sources", 1)[0]
    for query_id in ("Q1.1", "Q2.2", "Q5.3"):
        assert f"Full {query_id} result" in charts


def test_charts_are_embedded_in_their_sections():
    text = DOC.read_text()
    sections = {h: text.split(h, 1)[1].split("\n### ", 1)[0] for h in (
        "### 1. Volume", "### 2. Companies", "### 5. Growth drivers")}
    for heading, image in (("### 1. Volume", "assets/monthly_complaint_trends.png"),
                           ("### 2. Companies", "assets/company_concentration.png"),
                           ("### 5. Growth drivers", "assets/growth_drivers_waterfall.png")):
        assert f"]({image})" in sections[heading]
        assert (DOC.parent / image).exists()
