"""Checks the company concentration chart against the verified Q2.2 export."""

import copy
import json
import re
import shutil
import struct
import subprocess
from pathlib import Path

import pytest

from lakehouse import company_concentration_chart as chart
from lakehouse import sql_validation as sv

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "sql" / "results" / "validation_report.json"
REF_JSON = ROOT / "data" / "reference" / "cfpb_api_monthly_credit_reporting.json"


@pytest.fixture(scope="module")
def rows():
    return chart.load_rows()


@pytest.fixture(scope="module")
def svg():
    return chart.SVG_PATH.read_text()


def q22_entry():
    return next(q for q in json.loads(chart.JSON_PATH.read_text())["queries"] if q["id"] == "Q2.2")


# --- data integrity --------------------------------------------------------------------------

def test_export_holds_all_22_months_of_a_passing_q22(rows):
    entry = q22_entry()
    assert entry["status"] == "PASS" and entry["row_count"] == len(rows) == 22
    assert rows[0]["month_label"] == "2025-01" and rows[-1]["month_label"] == "2026-10"


def test_checks_pass_again_on_the_exported_rows(rows):
    entry = q22_entry()
    rerun = sv.check_result("Q2.2", entry["columns"], rows, entry["row_count"])
    assert rerun and all(c["passed"] for c in rerun)


def test_export_agrees_with_the_verified_report(rows):
    verified = next(q for q in json.loads(REPORT.read_text())["queries"] if q["id"] == "Q2.2")
    assert rows[:len(verified["preview"])] == verified["preview"]


@pytest.mark.skipif(not REF_JSON.exists(), reason="local reference data not present")
def test_every_plotted_metric_recomputes_from_reference_facets(rows):
    months = {m["month"]: m for m in json.loads(REF_JSON.read_text())["months"]}
    for r in rows:
        month = months[r["month_label"]]
        shares = sorted((n / month["total"] for n in month["aggregations"]["company"].values()), reverse=True)
        assert r["top1_complaint_pct"] == pytest.approx(100 * shares[0], abs=0.006)
        assert r["top3_complaint_pct"] == pytest.approx(100 * sum(shares[:3]), abs=0.006)
        assert r["complaint_concentration_hhi"] == round(10000 * sum(s * s for s in shares))


def test_requested_october_values_come_from_the_export(rows):
    october = rows[-1]
    assert (october["top1_complaint_pct"], october["top3_complaint_pct"], october["complaint_concentration_hhi"]) == (
        36.77, 97.68, 3219.0)


@pytest.mark.parametrize("mutate, message", [
    (lambda e: e.update(status="FAIL"), "did not pass"),
    (lambda e: e["preview"].pop(), "rows"),
    (lambda e: e["preview"][4].update(top1_complaint_pct=99.0), "out of order"),
    (lambda e: e["preview"][4].update(complaint_concentration_hhi=4000.0), "axis range"),
    (lambda e: e.update(columns=e["columns"][:-1]), "columns"),
])
def test_invalid_exports_are_rejected(tmp_path, mutate, message):
    report = json.loads(chart.JSON_PATH.read_text())
    entry = next(q for q in report["queries"] if q["id"] == "Q2.2")
    mutate(entry)
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(report))
    with pytest.raises(chart.ConcentrationDataError, match=message):
        chart.load_rows(path)


# --- chart content ---------------------------------------------------------------------------

def test_every_plotted_point_matches_the_export(rows, svg):
    for series, key, y_fn in (("top3", "top3_complaint_pct", chart._y_share),
                              ("top1", "top1_complaint_pct", chart._y_share),
                              ("hhi", "complaint_concentration_hhi", chart._y_hhi)):
        points = re.findall(rf'<circle cx="[\d.]+" cy="([\d.]+)" r="[\d.]+" fill="#\w+" stroke="[^"]+" '
                            rf'stroke-width="2" data-series="{series}" data-month="([\d-]+)" data-value="([\d.]+)"',
                            svg)
        assert [(m, float(v)) for _, m, v in points] == [(r["month_label"], float(r[key])) for r in rows]
        for cy, _, value in points:
            assert float(cy) == pytest.approx(y_fn(float(value)), abs=0.06)


def test_october_highlights_and_changes(rows, svg):
    first, last = rows[0], rows[-1]
    for key, text in (("top3_complaint_pct", "Top 3: 97.68%"), ("top1_complaint_pct", "Top 1: 36.77%"),
                      ("complaint_concentration_hhi", "HHI: 3,219")):
        group = re.search(rf'<g data-annotation="{key}" data-month="2026-10" data-value="{last[key]}">(.*?)</g>',
                          svg, re.DOTALL)
        assert group and text in group.group(1) and "Oct 2026" in group.group(1)
    assert f"{last['top3_complaint_pct'] - first['top3_complaint_pct']:+.2f} pp vs Jan 2025" == "+3.91 pp vs Jan 2025"
    assert "+3.91 pp vs Jan 2025" in svg and "+4.83 pp vs Jan 2025" in svg and "+286 vs Jan 2025" in svg


def test_title_two_panels_and_no_dual_axis(svg):
    assert chart.TITLE in svg
    assert "Share of monthly complaints" in svg and "Complaint concentration index (HHI)" in svg
    share_ticks = re.findall(r">(\d+)%</text>", svg)
    hhi_ticks = re.findall(r">(\d,\d{3})</text>", svg)
    assert share_ticks == ["0", "25", "50", "75", "100"]
    assert hhi_ticks[:6] == ["2,800", "2,900", "3,000", "3,100", "3,200", "3,300"]
    # Each panel's ticks sit inside its own panel: one y-scale per panel.
    for y in re.findall(r'<text x="80.0" y="([\d.]+)"[^>]*>\d+%</text>', svg):
        assert chart.P1_TOP <= float(y) - 4 <= chart.P1_BOTTOM
    for y in re.findall(r'<text x="80.0" y="([\d.]+)"[^>]*>\d,\d{3}</text>', svg):
        assert chart.P2_TOP <= float(y) - 4 <= chart.P2_BOTTOM


def test_excluded_content_is_absent(svg):
    assert "companies_with_complaints" not in svg and "companies with complaints" not in svg.lower()
    for company in ("TRANSUNION", "EQUIFAX", "Experian"):
        assert company.lower() not in svg.lower()


def test_footnote_and_source_attribution(svg):
    assert "NOT company market share or a measure of industry competition" in svg
    assert "not adjusted for company size" in svg
    assert "Source: CFPB Consumer Complaint Database" in svg
    assert "Q2.2" in svg and "Data snapshot: November 12, 2026" in svg
    assert 'role="img"' in svg and "Oct 2026: top-1 36.77%, top-3 97.68%, HHI 3,219" in svg


def test_text_never_uses_series_colours(svg):
    text_fills = set(re.findall(r'<text [^>]*fill="(#\w+)"', svg))
    assert not text_fills & {chart.TOP3, chart.TOP1, chart.HHI_COLOR}


# --- reproducibility ------------------------------------------------------------------------

def test_committed_svg_is_regenerated_from_the_export(rows, svg):
    assert svg == chart.build_svg(rows), "regenerate: PYTHONPATH=src python -m lakehouse.company_concentration_chart"


def test_png_is_high_resolution():
    data = chart.PNG_PATH.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n"
    assert struct.unpack(">II", data[16:24]) == (2400, 1520)


@pytest.mark.skipif(shutil.which("sips") is None, reason="macOS sips not available (e.g. Linux CI)")
def test_png_is_rendered_from_the_committed_svg(tmp_path):
    out = tmp_path / "check.png"
    subprocess.run(["sips", "-s", "format", "png", str(chart.SVG_PATH), "--out", str(out)],
                   check=True, capture_output=True)
    assert out.read_bytes() == chart.PNG_PATH.read_bytes()
