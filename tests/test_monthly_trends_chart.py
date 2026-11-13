"""Checks the monthly trends chart against the verified Q1.1 export."""

import csv
import json
import re
import shutil
import struct
import subprocess
from pathlib import Path

import pytest

from lakehouse import monthly_trends_chart as chart

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "sql" / "results" / "validation_report.json"
REF_JSON = ROOT / "data" / "reference" / "cfpb_api_monthly_credit_reporting.json"


@pytest.fixture(scope="module")
def rows():
    return chart.load_rows()


@pytest.fixture(scope="module")
def svg():
    return chart.SVG_PATH.read_text()


# --- the CSV ------------------------------------------------------------------------------

def test_csv_has_22_consecutive_months(rows):
    assert len(rows) == 22
    assert rows[0]["month_label"] == "2025-01" and rows[-1]["month_label"] == "2026-10"


def test_csv_growth_columns_are_internally_consistent(rows):
    for i, r in enumerate(rows):
        if i >= 1:
            assert r["mom_pct"] == round(100 * (r["complaints"] / rows[i - 1]["complaints"] - 1), 1)
        if i >= 12:
            assert r["yoy_pct"] == round(100 * (r["complaints"] / rows[i - 12]["complaints"] - 1), 1)
        else:
            assert r["yoy_pct"] is None


def test_csv_matches_verified_databricks_report(rows):
    q61 = next(e for e in json.loads(REPORT.read_text())["queries"] if e["id"] == "Q6.1")["preview"][0]
    october = rows[-1]
    assert (october["month_label"], october["complaints"], october["mom_pct"], october["yoy_pct"]) == (
        q61["latest_month"], q61["latest_month_complaints"], q61["mom_pct"], q61["yoy_pct"])
    assert [r["month_label"] for r in rows if r["provisional"]] == q61["provisional_response_months"].split(", ")
    assert sum(r["complaints"] for r in rows if r["month_label"].startswith("2026")) == q61["ytd_complaints"]


@pytest.mark.skipif(not REF_JSON.exists(), reason="local reference data not present")
def test_csv_matches_reference_monthly_totals(rows):
    totals = {m["month"]: m["total"] for m in json.loads(REF_JSON.read_text())["months"]}
    assert {r["month_label"]: r["complaints"] for r in rows} == totals


def test_requested_headline_values_come_from_the_csv(rows):
    peak = max(rows, key=lambda r: r["complaints"])
    assert (peak["month_label"], peak["complaints"], peak["mom_pct"], peak["yoy_pct"], peak["volume_rank"]) == (
        "2026-10", 711218, 16.9, 51.6, 1)


@pytest.mark.parametrize("mutate, message", [
    (lambda rs: rs.pop(5), "22 consecutive months"),
    (lambda rs: rs[3].update(complaints=0), "positive"),
    (lambda rs: rs[3].update(complaints=900_000), "y-axis maximum"),
])
def test_validation_rejects_bad_data(rows, mutate, message):
    broken = [dict(r) for r in rows]
    mutate(broken)
    with pytest.raises(chart.TrendDataError, match=message):
        chart.validate(broken)


def test_unexpected_columns_are_rejected(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("month,value\n2025-01-01,1\n")
    with pytest.raises(chart.TrendDataError, match="columns"):
        chart.load_rows(path)


# --- the chart ------------------------------------------------------------------------------

def test_committed_svg_is_regenerated_from_the_csv(rows, svg):
    assert svg == chart.build_svg(rows), "regenerate: PYTHONPATH=src python -m lakehouse.monthly_trends_chart"


def test_every_plotted_point_matches_the_csv(rows, svg):
    points = re.findall(r'<circle cx="([\d.]+)" cy="([\d.]+)" r="[\d.]+" fill="(#\w+)" [^>]*'
                        r'data-month="([\d-]+)" data-value="(\d+)"', svg)
    assert [(m, int(v)) for _, _, _, m, v in points] == [(r["month_label"], r["complaints"]) for r in rows]
    for _, cy, color, month, value in points:
        assert float(cy) == pytest.approx(chart._y(int(value)), abs=0.06)
        assert color == chart.SERIES[int(month[:4])]


def test_peak_annotation_matches_the_csv(rows, svg):
    peak = max(rows, key=lambda r: r["complaints"])
    group = re.search(r'<g data-annotation="peak" data-month="([\d-]+)" data-value="(\d+)" '
                      r'data-mom="([-\d.]+)" data-yoy="([-\d.]+)">(.*?)</g>', svg, re.DOTALL)
    assert group.groups()[:4] == (peak["month_label"], str(peak["complaints"]), str(peak["mom_pct"]), str(peak["yoy_pct"]))
    assert "Peak: 711,218" in group.group(5)
    assert "+16.9% vs Sep 2026" in group.group(5) and "+51.6% vs Oct 2025" in group.group(5)
    assert "711,218 in October 2026" in svg                     # title


def test_provisional_band_covers_exactly_the_flagged_months(rows, svg):
    band = re.search(r'data-provisional="([^"]+)"', svg).group(1).split(",")
    assert band == [r["month_label"] for r in rows if r["provisional"]] == ["2026-09", "2026-10"]
    assert "complaint volumes are counted at intake and remain usable" in svg


def test_source_note_and_accessibility(svg):
    assert "Source: CFPB Consumer Complaint Database" in svg
    assert "Q1.1" in svg and "Data snapshot: November 12, 2026" in svg
    assert 'role="img"' in svg and "<title" in svg and "<desc" in svg
    assert "Oct 2026: 711,218" in re.search(r"<desc[^>]*>(.*?)</desc>", svg).group(1)


def test_text_never_uses_series_colours(svg):
    text_fills = set(re.findall(r'<text [^>]*fill="(#\w+)"', svg))
    assert text_fills <= {chart.TEXT_PRIMARY, chart.TEXT_SECONDARY, chart.MUTED}
    assert not text_fills & set(chart.SERIES.values())


def test_png_is_high_resolution():
    data = chart.PNG_PATH.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n"
    width, height = struct.unpack(">II", data[16:24])
    assert (width, height) == (2400, 1350)


@pytest.mark.skipif(shutil.which("sips") is None, reason="macOS sips not available (e.g. Linux CI)")
def test_png_is_rendered_from_the_committed_svg(tmp_path):
    """sips output is deterministic, so a fresh render of the SVG must equal the committed PNG."""
    out = tmp_path / "check.png"
    subprocess.run(["sips", "-s", "format", "png", str(chart.SVG_PATH), "--out", str(out)],
                   check=True, capture_output=True)
    assert out.read_bytes() == chart.PNG_PATH.read_bytes()
