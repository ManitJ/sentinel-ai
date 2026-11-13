"""Checks the growth drivers waterfall against the verified Q5.3 export."""

import collections
import json
import re
import shutil
import struct
import subprocess
from pathlib import Path

import pytest

from lakehouse import growth_drivers_chart as chart
from lakehouse import sql_validation as sv

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "sql" / "results" / "validation_report.json"
REF_JSON = ROOT / "data" / "reference" / "cfpb_api_monthly_credit_reporting.json"


@pytest.fixture(scope="module")
def rows():
    return chart.load_rows()


@pytest.fixture(scope="module")
def steps(rows):
    return chart.build_steps(rows)


@pytest.fixture(scope="module")
def svg():
    return chart.SVG_PATH.read_text()


def entry(qid):
    return next(q for q in json.loads(chart.JSON_PATH.read_text())["queries"] if q["id"] == qid)


def verified(qid):
    return next(q for q in json.loads(REPORT.read_text())["queries"] if q["id"] == qid)


# --- data integrity --------------------------------------------------------------------------

def test_export_is_a_q52_q53_only_run_with_complete_q53(rows):
    report = json.loads(chart.JSON_PATH.read_text())
    statuses = collections.Counter(q["status"] for q in report["queries"])
    assert statuses == {"PASS": 2, "NOT RUN": 15}
    q53 = entry("Q5.3")
    assert q53["status"] == "PASS" and q53["row_count"] == len(rows) == 12


def test_checks_pass_again_on_the_exported_rows(rows):
    e = entry("Q5.3")
    rerun = sv.check_result("Q5.3", e["columns"], rows, e["row_count"])
    assert rerun and all(c["passed"] for c in rerun)


def test_export_agrees_with_the_verified_report(rows):
    assert rows[:len(verified("Q5.3")["preview"])] == verified("Q5.3")["preview"]


def test_totals_match_verified_q51(rows):
    q51 = verified("Q5.1")["preview"][0]
    start, end = chart.totals(rows)
    assert (start, end, end - start) == (q51["prior_complaints"], q51["current_complaints"],
                                         q51["net_change_complaints"])
    assert chart.pct_increase(rows) == q51["pct_growth"] == 45.6


@pytest.mark.skipif(not REF_JSON.exists(), reason="local reference data not present")
def test_issue_groups_recompute_from_reference_facets(rows):
    from lakehouse.gold_metrics import ISSUE_GROUPS
    groups = collections.defaultdict(lambda: [0, 0])
    for m in json.loads(REF_JSON.read_text())["months"]:
        year, month = m["month"][:4], int(m["month"][5:])
        if month <= 10:
            for issue, n in m["aggregations"]["issue"].items():
                groups[ISSUE_GROUPS.get(issue, issue)][year == "2026"] += n
    assert {r["issue_group"]: [r["prior_complaints"], r["current_complaints"]] for r in rows} == dict(groups)


# --- waterfall arithmetic --------------------------------------------------------------------

def test_waterfall_reconciles_exactly(rows, steps):
    start, end = chart.totals(rows)
    changes = steps[1:-1]
    assert steps[0]["value"] == start and steps[-1]["value"] == end
    assert sum(s["value"] for s in changes) == end - start == 1_783_087
    for prev, cur in zip(steps[:-2], changes):
        assert cur["base"] == prev["level"] and cur["level"] == cur["base"] + cur["value"]
    assert changes[-1]["level"] == end


def test_top_three_and_other_bar_come_from_q53(rows, steps):
    ranked = sorted(rows, key=lambda r: -r["net_change_complaints"])
    for step, r in zip(steps[1:4], ranked[:3]):
        assert (step["label"], step["value"], step["contribution"]) == (
            r["issue_group"], r["net_change_complaints"], r["contribution_to_net_change_pct"])
    other = steps[4]
    assert other["label"] == "Other 9 issue groups" and other["members"] == [r["issue_group"] for r in ranked[3:]]
    assert other["value"] == sum(r["net_change_complaints"] for r in ranked[3:]) == 8_604
    assert round(sum(s["contribution"] for s in steps[1:-1]), 2) == 100.0


def test_pct_growth_is_never_used(steps, svg):
    source = Path(chart.__file__).read_text()
    assert not re.search(r'\[["\']pct_growth["\']\]', source)
    for row_growth in ("50.1%", "57.6%", "26.3%"):
        assert row_growth not in svg


@pytest.mark.parametrize("mutate, message", [
    (lambda e: e.update(status="FAIL"), "did not pass"),
    (lambda e: e["preview"].pop(), "rows"),
    (lambda e: e.update(columns=e["columns"][:-1]), "columns"),
    (lambda e: e["preview"][0].update(net_change_complaints=1), "current - prior"),
    (lambda e: e["preview"][1].update(contribution_to_net_change_pct=50.0), "contribution"),
    (lambda e: e["preview"].append(dict(e["preview"][0])) or e.update(row_count=13), "duplicate"),
])
def test_invalid_exports_are_rejected(tmp_path, mutate, message):
    report = json.loads(chart.JSON_PATH.read_text())
    mutate(next(q for q in report["queries"] if q["id"] == "Q5.3"))
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(report))
    with pytest.raises(chart.GrowthDataError, match=message):
        chart.load_rows(path)


# --- chart content ---------------------------------------------------------------------------

def bars(svg):
    return re.findall(r'<rect x="([\d.]+)" y="([\d.]+)" width="\d+" height="([\d.]+)" fill="(#\w+)" '
                      r'data-kind="(\w+)" data-label="([^"]+)" data-value="(-?\d+)" data-base="(\d+)" '
                      r'data-level="(\d+)"/>', svg)


def test_every_bar_is_drawn_to_exact_scale(steps, svg):
    found = bars(svg)
    assert len(found) == len(steps) == 6
    for (_, y, h, fill, kind, label, value, base, level), s in zip(found, steps):
        assert (kind, int(value), int(base), int(level), label) == (s["kind"], s["value"], s["base"], s["level"],
                                                                    s["label"])
        assert fill == {"total": chart.TOTAL, "increase": chart.INCREASE, "decrease": chart.DECREASE}[kind]
        lo, hi = (0, s["value"]) if kind == "total" else sorted((s["base"], s["level"]))
        assert float(y) == pytest.approx(chart._y(hi), abs=0.006)
        assert float(h) == pytest.approx(chart._y(lo) - chart._y(hi), abs=0.011)


def test_only_the_sub_pixel_bar_gets_a_callout(steps, svg):
    other = steps[4]
    assert chart._y(other["base"]) - chart._y(other["level"]) < 1          # true height is under 1 unit
    callouts = re.findall(r'<line x1="([\d.]+)" y1="[\d.]+" x2="[\d.]+" y2="([\d.]+)" [^>]*data-callout="(-?\d+)"', svg)
    assert [(int(v)) for _, _, v in callouts] == [other["value"]]
    x, y2, _ = callouts[0]
    assert float(x) == pytest.approx(chart._slot(4, 6), abs=0.06)
    assert float(y2) == pytest.approx(chart._y(other["level"]), abs=0.006)  # leader ends on the bar


def test_connectors_link_running_levels(steps, svg):
    assert [int(v) for v in re.findall(r'data-connector="(\d+)"', svg)] == [s["level"] for s in steps[:-1]]


def test_labels_show_exact_changes_and_contributions(steps, svg):
    for s in steps[1:-1]:
        group = re.search(rf'<g data-annotation="{s["kind"]}" data-label="[^"]*" data-value="{s["value"]}" '
                          rf'data-contribution="{s["contribution"]}"( data-largest="true")?>(.*?)</g>', svg, re.DOTALL)
        assert group and f"{s['value']:+,}" in group.group(2) and f"{s['contribution']:.2f}% of net increase" in group.group(2)
    assert ">3,906,784<" in svg and ">5,689,871<" in svg and "+45.6% vs Jan–Oct 2025" in svg


def test_largest_driver_is_highlighted(svg):
    group = re.search(r'<g [^>]*data-largest="true">(.*?)</g>', svg, re.DOTALL)
    assert re.findall(r'data-largest="true"', svg) == ['data-largest="true"']
    assert "Largest driver" in group.group(1) and "+1,135,926" in group.group(1) and "63.71%" in group.group(1)
    assert 'data-label="Incorrect information on your report"' in group.group(0)


def test_title_subtitle_and_notes(rows, svg):
    assert chart.title(rows) == "What drove the 45.6% increase in credit-reporting complaints?"
    assert f">{chart.title(rows)}<" in svg
    assert "net change +1,783,087 complaints (+45.6%)" in svg
    assert "not full calendar years" in svg
    assert "2 increased (+9,438) and 7 decreased (−834), net +8,604" in svg
    assert "drawn to scale" in svg and "minimum visible height" not in svg
    assert "response metrics are provisional" in svg and "complaint volumes are counted at intake and remain usable" in svg
    assert "do not establish wrongdoing" in svg and "not adjusted for" in svg
    assert "Source: CFPB Consumer Complaint Database" in svg and "Q5.3" in svg
    assert "Data snapshot: November 12, 2026" in svg
    assert 'role="img"' in svg and "<desc" in svg


def test_single_zero_based_axis(svg):
    ticks = re.findall(r'text-anchor="end" font-weight="normal">(0|\dM)</text>', svg)
    assert ticks == ["0", "1M", "2M", "3M", "4M", "5M", "6M"]


def test_text_never_uses_bar_colours(svg):
    text_fills = set(re.findall(r'<text [^>]*fill="(#\w+)"', svg))
    assert not text_fills & {chart.TOTAL, chart.INCREASE, chart.DECREASE}


# --- reproducibility ------------------------------------------------------------------------

def test_committed_svg_is_regenerated_from_the_export(rows, svg):
    assert svg == chart.build_svg(rows), "regenerate: PYTHONPATH=src python -m lakehouse.growth_drivers_chart"


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
