"""Company complaint concentration chart (SVG + PNG) from the verified Q2.2 export.

    PYTHONPATH=src python -m lakehouse.company_concentration_chart

Input : sql/results/exports/company_concentration.json  (Q2.2 run with preview_rows = 22)
Output: docs/assets/company_concentration.svg and .png (2400x1520, rendered by macOS `sips`)

Two stacked panels sharing one month axis (never a dual-axis chart):
  top    - top-1 and combined top-3 company share of monthly complaints (0-100%)
  bottom - complaint concentration index (HHI), on its own axis
Every plotted value is read from the JSON. Individual company shares and the
"companies with complaints" column are deliberately not plotted.
"""

import json
import subprocess
from pathlib import Path
from xml.sax.saxutils import escape

from lakehouse.monthly_trends_chart import (
    FONT, GRID, MUTED, SURFACE, TEXT_PRIMARY, TEXT_SECONDARY, _month_name, _text,
)

ROOT = Path(__file__).resolve().parents[2]
JSON_PATH = ROOT / "sql" / "results" / "exports" / "company_concentration.json"
SVG_PATH = ROOT / "docs" / "assets" / "company_concentration.svg"
PNG_PATH = ROOT / "docs" / "assets" / "company_concentration.png"
SNAPSHOT = "November 12, 2026"
TITLE = "Credit-reporting complaints are increasingly concentrated among the largest companies"
COLUMNS = ["month_label", "companies_with_complaints", "top1_complaint_pct", "top3_complaint_pct",
           "complaint_concentration_hhi"]

# Reference palette: categorical slots 1-2 for the two share lines; a darker step of the
# blue ramp for the single HHI series (one series, so no legend is needed).
TOP3, TOP1, HHI_COLOR = "#2a78d6", "#eb6834", "#184f95"

W, H = 1200, 760
LEFT, RIGHT = 92, 1030
P1_TOP, P1_BOTTOM = 160, 380          # share panel
P2_TOP, P2_BOTTOM = 470, 600          # HHI panel
HHI_MIN, HHI_MAX, HHI_STEP = 2800, 3300, 100


class ConcentrationDataError(ValueError):
    """The export does not hold a complete, passing Q2.2 result."""


def load_rows(path=JSON_PATH):
    report = json.loads(Path(path).read_text())
    entry = next((q for q in report.get("queries", []) if q.get("id") == "Q2.2"), None)
    if entry is None:
        raise ConcentrationDataError("Q2.2 not found in the export")
    if entry["status"] != "PASS" or not all(c["passed"] for c in entry["checks"]):
        raise ConcentrationDataError("Q2.2 did not pass validation")
    if entry["columns"] != COLUMNS:
        raise ConcentrationDataError(f"unexpected columns: {entry['columns']}")
    rows = entry["preview"]
    if entry["row_count"] != len(rows):
        raise ConcentrationDataError(f"preview has {len(rows)} of {entry['row_count']} rows")
    validate(rows)
    return rows


def validate(rows):
    expected = [f"{y}-{m:02d}" for y in (2025, 2026) for m in range(1, 13)][:22]
    if [r["month_label"] for r in rows] != expected:
        raise ConcentrationDataError("expected the 22 consecutive months 2025-01 .. 2026-10")
    for r in rows:
        values = [r["top1_complaint_pct"], r["top3_complaint_pct"], r["complaint_concentration_hhi"]]
        if any(v is None for v in values):
            raise ConcentrationDataError(f"{r['month_label']}: missing value")
        if not 0 < r["top1_complaint_pct"] <= r["top3_complaint_pct"] <= 100:
            raise ConcentrationDataError(f"{r['month_label']}: shares out of order or range")
        if not HHI_MIN <= r["complaint_concentration_hhi"] <= HHI_MAX:
            raise ConcentrationDataError(f"{r['month_label']}: HHI outside the panel's axis range")


def _x(i, n):
    return LEFT + (RIGHT - LEFT) * i / (n - 1)


def _y_share(pct):
    return P1_BOTTOM - (P1_BOTTOM - P1_TOP) * pct / 100


def _y_hhi(value):
    return P2_BOTTOM - (P2_BOTTOM - P2_TOP) * (value - HHI_MIN) / (HHI_MAX - HHI_MIN)


def _fmt_hhi(value):
    return f"{round(value):,}"


def build_svg(rows, scale=2):
    n = len(rows)
    first, last = rows[0], rows[-1]
    last_name = _month_name(last["month_label"], "%b %Y")
    first_name = _month_name(first["month_label"], "%b %Y")
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W * scale}" height="{H * scale}" '
        f'viewBox="0 0 {W} {H}" role="img" aria-labelledby="title desc" font-family="{FONT}">',
        f'<title id="title">{escape(TITLE)}</title>',
        '<desc id="desc">Two panels, January 2025 to October 2026. Top: top-1 and top-3 company shares of '
        'monthly CFPB credit-reporting complaints. Bottom: complaint concentration index (HHI). ' + escape("; ".join(
            f"{_month_name(r['month_label'], '%b %Y')}: top-1 {r['top1_complaint_pct']}%, "
            f"top-3 {r['top3_complaint_pct']}%, HHI {_fmt_hhi(r['complaint_concentration_hhi'])}" for r in rows)) +
        ".</desc>",
        f'<rect width="{W}" height="{H}" fill="{SURFACE}"/>',
        _text(LEFT - 52, 52, TITLE, 24, TEXT_PRIMARY, weight="600"),
        _text(LEFT - 52, 80, "Share of monthly CFPB credit-reporting complaints received by the largest companies, "
              "January 2025 – October 2026 (exact counts)", 15),
    ]

    # ---- Panel 1: shares (0-100%) -------------------------------------------------------
    out.append(_text(LEFT - 52, P1_TOP - 26, "Share of monthly complaints", 14, TEXT_PRIMARY, weight="600"))
    for pct in range(0, 101, 25):
        y = _y_share(pct)
        out.append(f'<line x1="{LEFT}" y1="{y:.1f}" x2="{RIGHT}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        out.append(_text(LEFT - 12, y + 4, f"{pct}%", 12, MUTED, "end"))

    def series(key, color, y_fn, attr):
        pts = " ".join(f"{_x(i, n):.1f},{y_fn(r[key]):.1f}" for i, r in enumerate(rows))
        parts = [f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2.5" '
                 f'stroke-linejoin="round" stroke-linecap="round"/>']
        for i, r in enumerate(rows):
            radius = 6 if i == n - 1 else 3.5
            parts.append(f'<circle cx="{_x(i, n):.1f}" cy="{y_fn(r[key]):.1f}" r="{radius}" fill="{color}" '
                         f'stroke="{SURFACE}" stroke-width="2" data-series="{attr}" '
                         f'data-month="{r["month_label"]}" data-value="{r[key]}"/>')
        return parts

    out += series("top3_complaint_pct", TOP3, _y_share, "top3")
    out += series("top1_complaint_pct", TOP1, _y_share, "top1")

    # Start and end labels (selective: first and last month only).
    for key, label, below in (("top3_complaint_pct", "Top 3", True), ("top1_complaint_pct", "Top 1", False)):
        y0, y1 = _y_share(first[key]), _y_share(last[key])
        dy = 18 if below else -10
        out.append(_text(_x(0, n), y0 + dy, f"{first[key]:.2f}%", 12, TEXT_SECONDARY, "start"))
        out.append(f'<g data-annotation="{key}" data-month="{last["month_label"]}" data-value="{last[key]}">')
        out.append(_text(RIGHT + 14, y1 - 3, f"{label}: {last[key]:.2f}%", 13, TEXT_PRIMARY, weight="600"))
        out.append(_text(RIGHT + 14, y1 + 13, last_name, 11, MUTED))
        out.append(_text(RIGHT + 14, y1 + 28, f"{last[key] - first[key]:+.2f} pp vs {first_name}", 11,
                         TEXT_SECONDARY))
        out.append("</g>")

    # Legend (two series): line keys with text in text tokens.
    lx, ly = LEFT + 14, P1_TOP + 14 + (_y_share(75) - P1_TOP)
    for k, (label, color) in enumerate((("Top 3 companies combined", TOP3), ("Largest company", TOP1))):
        x = lx + k * 220
        out.append(f'<line x1="{x}" y1="{ly:.1f}" x2="{x + 22}" y2="{ly:.1f}" stroke="{color}" stroke-width="2.5" '
                   f'stroke-linecap="round"/>')
        out.append(f'<circle cx="{x + 11}" cy="{ly:.1f}" r="4" fill="{color}" stroke="{SURFACE}" stroke-width="2"/>')
        out.append(_text(x + 30, ly + 4, label, 13, TEXT_SECONDARY))

    # ---- Panel 2: HHI (own axis) --------------------------------------------------------
    out.append(_text(LEFT - 52, P2_TOP - 26, "Complaint concentration index (HHI)", 14, TEXT_PRIMARY, weight="600"))
    out.append(_text(LEFT + 238, P2_TOP - 26, f"0–10,000 scale; axis starts at {HHI_MIN:,}", 12, MUTED))
    for value in range(HHI_MIN, HHI_MAX + 1, HHI_STEP):
        y = _y_hhi(value)
        out.append(f'<line x1="{LEFT}" y1="{y:.1f}" x2="{RIGHT}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        out.append(_text(LEFT - 12, y + 4, f"{value:,}", 12, MUTED, "end"))
    out += series("complaint_concentration_hhi", HHI_COLOR, _y_hhi, "hhi")
    hhi_first, hhi_last = first["complaint_concentration_hhi"], last["complaint_concentration_hhi"]
    out.append(_text(_x(0, n), _y_hhi(hhi_first) - 10, _fmt_hhi(hhi_first), 12, TEXT_SECONDARY, "start"))
    out.append(f'<g data-annotation="complaint_concentration_hhi" data-month="{last["month_label"]}" '
               f'data-value="{hhi_last}">')
    out.append(_text(RIGHT + 14, _y_hhi(hhi_last) - 3, f"HHI: {_fmt_hhi(hhi_last)}", 13, TEXT_PRIMARY, weight="600"))
    out.append(_text(RIGHT + 14, _y_hhi(hhi_last) + 13, last_name, 11, MUTED))
    out.append(_text(RIGHT + 14, _y_hhi(hhi_last) + 28, f"{round(hhi_last - hhi_first):+,} vs {first_name}", 11,
                     TEXT_SECONDARY))
    out.append("</g>")

    # ---- Shared month axis ---------------------------------------------------------------
    first_2026 = next(i for i, r in enumerate(rows) if r["month_label"].startswith("2026"))
    for i, r in enumerate(rows):
        out.append(_text(_x(i, n), P2_BOTTOM + 22, _month_name(r["month_label"]), 11, MUTED, "middle"))
    for year, start, end in ((2025, 0, first_2026 - 1), (2026, first_2026, n - 1)):
        out.append(f'<line x1="{_x(start, n) - 8:.1f}" y1="{P2_BOTTOM + 34}" x2="{_x(end, n) + 8:.1f}" '
                   f'y2="{P2_BOTTOM + 34}" stroke="{MUTED}" stroke-width="1.5"/>')
        out.append(_text((_x(start, n) + _x(end, n)) / 2, P2_BOTTOM + 52, str(year), 13, TEXT_SECONDARY,
                         "middle", "600"))

    # ---- Footnotes and source ----------------------------------------------------------------
    notes = [
        "These figures measure how concentrated CFPB complaints are among companies. They are NOT company market "
        "share or a measure of industry competition,",
        "and complaint counts are not adjusted for company size. Largest company = the company with the most "
        "complaints in each month (it can differ by month).",
        "HHI = 10,000 × the sum of every company's squared share of the month's complaints. Recent months may be "
        "revised by later CFPB updates.",
    ]
    for k, note in enumerate(notes):
        out.append(_text(LEFT - 52, H - 82 + 16 * k, note, 11, TEXT_SECONDARY))
    out.append(_text(LEFT - 52, H - 24, "Source: CFPB Consumer Complaint Database (credit reporting), exact monthly "
                     "counts via Sentinel AI query Q2.2 (sql/02_company_benchmarking.sql). "
                     f"Data snapshot: {SNAPSHOT}.", 11, MUTED))
    out.append("</svg>")
    return "\n".join(out) + "\n"


def render(json_path=JSON_PATH, svg_path=SVG_PATH, png_path=PNG_PATH):
    rows = load_rows(json_path)
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    svg_path.write_text(build_svg(rows))
    subprocess.run(["sips", "-s", "format", "png", str(svg_path), "--out", str(png_path)],
                   check=True, capture_output=True)
    return rows


if __name__ == "__main__":
    data = render()
    print(f"{len(data)} months -> {SVG_PATH.relative_to(ROOT)}, {PNG_PATH.relative_to(ROOT)}")
