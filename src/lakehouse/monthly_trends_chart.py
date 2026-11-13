"""Monthly complaint trends chart (SVG + PNG) from the verified Q1.1 export.

    PYTHONPATH=src python -m lakehouse.monthly_trends_chart

Input : sql/results/exports/monthly_trends.csv  (Q1.1 output downloaded from Databricks)
Output: docs/assets/monthly_complaint_trends.svg and .png (2400x1350, rendered by macOS `sips`)

Every plotted and annotated value is read from the CSV; nothing is typed in by hand. Each point
carries data-month / data-value attributes so tests can verify the chart against the CSV.
Pure Python (no plotting library); colours follow the reference data-viz palette
(categorical slots 1-2: blue #2a78d6, orange #eb6834).
"""

import csv
import subprocess
from datetime import date
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = ROOT / "sql" / "results" / "exports" / "monthly_trends.csv"
SVG_PATH = ROOT / "docs" / "assets" / "monthly_complaint_trends.svg"
PNG_PATH = ROOT / "docs" / "assets" / "monthly_complaint_trends.png"
SNAPSHOT = "November 12, 2026"
EXPECTED_COLUMNS = ["month", "month_label", "population_complaints", "mom_change", "mom_pct", "yoy_change",
                    "yoy_pct", "rolling_3m_avg", "volume_rank", "response_data_provisional"]

# Reference palette (dataviz references/palette.md), light surface.
SURFACE, TEXT_PRIMARY, TEXT_SECONDARY, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
SERIES = {2025: "#2a78d6", 2026: "#eb6834"}
BAND = "#efeee9"
FONT = "Helvetica Neue, Helvetica, Arial, sans-serif"

W, H = 1200, 675                       # drawing coordinates (viewBox)
LEFT, RIGHT, TOP, BOTTOM = 92, 1140, 150, 540
Y_MAX, Y_TICK = 800_000, 200_000


class TrendDataError(ValueError):
    """The CSV does not have the expected 22 monthly records."""


def _number(text, cast=float):
    return None if text in ("", "null", None) else cast(text)


def load_rows(path=CSV_PATH):
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise TrendDataError(f"unexpected columns: {reader.fieldnames}")
        rows = []
        for raw in reader:
            rows.append({
                "month_label": raw["month_label"],
                "complaints": _number(raw["population_complaints"], int),
                "mom_pct": _number(raw["mom_pct"]),
                "yoy_pct": _number(raw["yoy_pct"]),
                "volume_rank": _number(raw["volume_rank"], int),
                "provisional": raw["response_data_provisional"].strip().lower() == "true",
            })
    validate(rows)
    return rows


def validate(rows):
    expected = [f"{y}-{m:02d}" for y in (2025, 2026) for m in range(1, 13)][:22]
    if [r["month_label"] for r in rows] != expected:
        raise TrendDataError("expected the 22 consecutive months 2025-01 .. 2026-10")
    if any(r["complaints"] is None or r["complaints"] <= 0 for r in rows):
        raise TrendDataError("every month needs a positive complaint count")
    if max(r["complaints"] for r in rows) > Y_MAX:
        raise TrendDataError("a value exceeds the chart's y-axis maximum")
    peak = max(rows, key=lambda r: r["complaints"])
    if peak["volume_rank"] != 1:
        raise TrendDataError("the highest month is not volume_rank 1 in the CSV")


def _x(i, n):
    return LEFT + (RIGHT - LEFT) * i / (n - 1)


def _y(value):
    return BOTTOM - (BOTTOM - TOP) * value / Y_MAX


def _month_name(label, fmt="%b"):
    return date(int(label[:4]), int(label[5:7]), 1).strftime(fmt)


def _text(x, y, content, size=13, fill=TEXT_SECONDARY, anchor="start", weight="normal"):
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{fill}" text-anchor="{anchor}" '
            f'font-weight="{weight}">{escape(content)}</text>')


def build_svg(rows, scale=2):
    n = len(rows)
    peak_index = max(range(n), key=lambda i: rows[i]["complaints"])
    peak = rows[peak_index]
    first_2026 = next(i for i, r in enumerate(rows) if r["month_label"].startswith("2026"))
    provisional = [i for i, r in enumerate(rows) if r["provisional"]]
    step = (RIGHT - LEFT) / (n - 1)
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W * scale}" height="{H * scale}" '
        f'viewBox="0 0 {W} {H}" role="img" aria-labelledby="title desc" font-family="{FONT}">',
        '<title id="title">Monthly credit-reporting complaints to the CFPB, January 2025 to October 2026</title>',
        '<desc id="desc">Line chart of exact monthly complaint counts. ' + escape("; ".join(
            f"{_month_name(r['month_label'], '%b %Y')}: {r['complaints']:,}" for r in rows)) +
        f". Peak {peak['complaints']:,} in {_month_name(peak['month_label'], '%B %Y')}.</desc>",
        f'<rect width="{W}" height="{H}" fill="{SURFACE}"/>',
    ]
    # Title and subtitle.
    out.append(_text(LEFT - 52, 52, f"Credit-reporting complaints reached {peak['complaints']:,} in "
                     f"{_month_name(peak['month_label'], '%B %Y')}", 24, TEXT_PRIMARY, weight="600"))
    out.append(_text(LEFT - 52, 80, "Monthly complaints received by the CFPB, January 2025 – October 2026 "
                     "(exact counts)", 15))

    # Provisional band (behind everything else in the plot).
    if provisional:
        x0 = _x(provisional[0], n) - step / 2
        x1 = min(_x(provisional[-1], n) + step / 2, W - 8)
        out.append(f'<rect x="{x0:.1f}" y="{TOP:.1f}" width="{x1 - x0:.1f}" height="{BOTTOM - TOP:.1f}" '
                   f'fill="{BAND}" data-provisional="{",".join(rows[i]["month_label"] for i in provisional)}"/>')
        out.append(_text((x0 + x1) / 2, BOTTOM - 30, "Response data", 11, TEXT_SECONDARY, "middle"))
        out.append(_text((x0 + x1) / 2, BOTTOM - 16, "provisional*", 11, TEXT_SECONDARY, "middle"))

    # Gridlines and y-axis labels (hairline, recessive).
    for value in range(0, Y_MAX + 1, Y_TICK):
        y = _y(value)
        out.append(f'<line x1="{LEFT}" y1="{y:.1f}" x2="{RIGHT + 30}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        out.append(_text(LEFT - 12, y + 4, "0" if value == 0 else f"{value // 1000:,}K", 12, MUTED, "end"))

    # X-axis month and year labels.
    for i, r in enumerate(rows):
        out.append(_text(_x(i, n), BOTTOM + 22, _month_name(r["month_label"]), 11, MUTED, "middle"))
    for year, start, end in ((2025, 0, first_2026 - 1), (2026, first_2026, n - 1)):
        mid = (_x(start, n) + _x(end, n)) / 2
        out.append(f'<line x1="{_x(start, n) - 8:.1f}" y1="{BOTTOM + 34}" x2="{_x(end, n) + 8:.1f}" '
                   f'y2="{BOTTOM + 34}" stroke="{SERIES[year]}" stroke-width="2"/>')
        out.append(_text(mid, BOTTOM + 52, str(year), 13, TEXT_SECONDARY, "middle", "600"))

    # Lines: 2025, a neutral connector, 2026.
    def polyline(indices, color, width=2.5):
        pts = " ".join(f"{_x(i, n):.1f},{_y(rows[i]['complaints']):.1f}" for i in indices)
        return (f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="{width}" '
                f'stroke-linejoin="round" stroke-linecap="round"/>')

    out.append(polyline([first_2026 - 1, first_2026], MUTED, 1.5))
    out.append(polyline(range(0, first_2026), SERIES[2025]))
    out.append(polyline(range(first_2026, n), SERIES[2026]))
    for i, r in enumerate(rows):
        year = int(r["month_label"][:4])
        radius = 6 if i == peak_index else 4
        out.append(f'<circle cx="{_x(i, n):.1f}" cy="{_y(r["complaints"]):.1f}" r="{radius}" '
                   f'fill="{SERIES[year]}" stroke="{SURFACE}" stroke-width="2" '
                   f'data-month="{r["month_label"]}" data-value="{r["complaints"]}"/>')

    # Peak annotation, directly above the peak point.
    px, py = _x(peak_index, n), _y(peak["complaints"])
    out.append(f'<g data-annotation="peak" data-month="{peak["month_label"]}" data-value="{peak["complaints"]}" '
               f'data-mom="{peak["mom_pct"]}" data-yoy="{peak["yoy_pct"]}">')
    # Placed in the clear band between the top gridline and the point, ending left of the point so it
    # clears both the gridline and the rising segment into the peak.
    top_room = _y(Y_MAX) + 21
    out.append(_text(px - 14, top_room, f"Peak: {peak['complaints']:,}", 15, TEXT_PRIMARY, "end", "600"))
    out.append(_text(px - 14, top_room + 17, f"{peak['mom_pct']:+.1f}% vs {_month_name(rows[peak_index - 1]['month_label'], '%b %Y')}"
                     f"  ·  {peak['yoy_pct']:+.1f}% vs {_month_name(rows[peak_index - 12]['month_label'], '%b %Y')}",
                     12, TEXT_SECONDARY, "end"))
    out.append("</g>")

    # Legend (two series): line keys with text in text tokens.
    lx, ly = LEFT + 14, TOP + 14
    for k, year in enumerate((2025, 2026)):
        x = lx + k * 86
        out.append(f'<line x1="{x}" y1="{ly}" x2="{x + 22}" y2="{ly}" stroke="{SERIES[year]}" stroke-width="2.5" '
                   f'stroke-linecap="round"/>')
        out.append(f'<circle cx="{x + 11}" cy="{ly}" r="4" fill="{SERIES[year]}" stroke="{SURFACE}" stroke-width="2"/>')
        out.append(_text(x + 30, ly + 4, str(year), 13, TEXT_SECONDARY))

    # Footnotes and source.
    months = " and ".join(_month_name(rows[i]["month_label"], "%B") for i in provisional)
    out.append(_text(LEFT - 52, H - 44, f"* {months} 2026 response metrics are provisional (at least 1% of complaints "
                     "still awaiting a company response); complaint volumes are counted at intake and remain usable.",
                     11, TEXT_SECONDARY))
    out.append(_text(LEFT - 52, H - 24, "Source: CFPB Consumer Complaint Database (credit reporting), exact monthly "
                     f"totals via Sentinel AI query Q1.1 (sql/01_monthly_trends.sql). Data snapshot: {SNAPSHOT}.",
                     11, MUTED))
    out.append("</svg>")
    return "\n".join(out) + "\n"


def render(csv_path=CSV_PATH, svg_path=SVG_PATH, png_path=PNG_PATH):
    rows = load_rows(csv_path)
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    svg_path.write_text(build_svg(rows))
    subprocess.run(["sips", "-s", "format", "png", str(svg_path), "--out", str(png_path)],
                   check=True, capture_output=True)
    return rows


if __name__ == "__main__":
    data = render()
    print(f"{len(data)} months -> {SVG_PATH.relative_to(ROOT)}, {PNG_PATH.relative_to(ROOT)}")
