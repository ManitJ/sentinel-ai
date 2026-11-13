"""Growth drivers waterfall chart (SVG + PNG) from the verified Q5.3 export.

    PYTHONPATH=src python -m lakehouse.growth_drivers_chart

Input : sql/results/exports/growth_drivers.json  (Q5.2/Q5.3 run with full previews)
Output: docs/assets/growth_drivers_waterfall.svg and .png (2400x1520, rendered by macOS `sips`)

A true waterfall on one zero-based axis: Jan-Oct 2025 total -> the three largest issue-group
changes -> the other nine groups combined -> Jan-Oct 2026 total. Only Q5.3 is used: it covers
every issue group, so its net changes add up exactly to the total change. Q5.2 (top 15
companies) is not a complete breakdown and is not used.

Bar geometry uses `net_change_complaints` only. `contribution_to_net_change_pct` is used only
for labels; the combined "Other" bar's share is recomputed from its exact net change, because
summing rounded row percentages would not reconcile to 100%. `pct_growth` is never used.
"""

import json
import subprocess
import textwrap
from pathlib import Path
from xml.sax.saxutils import escape

from lakehouse.monthly_trends_chart import FONT, GRID, MUTED, SURFACE, TEXT_PRIMARY, TEXT_SECONDARY, _text

ROOT = Path(__file__).resolve().parents[2]
JSON_PATH = ROOT / "sql" / "results" / "exports" / "growth_drivers.json"
SVG_PATH = ROOT / "docs" / "assets" / "growth_drivers_waterfall.svg"
PNG_PATH = ROOT / "docs" / "assets" / "growth_drivers_waterfall.png"
SNAPSHOT = "November 12, 2026"
TITLE_TEMPLATE = "What drove the {pct}% increase in credit-reporting complaints?"
COLUMNS = ["issue_group", "prior_complaints", "current_complaints", "net_change_complaints", "pct_growth",
           "contribution_to_net_change_pct"]
TOP_N = 3
PERIOD_PRIOR, PERIOD_CURRENT = "Jan–Oct 2025", "Jan–Oct 2026"

# Totals in a neutral (they are levels, not changes); increases blue and decreases orange
# (reference palette categorical slots 1-2).
TOTAL, INCREASE, DECREASE = "#6b6a64", "#2a78d6", "#eb6834"

W, H = 1200, 760
LEFT, RIGHT, TOP, BOTTOM = 110, 1150, 180, 520
Y_MAX, Y_TICK = 6_000_000, 1_000_000
BAR_WIDTH = 104
CALLOUT_BELOW_PX = 3.0                # bars thinner than this get a leader line to their label
CALLOUT_LIFT = 20                     # label offset above such a bar


class GrowthDataError(ValueError):
    """The export does not hold a complete, passing, reconciling Q5.3 result."""


def load_rows(path=JSON_PATH):
    report = json.loads(Path(path).read_text())
    entry = next((q for q in report.get("queries", []) if q.get("id") == "Q5.3"), None)
    if entry is None:
        raise GrowthDataError("Q5.3 not found in the export")
    if entry["status"] != "PASS" or not entry["checks"] or not all(c["passed"] for c in entry["checks"]):
        raise GrowthDataError("Q5.3 did not pass validation")
    if entry["columns"] != COLUMNS:
        raise GrowthDataError(f"unexpected columns: {entry['columns']}")
    rows = entry["preview"]
    if entry["row_count"] != len(rows):
        raise GrowthDataError(f"preview has {len(rows)} of {entry['row_count']} rows")
    validate(rows)
    return rows


def validate(rows):
    if len(rows) <= TOP_N:
        raise GrowthDataError("too few issue-group rows for a waterfall")
    if len({r["issue_group"] for r in rows}) != len(rows):
        raise GrowthDataError("duplicate issue groups")
    for r in rows:
        counts = (r["prior_complaints"], r["current_complaints"], r["net_change_complaints"])
        if any(v is None for v in counts) or min(counts[:2]) < 0:
            raise GrowthDataError(f"{r['issue_group']}: missing or negative counts")
        if r["net_change_complaints"] != r["current_complaints"] - r["prior_complaints"]:
            raise GrowthDataError(f"{r['issue_group']}: net change != current - prior")
    start, end = totals(rows)
    net = end - start
    if start <= 0 or net == 0:
        raise GrowthDataError("totals do not describe a change")
    for r in rows:
        if r["contribution_to_net_change_pct"] != round(100 * r["net_change_complaints"] / net, 2):
            raise GrowthDataError(f"{r['issue_group']}: contribution does not match its net change")
    if max(start, end) > Y_MAX:
        raise GrowthDataError("a total exceeds the chart's y-axis maximum")


def totals(rows):
    return sum(r["prior_complaints"] for r in rows), sum(r["current_complaints"] for r in rows)


def build_steps(rows):
    """Start total, top-N groups by net change, the remainder combined, end total."""
    start, end = totals(rows)
    net = end - start
    ranked = sorted(rows, key=lambda r: -r["net_change_complaints"])
    top, rest = ranked[:TOP_N], ranked[TOP_N:]
    steps = [{"kind": "total", "label": f"{PERIOD_PRIOR} total", "value": start, "base": 0, "level": start}]
    level = start
    for r in top:
        change = r["net_change_complaints"]
        steps.append({"kind": "increase" if change >= 0 else "decrease", "label": r["issue_group"],
                      "value": change, "base": level, "level": level + change,
                      "contribution": r["contribution_to_net_change_pct"], "members": [r["issue_group"]]})
        level += change
    other = sum(r["net_change_complaints"] for r in rest)
    steps.append({"kind": "increase" if other >= 0 else "decrease", "label": f"Other {len(rest)} issue groups",
                  "value": other, "base": level, "level": level + other,
                  "contribution": round(100 * other / net, 2), "members": [r["issue_group"] for r in rest],
                  "increases": sum(r["net_change_complaints"] for r in rest if r["net_change_complaints"] > 0),
                  "decreases": sum(r["net_change_complaints"] for r in rest if r["net_change_complaints"] < 0),
                  "n_up": sum(r["net_change_complaints"] > 0 for r in rest),
                  "n_down": sum(r["net_change_complaints"] < 0 for r in rest)})
    level += other
    if level != end:
        raise GrowthDataError("contributions do not reconcile to the ending total")
    steps.append({"kind": "total", "label": f"{PERIOD_CURRENT} total", "value": end, "base": 0, "level": end})
    return steps


def pct_increase(rows):
    start, end = totals(rows)
    return round(100 * (end - start) / start, 1)


def title(rows):
    return TITLE_TEMPLATE.format(pct=pct_increase(rows))


def _signed(value):
    """+1,234 / −1,234 with a true minus sign."""
    return f"{value:+,}".replace("-", "−")


def _y(value):
    return BOTTOM - (BOTTOM - TOP) * value / Y_MAX


def _slot(i, n):
    return LEFT + (RIGHT - LEFT) * (i + 0.5) / n


def _bar_span(step):
    """(y_top, y_bottom) in drawing units, always to exact scale."""
    lo, hi = sorted((step["base"], step["level"])) if step["kind"] != "total" else (0, step["value"])
    return _y(hi), _y(lo)


def _label_block(x, y_bottom_line, lines):
    """Lines of (text, size, fill, weight) stacked upward from y_bottom_line, on a surface backing
    so gridlines never run through the text."""
    parts, y = [], y_bottom_line
    widths = [len(t) * s * 0.56 for t, s, _, _ in lines]
    height = sum(s + 4 for _, s, _, _ in lines)
    w = max(widths) + 10
    parts.append(f'<rect x="{x - w / 2:.1f}" y="{y_bottom_line - height + 1:.1f}" width="{w:.1f}" '
                 f'height="{height + 3:.1f}" fill="{SURFACE}"/>')
    for text, size, fill, weight in reversed(lines):
        parts.append(_text(x, y, text, size, fill, "middle", weight))
        y -= size + 4
    return parts


def build_svg(rows, scale=2):
    steps = build_steps(rows)
    n = len(steps)
    start, end = steps[0]["value"], steps[-1]["value"]
    net, pct = end - start, pct_increase(rows)
    changes = steps[1:-1]
    largest = max(changes, key=lambda s: s["value"])
    other = changes[-1]
    chart_title = title(rows)

    desc = (f"Waterfall chart. {PERIOD_PRIOR}: {start:,} complaints. " +
            "; ".join(f"{s['label']}: {s['value']:+,} ({s['contribution']:.2f}% of the net increase)"
                      for s in changes) +
            f". {PERIOD_CURRENT}: {end:,} complaints. Net change {net:+,} ({pct:+.1f}%). "
            f"Largest driver: {largest['label']}.")
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W * scale}" height="{H * scale}" '
        f'viewBox="0 0 {W} {H}" role="img" aria-labelledby="title desc" font-family="{FONT}">',
        f'<title id="title">{escape(chart_title)}</title>',
        f'<desc id="desc">{escape(desc)}</desc>',
        f'<rect width="{W}" height="{H}" fill="{SURFACE}"/>',
        _text(LEFT - 70, 52, chart_title, 24, TEXT_PRIMARY, weight="600"),
        _text(LEFT - 70, 80, f"CFPB credit-reporting complaints by issue group, {PERIOD_PRIOR} vs {PERIOD_CURRENT}: "
              f"net change {net:+,} complaints ({pct:+.1f}%)", 15),
    ]

    # Legend: only the kinds that appear.
    kinds = [("total", "Total (Jan–Oct)", TOTAL), ("increase", "Increase", INCREASE),
             ("decrease", "Decrease", DECREASE)]
    lx, ly = LEFT - 70, 116
    for label, color in [(lbl, c) for k, lbl, c in kinds if any(s["kind"] == k for s in steps)]:
        out.append(f'<rect x="{lx}" y="{ly - 10}" width="12" height="12" rx="2" fill="{color}" data-legend="{label}"/>')
        out.append(_text(lx + 18, ly, label, 13, TEXT_SECONDARY))
        lx += 18 + len(label) * 6.6 + 24

    # Gridlines and y-axis.
    for value in range(0, Y_MAX + 1, Y_TICK):
        y = _y(value)
        out.append(f'<line x1="{LEFT}" y1="{y:.1f}" x2="{RIGHT}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        out.append(_text(LEFT - 12, y + 4, "0" if value == 0 else f"{value // 1_000_000}M", 12, MUTED, "end"))
    out.append(_text(LEFT - 70, TOP - 16, "Complaints", 12, MUTED))

    # Connectors: from each bar's running level to the next bar.
    for i in range(n - 1):
        level = steps[i]["level"]
        x1, x2 = _slot(i, n) + BAR_WIDTH / 2, _slot(i + 1, n) - BAR_WIDTH / 2
        out.append(f'<line x1="{x1:.1f}" y1="{_y(level):.1f}" x2="{x2:.1f}" y2="{_y(level):.1f}" stroke="{MUTED}" '
                   f'stroke-width="1" stroke-dasharray="3 3" data-connector="{level}"/>')

    # Bars.
    for i, s in enumerate(steps):
        y_top, y_bottom = _bar_span(s)
        color = {"total": TOTAL, "increase": INCREASE, "decrease": DECREASE}[s["kind"]]
        out.append(f'<rect x="{_slot(i, n) - BAR_WIDTH / 2:.1f}" y="{y_top:.2f}" width="{BAR_WIDTH}" '
                   f'height="{y_bottom - y_top:.2f}" fill="{color}" data-kind="{s["kind"]}" '
                   f'data-label="{escape(s["label"])}" data-value="{s["value"]}" data-base="{s["base"]}" '
                   f'data-level="{s["level"]}"/>')
        if y_bottom - y_top < CALLOUT_BELOW_PX:
            # Too thin to see at true scale: a leader from the label to the bar's exact position.
            out.append(f'<line x1="{_slot(i, n):.1f}" y1="{y_top - CALLOUT_LIFT + 2:.2f}" x2="{_slot(i, n):.1f}" '
                       f'y2="{y_top:.2f}" stroke="{TEXT_SECONDARY}" stroke-width="1" '
                       f'data-callout="{s["value"]}"/>')

    # Value labels above each bar (selective: one value + one share per bar).
    for i, s in enumerate(steps):
        y_top, _ = _bar_span(s)
        x = _slot(i, n)
        if s["kind"] == "total":
            lines = [(f"{s['value']:,}", 14, TEXT_PRIMARY, "600")]
            if i == n - 1:
                lines.append((f"{pct:+.1f}% vs {PERIOD_PRIOR}", 12, TEXT_SECONDARY, "normal"))
        else:
            star = "*" if s is other else ""
            lines = [(f"{_signed(s['value'])}{star}", 14, TEXT_PRIMARY, "600"),
                     (f"{s['contribution']:.2f}% of net increase", 12, TEXT_SECONDARY, "normal")]
            if s is largest:
                lines.insert(0, ("Largest driver", 12, TEXT_PRIMARY, "600"))
        out.append(f'<g data-annotation="{s["kind"]}" data-label="{escape(s["label"])}" data-value="{s["value"]}"'
                   + (f' data-contribution="{s["contribution"]}"' if s["kind"] != "total" else "")
                   + (' data-largest="true"' if s is largest else "") + ">")
        y_bottom = _bar_span(s)[1]
        out += _label_block(x, y_top - (CALLOUT_LIFT if y_bottom - y_top < CALLOUT_BELOW_PX else 8), lines)
        out.append("</g>")

    # Category labels (wrapped).
    for i, s in enumerate(steps):
        for k, line in enumerate(textwrap.wrap(s["label"], 26)):
            out.append(_text(_slot(i, n), BOTTOM + 22 + 15 * k, line, 12,
                             TEXT_PRIMARY if s["kind"] == "total" else TEXT_SECONDARY, "middle",
                             "600" if s["kind"] == "total" else "normal"))

    # Footnotes and source.
    notes = [
        f"Comparison covers January–October of each year ({PERIOD_PRIOR} vs {PERIOD_CURRENT}), not full calendar years. "
        "Contribution = issue group's net change ÷ total net change.",
        f"* {other['label']}: {other['n_up']} increased ({_signed(other['increases'])}) and {other['n_down']} decreased "
        f"({_signed(other['decreases'])}), net {_signed(other['value'])}. Bar drawn to scale (under 1 pixel tall); "
        "the callout marks its position.",
        "September and October 2026 response metrics are provisional (at least 1% of complaints still awaiting a "
        "company response); complaint volumes are counted at intake and remain usable.",
        "Complaints are unverified consumer reports: counts do not establish wrongdoing and are not adjusted for "
        "company size. Issue groups are CFPB categories.",
    ]
    for k, note in enumerate(notes):
        out.append(_text(LEFT - 70, H - 108 + 16 * k, note, 11, TEXT_SECONDARY))
    out.append(_text(LEFT - 70, H - 24, "Source: CFPB Consumer Complaint Database (credit reporting), exact counts via "
                     "Sentinel AI query Q5.3 (sql/05_growth_drivers.sql). "
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
    print(f"{len(data)} issue groups -> {SVG_PATH.relative_to(ROOT)}, {PNG_PATH.relative_to(ROOT)}")
