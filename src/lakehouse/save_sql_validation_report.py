"""Save a JSON report from the Databricks validation notebook into ``sql/results/``.

    PYTHONPATH=src python -m lakehouse.save_sql_validation_report path/to/report.json

Writes (overwriting earlier output of this tool only):
    sql/results/validation_report.json     the report as received (previews already limited)
    sql/results/validation_summary.md      PASS / FAIL / NOT RUN table and failed-check details
    sql/results/previews/<Qn.m>.md         up to 10 preview rows per executed query
Nothing is sent anywhere; the input is the report text copied from the notebook.
"""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = ROOT / "sql" / "results"
MAX_PREVIEW_ROWS = 10
STATUSES = ("PASS", "FAIL", "NOT RUN")


def _cell(value):
    return "" if value is None else str(value).replace("|", "\\|").replace("\n", " ")


def markdown_table(columns, rows):
    lines = ["| " + " | ".join(_cell(c) for c in columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(_cell(row.get(c)) for c in columns) + " |" for row in rows]
    return "\n".join(lines)


def save_report(report_text, out_dir=DEFAULT_OUT):
    report = json.loads(report_text)
    queries = report["queries"]
    for entry in queries:
        if entry.get("status") not in STATUSES or "id" not in entry:
            raise ValueError(f"not a validation report entry: {entry!r}"[:200])
    out = Path(out_dir)
    (out / "previews").mkdir(parents=True, exist_ok=True)
    for entry in queries:
        entry["preview"] = entry.get("preview", [])[:MAX_PREVIEW_ROWS]
    (out / "validation_report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")

    counts = {s: sum(e["status"] == s for e in queries) for s in STATUSES}
    lines = ["# SQL showcase validation", "",
             f"Generated in Databricks at {report.get('generated_at') or 'unknown'}; "
             f"saved from the notebook's JSON report.", "",
             f"**{counts['PASS']} PASS · {counts['FAIL']} FAIL · {counts['NOT RUN']} NOT RUN**", "",
             markdown_table(["query", "file", "status", "rows", "checks"],
                            [{"query": e["id"], "file": e["file"], "status": e["status"], "rows": e.get("row_count"),
                              "checks": f"{sum(c['passed'] for c in e.get('checks', []))}/{len(e.get('checks', []))}"}
                             for e in queries])]
    problems = [e for e in queries if e["status"] != "PASS"]
    if problems:
        lines += ["", "## Details for non-passing queries", ""]
        for e in problems:
            lines.append(f"- **{e['id']}** ({e['status']}): {e.get('error') or ''}")
            lines += [f"  - {c['check']}: expected `{c['expected']}`, got `{c['actual']}`"
                      for c in e.get("checks", []) if not c["passed"]]
    (out / "validation_summary.md").write_text("\n".join(lines) + "\n")

    written = 0
    for e in queries:
        if e["status"] == "NOT RUN" or not e.get("columns"):
            continue
        body = [f"# {e['id']} — {e['file']}", "",
                f"Status: **{e['status']}** · rows: {e.get('row_count')} · showing up to {MAX_PREVIEW_ROWS}", "",
                markdown_table(e["columns"], e["preview"])]
        (out / "previews" / f"{e['id']}.md").write_text("\n".join(body) + "\n")
        written += 1
    return {"counts": counts, "previews_written": written, "out_dir": str(out)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("report", help="JSON report copied from the validation notebook")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT))
    args = parser.parse_args()
    print(json.dumps(save_report(Path(args.report).read_text(), args.out_dir), indent=2))
