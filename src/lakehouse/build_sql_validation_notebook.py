"""Generate the Databricks validation notebook from the SQL files and ``sql_validation.py``.

    PYTHONPATH=src python -m lakehouse.build_sql_validation_notebook

The notebook embeds (1) the source of ``lakehouse/sql_validation.py`` and (2) the queries parsed
from ``sql/02``–``06``. A unit test regenerates it and fails if the committed notebook differs, so
the notebook can never drift from the SQL files or the tested Python code.
"""

import inspect
import json
from pathlib import Path

from lakehouse import sql_validation

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql"
NOTEBOOK = ROOT / "databricks" / "notebooks" / "06_sql_showcase_validation.py"

HEADER = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Sentinel AI — 06 · SQL showcase validation (read-only)
# MAGIC
# MAGIC **Generated file — do not edit.** Rebuild with
# MAGIC `PYTHONPATH=src python -m lakehouse.build_sql_validation_notebook`.
# MAGIC
# MAGIC Runs queries **Q2.1–Q6.2** from `sql/02`–`06` in this workspace session and checks the results
# MAGIC against benchmarks computed from the same reference data the Gold layer was built from.
# MAGIC
# MAGIC * **Read-only.** Every query must be a single `SELECT`/`WITH` statement; anything else is
# MAGIC   blocked before execution (status `NOT RUN`). The notebook writes no tables, files or views.
# MAGIC * **No credentials.** It uses the notebook's own Databricks session.
# MAGIC * **Output.** A summary table (PASS / FAIL / NOT RUN per query) and a JSON report with
# MAGIC   column names, row counts, check details, errors and up to `preview_rows` rows per query.
# MAGIC   Copy the JSON report back to the repository for `sql/results/`.
'''


def build():
    queries = sql_validation.load_queries(SQL_DIR)
    module_source = inspect.getsource(sql_validation)
    query_literal = json.dumps(queries, indent=1, ensure_ascii=False)
    parts = [
        HEADER,
        "# COMMAND ----------\n\n# MAGIC %md\n# MAGIC ## 1 · Validation code (from `src/lakehouse/sql_validation.py`)\n",
        "# COMMAND ----------\n\n" + module_source,
        "# COMMAND ----------\n\n# MAGIC %md\n# MAGIC ## 2 · Queries (parsed from `sql/02`–`06`)\n",
        "# COMMAND ----------\n\nQUERIES = json.loads(r'''" + query_literal + "''')\n"
        "print(f\"{len(QUERIES)} queries: {[q['id'] for q in QUERIES]}\")\n",
        "# COMMAND ----------\n\n# MAGIC %md\n# MAGIC ## 3 · Run (read-only)\n"
        "# MAGIC `queries`: `all` or comma-separated ids such as `Q2.1,Q6.1`. "
        "`preview_rows`: rows kept per query in the report.\n",
        '''# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("queries", "all")
dbutils.widgets.text("preview_rows", "10")
requested = dbutils.widgets.get("queries").strip()
SELECTED = None if requested.lower() == "all" else {q.strip() for q in requested.split(",") if q.strip()}
PREVIEW_ROWS = max(0, min(int(dbutils.widgets.get("preview_rows")), 50))


def execute(sql):
    """Run one read-only query; fetch at most MAX_ROWS + 1 rows to detect larger results."""
    df = spark.sql(sql)
    rows = [r.asDict(recursive=True) for r in df.limit(MAX_ROWS + 1).collect()]
    row_count = len(rows) if len(rows) <= MAX_ROWS else df.count()
    return df.columns, rows[:MAX_ROWS], row_count


report = run_validation(QUERIES, execute, selected=SELECTED, preview_rows=PREVIEW_ROWS)
SUMMARY_COLUMNS = ["query", "file", "status", "rows", "checks_passed", "checks_total", "failed_checks", "error"]
display(spark.createDataFrame(
    [tuple(r[c] for c in SUMMARY_COLUMNS) for r in summary_rows(report)],
    "query string, file string, status string, rows bigint, checks_passed int, checks_total int, "
    "failed_checks string, error string"))  # explicit schema: columns can be entirely NULL
counts = {s: sum(e["status"] == s for e in report) for s in (PASS, FAIL, NOT_RUN)}
print(counts)
''',
        "# COMMAND ----------\n\n# MAGIC %md\n# MAGIC ## 4 · JSON report\n"
        "# MAGIC Copy the full output of the next cell and send it back for `sql/results/`.\n",
        '''# COMMAND ----------

print(report_json(report, generated_at=datetime.now(timezone.utc).isoformat()))
''',
    ]
    return "\n".join(parts)


if __name__ == "__main__":
    NOTEBOOK.write_text(build())
    print(f"wrote {NOTEBOOK.relative_to(ROOT)}")
