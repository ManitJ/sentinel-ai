# Validation results

This folder holds output from the read-only Databricks validation notebook
(`databricks/notebooks/06_sql_showcase_validation.py`), saved locally with:

```bash
PYTHONPATH=src python -m lakehouse.save_sql_validation_report path/to/report.json
```

| File | Content |
|---|---|
| `validation_report.json` | Full report: status, row count, columns, checks and errors per query; previews limited to 10 rows |
| `validation_summary.md` | PASS / FAIL / NOT RUN table and details of any failed checks |
| `previews/<Qn.m>.md` | Up to 10 result rows per executed query |
| `exports/` | Full results used for the charts: `monthly_trends.csv` (Q1.1, CSV downloaded from Databricks), `company_concentration.json` (Q2.2) and `growth_drivers.json` (Q5.2, Q5.3), both notebook runs with `preview_rows` set to the full row count |

Only output produced by actual Databricks runs is stored here. Previews contain aggregate public
CFPB statistics only (no complaint records).
