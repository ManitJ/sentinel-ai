# SQL Analytics Showcase

Read-only Databricks SQL analyses of the Sentinel AI Gold layer (`workspace.sentinel`), covering
credit-reporting complaints received **January 2025 – October 2026** (10,500,160 complaints, exact
CFPB API totals). Every file states its business questions, metric definitions and caveats in a
header comment.

| File | Business questions | Sources | Measure |
|---|---|---|---|
| [`00_verify_schema.sql`](00_verify_schema.sql) | Do the Gold schemas, row counts, months and totals match expectations? | all 10 Gold tables | checks |
| [`01_monthly_trends.sql`](01_monthly_trends.sql) | How is monthly volume developing (MoM, YoY, quarterly, peak months)? | `gold_monthly_volume` | exact |
| [`02_company_benchmarking.sql`](02_company_benchmarking.sql) | Which companies receive most complaints, how concentrated are complaints, how have leaders moved, how do their outcomes compare? | `gold_company_monthly`; `gold_company_response_estimates` | exact; estimated (2025, reliable, 95% CI) |
| [`03_issue_trends.sql`](03_issue_trends.sql) | Which issues dominate and how is the mix shifting? Which sub-issues are most common? | `gold_issue_monthly`, `gold_issue_category_map`; `gold_sub_issue_estimates` | exact; estimated (reliable, 95% CI) |
| [`04_geographic_distribution.sql`](04_geographic_distribution.sql) | Where do complaints come from and how has each state changed year to date? | `gold_state_monthly` | exact |
| [`05_growth_drivers.sql`](05_growth_drivers.sql) | How much did complaints grow year to date, and which companies, issues and states account for the net change? | volume, company, issue, state tables | exact |
| [`06_executive_scorecard.sql`](06_executive_scorecard.sql) | One-row KPI scorecard; response performance in months with complete response data | volume, company, issue, state, response tables | exact |

## How to run

1. In Databricks, open **SQL Editor** and select your SQL warehouse (Free Edition: the serverless
   starter warehouse).
2. Run `00_verify_schema.sql` first; every check should return `PASS`.
3. Open an analysis file and run its queries one at a time (place the cursor in a query and press
   **Cmd/Ctrl + Enter**). Each query is self-contained and labelled `Q<file>.<n>`.
4. All statements are `SELECT`, `WITH` or `DESCRIBE`; nothing is created or modified.

Periods are derived from the data (latest month, same month a year earlier, year to date), so the
queries keep working when new months are added.

## Automated validation

No Databricks credentials are configured locally, so validation runs **inside Databricks** with the
notebook's own session:

1. Import `databricks/notebooks/06_sql_showcase_validation.py` into the workspace (Workspace →
   ⋮ → Import → File) and attach it to serverless compute.
2. **Run all.** It executes Q2.1–Q6.2 one at a time and shows a PASS / FAIL / NOT RUN summary;
   widget `queries` can limit the run (e.g. `Q2.1,Q6.1`).
3. Copy the JSON report printed by the last cell into a local file and save it into `results/`:
   `PYTHONPATH=src python -m lakehouse.save_sql_validation_report report.json`.

The notebook is generated from the `.sql` files and `src/lakehouse/sql_validation.py`
(`PYTHONPATH=src python -m lakehouse.build_sql_validation_notebook`); a unit test fails if it is out of
date. Every query is checked by a read-only guard before execution — anything other than a single
`SELECT`/`WITH` statement is not run — and the notebook writes no tables, views or files. Benchmarks
were computed locally from the same reference data the Gold layer was built from.

## Conventions and safeguards

- **Exact first.** Volumes and one-way breakdowns use exact CFPB population counts. Pooled shares
  are `SUM(counts) / SUM(totals)`, never an average of monthly shares.
- **Estimates only with uncertainty.** Sample-based tables are used only where no exact figure
  exists, only where `is_reliable = TRUE`, and always with 95% confidence intervals. Estimated shares
  are never converted into complaint counts.
- **Provisional response data.** A month is provisional while ≥ 1% of its complaints are still
  "In progress" (currently September and October 2026). This is a **reporting rule, not proof that
  other months are complete**. Response, timeliness and channel comparisons exclude provisional
  months; company outcome estimates use the `2025` period only, because the `ALL` and `2026-YTD`
  periods include provisional months.
- **Complaint concentration, not market share.** HHI (`10,000 × Σ share²`) measures how concentrated
  *complaints* are among companies. It says nothing about companies' market size.
- **Net-growth contribution vs percentage growth.** `pct_growth` is how fast one segment grew;
  `contribution_to_net_change_pct` is how much of the total change that segment accounts for. A
  small segment can grow fast yet contribute little. Percentage growth is suppressed (NULL) when the
  base is below 100 complaints.
- **Denominators are explicit.** Location shares state whether they divide by all complaints
  (including those with no state) or by the 50 states + DC only.
- **No per-capita rates.** No population data is used, so state figures are complaint counts.

## Limitations

- Counts are a snapshot (reference data fetched 2026-11-12) and can be revised by later CFPB updates.
- 2026 covers January–October; compare averages or matched months, not calendar-year totals.
- Company names are CFPB labels; related entities are not merged.
- Complaints are consumer submissions. Counts are not adjusted for company size and are not
  findings of wrongdoing.

## Results

Verified outputs are in [`results/`](results/): the validation report and summary (17 of 17 queries
passed), result previews, and full exports of Q1.1, Q2.2 and Q5.2/Q5.3 used for the charts. Findings
are written up in [`../docs/sql_analytics.md`](../docs/sql_analytics.md).
