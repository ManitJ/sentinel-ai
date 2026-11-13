# Gold Layer — Tables, Metrics and Statistical Method

## Principle: exact vs estimated, never mixed
| `measure_type` | Source | Used for |
|---|---|---|
| **exact** | CFPB search API population counts (`ref_cfpb_monthly_*`; facets are complete, `sum_other_doc_count = 0`) | volumes, growth, and every one-way breakdown (company, issue, state, response, timeliness, channel, public response, tags) |
| **estimated** | stratified sample in `silver_complaints`, weighted by `sample_weight = N_h / n_h` | only questions the API cannot answer exactly: company × response type, company × timeliness, sub-issues |

The sample allocates 2,273 records to every month regardless of volume, so:
- sample counts are **never** used for volume or growth;
- pooled estimates are **always** weighted (unweighted pooling would over-represent low-volume months);
- within a single month all weights are equal, so single-month estimates are simple proportions.

## Tables (`workspace.sentinel.*`)
| Table | Grain | Type | Key metrics |
|---|---|---|---|
| `ref_cfpb_monthly_totals` | month | exact | population complaints |
| `ref_cfpb_monthly_facets` | month × dimension × category | exact | count; residual row (`category NULL`) = no value |
| `gold_monthly_volume` | month | exact | complaints, MoM/YoY change and %, 3-month rolling average, sample size/fraction |
| `gold_company_monthly` | month × company | exact | complaints, share of month, rank, MoM |
| `gold_issue_monthly` | month × original issue | exact | complaints, share, rank, MoM, documented `issue_group` |
| `gold_issue_category_map` | issue | reference | original issue → `issue_group`, `is_combined_group`, note |
| `gold_state_monthly` | month × state | exact | complaints, share, rank, MoM, `state_code`, `state_category` |
| `gold_response_monthly` | month × dimension × category | exact | company response, timeliness, channel, public response, tags |
| `gold_company_response_estimates` | period × company × dimension × category | estimated | share, SE, 95% CI, sample sizes, effective n, `is_reliable` |
| `gold_sub_issue_estimates` | period × issue › sub-issue | estimated | share of all complaints, SE, 95% CI, `is_reliable` |
| `gold_estimator_calibration` | month × dimension × category | QC | sample estimate vs exact share, `inside_ci95`, z-score |
| `gold_insight_facts` | fact | exact + estimated | LLM-ready atomic facts with `fact_id`, CI, reliability, `source_table` + `source_key` |

Estimation periods: `ALL` (Jan 2025–Oct 2026), `2025`, `2026-YTD` (Jan–Oct 2026; **not** a full year).

## Issue categories
Original CFPB issue labels are always preserved. The only grouping is documented in
`gold_issue_category_map`: "…investigation into an existing **problem**" and "…existing **issue**"
share `issue_group` = "…existing problem or issue". Both labels occur in every month (verified),
so they are distinct CFPB categories; the group is an optional analysis column.

## Estimator
Stratified simple random sampling, strata = months. For domain *d* (e.g. a company) and category *c*:

- `R = Σ_h w_h a_h / Σ_h w_h b_h`, with `a_h` = sample records in *d* with *c*, `b_h` = sample records in *d*.
- Variance by Taylor linearisation with finite-population correction:
  `Var(R) = Σ_h N_h² (1 − n_h/N_h) s²_h / n_h`.
- 95% CI: Wilson score interval with effective n = R(1−R)/Var (domain sample size when Var = 0).
- **Reliable** (project rule): ≥ 30 domain sample records **and** CI width ≤ 0.20. Unreliable rows
  stay in the table for transparency but are excluded from `gold_insight_facts`.

### Verification
- Offline Monte Carlo test: repeated stratified samples from a known population give ≈ 95% CI coverage.
- `gold_estimator_calibration` on real data: single-month CIs contained the exact API share in
  924 of 974 comparisons (94.9%) among categories with ≥ 10 expected sample records. The build fails
  below 90%.

## Using Gold correctly
- Volumes and trends: `gold_monthly_volume`, `gold_*_monthly` (exact).
- "What share of Company X's complaints were closed with relief?": `gold_company_response_estimates`
  — report the CI and only when `is_reliable`.
- Do not multiply an estimated share by an exact total without carrying the CI through.
- `2026-YTD` covers 10 months; compare with `2025` only as shares, not totals.

## LLM readiness (integration not implemented)
`gold_insight_facts` is the only table an LLM should read. Each fact has a stable `fact_id`, a typed
value with `unit`, `measure_type`, CI and `sample_n` for estimates, and `source_table` + `source_key`
(JSON of the Gold primary key). A future insight generator can require every number in its output to
cite a `fact_id`, then verify each cited value against the table automatically.
