# CFPB Credit Reporting Sample — Method and Caveats

## Scope
- **Source:** CFPB bulk export `https://files.consumerfinance.gov/ccdb/complaints.csv.zip`
  (single `complaints.csv`, deflate, zip64; 15 columns; dates as `YYYY-MM-DD`).
- **Frame:** `Product == "Credit reporting or other personal consumer reports"` and
  `Date received` in `[2025-01-01, 2026-11-01)` — 22 complete calendar months.
- **Sample:** 2,273 complaints per month (50,006 total), equal allocation.

## Selection
1. `key = int(SHA-256("sentinel-ai-cfpb-sample-v1:" + complaint_id)[:8 bytes])`.
2. Within each month, keep the 2,273 rows with the smallest keys ("bottom-k").

This is a simple random sample without replacement within each month. It is
deterministic given the salt and the set of complaint IDs in the source:
row order, block size and reruns do not change it.

## Weights and volume
- Record weight `= N_h / n_h` (month population in the bulk frame / sample size).
- **Complaint-volume trends must use exact API monthly totals**
  (`data/reference/cfpb_api_monthly_credit_reporting.json`), never sample
  counts: every month has the same sample size by design.

## Validation
`compare-sample` compares sample category shares (issue, company, state,
company response, timeliness, submission channel, public response, tags) with
exact API aggregations per month (`|z| > 3` flagged, with the expected number
of chance flags reported) and as weighted overall shares. It also reconciles
bulk-frame monthly counts against API totals.

## Caveats
- **Sample stability:** if CFPB later adds historical complaints whose keys
  rank below a month's current 2,273rd key, a rerun will include them and drop
  the same number of previously selected rows. Removed or corrected source
  records likewise change the frame. Each run's manifest records the source
  file's SHA-256 and Last-Modified header so a sample can be tied to an exact
  snapshot.
- The bulk file and API are refreshed independently, so small month-count
  differences between them are expected; they are reported, not hidden.
- The bulk file has dates without time of day; API records include timestamps.

## Privacy
- Narrative text is not present in the current bulk file. The column
  `Consumer complaint narrative` is nevertheless explicitly excluded and never
  written (including `_raw_json`).
- Narrative text must not be sent to an LLM unless privacy safeguards are
  implemented first.

## Storage (outside Git)
| Path | Content |
|---|---|
| `data/external/cfpb/complaints.csv.zip` (+ `.meta.json`) | Bulk source, SHA-256, download timing |
| `data/bronze/cfpb_credit_reporting_sample/stratum=YYYY-MM/` | Per-month Parquet (zstd) |
| `data/bronze/cfpb_credit_reporting_sample/_upload/*.parquet` | Single combined file for Databricks upload |
| `data/bronze/cfpb_credit_reporting_sample/_manifest.json` | Counts, weights, scan stats, resources |
| `data/reference/*.json` | API monthly totals/aggregations and validation report |

## Reproduce
```bash
PYTHONPATH=src python -m ingestion bulk-download
PYTHONPATH=src python -m ingestion bulk-sample
PYTHONPATH=src python -m ingestion api-benchmarks
PYTHONPATH=src python -m ingestion compare-sample
```
