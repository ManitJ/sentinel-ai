# Sentinel AI — Financial Complaint Data Lakehouse

Sentinel AI is an end-to-end data engineering project built on public
**CFPB Consumer Complaint Database** records. It ingests credit-reporting
complaints, builds a reproducible statistical sample, and turns it into a
Bronze → Silver → Gold Delta Lake lakehouse on Databricks Free Edition, with
analytics tables designed to feed evidence-grounded insights later.

The project runs at zero cost: public data, open-source Python, and
Databricks Free Edition serverless compute.

## Project status

| Area | Status |
|---|---|
| CFPB API ingestion (pagination, retries, checkpoints, Parquet) | ✅ Implemented and tested |
| Reproducible stratified sample from the CFPB bulk export | ✅ Implemented, validated against exact API counts |
| Databricks Bronze, Silver and Gold layers (4 notebooks) | ✅ Implemented in Databricks Free Edition; Bronze, Silver and Gold builds each run twice (second run: 0 rows changed); reference tables loaded |
| Offline test suite (no network access) | ✅ 210 tests |
| SQL analysis of complaint trends | 🔜 Planned (Gold tables are ready to query) |
| LLM-generated monthly insights with numerical verification | 🔜 Planned — **not implemented** |
| GitHub Actions CI, architecture document, reproducible demo | 🔜 Planned |

## Dataset

- **Source:** [CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/)
  via its public search API and its bulk CSV export.
- **Scope:** product *"Credit reporting or other personal consumer reports"*,
  complaints received **January 2025 – October 2026** (22 complete months).
- **Population:** 10,500,160 complaints (exact monthly totals from the API; the
  bulk file reproduces every monthly total exactly).
- **Sample:** 50,006 complaints — 2,273 per month, selected by SHA-256 hash
  (simple random sample within each month, reproducible from a fixed salt).
  Each record carries a weight `N_month / 2,273` for population estimates.
- **Privacy:** consumer narrative text is excluded and never stored.
  Raw data, the bulk download and all generated datasets are kept out of Git.

Because every month has the same sample size, **sample counts are never used
for complaint volumes**. Volumes and one-way breakdowns come from exact API
counts; the sample is used only for estimates the API cannot provide, always
with standard errors and 95% confidence intervals. See
[`docs/data_sampling.md`](docs/data_sampling.md) and
[`docs/gold_methodology.md`](docs/gold_methodology.md).

## Architecture

```
CFPB search API ──► ingestion (pagination, retries, checkpoints) ──► exact monthly totals & facets
CFPB bulk CSV   ──► streaming SHA-256 sampler (50,006 rows)      ──► Parquet
                                   │  upload to Unity Catalog volume
                                   ▼
Databricks Free Edition (serverless, Delta Lake, workspace.sentinel)
  01 Bronze  bronze_complaints        source columns as received + lineage, MERGE on complaint_id
  02 Silver  silver_complaints        typed dates, booleans, USPS state codes, tag flags; originals kept
  03 Ref     ref_cfpb_monthly_*       exact API population counts
  04 Gold    gold_* (10 tables)       exact volumes/breakdowns, weighted estimates with CIs,
                                      estimator calibration, LLM-ready fact table
```

**Gold tables:** monthly volume and growth; company, issue, state and response
breakdowns (exact); company × response/timeliness and sub-issue estimates
(weighted, with CIs and a reliability flag); an estimator calibration table; and
`gold_insight_facts`, one citable fact per row with a stable `fact_id` and a
pointer to its source row.

**Engineering properties**
- Idempotent loads: every layer uses Delta `MERGE` with content hashes; reruns change nothing.
- Validation before and after every write; notebooks fail loudly on any discrepancy.
- Silver and Gold logic is mirrored by tested Python reference implementations, and the
  notebooks check their Spark results against fingerprints computed by that reference.
- Single-month sample estimates covered the exact API share in 924 of 974 tested
  comparisons (94.9%; nominal 95%).

## Technologies

Python 3.11 · pandas · PyArrow · requests · pytest · Databricks Free Edition ·
PySpark · Delta Lake · Unity Catalog · SQL

## Project structure

```
sentinel-ai/
├── src/
│   ├── ingestion/          CFPB API client, windowed pipeline, checkpoints, bulk download,
│   │                       stratified sampler, API benchmarks, CLI (python -m ingestion)
│   └── lakehouse/          Silver and Gold reference rules, reference-data export
├── databricks/notebooks/   01_bronze_ingest · 02_silver_transform ·
│                           03_gold_reference_load · 04_gold_build
├── tests/                  offline unit tests (network access is blocked)
├── docs/                   sampling method; Gold methodology
├── requirements.txt
└── pytest.ini
```

## Setup

Requirements: Python 3.11 (developed in a Conda environment on an 8 GB Apple M2).

```bash
conda create -n sentinel-ai python=3.11 -y
conda activate sentinel-ai
pip install -r requirements.txt
python -m pytest            # offline; tests needing local data are skipped if it is absent
```

### Rebuild the dataset locally

```bash
export PYTHONPATH=src
python -m ingestion bulk-download        # ~366 MB zip -> data/external/ (resumable)
python -m ingestion bulk-sample          # 50,006-row sample -> data/bronze/cfpb_credit_reporting_sample/
python -m ingestion api-benchmarks       # exact monthly totals + facets (22 small API requests)
python -m ingestion compare-sample       # sample estimates vs exact population shares
python -m lakehouse.reference_export     # reference Parquet for Databricks -> data/reference/upload/
```

The API pipeline can also ingest complete date windows directly, e.g.
`python -m ingestion backfill --start-date 2026-11-01 --end-date 2026-11-02 --dry-run`.

Measured on the development machine: bulk download 14.3 s, full scan and sample
41.3 s over 19.1 million rows, peak memory 708 MB, combined sample Parquet 2.2 MB.

### Run the lakehouse in Databricks Free Edition

1. Create the schema and volume:
   `CREATE SCHEMA IF NOT EXISTS workspace.sentinel;`
   `CREATE VOLUME IF NOT EXISTS workspace.sentinel.raw_files;`
2. Upload to `/Volumes/workspace/sentinel/raw_files/`:
   `cfpb_credit_reporting_sample.parquet` (from `data/bronze/cfpb_credit_reporting_sample/_upload/`)
   and the two files in `data/reference/upload/`.
3. Import the four notebooks from `databricks/notebooks/` and run them in order on
   serverless compute. Each validates its inputs and outputs and can be rerun safely.

## License and data

No license has been chosen yet. CFPB complaint data is published by the U.S.
Consumer Financial Protection Bureau; see the CFPB website for its terms of use.
