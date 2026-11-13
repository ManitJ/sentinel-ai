"""Command-line interface for CFPB Bronze ingestion.

Run from the repository root::

    PYTHONPATH=src python -m ingestion backfill --start-date 2026-11-01 --end-date 2026-11-03 --dry-run
    PYTHONPATH=src python -m ingestion backfill --start-date 2026-11-01 --end-date 2026-11-03 --resume
    PYTHONPATH=src python -m ingestion incremental --overlap-days 3
    PYTHONPATH=src python -m ingestion validate
    PYTHONPATH=src python -m ingestion bulk-download
    PYTHONPATH=src python -m ingestion bulk-sample
    PYTHONPATH=src python -m ingestion api-benchmarks
    PYTHONPATH=src python -m ingestion compare-sample

Dates are ISO (YYYY-MM-DD); --end-date is exclusive.
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import requests

from ingestion.benchmarks import compare_sample, fetch_monthly_benchmarks, save_json
from ingestion.bulk_download import download_file
from ingestion.bulk_sample import (
    BULK_URL,
    CREDIT_REPORTING,
    DEFAULT_SALT,
    SampleConfig,
    open_bulk_csv,
    open_zip_prefix,
    run_sample,
)
from ingestion.client import MAX_PAGE_SIZE, MAX_RETRIES, MAX_RETRY_AFTER
from ingestion.logging_utils import configure_logging
from ingestion.pipeline import (
    IngestConfig,
    plan_windows,
    run_backfill,
    run_incremental,
)
from ingestion.storage import validate_dataset

DEFAULT_OUTPUT_DIR = "data/bronze/cfpb_complaints"
DEFAULT_BULK_ZIP = "data/external/cfpb/complaints.csv.zip"
DEFAULT_SAMPLE_DIR = "data/bronze/cfpb_credit_reporting_sample"
DEFAULT_BENCHMARKS = "data/reference/cfpb_api_monthly_credit_reporting.json"
DEFAULT_COMPARISON = "data/reference/sample_validation.json"


def _batch_size(value):
    size = int(value)
    if not 1 <= size <= MAX_PAGE_SIZE:
        raise argparse.ArgumentTypeError(f"must be between 1 and {MAX_PAGE_SIZE}")
    return size


def _positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return number


def _non_negative_float(value):
    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return number


def build_parser():
    parser = argparse.ArgumentParser(prog="ingestion", description="CFPB complaint ingestion")
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--output-dir", type=Path, default=Path(DEFAULT_OUTPUT_DIR))
    common.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    fetch = argparse.ArgumentParser(add_help=False, parents=[common])
    fetch.add_argument("--batch-size", type=_batch_size, default=MAX_PAGE_SIZE,
                       help=f"records per API request (1-{MAX_PAGE_SIZE})")
    fetch.add_argument("--window-days", type=_positive_int, default=1)
    fetch.add_argument("--request-delay", type=_non_negative_float, default=1.0,
                       help="seconds to wait between page requests")
    fetch.add_argument("--max-retries", type=_positive_int, default=MAX_RETRIES)
    fetch.add_argument("--max-retry-after", type=_non_negative_float, default=MAX_RETRY_AFTER,
                       help="longest server-requested Retry-After wait (s) before aborting")
    fetch.add_argument("--end-date", type=date.fromisoformat, help="exclusive end date")

    backfill = sub.add_parser("backfill", parents=[fetch], help="historical backfill")
    backfill.add_argument("--start-date", type=date.fromisoformat, required=True)
    backfill.add_argument("--resume", action="store_true",
                          help="skip windows already completed in the checkpoint")
    backfill.add_argument("--dry-run", action="store_true",
                          help="only report record counts per window")

    incremental = sub.add_parser("incremental", parents=[fetch],
                                 help="ingest from the checkpoint watermark")
    incremental.add_argument("--overlap-days", type=int, default=3,
                             help="re-fetch this many days before the watermark")
    incremental.add_argument("--start-date", type=date.fromisoformat,
                             help="start date if no watermark exists yet")

    sub.add_parser("validate", parents=[common], help="check output for duplicate IDs")

    dl = sub.add_parser("bulk-download", help="download the CFPB bulk CSV zip (resumable)")
    dl.add_argument("--dest", type=Path, default=Path(DEFAULT_BULK_ZIP))
    dl.add_argument("--force", action="store_true", help="re-download even if present")

    window = argparse.ArgumentParser(add_help=False)
    window.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 1))
    window.add_argument("--end-date", type=date.fromisoformat, default=date(2026, 11, 1),
                        help="exclusive end date")
    window.add_argument("--product", default=CREDIT_REPORTING)

    sample = sub.add_parser("bulk-sample", parents=[window], help="stratified sample from bulk zip")
    sample.add_argument("--source", type=Path, default=Path(DEFAULT_BULK_ZIP))
    sample.add_argument("--prefix", action="store_true",
                        help="source is a truncated prefix of the zip (validation only)")
    sample.add_argument("--output-dir", type=Path, default=Path(DEFAULT_SAMPLE_DIR))
    sample.add_argument("--per-month", type=_positive_int, default=2273)
    sample.add_argument("--salt", default=DEFAULT_SALT)

    bench = sub.add_parser("api-benchmarks", parents=[window],
                           help="fetch exact monthly totals + aggregations from the API")
    bench.add_argument("--out", type=Path, default=Path(DEFAULT_BENCHMARKS))
    bench.add_argument("--request-delay", type=_non_negative_float, default=1.0)

    compare = sub.add_parser("compare-sample", help="compare sample vs API population")
    compare.add_argument("--sample-dir", type=Path, default=Path(DEFAULT_SAMPLE_DIR))
    compare.add_argument("--benchmarks", type=Path, default=Path(DEFAULT_BENCHMARKS))
    compare.add_argument("--out", type=Path, default=Path(DEFAULT_COMPARISON))
    return parser


def main(argv=None, get=requests.get):
    args = build_parser().parse_args(argv)
    log_dir = None if getattr(args, "dry_run", False) or args.command == "validate" \
        or args.command in BULK_COMMANDS else args.output_dir / "_logs"
    log_path = configure_logging(log_dir, getattr(args, "log_level", "INFO"))

    if args.command in BULK_COMMANDS:
        return BULK_COMMANDS[args.command](args, get)

    if args.command == "validate":
        result = validate_dataset(args.output_dir)
        print(json.dumps(result, indent=2))
        return 1 if result["duplicate_ids"] or result["null_ids"] else 0

    config = IngestConfig(output_dir=args.output_dir, batch_size=args.batch_size,
                          window_days=args.window_days, request_delay=args.request_delay,
                          max_retries=args.max_retries,
                          max_retry_after=args.max_retry_after)

    if args.command == "backfill":
        if args.end_date is None:
            raise SystemExit("backfill requires --end-date")
        if args.dry_run:
            plan = plan_windows(args.start_date, args.end_date, config, get=get)
            total = sum(item["records"] or 0 for item in plan)
            requests_needed = sum(item["requests_needed"] for item in plan)
            print(json.dumps({"windows": plan, "total_records": total,
                              "requests_needed": requests_needed}, indent=2))
            return 0
        summary = run_backfill(args.start_date, args.end_date, config,
                               resume=args.resume, get=get)
    else:
        summary = run_incremental(config, overlap_days=args.overlap_days,
                                  end=args.end_date, start_if_empty=args.start_date,
                                  get=get)

    summary["log_file"] = str(log_path) if log_path else None
    print(json.dumps(summary, indent=2, default=str))
    return 0


def _bulk_download(args, get):
    meta_path = args.dest.with_name(args.dest.name + ".meta.json")
    if args.dest.exists() and meta_path.exists() and not args.force:
        print(json.dumps({"skipped": "already downloaded", **json.loads(meta_path.read_text())},
                         indent=2))
        return 0
    print(json.dumps(download_file(BULK_URL, args.dest, get=get), indent=2))
    return 0


def _bulk_sample(args, get):
    config = SampleConfig(start=args.start_date, end=args.end_date, product=args.product,
                          per_month=args.per_month, salt=args.salt)
    source_meta = {"path": str(args.source), "bytes": args.source.stat().st_size,
                   "prefix_only": args.prefix}
    meta_path = args.source.with_name(args.source.name + ".meta.json")
    if meta_path.exists():
        source_meta["download"] = json.loads(meta_path.read_text())
    stream = open_zip_prefix(args.source) if args.prefix else open_bulk_csv(args.source)
    with stream:
        manifest = run_sample(stream, args.output_dir, config, {"source": source_meta})
    print(json.dumps({k: manifest[k] for k in
                      ("sample_total", "population_total", "under_quota_months",
                       "scan", "files", "resources")}, indent=2, default=str))
    return 0


def _api_benchmarks(args, get):
    result = fetch_monthly_benchmarks(args.start_date, args.end_date, args.product, get=get,
                                      request_delay=args.request_delay)
    save_json(result, args.out)
    print(json.dumps({"out": str(args.out),
                      "months": {m["month"]: m["total"] for m in result["months"]}}, indent=2))
    return 0


def _compare_sample(args, get):
    manifest = json.loads((args.sample_dir / "_manifest.json").read_text())
    sample_file = args.sample_dir / manifest["files"]["combined_file"]
    report = compare_sample(sample_file, json.loads(args.benchmarks.read_text()), manifest)
    save_json(report, args.out)
    print(json.dumps({"out": str(args.out), "tests": report["tests"],
                      "flags": len(report["flags"]),
                      "expected_false_flags": report["expected_false_flags"],
                      "overall_flags": report["overall_flags"],
                      "categories_missing_from_population":
                          len(report["categories_missing_from_population"])}, indent=2))
    return 0


BULK_COMMANDS = {"bulk-download": _bulk_download, "bulk-sample": _bulk_sample,
                 "api-benchmarks": _api_benchmarks, "compare-sample": _compare_sample}


if __name__ == "__main__":
    sys.exit(main())
