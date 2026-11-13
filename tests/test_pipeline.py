"""Offline tests for windowed backfill, incremental runs and storage."""

import json
from datetime import date

import pyarrow.parquet as pq
import pytest
import requests

from fakes import FakeCFPB, FakeResponse, make_record
from ingestion import cli
from ingestion.checkpoint import Checkpoint, CheckpointError
from ingestion.client import PermanentAPIError, fetch_page
from ingestion.pipeline import (
    CountMismatchError,
    DuplicateRecordsError,
    IngestConfig,
    Window,
    build_windows,
    incremental_range,
    plan_windows,
    run_backfill,
    run_incremental,
)
from ingestion.storage import (
    SCHEMA,
    PartitionConflictError,
    WindowWriter,
    list_partitions,
    validate_dataset,
)

NO_SLEEP = lambda seconds: None  # noqa: E731


def three_days():
    # Day 1: 7 records, day 2: 3 (two share a timestamp), day 3: 0, day 4: 5
    records = [make_record(100 + i, "2024-01-01", f"10:{i:02d}:00") for i in range(7)]
    records += [make_record(200, "2024-01-02", "09:00:00"),
                make_record(201, "2024-01-02", "09:00:00"),
                make_record(202, "2024-01-02", "23:59:59")]
    records += [make_record(400 + i, "2024-01-04", f"08:{i:02d}:00") for i in range(5)]
    return records


def config(tmp_path, **overrides):
    values = {"output_dir": tmp_path / "bronze", "batch_size": 3, "request_delay": 0}
    values.update(overrides)
    return IngestConfig(**values)


def read_ids(output_dir):
    ids = []
    for path, _s, _e in list_partitions(output_dir):
        ids += pq.read_table(path, columns=["complaint_id"]).column("complaint_id").to_pylist()
    return ids


# --- windows ---------------------------------------------------------------

def test_build_windows_daily():
    windows = build_windows(date(2024, 1, 1), date(2024, 1, 4), 1)
    assert [w.key for w in windows] == [
        "2024-01-01_2024-01-02", "2024-01-02_2024-01-03", "2024-01-03_2024-01-04"]


def test_build_windows_are_epoch_aligned_for_multi_day():
    a = build_windows(date(2024, 1, 1), date(2024, 1, 20), 7)
    b = build_windows(date(2024, 1, 10), date(2024, 1, 20), 7)
    assert {w.key for w in b} <= {w.key for w in a}
    assert a[0].start <= date(2024, 1, 1) and a[-1].end >= date(2024, 1, 20)


def test_build_windows_rejects_bad_range():
    with pytest.raises(ValueError):
        build_windows(date(2024, 1, 2), date(2024, 1, 2), 1)


def test_window_api_filters_use_inclusive_max():
    window = Window(date(2024, 1, 1), date(2024, 1, 2))
    assert window.api_filters() == {"date_received_min": "2024-01-01",
                                    "date_received_max": "2024-01-01"}


# --- backfill --------------------------------------------------------------

def test_backfill_writes_one_partition_per_window(tmp_path):
    api = FakeCFPB(three_days())
    cfg = config(tmp_path)
    summary = run_backfill(date(2024, 1, 1), date(2024, 1, 5), cfg, get=api, sleep=NO_SLEEP)

    assert summary["windows_ingested"] == 4
    assert summary["records_ingested"] == 15
    assert summary["validation"]["duplicate_ids"] == 0
    assert summary["watermark"] == "2024-01-04"

    parts = {start: path for path, start, _end in list_partitions(cfg.output_dir)}
    assert sorted(parts) == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
    assert pq.read_metadata(parts["2024-01-03"]).num_rows == 0  # empty window
    assert sorted(read_ids(cfg.output_dir)) == sorted(r["complaint_id"] for r in three_days())

    day1 = pq.ParquetFile(parts["2024-01-01"])
    assert day1.schema_arrow.equals(SCHEMA)
    assert day1.metadata.num_row_groups == 3  # 7 records / page size 3: streamed per page
    assert day1.metadata.row_group(0).column(0).compression == "ZSTD"

    row = pq.read_table(parts["2024-01-01"]).to_pylist()[0]
    assert json.loads(row["_raw_json"])["complaint_id"] == row["complaint_id"]
    assert row["_window_start"] == "2024-01-01" and row["_window_end"] == "2024-01-02"


def test_identical_timestamps_paginate_without_loss(tmp_path):
    same_ts = [make_record(500 + i, "2024-02-01", "12:00:00") for i in range(10)]
    api = FakeCFPB(same_ts)
    summary = run_backfill(date(2024, 2, 1), date(2024, 2, 2), config(tmp_path),
                           get=api, sleep=NO_SLEEP)
    assert summary["records_ingested"] == 10


def test_unknown_fields_are_preserved_in_raw_json(tmp_path):
    api = FakeCFPB([make_record(1, "2024-01-01", has_narrative=True)])
    cfg = config(tmp_path)
    run_backfill(date(2024, 1, 1), date(2024, 1, 2), cfg, get=api, sleep=NO_SLEEP)
    path = next(list_partitions(cfg.output_dir))[0]
    row = pq.read_table(path).to_pylist()[0]
    assert json.loads(row["_raw_json"])["has_narrative"] is True


# --- checkpoint / resume ---------------------------------------------------

def test_failed_window_is_not_checkpointed_and_resume_continues(tmp_path):
    def fail_day_two(params, _n):
        if params["date_received_min"] == "2024-01-02":
            return FakeResponse(status=400)

    cfg = config(tmp_path)
    with pytest.raises(PermanentAPIError):
        run_backfill(date(2024, 1, 1), date(2024, 1, 5), cfg,
                     get=FakeCFPB(three_days(), fail_day_two), sleep=NO_SLEEP)

    checkpoint = Checkpoint(cfg.output_dir / "_checkpoint.json")
    assert list(checkpoint.data["windows"]) == ["2024-01-01_2024-01-02"]
    assert not list(cfg.output_dir.glob("**/.tmp-*"))
    assert [s for _p, s, _e in list_partitions(cfg.output_dir)] == ["2024-01-01"]

    api = FakeCFPB(three_days())
    summary = run_backfill(date(2024, 1, 1), date(2024, 1, 5), cfg, resume=True,
                           get=api, sleep=NO_SLEEP)
    assert summary["windows_skipped"] == 1
    assert summary["windows_ingested"] == 3
    assert [m for m, _ in api.window_requests()] == ["2024-01-02", "2024-01-03", "2024-01-04"]
    assert summary["validation"]["records"] == 15


def test_rerun_without_resume_replaces_partitions_without_duplicates(tmp_path):
    cfg = config(tmp_path)
    for _ in range(2):
        summary = run_backfill(date(2024, 1, 1), date(2024, 1, 5), cfg,
                               get=FakeCFPB(three_days()), sleep=NO_SLEEP)
    assert summary["validation"] == {**summary["validation"], "records": 15,
                                     "unique_ids": 15, "duplicate_ids": 0}


def test_corrupt_checkpoint_raises(tmp_path):
    path = tmp_path / "_checkpoint.json"
    path.write_text("{not json")
    with pytest.raises(CheckpointError):
        Checkpoint(path)


def test_checkpoint_roundtrip(tmp_path):
    checkpoint = Checkpoint(tmp_path / "_checkpoint.json")
    checkpoint.mark_complete("2024-01-01_2024-01-02", 7, "f.parquet", "2024-01-01", 1.5)
    checkpoint.mark_complete("2023-12-31_2024-01-01", 2, "g.parquet", "2023-12-31", 1.0)
    reloaded = Checkpoint(tmp_path / "_checkpoint.json")
    assert reloaded.is_complete("2024-01-01_2024-01-02")
    assert reloaded.watermark == "2024-01-01"  # never moves backwards


# --- incremental -----------------------------------------------------------

def test_incremental_range_applies_overlap():
    assert incremental_range("2024-01-10", 3, today=date(2024, 1, 12)) == (
        date(2024, 1, 7), date(2024, 1, 13))


def test_incremental_picks_up_late_and_new_records(tmp_path):
    records = three_days()
    cfg = config(tmp_path)
    run_backfill(date(2024, 1, 1), date(2024, 1, 5), cfg, get=FakeCFPB(records), sleep=NO_SLEEP)

    records += [make_record(299, "2024-01-02", "18:00:00"),   # late arrival (overlap)
                make_record(500, "2024-01-05", "07:00:00")]   # new day
    api = FakeCFPB(records)
    summary = run_incremental(cfg, overlap_days=2, today=date(2024, 1, 5), get=api, sleep=NO_SLEEP)

    assert [m for m, _ in api.window_requests()] == [
        "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    assert summary["watermark"] == "2024-01-05"
    ids = read_ids(cfg.output_dir)
    assert len(ids) == len(set(ids)) == 17
    assert {"299", "500"} <= set(ids)


def test_incremental_without_watermark_requires_start(tmp_path):
    with pytest.raises(ValueError, match="No watermark"):
        run_incremental(config(tmp_path), get=FakeCFPB([]), sleep=NO_SLEEP)


# --- validation / duplicates ----------------------------------------------

def test_count_mismatch_fails_window(tmp_path):
    cfg = config(tmp_path)
    with pytest.raises(CountMismatchError):
        run_backfill(date(2024, 1, 1), date(2024, 1, 2), cfg,
                     get=FakeCFPB(three_days(), total_offset=1), sleep=NO_SLEEP)
    assert not list(list_partitions(cfg.output_dir))
    assert not Checkpoint(cfg.output_dir / "_checkpoint.json").data["windows"]


def test_duplicates_across_partitions_are_reported(tmp_path):
    output = tmp_path / "bronze"
    for window in (Window(date(2024, 1, 1), date(2024, 1, 2)),
                   Window(date(2024, 1, 2), date(2024, 1, 3))):
        writer = WindowWriter(output, window)
        writer.write([make_record(1, "2024-01-01"), make_record(window.start.day + 10, "2024-01-01")])
        writer.commit()

    result = validate_dataset(output)
    assert result["records"] == 4 and result["unique_ids"] == 3
    assert result["duplicate_ids"] == 1 and result["duplicate_examples"] == ["1"]

    with pytest.raises(DuplicateRecordsError):
        run_backfill(date(2024, 1, 3), date(2024, 1, 4), config(tmp_path),
                     get=FakeCFPB([]), sleep=NO_SLEEP)


def test_partition_grid_mismatch_is_rejected(tmp_path):
    cfg = config(tmp_path, window_days=2)
    run_backfill(date(2024, 1, 1), date(2024, 1, 3), cfg, get=FakeCFPB(three_days()), sleep=NO_SLEEP)
    with pytest.raises(PartitionConflictError):
        run_backfill(date(2024, 1, 1), date(2024, 1, 3), config(tmp_path, window_days=1),
                     get=FakeCFPB(three_days()), sleep=NO_SLEEP)


# --- retries ---------------------------------------------------------------

def test_http_400_is_not_retried():
    api = FakeCFPB([], lambda p, n: FakeResponse(status=400))
    with pytest.raises(PermanentAPIError) as excinfo:
        fetch_page(0, 5, get=api, sleep=NO_SLEEP)
    assert excinfo.value.status_code == 400
    assert len(api.calls) == 1


@pytest.mark.parametrize("failure", [
    FakeResponse(status=503),
    FakeResponse(status=429),
    requests.Timeout("slow"),
    requests.ConnectionError("reset"),
    FakeResponse(ValueError("bad json")),
])
def test_transient_failures_retry_with_exponential_backoff(failure):
    sleeps = []
    api = FakeCFPB([make_record(1, "2024-01-01")], lambda p, n: failure if n < 3 else None)
    payload = fetch_page(0, 5, get=api, sleep=sleeps.append, max_retries=3)
    assert len(payload["hits"]["hits"]) == 1
    assert sleeps == [2.0, 4.0]


def test_transient_failure_gives_up_after_max_retries():
    sleeps = []
    api = FakeCFPB([], lambda p, n: FakeResponse(status=500))
    with pytest.raises(requests.HTTPError):
        fetch_page(0, 5, get=api, sleep=sleeps.append, max_retries=3)
    assert len(api.calls) == 3 and sleeps == [2.0, 4.0]


# --- dry run, logging and CLI ---------------------------------------------

def test_plan_windows_reports_counts(tmp_path):
    api = FakeCFPB(three_days())
    plan = plan_windows(date(2024, 1, 1), date(2024, 1, 3), config(tmp_path),
                        get=api, sleep=NO_SLEEP)
    assert [(p["records"], p["requests_needed"]) for p in plan] == [(7, 3), (3, 1)]
    assert all(c["size"] == 1 for c in api.calls)


def test_cli_backfill_resume_and_structured_logs(tmp_path, capsys):
    output = tmp_path / "out"
    argv = ["backfill", "--start-date", "2024-01-01", "--end-date", "2024-01-05",
            "--batch-size", "3", "--request-delay", "0", "--output-dir", str(output)]

    assert cli.main(argv, get=FakeCFPB(three_days())) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["records_ingested"] == 15

    api = FakeCFPB(three_days())
    assert cli.main(argv + ["--resume"], get=api) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["windows_skipped"] == 4 and not api.calls

    log_lines = [json.loads(line) for line in open(first["log_file"])]
    events = {line["event"] for line in log_lines}
    assert {"run_started", "window_started", "api_request", "page_ingested",
            "window_complete", "run_complete"} <= events
    request = next(l for l in log_lines if l["event"] == "api_request")
    assert {"frm", "size", "attempt", "duration_ms", "date_received_min"} <= set(request)
    complete = next(l for l in log_lines if l["event"] == "window_complete")
    assert {"window", "records", "duration_s"} <= set(complete)

    assert cli.main(["validate", "--output-dir", str(output)]) == 0


def test_cli_rejects_invalid_batch_size():
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            ["backfill", "--start-date", "2024-01-01", "--batch-size", "101"])
