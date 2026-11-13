"""Windowed backfill and incremental CFPB ingestion into Bronze Parquet."""

import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import requests

from ingestion.checkpoint import Checkpoint
from ingestion.client import (
    MAX_FRM,
    MAX_RETRIES,
    MAX_RETRY_AFTER,
    iter_pages,
    total_hits,
    validate_new_ids,
)
from ingestion.logging_utils import log_event
from ingestion.storage import (
    SOURCE_FIELDS,
    WindowWriter,
    check_partition_grid,
    remove_stale_temp_files,
    validate_dataset,
)

logger = logging.getLogger(__name__)

# Windows are aligned to this fixed epoch so that backfill and incremental
# runs always produce the same window boundaries for a given window_days.
WINDOW_EPOCH = date(1970, 1, 1)
CHECKPOINT_NAME = "_checkpoint.json"


class CountMismatchError(Exception):
    """Records written for a window differ from the API's reported total."""


class WindowTooLargeError(Exception):
    """A window holds more records than the API can paginate."""


class DuplicateRecordsError(Exception):
    """The output dataset contains duplicate complaint IDs."""


@dataclass(frozen=True)
class Window:
    start: date  # inclusive
    end: date    # exclusive

    @property
    def key(self):
        return f"{self.start.isoformat()}_{self.end.isoformat()}"

    def api_filters(self):
        # The API treats both bounds as inclusive days, so the exclusive end
        # becomes date_received_max = end - 1 day.
        return {
            "date_received_min": self.start.isoformat(),
            "date_received_max": (self.end - timedelta(days=1)).isoformat(),
        }


@dataclass
class IngestConfig:
    output_dir: Path
    batch_size: int = 100
    window_days: int = 1
    request_delay: float = 1.0
    max_retries: int = MAX_RETRIES
    max_retry_after: float = MAX_RETRY_AFTER


def build_windows(start, end, window_days):
    """Split ``[start, end)`` into epoch-aligned windows of ``window_days``.

    ``start`` is snapped down and ``end`` snapped up to the window grid.
    """
    if window_days < 1:
        raise ValueError("window_days must be >= 1")
    if start >= end:
        raise ValueError(f"start ({start}) must be before end ({end})")

    offset = (start - WINDOW_EPOCH).days % window_days
    current = start - timedelta(days=offset)
    windows = []
    while current < end:
        windows.append(Window(current, current + timedelta(days=window_days)))
        current += timedelta(days=window_days)
    return windows


def ingest_window(window, config, checkpoint, get=requests.get, sleep=time.sleep):
    """Fetch one window, stream it to Parquet, validate, then checkpoint it."""
    started = time.monotonic()
    output_dir = Path(config.output_dir)
    check_partition_grid(output_dir, window)

    writer = WindowWriter(output_dir, window)
    seen_ids = set()
    expected_total = None
    max_date = None
    out_of_window = 0
    unknown_fields = set()
    start_iso, end_iso = window.start.isoformat(), window.end.isoformat()

    try:
        pages = iter_pages(config.batch_size, window.api_filters(), get=get, sleep=sleep,
                           request_delay=config.request_delay,
                           max_retries=config.max_retries,
                           max_retry_after=config.max_retry_after)
        for page, payload, hits in pages:
            if expected_total is None:
                expected_total = total_hits(payload)
                log_event(logger, logging.INFO, "window_started", window=window.key,
                          expected_total=expected_total)
                if expected_total is not None and expected_total > MAX_FRM:
                    raise WindowTooLargeError(
                        f"Window {window.key} has {expected_total} records (> {MAX_FRM}); "
                        "use a smaller --window-days"
                    )
            if not hits:
                break

            page_ids = validate_new_ids(hits, seen_ids, page)
            sources = [hit["_source"] for hit in hits]
            for source in sources:
                received = (source.get("date_received") or "")[:10]
                if not start_iso <= received < end_iso:
                    out_of_window += 1
                if received and (max_date is None or received > max_date):
                    max_date = received
                unknown_fields.update(set(source) - set(SOURCE_FIELDS))

            writer.write(sources)
            seen_ids.update(page_ids)
            log_event(logger, logging.INFO, "page_ingested", window=window.key, page=page,
                      records=len(hits), window_records=len(seen_ids),
                      expected_total=expected_total)

        if expected_total is None:
            log_event(logger, logging.WARNING, "total_unavailable", window=window.key)
        elif len(seen_ids) != expected_total:
            raise CountMismatchError(
                f"Window {window.key}: wrote {len(seen_ids)} records, "
                f"API reported {expected_total}"
            )

        path = writer.commit()
    except Exception as error:
        writer.abort()
        log_event(logger, logging.ERROR, "window_failed", window=window.key,
                  records_fetched=len(seen_ids), error_type=type(error).__name__,
                  error=str(error)[:300])
        raise

    if out_of_window:
        log_event(logger, logging.WARNING, "records_outside_window", window=window.key,
                  count=out_of_window)
    if unknown_fields:
        log_event(logger, logging.WARNING, "unknown_fields", window=window.key,
                  fields=sorted(unknown_fields))

    duration_s = round(time.monotonic() - started, 2)
    checkpoint.mark_complete(window.key, len(seen_ids),
                             str(path.relative_to(output_dir)), max_date, duration_s)
    log_event(logger, logging.INFO, "window_complete", window=window.key,
              records=len(seen_ids), duration_s=duration_s, file=str(path))
    return {"window": window.key, "records": len(seen_ids), "duration_s": duration_s}


def run_windows(windows, config, skip_completed, get=requests.get, sleep=time.sleep):
    """Ingest windows in order; stops at the first failure (resume continues)."""
    started = time.monotonic()
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    remove_stale_temp_files(output_dir)
    checkpoint = Checkpoint(output_dir / CHECKPOINT_NAME)

    summary = {"windows_total": len(windows), "windows_ingested": 0,
               "windows_skipped": 0, "records_ingested": 0}
    log_event(logger, logging.INFO, "run_started", windows=len(windows),
              first=windows[0].key, last=windows[-1].key,
              batch_size=config.batch_size, window_days=config.window_days,
              skip_completed=skip_completed, output_dir=str(output_dir))

    for window in windows:
        if skip_completed and checkpoint.is_complete(window.key):
            summary["windows_skipped"] += 1
            log_event(logger, logging.INFO, "window_skipped", window=window.key,
                      reason="completed in checkpoint")
            continue
        result = ingest_window(window, config, checkpoint, get=get, sleep=sleep)
        summary["windows_ingested"] += 1
        summary["records_ingested"] += result["records"]

    validation = validate_dataset(output_dir)
    summary.update(validation=validation, watermark=checkpoint.watermark,
                   duration_s=round(time.monotonic() - started, 2))
    log_event(logger, logging.INFO, "run_complete", **summary)

    if validation["duplicate_ids"] or validation["null_ids"]:
        raise DuplicateRecordsError(
            f"Dataset has {validation['duplicate_ids']} duplicate and "
            f"{validation['null_ids']} null complaint IDs, e.g. "
            f"{validation['duplicate_examples']}"
        )
    return summary


def run_backfill(start, end, config, resume=False, get=requests.get, sleep=time.sleep):
    """Ingest ``[start, end)``. With ``resume``, skip checkpointed windows."""
    windows = build_windows(start, end, config.window_days)
    return run_windows(windows, config, skip_completed=resume, get=get, sleep=sleep)


def incremental_range(watermark, overlap_days, end=None, today=None):
    """Return ``(start, end)`` for an incremental run.

    Starts ``overlap_days`` before the watermark day so late-arriving
    complaints are picked up; those windows are re-fetched and replaced.
    """
    if overlap_days < 0:
        raise ValueError("overlap_days must be >= 0")
    start = date.fromisoformat(watermark) - timedelta(days=overlap_days)
    end = end or (today or date.today()) + timedelta(days=1)
    return start, end


def run_incremental(config, overlap_days=3, end=None, start_if_empty=None,
                    today=None, get=requests.get, sleep=time.sleep):
    checkpoint = Checkpoint(Path(config.output_dir) / CHECKPOINT_NAME)
    if checkpoint.watermark is None:
        if start_if_empty is None:
            raise ValueError("No watermark in checkpoint; run a backfill or pass --start-date")
        start = start_if_empty
        end = end or (today or date.today()) + timedelta(days=1)
    else:
        start, end = incremental_range(checkpoint.watermark, overlap_days, end, today)
    log_event(logger, logging.INFO, "incremental_range", watermark=checkpoint.watermark,
              overlap_days=overlap_days, start=start.isoformat(), end=end.isoformat())
    windows = build_windows(start, end, config.window_days)
    return run_windows(windows, config, skip_completed=False, get=get, sleep=sleep)


def plan_windows(start, end, config, get=requests.get, sleep=time.sleep):
    """Dry run: report each window's record count with one size=1 request."""
    plan = []
    for index, window in enumerate(build_windows(start, end, config.window_days)):
        if index and config.request_delay:
            sleep(config.request_delay)
        _page, payload, _hits = next(iter_pages(1, window.api_filters(), max_pages=1,
                                                get=get, sleep=sleep,
                                                max_retries=config.max_retries,
                                                max_retry_after=config.max_retry_after))
        total = total_hits(payload)
        requests_needed = -(-total // config.batch_size) if total else 1
        plan.append({"window": window.key, "records": total,
                     "requests_needed": requests_needed,
                     "too_large": bool(total and total > MAX_FRM)})
        log_event(logger, logging.INFO, "window_planned", **plan[-1])
    return plan
