"""Bronze Parquet storage: one zstd-compressed file per ingestion window.

Layout (Hive-style, readable by Spark/Databricks)::

    <output_dir>/window_start=YYYY-MM-DD/part-<start>_<end>.parquet
    <output_dir>/_checkpoint.json        (ignored by Spark: leading "_")
    <output_dir>/_logs/*.jsonl

Pages are streamed into the file one row group at a time, so memory use is
bounded by the page size, not the dataset size. Files are written to a
hidden temp name and atomically renamed only when the window is complete.
"""

import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Fields observed in the live API's _source (2026-11-12). Unknown fields are
# not dropped: the full record is always kept in _raw_json.
SOURCE_FIELDS = [
    "complaint_id", "date_received", "date_sent_to_company", "product",
    "sub_product", "issue", "sub_issue", "company", "company_response",
    "company_public_response", "timely", "submitted_via", "state",
    "zip_code", "tags",
]
METADATA_FIELDS = ["_raw_json", "_window_start", "_window_end", "_ingested_at"]
SCHEMA = pa.schema([(name, pa.string()) for name in SOURCE_FIELDS + METADATA_FIELDS])
COMPRESSION = "zstd"
PART_PATTERN = re.compile(r"part-(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.parquet$")


class PartitionConflictError(Exception):
    """Raised when an existing partition overlaps a window on a different grid."""


def _as_string(value):
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


def to_rows(sources, window, ingested_at):
    rows = []
    for source in sources:
        row = {name: _as_string(source.get(name)) for name in SOURCE_FIELDS}
        row["_raw_json"] = json.dumps(source, sort_keys=True)
        row["_window_start"] = window.start.isoformat()
        row["_window_end"] = window.end.isoformat()
        row["_ingested_at"] = ingested_at
        rows.append(row)
    return rows


def partition_path(output_dir, window):
    return (Path(output_dir) / f"window_start={window.start.isoformat()}"
            / f"part-{window.start.isoformat()}_{window.end.isoformat()}.parquet")


def list_partitions(output_dir):
    """Yield ``(path, start_iso, end_iso)`` for every committed partition file."""
    for path in sorted(Path(output_dir).glob("window_start=*/part-*.parquet")):
        match = PART_PATTERN.search(path.name)
        if match:
            yield path, match.group(1), match.group(2)


def check_partition_grid(output_dir, window):
    """Refuse to write a window that partially overlaps an existing partition.

    An exact match is fine (it is replaced atomically). A partial overlap
    means a different ``window_days`` grid was used, which would create
    duplicate or orphaned records, so it is an explicit error.
    """
    start, end = window.start.isoformat(), window.end.isoformat()
    for path, other_start, other_end in list_partitions(output_dir):
        overlaps = other_start < end and start < other_end
        if overlaps and (other_start, other_end) != (start, end):
            raise PartitionConflictError(
                f"Window {start}..{end} overlaps existing partition {path}; "
                "use the same --window-days as the existing data or a new output dir"
            )


def remove_stale_temp_files(output_dir):
    for tmp in Path(output_dir).glob("window_start=*/.tmp-*"):
        tmp.unlink()


class WindowWriter:
    """Streams pages into a temp Parquet file; commit() publishes it atomically."""

    def __init__(self, output_dir, window):
        self.window = window
        self.final_path = partition_path(output_dir, window)
        self.tmp_path = self.final_path.with_name(".tmp-" + self.final_path.name)
        self.ingested_at = datetime.now(timezone.utc).isoformat()
        self.rows_written = 0
        self._writer = None

    def write(self, sources):
        if not sources:
            return
        if self._writer is None:
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self.tmp_path, SCHEMA, compression=COMPRESSION)
        table = pa.Table.from_pylist(to_rows(sources, self.window, self.ingested_at), schema=SCHEMA)
        self._writer.write_table(table)
        self.rows_written += len(sources)

    def commit(self):
        if self._writer is None:  # empty window: still publish an empty file
            self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(SCHEMA.empty_table(), self.tmp_path, compression=COMPRESSION)
        else:
            self._writer.close()
            self._writer = None
        os.replace(self.tmp_path, self.final_path)
        return self.final_path

    def abort(self):
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        if self.tmp_path.exists():
            self.tmp_path.unlink()


def validate_dataset(output_dir):
    """Check complaint_id uniqueness across all partitions.

    Reads only the complaint_id column, so memory use stays small.
    """
    counts = Counter()
    files = 0
    for path, _start, _end in list_partitions(output_dir):
        files += 1
        column = pq.read_table(path, columns=["complaint_id"]).column("complaint_id")
        counts.update(column.to_pylist())

    duplicates = {cid: n for cid, n in counts.items() if n > 1}
    return {
        "files": files,
        "records": sum(counts.values()),
        "unique_ids": len(counts),
        "null_ids": counts.get(None, 0),
        "duplicate_ids": len(duplicates),
        "duplicate_examples": sorted(duplicates, key=str)[:20],
    }
