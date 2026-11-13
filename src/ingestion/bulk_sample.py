"""Reproducible stratified sample from the CFPB bulk complaints CSV.

Method (see docs/data_sampling.md):

* Frame: rows of the bulk CSV whose Product equals the configured product and
  whose Date received falls in ``[start, end)``. Strata are calendar months.
* Selection: every complaint gets ``key = first 8 bytes of SHA-256(salt:id)``.
  In each month the ``per_month`` rows with the smallest keys are kept
  ("bottom-k"), which is a simple random sample without replacement within
  the month, fully determined by (salt, IDs in the source). Row order, block
  size and reruns do not change the result.
* Weights: ``N_h / n_h`` where ``N_h`` is the month's row count in the frame.
* Caveat: if CFPB later adds historical complaints whose keys are smaller
  than a month's current k-th key, they displace selected rows. Previously
  selected complaints are otherwise stable.

Memory: the CSV is streamed in blocks (pyarrow); only the current block, the
selected rows (<= months x per_month) and an 8-byte key per frame row (for
duplicate detection) are held in memory.

Privacy: narrative columns (e.g. "Consumer complaint narrative") are never
stored, not even in ``_raw_json``.
"""

import hashlib
import heapq
import io
import json
import os
import shutil
import struct
import time
import uuid
import zipfile
import zlib
from array import array
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from ingestion.storage import COMPRESSION, SCHEMA

BULK_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
CSV_MEMBER = "complaints.csv"
CREDIT_REPORTING = "Credit reporting or other personal consumer reports"
DEFAULT_SALT = "sentinel-ai-cfpb-sample-v1"

# Bulk CSV header (verified 2026-11-12) -> Bronze field names (API names).
COLUMN_MAP = {
    "Complaint ID": "complaint_id",
    "Date received": "date_received",
    "Date sent to company": "date_sent_to_company",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Company": "company",
    "Company response to consumer": "company_response",
    "Company public response": "company_public_response",
    "Timely response?": "timely",
    "Submitted via": "submitted_via",
    "State": "state",
    "ZIP code": "zip_code",
    "Tags": "tags",
}
# Never stored anywhere (privacy). Not present in the current file, but the
# historical export included it, so it is excluded defensively.
EXCLUDED_COLUMNS = {"Consumer complaint narrative"}

SAMPLE_SCHEMA = pa.schema(list(SCHEMA) + [
    pa.field("_source_system", pa.string()),
    pa.field("_stratum", pa.string()),
    pa.field("_sample_key", pa.string()),
    pa.field("_stratum_population", pa.int64()),
    pa.field("_sample_weight", pa.float64()),
])

DATE_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])"
ID_PATTERN = r"^\d+$"


class SchemaError(Exception):
    """The CSV header does not contain the expected columns."""


class SampleIntegrityError(Exception):
    """Conflicting duplicates or too many malformed rows."""


@dataclass(frozen=True)
class SampleConfig:
    start: date = date(2025, 1, 1)
    end: date = date(2026, 11, 1)  # exclusive
    product: str = CREDIT_REPORTING
    per_month: int = 2273
    salt: str = DEFAULT_SALT
    block_size: int = 8 << 20
    max_malformed_fraction: float = 0.001


@dataclass
class ScanStats:
    rows_scanned: int = 0
    product_rows: int = 0
    frame_rows: int = 0
    malformed_structure: int = 0
    malformed_date: int = 0
    malformed_id: int = 0
    duplicate_ids_in_frame: int = 0
    identical_duplicates_offered: int = 0
    unknown_columns: list = field(default_factory=list)
    excluded_columns_seen: list = field(default_factory=list)
    malformed_examples: list = field(default_factory=list)

    @property
    def malformed_total(self):
        return self.malformed_structure + self.malformed_date + self.malformed_id


@dataclass
class SampleResult:
    config: SampleConfig
    population: dict          # month -> N_h (frame rows)
    selected: dict            # month -> list of (key, raw_row) sorted by key
    stats: ScanStats


def sample_key(complaint_id, salt=DEFAULT_SALT):
    digest = hashlib.sha256(f"{salt}:{complaint_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def month_bounds(month):
    year, mon = int(month[:4]), int(month[5:7])
    start = date(year, mon, 1)
    end = date(year + (mon == 12), mon % 12 + 1, 1)
    return start, end


class BottomKSampler:
    """Keeps the ``k`` smallest keys per stratum, with duplicate checks."""

    def __init__(self, k):
        self.k = k
        self.heaps = defaultdict(list)  # month -> max-heap of (-key, id)
        self.rows = {}                  # id -> (month, key, row)
        self.identical_duplicates = 0

    def wants(self, month, key):
        heap = self.heaps[month]
        return len(heap) < self.k or key <= -heap[0][0]

    def offer(self, month, key, complaint_id, row):
        if complaint_id in self.rows:
            if self.rows[complaint_id][2] == row:
                self.identical_duplicates += 1
                return
            raise SampleIntegrityError(
                f"Complaint ID {complaint_id} appears more than once with different content"
            )
        heap = self.heaps[month]
        if len(heap) < self.k:
            heapq.heappush(heap, (-key, complaint_id))
        elif key < -heap[0][0]:
            _, evicted = heapq.heapreplace(heap, (-key, complaint_id))
            del self.rows[evicted]
        else:
            return
        self.rows[complaint_id] = (month, key, row)

    def result(self):
        selected = defaultdict(list)
        for month, key, row in self.rows.values():
            selected[month].append((key, row))
        return {m: sorted(v, key=lambda item: item[0]) for m, v in sorted(selected.items())}


def _csv_reader(stream, config, stats):
    def on_invalid(row):
        stats.malformed_structure += 1
        if len(stats.malformed_examples) < 5:
            stats.malformed_examples.append(
                {"line": row.number, "expected_columns": row.expected_columns,
                 "actual_columns": row.actual_columns})
        return "skip"

    return pacsv.open_csv(
        stream,
        read_options=pacsv.ReadOptions(block_size=config.block_size),
        parse_options=pacsv.ParseOptions(newlines_in_values=True, invalid_row_handler=on_invalid),
        convert_options=pacsv.ConvertOptions(
            column_types={name: pa.string() for name in list(COLUMN_MAP) + sorted(EXCLUDED_COLUMNS)},
            strings_can_be_null=True,
        ),
    )


def _check_header(names, stats):
    missing = [c for c in COLUMN_MAP if c not in names]
    if missing:
        raise SchemaError(f"Bulk CSV is missing expected columns: {missing}")
    stats.unknown_columns = [c for c in names if c not in COLUMN_MAP and c not in EXCLUDED_COLUMNS]
    stats.excluded_columns_seen = [c for c in names if c in EXCLUDED_COLUMNS]


def _bool_mask(array_like):
    return pc.fill_null(array_like, False)


def sample_stream(stream, config=SampleConfig()):
    """Single pass over a CSV byte stream; returns the stratified sample."""
    stats = ScanStats()
    sampler = BottomKSampler(config.per_month)
    population = Counter()
    frame_keys = array("Q")
    start_iso, end_iso = config.start.isoformat(), config.end.isoformat()
    reader = _csv_reader(stream, config, stats)
    _check_header(reader.schema.names, stats)
    keep_columns = [c for c in reader.schema.names if c not in EXCLUDED_COLUMNS]

    for batch in reader:
        stats.rows_scanned += batch.num_rows
        batch = pa.Table.from_batches([batch]).select(keep_columns)

        product_mask = _bool_mask(pc.equal(batch["Product"], config.product))
        batch = batch.filter(product_mask)
        stats.product_rows += batch.num_rows
        if not batch.num_rows:
            continue

        date_ok = _bool_mask(pc.match_substring_regex(batch["Date received"], DATE_PATTERN))
        stats.malformed_date += batch.num_rows - pc.sum(date_ok).as_py()
        batch = batch.filter(date_ok)

        day = pc.utf8_slice_codeunits(batch["Date received"], 0, 10)
        in_range = pc.and_(pc.greater_equal(day, start_iso), pc.less(day, end_iso))
        batch = batch.filter(in_range)
        if not batch.num_rows:
            continue

        id_ok = _bool_mask(pc.match_substring_regex(batch["Complaint ID"], ID_PATTERN))
        stats.malformed_id += batch.num_rows - pc.sum(id_ok).as_py()
        batch = batch.filter(id_ok)

        ids = batch["Complaint ID"].to_pylist()
        months = pc.utf8_slice_codeunits(batch["Date received"], 0, 7).to_pylist()
        for index, (complaint_id, month) in enumerate(zip(ids, months)):
            population[month] += 1
            key = sample_key(complaint_id, config.salt)
            frame_keys.append(key)
            if sampler.wants(month, key):
                row = batch.slice(index, 1).to_pylist()[0]
                sampler.offer(month, key, complaint_id, row)

    stats.frame_rows = sum(population.values())
    keys = np.frombuffer(frame_keys, dtype=np.uint64) if len(frame_keys) else np.array([], np.uint64)
    stats.duplicate_ids_in_frame = int(keys.size - np.unique(keys).size)
    stats.identical_duplicates_offered = sampler.identical_duplicates

    if stats.rows_scanned and stats.malformed_total / stats.rows_scanned > config.max_malformed_fraction:
        raise SampleIntegrityError(
            f"{stats.malformed_total} malformed rows of {stats.rows_scanned} exceeds "
            f"max_malformed_fraction={config.max_malformed_fraction}"
        )
    return SampleResult(config, dict(sorted(population.items())), sampler.result(), stats)


# --- source streams --------------------------------------------------------

def inspect_zip(path):
    """Verify the bulk zip holds exactly one deflated complaints.csv."""
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        if [i.filename for i in infos] != [CSV_MEMBER]:
            raise SchemaError(f"Unexpected zip members: {[i.filename for i in infos]}")
        info = infos[0]
        if info.compress_type != zipfile.ZIP_DEFLATED:
            raise SchemaError(f"Unexpected compression type {info.compress_type}")
        return {"member": info.filename, "compressed_bytes": info.compress_size,
                "uncompressed_bytes": info.file_size, "compression": "deflate",
                "zip64": info.file_size >= 0xFFFFFFFF or info.compress_size >= 0xFFFFFFFF}


def open_bulk_csv(path):
    """Return a streaming binary file of the CSV inside the bulk zip.

    ``zipfile`` decompresses incrementally and verifies the CRC at EOF.
    """
    inspect_zip(path)
    archive = zipfile.ZipFile(path)
    return archive.open(CSV_MEMBER)


class _DeflatePrefixReader(io.RawIOBase):
    """Decompresses a truncated zip prefix; emits only complete lines."""

    def __init__(self, handle, offset):
        self.handle = handle
        self.handle.seek(offset)
        self.decompressor = zlib.decompressobj(-15)
        self.buffer = b""
        self.tail = b""
        self.done = False

    def readable(self):
        return True

    def readinto(self, target):
        while not self.buffer and not self.done:
            chunk = self.handle.read(1 << 20)
            if chunk:
                data = self.tail + self.decompressor.decompress(chunk)
            else:
                self.done = True
                data = self.tail + self.decompressor.flush()
            cut = data.rfind(b"\n") + 1
            self.buffer, self.tail = data[:cut], data[cut:]
            if self.done:
                self.tail = b""
        size = min(len(target), len(self.buffer))
        target[:size] = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return size


def open_zip_prefix(path):
    """Stream complete CSV lines from the first bytes of the bulk zip.

    Used for small real-data validation before downloading the full file.
    """
    handle = open(path, "rb")
    header = handle.read(30)
    signature, *_rest, name_len, extra_len = struct.unpack("<IHHHHHIIIHH", header)
    if signature != 0x04034B50:
        raise SchemaError("Not a zip local file header")
    name = handle.read(name_len).decode()
    if name != CSV_MEMBER:
        raise SchemaError(f"Unexpected first zip member {name!r}")
    return io.BufferedReader(_DeflatePrefixReader(handle, 30 + name_len + extra_len))


# --- output ----------------------------------------------------------------

def _to_record(row, key, month, population, sample_size, ingested_at):
    record = {COLUMN_MAP[col]: row.get(col) for col in COLUMN_MAP}
    record["_raw_json"] = json.dumps(row, sort_keys=True)
    start, end = month_bounds(month)
    record.update(
        _window_start=start.isoformat(), _window_end=end.isoformat(),
        _ingested_at=ingested_at, _source_system="cfpb_bulk_csv", _stratum=month,
        _sample_key=f"{key:016x}", _stratum_population=population,
        _sample_weight=population / sample_size,
    )
    return record


def sample_tables(result, ingested_at=None):
    """Yield ``(month, pyarrow.Table)`` with Bronze-mapped sample rows."""
    ingested_at = ingested_at or datetime.now(timezone.utc).isoformat()
    for month, items in result.selected.items():
        rows = [_to_record(row, key, month, result.population[month], len(items), ingested_at)
                for key, row in items]
        yield month, pa.Table.from_pylist(rows, schema=SAMPLE_SCHEMA)


def write_sample(result, output_dir, manifest_extra=None):
    """Write per-month partitions, a combined upload file and a manifest.

    Everything is written to a hidden temp directory and swapped into place
    at the end, so an interrupted run never leaves a partial sample.
    """
    output_dir = Path(output_dir)
    parent = output_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    for stale in parent.glob(f".{output_dir.name}.tmp-*"):
        shutil.rmtree(stale)

    tmp = parent / f".{output_dir.name}.tmp-{uuid.uuid4().hex[:8]}"
    try:
        tables = []
        for month, table in sample_tables(result):
            part = tmp / f"stratum={month}" / f"part-{month}.parquet"
            part.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, part, compression=COMPRESSION)
            tables.append(table)

        combined = tmp / "_upload" / f"{output_dir.name}.parquet"
        combined.parent.mkdir(parents=True)
        combined_table = pa.concat_tables(tables) if tables else SAMPLE_SCHEMA.empty_table()
        pq.write_table(combined_table, combined, compression=COMPRESSION)

        manifest = build_manifest(result, manifest_extra)
        manifest["files"] = {
            "partitions_bytes": sum(p.stat().st_size for p in tmp.glob("stratum=*/*.parquet")),
            "combined_file": str(Path("_upload") / combined.name),
            "combined_bytes": combined.stat().st_size,
        }
        (tmp / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=str))

        backup = None
        if output_dir.exists():
            backup = parent / f".{output_dir.name}.old-{uuid.uuid4().hex[:8]}"
            os.rename(output_dir, backup)
        os.rename(tmp, output_dir)
        if backup:
            shutil.rmtree(backup)
        return manifest
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise


def build_manifest(result, extra=None):
    config = asdict(result.config)
    config.update(start=result.config.start.isoformat(), end=result.config.end.isoformat())
    months = {
        month: {"population": result.population.get(month, 0),
                "sample": len(result.selected.get(month, [])),
                "weight": (result.population[month] / len(result.selected[month])
                           if result.selected.get(month) else None)}
        for month in result.population
    }
    stats = asdict(result.stats)
    stats["malformed_total"] = result.stats.malformed_total
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "method": "stratified (calendar month) bottom-k SHA-256 sample; "
                  "simple random sample without replacement within each month",
        "config": config,
        "sample_total": sum(m["sample"] for m in months.values()),
        "population_total": sum(m["population"] for m in months.values()),
        "under_quota_months": [m for m, v in months.items() if v["sample"] < result.config.per_month],
        "months": months,
        "scan": stats,
        "notes": [
            "Use API monthly totals (not sample counts) for complaint-volume trends; "
            "the sample allocates an equal number of records to every month.",
            "Weights = month population in the bulk file / month sample size.",
            "If CFPB adds historical complaints whose keys rank below a month's current "
            "k-th key, they will displace selected rows on a rerun; selection is otherwise stable.",
            "Narrative columns are excluded and never stored.",
        ],
    }
    manifest.update(extra or {})
    return manifest


def peak_rss_bytes():
    import resource
    import sys
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def run_sample(stream, output_dir, config=SampleConfig(), manifest_extra=None):
    started = time.monotonic()
    result = sample_stream(stream, config)
    scan_seconds = round(time.monotonic() - started, 2)
    extra = {"resources": {"scan_seconds": scan_seconds}}
    extra.update(manifest_extra or {})
    manifest = write_sample(result, output_dir, extra)
    manifest["resources"].update(total_seconds=round(time.monotonic() - started, 2),
                                 peak_rss_bytes=peak_rss_bytes())
    (Path(output_dir) / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    return manifest
