"""Offline tests for the bulk CSV sampler, using synthetic data only."""

import csv
import io
import json
import random
import zipfile
from datetime import date

import pyarrow.parquet as pq
import pytest

from ingestion import bulk_sample as bs
from ingestion.bulk_sample import (
    COLUMN_MAP,
    CREDIT_REPORTING,
    SAMPLE_SCHEMA,
    SampleConfig,
    SampleIntegrityError,
    SchemaError,
    inspect_zip,
    open_bulk_csv,
    open_zip_prefix,
    run_sample,
    sample_key,
    sample_stream,
)

HEADER = list(COLUMN_MAP)  # same order as the real file
CONFIG = SampleConfig(start=date(2025, 1, 1), end=date(2025, 4, 1), per_month=10,
                      block_size=4096)
SIZES = {"2025-01": 50, "2025-02": 30, "2025-03": 6}


def row(cid, day, product=CREDIT_REPORTING, **overrides):
    values = {c: "" for c in HEADER}
    values.update({"Complaint ID": str(cid), "Date received": day, "Product": product,
                   "Issue": "Incorrect information on your report", "Company": "ACME",
                   "Timely response?": "Yes", "Submitted via": "Web", "State": "CA",
                   "Company response to consumer": "Closed with explanation"})
    values.update(overrides)
    return values


def synthetic_rows():
    rows, cid = [], 1000
    for month, n in SIZES.items():
        for i in range(n):
            rows.append(row(cid, f"{month}-{i % 28 + 1:02d}"))
            cid += 1
    rows += [row(1, "2024-12-31"), row(2, "2025-04-01"),           # out of range
             row(3, "2025-01-15", product="Debt collection")]       # other product
    return rows


def to_csv(rows, header=HEADER):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=header, quoting=csv.QUOTE_MINIMAL,
                            lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: r.get(k, "") for k in header})
    return buffer.getvalue().encode()


def stream(rows, header=HEADER):
    return io.BytesIO(to_csv(rows, header))


def selected_ids(result):
    return {m: [r["Complaint ID"] for _k, r in items] for m, items in result.selected.items()}


# --- quotas and sampling correctness --------------------------------------

def test_monthly_quotas_and_frame_counts():
    result = sample_stream(stream(synthetic_rows()), CONFIG)
    assert result.population == SIZES
    assert {m: len(v) for m, v in result.selected.items()} == {
        "2025-01": 10, "2025-02": 10, "2025-03": 6}
    assert result.stats.rows_scanned == sum(SIZES.values()) + 3
    assert result.stats.product_rows == sum(SIZES.values()) + 2


def test_selection_is_exactly_the_k_smallest_hashes_per_month():
    rows = synthetic_rows()
    result = sample_stream(stream(rows), CONFIG)
    for month in SIZES:
        ids = [r["Complaint ID"] for r in rows
               if r["Date received"].startswith(month) and r["Product"] == CREDIT_REPORTING]
        expected = sorted(ids, key=lambda i: sample_key(i, CONFIG.salt))[:CONFIG.per_month]
        assert selected_ids(result)[month] == expected


def test_reproducible_across_row_order_and_block_size():
    rows = synthetic_rows()
    baseline = selected_ids(sample_stream(stream(rows), CONFIG))
    shuffled = rows[:]
    random.Random(7).shuffle(shuffled)
    small_blocks = SampleConfig(**{**CONFIG.__dict__, "block_size": 1024})
    assert selected_ids(sample_stream(stream(shuffled), small_blocks)) == baseline
    assert selected_ids(sample_stream(stream(rows), CONFIG)) == baseline


def test_different_salt_changes_sample():
    other = SampleConfig(**{**CONFIG.__dict__, "salt": "another-salt"})
    a = selected_ids(sample_stream(stream(synthetic_rows()), CONFIG))
    b = selected_ids(sample_stream(stream(synthetic_rows()), other))
    assert a["2025-01"] != b["2025-01"]
    assert sorted(a["2025-03"]) == sorted(b["2025-03"])  # under quota: all rows kept


def test_sample_key_is_stable_sha256_prefix():
    import hashlib
    digest = hashlib.sha256(b"salt:12345").digest()
    assert sample_key("12345", "salt") == int.from_bytes(digest[:8], "big")


# --- schema mapping and privacy -------------------------------------------

def test_output_schema_mapping_and_weights(tmp_path):
    rows = synthetic_rows()
    rows[-4]["Complaint ID"] = "0001234"  # in 2025-03 (under quota, always selected)
    manifest = run_sample(stream(rows), tmp_path / "sample", CONFIG)

    combined = tmp_path / "sample" / manifest["files"]["combined_file"]
    table = pq.read_table(combined)
    assert table.schema.equals(SAMPLE_SCHEMA)
    assert table.num_rows == manifest["sample_total"] == 26

    records = table.to_pylist()
    ids = [r["complaint_id"] for r in records]
    assert len(ids) == len(set(ids))
    first = next(r for r in records if r["_stratum"] == "2025-01")
    raw = json.loads(first["_raw_json"])
    assert raw["Complaint ID"] == first["complaint_id"]
    assert first["company"] == raw["Company"] == "ACME"
    assert first["timely"] == "Yes" and first["company_response"] == "Closed with explanation"
    assert first["sub_issue"] is None  # empty CSV field -> null
    assert first["_sample_weight"] == pytest.approx(50 / 10)
    assert first["_stratum_population"] == 50
    assert first["_window_start"] == "2025-01-01" and first["_window_end"] == "2025-02-01"
    assert first["_source_system"] == "cfpb_bulk_csv"
    assert len(first["_sample_key"]) == 16

    parts = sorted(p.parent.name for p in (tmp_path / "sample").glob("stratum=*/*.parquet"))
    assert parts == ["stratum=2025-01", "stratum=2025-02", "stratum=2025-03"]
    assert manifest["months"]["2025-03"] == {"population": 6, "sample": 6, "weight": 1.0}
    assert manifest["under_quota_months"] == ["2025-03"]

    assert "0001234" in table.column("complaint_id").to_pylist()  # leading zeros kept


def test_narrative_column_is_never_stored(tmp_path):
    header = HEADER + ["Consumer complaint narrative"]
    rows = [row(10 + i, "2025-01-05", **{"Consumer complaint narrative": f"SECRET STORY {i}"})
            for i in range(5)]
    out = tmp_path / "sample"
    manifest = run_sample(stream(rows, header), out, CONFIG)
    assert manifest["scan"]["excluded_columns_seen"] == ["Consumer complaint narrative"]
    for path in out.rglob("*.parquet"):
        for record in pq.read_table(path).to_pylist():
            assert "SECRET" not in json.dumps(record)
    assert "SECRET" not in (out / "_manifest.json").read_text()


def test_unknown_extra_columns_are_kept_in_raw_json(tmp_path):
    header = HEADER + ["Consumer consent provided?"]
    rows = [row(10, "2025-01-05", **{"Consumer consent provided?": "Consent provided"})]
    result = sample_stream(stream(rows, header), CONFIG)
    assert result.stats.unknown_columns == ["Consumer consent provided?"]
    (_key, raw), = result.selected["2025-01"]
    assert raw["Consumer consent provided?"] == "Consent provided"


def test_missing_required_column_raises():
    header = [c for c in HEADER if c != "Company"]
    with pytest.raises(SchemaError, match="Company"):
        sample_stream(stream([row(1, "2025-01-01")], header), CONFIG)


# --- duplicates ------------------------------------------------------------

def test_identical_duplicates_are_detected_and_kept_once():
    rows = synthetic_rows()
    rows.append(dict(rows[0]))
    result = sample_stream(stream(rows), SampleConfig(**{**CONFIG.__dict__, "per_month": 100}))
    assert result.stats.duplicate_ids_in_frame == 1
    assert result.stats.identical_duplicates_offered == 1
    ids = [r["Complaint ID"] for items in result.selected.values() for _k, r in items]
    assert len(ids) == len(set(ids)) == sum(SIZES.values())


def test_conflicting_duplicate_in_sample_raises():
    rows = synthetic_rows()
    rows.append({**rows[0], "Company": "OTHER CO"})
    with pytest.raises(SampleIntegrityError, match="different content"):
        sample_stream(stream(rows), SampleConfig(**{**CONFIG.__dict__, "per_month": 100}))


# --- malformed records -----------------------------------------------------

def test_malformed_rows_are_counted_and_skipped():
    good = to_csv(synthetic_rows())
    bad_lines = [
        b'9001,2025-01-02,too,few,columns\n',                         # structural
        to_csv([row(9002, "01/02/2025")]).split(b"\n", 1)[1],         # bad date
        to_csv([row("ABC", "2025-01-03")]).split(b"\n", 1)[1],        # bad id
    ]
    data = good + b"".join(bad_lines)
    lenient = SampleConfig(**{**CONFIG.__dict__, "max_malformed_fraction": 0.5})
    result = sample_stream(io.BytesIO(data), lenient)
    assert (result.stats.malformed_structure, result.stats.malformed_date,
            result.stats.malformed_id) == (1, 1, 1)
    assert result.population == SIZES
    assert result.stats.malformed_examples[0]["actual_columns"] == 5


def test_too_many_malformed_rows_raise():
    data = to_csv(synthetic_rows()) + b"1,2,3\n" * 10
    with pytest.raises(SampleIntegrityError, match="malformed"):
        sample_stream(io.BytesIO(data), CONFIG)


# --- interrupted processing ------------------------------------------------

class FailingStream(io.RawIOBase):
    def __init__(self, data, fail_at):
        self.data, self.pos, self.fail_at = data, 0, fail_at

    def readable(self):
        return True

    def readinto(self, buffer):
        if self.pos >= self.fail_at:
            raise ConnectionResetError("simulated interruption")
        chunk = self.data[self.pos:min(self.pos + len(buffer), self.fail_at)]
        buffer[:len(chunk)] = chunk
        self.pos += len(chunk)
        return len(chunk)


def snapshot(directory):
    return {str(p.relative_to(directory)): p.read_bytes()
            for p in sorted(directory.rglob("*")) if p.is_file() and p.name != "_manifest.json"}


def test_interrupted_scan_leaves_previous_output_intact(tmp_path):
    out = tmp_path / "sample"
    run_sample(stream(synthetic_rows()), out, CONFIG)
    before = snapshot(out)

    data = to_csv(synthetic_rows())
    small = SampleConfig(**{**CONFIG.__dict__, "block_size": 1024})
    with pytest.raises(ConnectionResetError):
        run_sample(io.BufferedReader(FailingStream(data, len(data) // 2)), out, small)

    assert snapshot(out) == before
    assert not list(tmp_path.glob(".sample.*"))


def test_interrupted_write_is_rolled_back_and_rerun_is_identical(tmp_path, monkeypatch):
    out = tmp_path / "sample"
    real_write = bs.pq.write_table
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise OSError("disk full (simulated)")
        return real_write(*args, **kwargs)

    monkeypatch.setattr(bs.pq, "write_table", flaky)
    with pytest.raises(OSError):
        run_sample(stream(synthetic_rows()), out, CONFIG)
    assert not out.exists() and not list(tmp_path.glob(".sample.*"))

    monkeypatch.setattr(bs.pq, "write_table", real_write)
    run_sample(stream(synthetic_rows()), out, CONFIG)
    first = pq.read_table(out / "_upload" / "sample.parquet").drop(["_ingested_at"])
    run_sample(stream(synthetic_rows()), out, CONFIG)
    second = pq.read_table(out / "_upload" / "sample.parquet").drop(["_ingested_at"])
    assert first.equals(second)


def test_stale_temp_dirs_are_cleaned(tmp_path):
    stale = tmp_path / ".sample.tmp-deadbeef"
    stale.mkdir()
    (stale / "junk").write_text("x")
    run_sample(stream(synthetic_rows()), tmp_path / "sample", CONFIG)
    assert not stale.exists()


# --- zip sources -----------------------------------------------------------

def make_zip(path, data, name="complaints.csv"):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, data)
    return path


def test_zip_inspection_and_streaming(tmp_path):
    data = to_csv(synthetic_rows())
    path = make_zip(tmp_path / "complaints.csv.zip", data)
    info = inspect_zip(path)
    assert info["member"] == "complaints.csv" and info["uncompressed_bytes"] == len(data)
    with open_bulk_csv(path) as handle:
        result = sample_stream(handle, CONFIG)
    assert result.population == SIZES


def test_unexpected_zip_member_rejected(tmp_path):
    path = make_zip(tmp_path / "x.zip", b"a,b\n", name="other.csv")
    with pytest.raises(SchemaError):
        inspect_zip(path)


def test_zip_prefix_reader_emits_only_complete_rows(tmp_path):
    rows = [row(5000 + i, f"2025-01-{i % 28 + 1:02d}") for i in range(3000)]
    full = make_zip(tmp_path / "full.zip", to_csv(rows))
    prefix = tmp_path / "prefix.zip"
    prefix.write_bytes(full.read_bytes()[: full.stat().st_size // 2])

    with open_zip_prefix(prefix) as handle:
        result = sample_stream(handle, SampleConfig(**{**CONFIG.__dict__, "per_month": 5}))
    assert 0 < result.population["2025-01"] < 3000
    assert result.stats.malformed_total == 0


# --- CLI end to end (offline) ---------------------------------------------

def test_cli_bulk_sample_and_compare(tmp_path, capsys):
    from ingestion import cli
    from ingestion.benchmarks import save_json

    source = make_zip(tmp_path / "complaints.csv.zip", to_csv(synthetic_rows()))
    out = tmp_path / "sample"
    args = ["bulk-sample", "--source", str(source), "--output-dir", str(out),
            "--start-date", "2025-01-01", "--end-date", "2025-04-01", "--per-month", "10"]
    assert cli.main(args) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["sample_total"] == 26 and summary["resources"]["peak_rss_bytes"] > 0

    benchmarks = {"months": [{"month": m, "total": n, "aggregations": {
        "timely": {"Yes": n}, "issue": {"Incorrect information on your report": n}}}
        for m, n in SIZES.items()]}
    save_json(benchmarks, tmp_path / "bench.json")
    assert cli.main(["compare-sample", "--sample-dir", str(out), "--benchmarks",
                     str(tmp_path / "bench.json"), "--out", str(tmp_path / "cmp.json")]) == 0
    report = json.loads((tmp_path / "cmp.json").read_text())
    assert report["flags"] == []
    assert [r["difference"] for r in report["reconciliation"]] == [0, 0, 0]
