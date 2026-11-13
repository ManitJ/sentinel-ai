"""Offline tests for resumable bulk download and sample-vs-API validation."""

import hashlib
import json
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from fakes import FakeResponse
from ingestion.benchmarks import (
    compare_sample,
    fetch_monthly_benchmarks,
    months_between,
    parse_aggregations,
)
from ingestion.bulk_download import IncompleteDownloadError, download_file

DATA = bytes(range(256)) * 400  # 102,400 bytes


class StreamResponse:
    def __init__(self, data, status=200, headers=None, fail_after=None):
        self.data, self.status_code = data, status
        self.headers = headers or {}
        self.fail_after = fail_after

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def iter_content(self, size):
        sent = 0
        for i in range(0, len(self.data), 10_000):
            if self.fail_after is not None and sent >= self.fail_after:
                raise requests.ConnectionError("connection reset (simulated)")
            chunk = self.data[i:i + 10_000]
            sent += len(chunk)
            yield chunk


class FakeServer:
    def __init__(self, fail_first_after=None, honour_range=True):
        self.calls = []
        self.fail_first_after = fail_first_after
        self.honour_range = honour_range

    def __call__(self, url, headers=None, stream=None, timeout=None):
        headers = headers or {}
        self.calls.append(headers)
        fail = self.fail_first_after if len(self.calls) == 1 else None
        common = {"Last-Modified": "Thu, 12 Nov 2026 10:18:05 GMT", "ETag": '"abc"'}
        if "Range" in headers and self.honour_range:
            start = int(headers["Range"].split("=")[1].rstrip("-"))
            body = DATA[start:]
            return StreamResponse(body, 206, {**common, "Content-Length": str(len(body)),
                                              "Content-Range": f"bytes {start}-{len(DATA)-1}/{len(DATA)}"}, fail)
        return StreamResponse(DATA, 200, {**common, "Content-Length": str(len(DATA))}, fail)


def test_download_resumes_after_interruption(tmp_path):
    server = FakeServer(fail_first_after=30_000)
    dest = tmp_path / "complaints.csv.zip"
    meta = download_file("http://x", dest, get=server, sleep=lambda s: None)
    assert dest.read_bytes() == DATA
    assert server.calls[1]["Range"] == "bytes=30000-"
    assert meta["sha256"] == hashlib.sha256(DATA).hexdigest()
    assert meta["bytes"] == len(DATA)
    assert meta["last_modified"] == "Thu, 12 Nov 2026 10:18:05 GMT"
    assert json.loads((tmp_path / "complaints.csv.zip.meta.json").read_text())["bytes"] == len(DATA)
    assert not (tmp_path / "complaints.csv.zip.part").exists()


def test_download_restarts_when_server_ignores_range(tmp_path):
    dest = tmp_path / "f.zip"
    (tmp_path / "f.zip.part").write_bytes(b"garbage-from-old-version")
    download_file("http://x", dest, get=FakeServer(honour_range=False), sleep=lambda s: None)
    assert dest.read_bytes() == DATA


def test_download_gives_up_and_keeps_part_file(tmp_path):
    def always_fails(url, headers=None, stream=None, timeout=None):
        return StreamResponse(DATA, 200, {"Content-Length": str(len(DATA))}, fail_after=0)

    dest = tmp_path / "f.zip"
    with pytest.raises(requests.ConnectionError):
        download_file("http://x", dest, get=always_fails, sleep=lambda s: None, max_attempts=2)
    assert not dest.exists()


def test_truncated_body_is_detected(tmp_path):
    def short(url, headers=None, stream=None, timeout=None):
        return StreamResponse(DATA[:500], 200, {"Content-Length": str(len(DATA))})

    with pytest.raises(IncompleteDownloadError):
        download_file("http://x", tmp_path / "f.zip", get=short, sleep=lambda s: None,
                      max_attempts=1)


# --- benchmarks ------------------------------------------------------------

def agg_payload(total, dims):
    aggregations = {dim: {"doc_count": total, dim: {"sum_other_doc_count": 0,
                    "buckets": [{"key": k, "doc_count": v} for k, v in buckets.items()]}}
                    for dim, buckets in dims.items()}
    return {"hits": {"total": {"value": total}, "hits": []}, "aggregations": aggregations}


def test_parse_aggregations_matches_api_shape():
    payload = agg_payload(100, {"timely": {"Yes": 98, "No": 2}, "state": {"CA": 60}})
    parsed = parse_aggregations(payload)
    assert parsed["timely"] == {"Yes": 98, "No": 2}
    assert parsed["state"] == {"CA": 60}
    assert parsed["company"] == {}


def test_fetch_monthly_benchmarks_uses_inclusive_month_bounds():
    calls = []

    def get(url, params=None, timeout=None):
        calls.append(params)
        return FakeResponse(agg_payload(10, {"timely": {"Yes": 10}}))

    result = fetch_monthly_benchmarks(date(2025, 1, 1), date(2025, 3, 1), "Credit reporting",
                                      get=get, sleep=lambda s: None)
    assert [m["month"] for m in result["months"]] == ["2025-01", "2025-02"]
    assert calls[1]["date_received_min"] == "2025-02-01"
    assert calls[1]["date_received_max"] == "2025-02-28"
    assert calls[0]["no_aggs"] == "false" and calls[0]["product"] == ["Credit reporting"]


def test_months_between_handles_year_boundary():
    assert months_between(date(2025, 11, 1), date(2026, 2, 1)) == ["2025-11", "2025-12", "2026-01"]


def write_sample(path, month_rows):
    rows = []
    for month, values in month_rows.items():
        for timely in values:
            rows.append({"_stratum": month, "issue": "X", "timely": timely, "company_response": "C",
                         "submitted_via": "Web", "company": "A", "state": "CA",
                         "company_public_response": None, "tags": None})
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def bench(month_totals):
    return {"months": [{"month": m, "total": total, "aggregations": {
        "issue": {"X": total}, "timely": timely, "company_response": {"C": total},
        "submitted_via": {"Web": total}, "company": {"A": total}, "state": {"CA": total},
        "company_public_response": {}, "tags": {}}} for m, (total, timely) in month_totals.items()]}


def test_compare_sample_no_flags_when_shares_match(tmp_path):
    path = write_sample(tmp_path / "s.parquet", {"2025-01": ["Yes"] * 900 + ["No"] * 100})
    report = compare_sample(path, bench({"2025-01": (10_000, {"Yes": 9_000, "No": 1_000})}))
    assert report["flags"] == [] and report["overall_flags"] == []
    assert report["tests"] >= 2


def test_compare_sample_flags_biased_sample(tmp_path):
    path = write_sample(tmp_path / "s.parquet", {"2025-01": ["Yes"] * 990 + ["No"] * 10})
    report = compare_sample(path, bench({"2025-01": (10_000, {"Yes": 9_000, "No": 1_000})}))
    assert {f["key"] for f in report["flags"]} == {"Yes", "No"}
    assert report["overall_flags"]


def test_compare_sample_reconciliation_and_missing_categories(tmp_path):
    path = write_sample(tmp_path / "s.parquet", {"2025-01": ["Yes"] * 99 + ["Maybe"]})
    manifest = {"months": {"2025-01": {"population": 10_010, "sample": 100}}}
    report = compare_sample(path, bench({"2025-01": (10_000, {"Yes": 10_000})}), manifest)
    assert report["reconciliation"] == [{"month": "2025-01", "bulk_frame": 10_010,
                                         "api_total": 10_000, "difference": 10, "pct": 0.1}]
    assert report["categories_missing_from_population"][0]["key"] == "Maybe"
