"""Offline tests for CFPB pagination (no network access)."""

import pytest
import requests

from ingestion import cfpb_batch_ingest as ingest
from ingestion import cfpb_ingest


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        return self._payload


def hit(complaint_id, ts):
    return {"_source": {"complaint_id": complaint_id}, "sort": [ts, complaint_id]}


def payload(hits, break_points=None):
    body = {"hits": {"hits": hits}}
    if break_points is not None:
        body["_meta"] = {"break_points": break_points}
    return body


class FakeGet:
    """Replays queued responses and records request params."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item if isinstance(item, FakeResponse) else FakeResponse(item)


PAGE_1 = [hit(str(100 - i), 1000 - i) for i in range(5)]   # ids 100..96
PAGE_2 = [hit(str(95 - i), 990 - i) for i in range(5)]     # ids 95..91


# --- successful pagination -------------------------------------------------

def test_two_pages_yield_ten_unique_ids_and_correct_cursor():
    get = FakeGet([
        payload(PAGE_1, break_points={"2": [996, "96"]}),
        payload(PAGE_2),
    ])

    records = ingest.fetch_pages(2, 5, get=get, sleep=lambda s: None)

    ids = [r["complaint_id"] for r in records]
    assert len(ids) == 10 and len(set(ids)) == 10
    assert "search_after" not in get.calls[0]
    assert get.calls[0]["frm"] == 0
    assert get.calls[1]["frm"] == 5
    assert get.calls[1]["search_after"] == "996_96"
    assert all(c["sort"] == "created_date_desc" for c in get.calls)


def test_no_cursor_built_after_final_page():
    # Last page has no sort values; must not fail since no next page is needed.
    get = FakeGet([payload([{"_source": {"complaint_id": "1"}}])])
    records = ingest.fetch_pages(1, 5, get=get, sleep=lambda s: None)
    assert [r["complaint_id"] for r in records] == ["1"]


def test_transient_error_is_retried():
    get = FakeGet([requests.ConnectionError("boom"), payload(PAGE_1)])
    records = ingest.fetch_pages(1, 5, get=get, sleep=lambda s: None)
    assert len(records) == 5
    assert len(get.calls) == 2


# --- duplicate IDs ---------------------------------------------------------

def test_overlapping_pages_raise():
    # The original bug: page 2 returns the same IDs as page 1.
    get = FakeGet([payload(PAGE_1), payload(PAGE_1)])
    with pytest.raises(ingest.PaginationError, match="Duplicate complaint_id"):
        ingest.fetch_pages(2, 5, get=get, sleep=lambda s: None)


def test_duplicate_within_page_raises():
    get = FakeGet([payload([hit("1", 10), hit("1", 9)])])
    with pytest.raises(ingest.PaginationError, match="Duplicate complaint_id"):
        ingest.fetch_pages(1, 5, get=get, sleep=lambda s: None)


# --- missing / malformed cursors ------------------------------------------

@pytest.mark.parametrize("bad_hit", [
    {"_source": {"complaint_id": "1"}},                       # no sort
    {"_source": {"complaint_id": "1"}, "sort": None},
    {"_source": {"complaint_id": "1"}, "sort": [123]},        # wrong length
    {"_source": {"complaint_id": "1"}, "sort": ["abc", "1"]}, # non-numeric key
    {"_source": {"complaint_id": "1"}, "sort": [123, ""]},    # empty id
    {"_source": {"complaint_id": "1"}, "sort": [True, "1"]},  # bool key
])
def test_malformed_cursor_raises(bad_hit):
    with pytest.raises(ingest.PaginationError):
        ingest.build_cursor(bad_hit)


def test_missing_cursor_stops_before_next_request():
    bad_page = PAGE_1[:-1] + [{"_source": {"complaint_id": "96"}}]
    get = FakeGet([payload(bad_page), payload(PAGE_2)])
    with pytest.raises(ingest.PaginationError):
        ingest.fetch_pages(2, 5, get=get, sleep=lambda s: None)
    assert len(get.calls) == 1  # page 2 never requested


def test_cursor_mismatch_with_break_points_raises():
    get = FakeGet([payload(PAGE_1, break_points={"2": [555, "999"]}), payload(PAGE_2)])
    with pytest.raises(ingest.PaginationError, match="Cursor mismatch"):
        ingest.fetch_pages(2, 5, get=get, sleep=lambda s: None)
    assert len(get.calls) == 1


def test_malformed_response_raises():
    get = FakeGet([{"unexpected": True}])
    with pytest.raises(ingest.PaginationError, match="missing hits"):
        ingest.fetch_pages(1, 5, get=get, sleep=lambda s: None)


# --- empty pages -----------------------------------------------------------

def test_empty_first_page_returns_no_records():
    get = FakeGet([payload([])])
    assert ingest.fetch_pages(2, 5, get=get, sleep=lambda s: None) == []
    assert len(get.calls) == 1


def test_empty_second_page_stops_cleanly():
    get = FakeGet([payload(PAGE_1), payload([])])
    records = ingest.fetch_pages(3, 5, get=get, sleep=lambda s: None)
    assert len(records) == 5
    assert len(get.calls) == 2


# --- existing single-batch ingestion still works --------------------------

def test_fetch_complaints_returns_sources(monkeypatch):
    monkeypatch.setattr(cfpb_ingest.requests, "get", FakeGet([payload(PAGE_1)]))
    complaints = cfpb_ingest.fetch_complaints(size=5)
    assert [c["complaint_id"] for c in complaints] == ["100", "99", "98", "97", "96"]
