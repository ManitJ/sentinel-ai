"""Offline tests for HTTP 429 / Retry-After handling."""

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
import requests

from fakes import FakeCFPB, FakeResponse, make_record
from ingestion.client import RateLimitError, fetch_page, parse_retry_after

RECORDS = [make_record(1, "2024-01-01")]


def fail_first(n_failures, response):
    return lambda params, n: response if n <= n_failures else None


def test_parse_retry_after_seconds():
    assert parse_retry_after("7") == 7.0
    assert parse_retry_after(" 0 ") == 0.0


def test_parse_retry_after_http_date():
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=timezone.utc)
    header = format_datetime(now + timedelta(seconds=30), usegmt=True)
    assert parse_retry_after(header, now=now) == 30.0


def test_parse_retry_after_past_date_is_zero():
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=timezone.utc)
    header = format_datetime(now - timedelta(minutes=5), usegmt=True)
    assert parse_retry_after(header, now=now) == 0.0


@pytest.mark.parametrize("value", [None, "", "soon", "-5", "1.5"])
def test_parse_retry_after_invalid(value):
    assert parse_retry_after(value) is None


def test_429_waits_for_retry_after_when_longer_than_backoff():
    sleeps = []
    api = FakeCFPB(RECORDS, fail_first(1, FakeResponse(status=429, headers={"Retry-After": "17"})))
    payload = fetch_page(0, 5, get=api, sleep=sleeps.append)
    assert len(payload["hits"]["hits"]) == 1
    assert sleeps == [17.0] and len(api.calls) == 2


def test_short_retry_after_never_undercuts_backoff():
    sleeps = []
    api = FakeCFPB(RECORDS, fail_first(2, FakeResponse(status=429, headers={"Retry-After": "1"})))
    fetch_page(0, 5, get=api, sleep=sleeps.append)
    assert sleeps == [2.0, 4.0]


def test_503_with_http_date_retry_after_is_honoured():
    sleeps = []
    when = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=40), usegmt=True)
    api = FakeCFPB(RECORDS, fail_first(1, FakeResponse(status=503, headers={"Retry-After": when})))
    fetch_page(0, 5, get=api, sleep=sleeps.append)
    assert len(sleeps) == 1 and 35 <= sleeps[0] <= 40


def test_429_without_header_uses_exponential_backoff():
    sleeps = []
    api = FakeCFPB(RECORDS, fail_first(2, FakeResponse(status=429)))
    fetch_page(0, 5, get=api, sleep=sleeps.append)
    assert sleeps == [2.0, 4.0]


def test_retry_after_above_cap_aborts_without_sleeping():
    sleeps = []
    api = FakeCFPB(RECORDS, fail_first(1, FakeResponse(status=429, headers={"Retry-After": "3600"})))
    with pytest.raises(RateLimitError) as excinfo:
        fetch_page(0, 5, get=api, sleep=sleeps.append, max_retry_after=300)
    assert excinfo.value.retry_after == 3600.0
    assert sleeps == [] and len(api.calls) == 1


def test_persistent_429_gives_up_after_max_retries():
    sleeps = []
    api = FakeCFPB(RECORDS, lambda p, n: FakeResponse(status=429, headers={"Retry-After": "5"}))
    with pytest.raises(requests.HTTPError):
        fetch_page(0, 5, get=api, sleep=sleeps.append, max_retries=3)
    assert sleeps == [5.0, 5.0] and len(api.calls) == 3


def test_retry_after_ignored_on_permanent_errors():
    sleeps = []
    api = FakeCFPB(RECORDS, lambda p, n: FakeResponse(status=400, headers={"Retry-After": "5"}))
    with pytest.raises(Exception, match="400"):
        fetch_page(0, 5, get=api, sleep=sleeps.append)
    assert sleeps == [] and len(api.calls) == 1
