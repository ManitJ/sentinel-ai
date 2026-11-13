"""Offline fakes mimicking the verified CFPB search API behaviour."""

from datetime import datetime, timezone

import requests


class FakeResponse:
    def __init__(self, payload=None, status=200, headers=None):
        self._payload = payload
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def make_record(complaint_id, day, time="12:00:00", **extra):
    record = {
        "complaint_id": str(complaint_id),
        "date_received": f"{day}T{time}.000Z",
        "product": "Debt collection",
        "company": "Example Co",
        "issue": "Example issue",
        "tags": None,
    }
    record.update(extra)
    return record


def _ts(record):
    parsed = datetime.strptime(record["date_received"], "%Y-%m-%dT%H:%M:%S.%fZ")
    return int(parsed.replace(tzinfo=timezone.utc).timestamp() * 1000)


class FakeCFPB:
    """Callable stand-in for ``requests.get`` against the search endpoint.

    * date_received_min / _max are inclusive days (as observed live).
    * Sorted by (date desc, complaint_id desc); ``search_after`` continues
      strictly after the cursor; ``frm`` is ignored, so forgetting the cursor
      reproduces the original duplicate-page bug.
    * ``interceptor(params, call_number)`` may return a FakeResponse or an
      Exception to inject failures.
    """

    def __init__(self, records, interceptor=None, total_offset=0):
        self.records = list(records)
        self.interceptor = interceptor
        self.total_offset = total_offset
        self.calls = []

    def __call__(self, url, params=None, timeout=None):
        params = dict(params or {})
        self.calls.append(params)
        if self.interceptor:
            injected = self.interceptor(params, len(self.calls))
            if isinstance(injected, Exception):
                raise injected
            if injected is not None:
                return injected

        low, high = params.get("date_received_min"), params.get("date_received_max")
        rows = [r for r in self.records
                if (low is None or r["date_received"][:10] >= low)
                and (high is None or r["date_received"][:10] <= high)]
        total = len(rows) + self.total_offset

        def key(r):
            return (_ts(r), r["complaint_id"])

        rows.sort(key=key, reverse=True)
        cursor = params.get("search_after")
        if cursor:
            sort_key, cid = cursor.split("_", 1)
            rows = [r for r in rows if key(r) < (int(sort_key), cid)]

        page = rows[: int(params["size"])]
        hits = [{"_source": dict(r), "sort": [_ts(r), r["complaint_id"]]} for r in page]
        return FakeResponse({"hits": {"total": {"value": total, "relation": "eq"},
                                      "hits": hits}})

    def window_requests(self):
        """(min, max) date filters of first-page requests, in order."""
        return [(c.get("date_received_min"), c.get("date_received_max"))
                for c in self.calls if "search_after" not in c]
