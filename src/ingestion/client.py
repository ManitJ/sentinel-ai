"""HTTP client and cursor pagination for the CFPB complaint search API.

API behaviour verified against the live API (2026-11-12) and the official
spec (github.com/cfpb/ccdb5-api, swagger-config.yaml):

* Pages advance via ``search_after``, built from the last hit's ``sort``
  value ``[sort_key, complaint_id]`` as ``"<sort_key>_<id>"``. ``frm`` alone
  does not advance pages; it must be 0 or a multiple of ``size`` (else HTTP
  400) and acts as a page marker. ``frm`` values above 10,000 work with a
  cursor; the spec caps ``frm`` at 100,000 and ``size`` at 100.
* A deterministic sort (``created_date_desc``) is required; the default
  relevance sort gives every hit the same score.
* ``date_received_min`` and ``date_received_max`` are BOTH inclusive at day
  granularity (the spec describes max as exclusive; observed behaviour
  differs).
* ``_meta.break_points`` maps page number -> cursor; used as a cross-check.
"""

import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from ingestion.logging_utils import log_event

API_URL = (
    "https://www.consumerfinance.gov/"
    "data-research/consumer-complaints/search/api/v1/"
)
SORT_ORDER = "created_date_desc"
MAX_PAGE_SIZE = 100
MAX_FRM = 100_000
MAX_RETRIES = 3
REQUEST_TIMEOUT = (10, 120)
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
RETRY_AFTER_STATUS = {429, 503}
MAX_RETRY_AFTER = 300.0  # seconds; longer server waits abort the run

logger = logging.getLogger(__name__)


class PaginationError(Exception):
    """Raised when the API response cannot be paginated safely."""


class RateLimitError(Exception):
    """Raised when the server asks us to wait longer than ``max_retry_after``."""

    def __init__(self, message, retry_after):
        super().__init__(message)
        self.retry_after = retry_after


class PermanentAPIError(Exception):
    """Raised for non-retryable HTTP errors (e.g. 400 Bad Request)."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


def build_cursor(hit):
    """Derive a ``search_after`` cursor from a hit's ``sort`` value."""
    sort_value = hit.get("sort") if isinstance(hit, dict) else None

    if not isinstance(sort_value, (list, tuple)) or len(sort_value) != 2:
        raise PaginationError(f"Missing or malformed sort value: {sort_value!r}")

    sort_key, complaint_id = sort_value

    if isinstance(sort_key, bool) or not isinstance(sort_key, (int, float)):
        raise PaginationError(f"Sort key is not numeric: {sort_key!r}")
    if complaint_id is None or str(complaint_id).strip() == "":
        raise PaginationError(f"Sort tiebreaker ID is empty: {complaint_id!r}")

    return f"{int(sort_key)}_{complaint_id}"


def check_break_point(payload, next_page, cursor):
    """Cross-check the derived cursor against ``_meta.break_points``."""
    break_points = (payload.get("_meta") or {}).get("break_points") or {}
    expected = break_points.get(str(next_page))

    if expected is None:
        return

    try:
        expected_cursor = build_cursor({"sort": expected})
    except PaginationError as error:
        raise PaginationError(
            f"Malformed break point for page {next_page}: {expected!r}"
        ) from error

    if expected_cursor != cursor:
        raise PaginationError(
            f"Cursor mismatch for page {next_page}: derived {cursor!r}, "
            f"API break point {expected_cursor!r}"
        )


def extract_hits(payload):
    """Return the hits list from a search payload, validating its shape."""
    try:
        hits = payload["hits"]["hits"]
    except (KeyError, TypeError) as error:
        raise PaginationError("Response is missing hits.hits") from error

    if not isinstance(hits, list):
        raise PaginationError("hits.hits is not a list")

    return hits


def total_hits(payload):
    """Return the API's reported total match count, or None if absent."""
    total = (payload.get("hits") or {}).get("total")
    if isinstance(total, dict):
        total = total.get("value")
    return total if isinstance(total, int) and not isinstance(total, bool) else None


def _status_code(error):
    response = getattr(error, "response", None)
    return getattr(response, "status_code", None)


def parse_retry_after(value, now=None):
    """Parse a Retry-After header (delta-seconds or HTTP-date) into seconds.

    Returns None when the header is absent or unparseable.
    """
    if value is None:
        return None
    value = str(value).strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return max(0.0, (when - now).total_seconds())


def _retry_after(error):
    response = getattr(error, "response", None)
    if _status_code(error) not in RETRY_AFTER_STATUS or response is None:
        return None
    headers = getattr(response, "headers", None) or {}
    return parse_retry_after(headers.get("Retry-After"))


def is_retryable(error):
    """Transient network/server errors are retried; client errors are not."""
    if isinstance(error, requests.HTTPError):
        return _status_code(error) in RETRYABLE_STATUS
    return isinstance(
        error,
        (requests.ConnectionError, requests.Timeout,
         requests.exceptions.ChunkedEncodingError, ValueError),
    )


def fetch_page(frm, size, search_after=None, filters=None, get=requests.get,
               sleep=time.sleep, max_retries=MAX_RETRIES, backoff_base=2.0,
               backoff_cap=60.0, max_retry_after=MAX_RETRY_AFTER):
    """Fetch one page, retrying transient failures with exponential backoff.

    For 429/503 responses with a Retry-After header, waits
    ``max(Retry-After, backoff)``; if the server asks for more than
    ``max_retry_after`` seconds, raises ``RateLimitError`` instead of sleeping.
    """
    params = {
        "size": size,
        "frm": frm,
        "sort": SORT_ORDER,
        "no_aggs": "true",
        "no_highlight": "true",
    }
    params.update(filters or {})
    if search_after is not None:
        params["search_after"] = search_after

    for attempt in range(1, max_retries + 1):
        started = time.monotonic()
        try:
            response = get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as error:
            duration_ms = round((time.monotonic() - started) * 1000)
            retryable = is_retryable(error)
            retry_after = _retry_after(error)
            delay = min(backoff_cap, backoff_base * 2 ** (attempt - 1))
            if retry_after is not None:
                delay = max(delay, retry_after)
            log_event(
                logger, logging.WARNING, "api_request_failed",
                frm=frm, attempt=attempt, max_retries=max_retries,
                status=_status_code(error), retryable=retryable,
                retry_after=retry_after, next_delay_s=delay,
                duration_ms=duration_ms, error=str(error)[:300],
            )
            if not retryable:
                raise PermanentAPIError(str(error), _status_code(error)) from error
            if retry_after is not None and retry_after > max_retry_after:
                raise RateLimitError(
                    f"Server requested Retry-After={retry_after:.0f}s "
                    f"(> {max_retry_after:.0f}s); stop and resume later",
                    retry_after,
                ) from error
            if attempt == max_retries:
                raise
            sleep(delay)
        else:
            log_event(
                logger, logging.DEBUG, "api_request",
                frm=frm, size=size, search_after=search_after, attempt=attempt,
                duration_ms=round((time.monotonic() - started) * 1000),
                **{k: v for k, v in (filters or {}).items()},
            )
            return payload


def iter_pages(page_size, filters=None, max_pages=None, get=requests.get,
               sleep=time.sleep, request_delay=0.0, max_retries=MAX_RETRIES,
               max_retry_after=MAX_RETRY_AFTER):
    """Yield ``(page_number, payload, hits)`` using ``search_after`` cursors.

    Stops after a short (or empty) page, or after ``max_pages``. The cursor
    for the next page is validated before it is requested.
    """
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be 1..{MAX_PAGE_SIZE}, got {page_size}")

    cursor = None
    page = 0
    while True:
        page += 1
        frm = (page - 1) * page_size
        if frm > MAX_FRM:
            raise PaginationError(
                f"frm={frm} exceeds the API limit of {MAX_FRM}; use a smaller window"
            )
        if page > 1 and request_delay:
            sleep(request_delay)

        payload = fetch_page(frm, page_size, cursor, filters, get=get,
                             sleep=sleep, max_retries=max_retries,
                             max_retry_after=max_retry_after)
        hits = extract_hits(payload)
        yield page, payload, hits

        if len(hits) < page_size:
            return
        if max_pages is not None and page >= max_pages:
            return

        cursor = build_cursor(hits[-1])
        check_break_point(payload, page + 1, cursor)


def validate_new_ids(hits, seen_ids, page):
    """Return the page's complaint IDs; raise on missing or duplicate IDs."""
    page_ids = []
    for hit in hits:
        source = hit.get("_source") or {}
        complaint_id = source.get("complaint_id")
        if complaint_id is None:
            raise PaginationError(f"Hit on page {page} has no complaint_id")
        if complaint_id in seen_ids or complaint_id in page_ids:
            raise PaginationError(f"Duplicate complaint_id {complaint_id} on page {page}")
        page_ids.append(complaint_id)
    return page_ids


def fetch_pages(num_pages, batch_size=5, get=requests.get, sleep=time.sleep,
                filters=None):
    """Fetch up to ``num_pages`` pages and return complaint records."""
    records = []
    seen_ids = set()

    for page, _payload, hits in iter_pages(batch_size, filters, num_pages,
                                           get=get, sleep=sleep):
        if not hits:
            log_event(logger, logging.INFO, "empty_page", page=page)
            break
        page_ids = validate_new_ids(hits, seen_ids, page)
        seen_ids.update(page_ids)
        records.extend(hit["_source"] for hit in hits)
        log_event(logger, logging.INFO, "page_fetched", page=page, ids=page_ids)

    return records
