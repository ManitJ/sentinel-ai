"""Resumable download of the CFPB bulk export (HTTP Range + .part file)."""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from ingestion.logging_utils import log_event

logger = logging.getLogger(__name__)
CHUNK_SIZE = 1 << 20


class IncompleteDownloadError(Exception):
    """The server closed the connection before the full file arrived."""


def _expected_total(response, offset):
    content_range = response.headers.get("Content-Range", "")
    if "/" in content_range and not content_range.endswith("/*"):
        return int(content_range.rsplit("/", 1)[1])
    length = response.headers.get("Content-Length")
    return offset + int(length) if length is not None else None


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_file(url, dest, get=requests.get, sleep=time.sleep, max_attempts=5,
                  headers=None):
    """Download ``url`` to ``dest``, resuming from ``dest.part`` if present.

    Writes ``dest.meta.json`` with size, SHA-256, Last-Modified and timing.
    """
    dest = Path(dest)
    part = dest.with_name(dest.name + ".part")
    dest.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    resumed_from = part.stat().st_size if part.exists() else 0
    last_modified = etag = total = None

    for attempt in range(1, max_attempts + 1):
        offset = part.stat().st_size if part.exists() else 0
        request_headers = dict(headers or {})
        if offset:
            request_headers["Range"] = f"bytes={offset}-"
        try:
            response = get(url, headers=request_headers, stream=True, timeout=(10, 120))
            if offset and response.status_code == 416:  # already complete
                total = offset
                break
            response.raise_for_status()
            if offset and response.status_code == 200:
                log_event(logger, logging.WARNING, "range_ignored_restarting", offset=offset)
                offset = 0
            mode = "ab" if offset else "wb"
            total = _expected_total(response, offset)
            last_modified = response.headers.get("Last-Modified", last_modified)
            etag = response.headers.get("ETag", etag)
            with open(part, mode) as handle:
                for chunk in response.iter_content(CHUNK_SIZE):
                    handle.write(chunk)
            size = part.stat().st_size
            if total is not None and size != total:
                raise IncompleteDownloadError(f"Got {size} of {total} bytes")
            break
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError, IncompleteDownloadError) as error:
            log_event(logger, logging.WARNING, "download_interrupted", attempt=attempt,
                      bytes_on_disk=part.stat().st_size if part.exists() else 0,
                      error=str(error)[:200])
            if attempt == max_attempts:
                raise
            sleep(min(60, 2 ** attempt))

    size = part.stat().st_size
    if total is not None and size != total:
        raise IncompleteDownloadError(f"Got {size} of {total} bytes")
    os.replace(part, dest)
    meta = {
        "url": url,
        "bytes": size,
        "sha256": sha256_file(dest),
        "last_modified": last_modified,
        "etag": etag,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.monotonic() - started, 2),
        "resumed_from_bytes": resumed_from,
    }
    dest.with_name(dest.name + ".meta.json").write_text(json.dumps(meta, indent=2))
    log_event(logger, logging.INFO, "download_complete", **meta)
    return meta
