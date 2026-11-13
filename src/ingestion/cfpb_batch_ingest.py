"""Two-page CFPB pagination smoke test for Sentinel AI.

The pagination logic now lives in ``ingestion.client``; this script keeps
its original behaviour (fetch 2 x 5 records, write a test Parquet file) and
re-exports the client functions for backwards compatibility.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

if __package__ in (None, ""):  # allow `python src/ingestion/cfpb_batch_ingest.py`
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.client import (  # noqa: F401  (re-exported)
    API_URL,
    SORT_ORDER,
    PaginationError,
    build_cursor,
    check_break_point,
    extract_hits,
    fetch_page,
    fetch_pages,
)
from ingestion.logging_utils import configure_logging

logger = logging.getLogger(__name__)


def main():
    configure_logging()

    records = fetch_pages(num_pages=2, batch_size=5)
    df = pd.DataFrame(records)

    unique_ids = df["complaint_id"].nunique() if not df.empty else 0
    logger.info("Records retrieved: %d | Unique complaint IDs: %d", len(df), unique_ids)

    output_path = Path("data/raw/cfpb_batch_test.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    logger.info("Saved dataset to %s", output_path)


if __name__ == "__main__":
    main()
