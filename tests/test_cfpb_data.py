
"""Data quality tests for CFPB complaint ingestion."""

from pathlib import Path

import pandas as pd
import pytest


DATA_PATH = Path("data/raw/cfpb_sample.parquet")

# The sample file is local data and is intentionally not committed to Git.
# On a fresh clone (or in CI) these checks are skipped rather than failing.
pytestmark = pytest.mark.skipif(not DATA_PATH.exists(),
                                reason=f"local data file {DATA_PATH} not present")


def test_parquet_file_exists():
    """Verify that the ingestion output exists."""
    assert DATA_PATH.exists(), "Parquet file is missing"


def test_complaint_ids_are_unique():
    """Verify complaint IDs are not duplicated."""
    df = pd.read_parquet(DATA_PATH)

    assert df["complaint_id"].is_unique, "Duplicate complaint IDs found"


def test_required_columns_exist():
    """Verify essential financial complaint fields."""
    df = pd.read_parquet(DATA_PATH)

    required_columns = {
        "complaint_id",
        "company",
        "product",
        "date_received",
        "issue",
    }

    assert required_columns.issubset(df.columns), "Required columns missing"


def test_complaint_ids_not_null():
    """Verify every record has a complaint ID."""
    df = pd.read_parquet(DATA_PATH)

    assert df["complaint_id"].notna().all(), "Null complaint IDs found"
