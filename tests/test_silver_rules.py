"""Offline tests for Silver transformation and validation rules."""

import ast
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lakehouse import silver_rules as sr
from lakehouse.silver_rules import (
    normalize_text,
    parse_date,
    profile,
    split_tags,
    state_category,
    state_code,
    transform_record,
    validate,
)

ROOT = Path(__file__).resolve().parents[1]
SILVER_NB = ROOT / "databricks/notebooks/02_silver_transform.py"
BRONZE_NB = ROOT / "databricks/notebooks/01_bronze_ingest.py"
SAMPLE = ROOT / "data/bronze/cfpb_credit_reporting_sample/_upload/cfpb_credit_reporting_sample.parquet"


def notebook_constant(path, name):
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not in {path.name}")


def bronze_row(cid="1001", received="2025-01-15", sent="2025-01-16", stratum="2025-01",
               population=6, weight=2.0, **overrides):
    row = {
        "complaint_id": cid, "date_received": received, "date_sent_to_company": sent,
        "product": "Credit reporting or other personal consumer reports",
        "sub_product": "Credit reporting", "issue": "Incorrect information on your report",
        "sub_issue": None, "company": "EQUIFAX, INC.", "company_response": "Closed with explanation",
        "company_public_response": None, "timely": "Yes", "submitted_via": "Web",
        "state": "CA", "zip_code": "940XX", "tags": None,
        "_stratum": stratum, "_stratum_population": population, "_sample_weight": weight,
        "_sample_key": "00ff", "_source_system": "cfpb_bulk_csv",
    }
    row.update(overrides)
    return row


# --- individual rules ------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, None), ("", None), ("   ", None), ("Web", "Web"),
    ("  Closed  with\texplanation ", "Closed with explanation"),
    ("a\nb", "a b"),
    ("a b", "a b"),  # non-breaking space untouched (same as Spark/Java class)
])
def test_normalize_text(raw, expected):
    assert normalize_text(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("2025-01-31", date(2025, 1, 31)), ("2024-02-29", date(2024, 2, 29)),
    ("2025-02-30", None), ("01/31/2025", None), ("2025-1-31", None),
    ("2025-01-31T00:00:00", None), (None, None), ("", None),
])
def test_parse_date_is_strict(raw, expected):
    assert parse_date(raw) == expected


@pytest.mark.parametrize("state, code, category", [
    ("CA", "CA", "state"), ("DC", "DC", "district"), ("PR", "PR", "territory"),
    ("AE", "AE", "military"), ("UNITED STATES MINOR OUTLYING ISLANDS", "UM", "territory"),
    ("ZZ", "ZZ", "unknown"), (None, None, None),
])
def test_state_code_and_category(state, code, category):
    assert state_code(state) == code
    assert state_category(code) == category


def test_usps_lists_are_disjoint_and_complete():
    groups = [sr.US_STATES, sr.DISTRICT, sr.TERRITORIES, sr.MILITARY]
    flat = [c for g in groups for c in g]
    assert len(flat) == len(set(flat))
    assert len(sr.US_STATES) == 50


@pytest.mark.parametrize("raw, expected", [
    (None, []), ("Servicemember", ["Servicemember"]),
    ("Older American, Servicemember", ["Older American", "Servicemember"]),
    ("Older American,Servicemember", ["Older American", "Servicemember"]),
])
def test_split_tags(raw, expected):
    assert split_tags(raw) == expected


def test_transform_record_preserves_originals_and_derives_columns():
    out = transform_record(bronze_row(
        received="2025-01-15", sent="2025-01-22", timely="No",
        state="UNITED STATES MINOR OUTLYING ISLANDS", tags="Older American, Servicemember"))
    assert list(out) == [name for name, _ in sr.SILVER_COLUMNS]
    assert out["complaint_id"] == "1001"
    assert out["date_received"] == date(2025, 1, 15) and out["days_to_company"] == 7
    assert out["timely"] == "No" and out["is_timely"] is False
    assert out["state"] == "UNITED STATES MINOR OUTLYING ISLANDS"
    assert out["state_code"] == "UM" and out["state_category"] == "territory"
    assert out["tags"] == "Older American, Servicemember"
    assert out["is_servicemember"] and out["is_older_american"]
    assert (out["sample_stratum"], out["stratum_population"], out["sample_weight"],
            out["sample_key"], out["source_system"]) == ("2025-01", 6, 2.0, "00ff", "cfpb_bulk_csv")
    assert out["zip_code"] == "940XX" and out["sub_issue"] is None


def test_transform_keeps_unknown_timely_as_null():
    assert transform_record(bronze_row(timely="Maybe"))["is_timely"] is None


# --- validation --------------------------------------------------------------

@pytest.fixture
def small_design(monkeypatch):
    """Two strata x 3 rows; population 6 + 9 = 15."""
    monkeypatch.setattr(sr, "EXPECTED_ROWS", 6)
    monkeypatch.setattr(sr, "EXPECTED_STRATA", 2)
    monkeypatch.setattr(sr, "EXPECTED_PER_STRATUM", 3)
    monkeypatch.setattr(sr, "EXPECTED_POPULATION", 15)


def good_rows():
    rows = [bronze_row(cid=str(100 + i), received=f"2025-01-{10 + i}", sent=f"2025-01-{11 + i}",
                       stratum="2025-01", population=6, weight=2.0) for i in range(3)]
    rows += [bronze_row(cid=str(200 + i), received=f"2025-02-{10 + i}", sent=f"2025-02-{10 + i}",
                        stratum="2025-02", population=9, weight=3.0) for i in range(3)]
    return rows


def run(rows):
    return validate([transform_record(r) for r in rows])


def test_clean_data_passes(small_design):
    assert run(good_rows()) == ([], [])


@pytest.mark.parametrize("mutate, message", [
    (lambda rows: rows.__setitem__(1, {**rows[1], "complaint_id": rows[0]["complaint_id"]}), "duplicate"),
    (lambda rows: rows[0].update(complaint_id=None), "complaint_id"),
    (lambda rows: rows[0].update(complaint_id="12A"), "non-numeric"),
    (lambda rows: rows[0].update(date_received="2024-12-31"), "outside"),
    (lambda rows: rows[0].update(date_received="2025-02-15"), "sample_stratum"),
    (lambda rows: rows[0].update(date_sent_to_company="2025-01-01"), "before date_received"),
    (lambda rows: rows[0].update(date_received="15/01/2025"), "date_received has 1 nulls"),
    (lambda rows: rows[0].update(timely="Maybe"), "timely"),
    (lambda rows: rows[0].update(tags="Student"), "unknown tags"),
    (lambda rows: rows[0].update(_sample_weight=2.5), "sample_weight"),
    (lambda rows: rows[0].update(_stratum_population=7), "population"),
    (lambda rows: rows[0].update(company=None), "company has 1 nulls"),
    (lambda rows: rows.pop(), "rows"),
])
def test_validation_catches_problems(small_design, mutate, message):
    rows = good_rows()
    mutate(rows)
    failures, _ = run(rows)
    assert any(message in f for f in failures), failures


def test_unknown_state_is_a_warning_not_a_failure(small_design):
    rows = good_rows()
    rows[0]["state"] = "ZZ"
    failures, warnings = run(rows)
    assert failures == [] and warnings == ["unrecognised state values: ['ZZ']"]


# --- notebook parity ---------------------------------------------------------

SHARED_CONSTANTS = [
    "US_STATES", "DISTRICT", "TERRITORIES", "MILITARY", "STATE_NAME_TO_CODE", "KNOWN_TAGS",
    "TEXT_COLUMNS", "REQUIRED_NON_NULL", "NULLABLE_COLUMNS", "WHITESPACE_PATTERN",
    "DATE_PATTERN", "ID_PATTERN", "TAG_SPLIT_PATTERN", "DATE_MIN", "DATE_MAX",
    "EXPECTED_ROWS", "EXPECTED_STRATA", "EXPECTED_PER_STRATUM", "EXPECTED_POPULATION",
    "WEIGHT_TOLERANCE", "SILVER_COLUMNS", "LINEAGE_COLUMNS", "HASH_EXCLUDED",
]


@pytest.mark.parametrize("name", SHARED_CONSTANTS)
def test_notebook_constants_match_reference(name):
    assert notebook_constant(SILVER_NB, name) == getattr(sr, name)


def test_silver_notebook_compiles_in_databricks_format():
    text = SILVER_NB.read_text()
    assert text.startswith("# Databricks notebook source\n")
    compile(text, str(SILVER_NB), "exec")


def test_rules_only_read_columns_that_exist_in_bronze():
    bronze_columns = {name for name, _ in notebook_constant(BRONZE_NB, "EXPECTED_COLUMNS")}
    row = {c: None for c in bronze_columns}
    accessed = set()

    class Spy(dict):
        def get(self, key, default=None):
            accessed.add(key)
            return super().get(key, default)

    transform_record(Spy(row))
    assert accessed <= bronze_columns, accessed - bronze_columns


@pytest.mark.skipif(not SAMPLE.exists(), reason="local sample not present")
def test_expected_profile_matches_reference_on_real_bronze_data():
    rows = pq.read_table(SAMPLE).to_pylist()
    silver = [transform_record(r) for r in rows]
    assert validate(silver) == ([], [])
    assert notebook_constant(SILVER_NB, "EXPECTED_PROFILE") == profile(silver)
