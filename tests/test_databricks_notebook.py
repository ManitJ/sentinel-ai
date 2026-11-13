"""Keep the Databricks Bronze notebook's contract in sync with the sampler."""

import ast
from pathlib import Path

import pyarrow as pa

from ingestion.bulk_sample import SAMPLE_SCHEMA

NOTEBOOK = Path(__file__).resolve().parents[1] / "databricks/notebooks/01_bronze_ingest.py"
ARROW_TO_SPARK = {pa.string(): "string", pa.int64(): "bigint", pa.float64(): "double"}


def notebook_constant(name):
    tree = ast.parse(NOTEBOOK.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in notebook")


def test_notebook_is_databricks_source_format():
    text = NOTEBOOK.read_text()
    assert text.startswith("# Databricks notebook source\n")
    assert "# COMMAND ----------" in text
    compile(text, str(NOTEBOOK), "exec")  # syntax check (dbutils/spark resolve at runtime)


def test_expected_columns_match_sample_schema():
    expected = [(f.name, ARROW_TO_SPARK[f.type]) for f in SAMPLE_SCHEMA]
    assert notebook_constant("EXPECTED_COLUMNS") == expected
    assert len(expected) == 24


def test_expected_counts_match_sampling_design():
    assert notebook_constant("EXPECTED_ROWS") == 50_006
    assert notebook_constant("EXPECTED_STRATA") * notebook_constant("EXPECTED_PER_STRATUM") == 50_006
