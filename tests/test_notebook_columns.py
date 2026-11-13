"""Static checks on Databricks notebooks for Spark column-name hazards.

Spark resolves column names case-insensitively by default
(spark.sql.caseSensitive = false). Two columns whose names differ only in case
(e.g. ``n_h`` and ``N_h``) can be created, but any later reference to either is
an AMBIGUOUS_REFERENCE error at runtime. The Python reference implementations
use case-sensitive dict keys, so this class of bug is invisible to them; this
test scans every notebook for column names that collide case-insensitively.
"""

import ast
import re
from collections import defaultdict
from pathlib import Path

import pytest

NOTEBOOKS = sorted((Path(__file__).resolve().parents[1] / "databricks/notebooks").glob("*.py"))

# Call targets whose string arguments are Spark column names.
COLUMN_CALLS = {
    "alias", "col", "withColumn", "withColumnRenamed", "select", "groupBy", "join", "drop",
    "orderBy", "partitionBy", "sum", "max", "min", "avg", "count", "countDistinct", "coalesce",
    "sqrt", "explode", "struct", "fillna", "eqNullSafe", "date_format", "substring",
    "array_contains", "size", "first", "collect_set", "sort_array", "add_months", "isin",
}
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def column_names(path):
    """String literals used as column names, with the line numbers where they appear."""
    names = defaultdict(set)

    def collect(node, line):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and IDENTIFIER.match(node.value):
            names[node.value].add(line)
        elif isinstance(node, (ast.List, ast.Tuple)):
            for element in node.elts:
                collect(element, line)

    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name in COLUMN_CALLS:
                for arg in list(node.args) + [kw.value for kw in node.keywords if kw.arg == "subset"]:
                    collect(arg, node.lineno)
    return names


def case_collisions(names):
    groups = defaultdict(set)
    for name in names:
        groups[name.lower()].add(name)
    return {k: sorted(v) for k, v in groups.items() if len(v) > 1}


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda p: p.name)
def test_no_case_insensitive_column_name_collisions(notebook):
    names = column_names(notebook)
    collisions = case_collisions(names)
    detail = {k: {n: sorted(names[n]) for n in v} for k, v in collisions.items()}
    assert not collisions, f"Spark would treat these as the same column (name -> lines): {detail}"


def test_detector_flags_the_original_n_h_bug():
    """Regression guard: the exact pattern that failed in Databricks must be detected."""
    source = ('strata = silver.groupBy("stratum").agg(\n'
              '    F.count(F.lit(1)).alias("n_h"), F.max("stratum_population").alias("N_h"))\n'
              'w = F.col("N_h") / F.col("n_h")\n')
    tmp_names = defaultdict(set)
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", None) in COLUMN_CALLS:
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    tmp_names[arg.value].add(node.lineno)
    assert case_collisions(tmp_names) == {"n_h": ["N_h", "n_h"]}


def test_scanner_sees_the_gold_notebook_columns():
    gold = next(p for p in NOTEBOOKS if p.name == "04_gold_build.py")
    names = column_names(gold)
    assert {"est_share", "ci95_low", "month_label", "stratum"} <= set(names)
