"""Export the API monthly reference JSON to Parquet for upload to Databricks.

    PYTHONPATH=src python -m lakehouse.reference_export

Writes (outside Git, under data/reference/upload/):
    cfpb_api_monthly_totals.parquet   month, population_complaints, product, fetched_at
    cfpb_api_monthly_facets.parquet   month, dimension, category, exact_count, is_residual,
                                      product, fetched_at
Residual rows (category NULL) hold the exact count of complaints with no value
for that dimension (total - sum of listed categories), so every (month,
dimension) sums to the monthly total.
"""

import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lakehouse.gold_metrics import reference_tables

TOTALS_SCHEMA = pa.schema([("month", pa.string()), ("population_complaints", pa.int64()),
                           ("product", pa.string()), ("fetched_at", pa.string())])
FACETS_SCHEMA = pa.schema([("month", pa.string()), ("dimension", pa.string()),
                           ("category", pa.string()), ("exact_count", pa.int64()),
                           ("is_residual", pa.bool_()), ("product", pa.string()),
                           ("fetched_at", pa.string())])
DEFAULT_SOURCE = "data/reference/cfpb_api_monthly_credit_reporting.json"
DEFAULT_OUT = "data/reference/upload"


def export(source, out_dir):
    ref = json.loads(Path(source).read_text())
    totals, facets = reference_tables(ref)
    for f in facets:
        f.update(product=ref["product"], fetched_at=ref["fetched_at"])
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {"totals": out / "cfpb_api_monthly_totals.parquet",
             "facets": out / "cfpb_api_monthly_facets.parquet"}
    pq.write_table(pa.Table.from_pylist(totals, schema=TOTALS_SCHEMA), paths["totals"])
    pq.write_table(pa.Table.from_pylist(facets, schema=FACETS_SCHEMA), paths["facets"])
    return {name: {"path": str(p), "rows": pq.read_metadata(p).num_rows, "bytes": p.stat().st_size}
            for name, p in paths.items()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--out-dir", default=DEFAULT_OUT)
    args = parser.parse_args()
    print(json.dumps(export(args.source, args.out_dir), indent=2))
