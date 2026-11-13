
"""CFPB Consumer Complaint Data Ingestion.

Sentinel AI — Financial Data Lakehouse
"""
from pathlib import Path
import pandas as pd
import time
import requests

CFPB_API_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"


def fetch_complaints(size=5, max_retries=3):
    """Retrieve CFPB complaints with retry handling."""

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Fetching complaints (attempt {attempt}/{max_retries})...")

            response = requests.get(
                CFPB_API_URL,
                params={"size": size},
                timeout=(10, 60),
            )
            response.raise_for_status()

            payload = response.json()

            complaints = [hit["_source"] for hit in payload["hits"]["hits"]]

            for complaint in complaints:
                print("-" * 50)
                print(f"Complaint ID: {complaint.get('complaint_id')}")
                print(f"Product: {complaint.get('product')}")
                print(f"Company: {complaint.get('company')}")
                print(f"Date received: {complaint.get('date_received')}")

            return complaints

        except (requests.RequestException, ValueError, KeyError) as error:
            print(f"Attempt {attempt} failed: {error}")

            if attempt < max_retries:
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("All attempts failed.")
                raise



if __name__ == "__main__":
    complaints = fetch_complaints()

    if complaints:
        output_dir = Path("data/raw")
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / "cfpb_sample.parquet"

        df = pd.DataFrame(complaints)
        df.to_parquet(output_file, index=False)

        print(f"\nSaved {len(df)} records to {output_file}")
        print(f"Dataset contains {len(df.columns)} columns.")
