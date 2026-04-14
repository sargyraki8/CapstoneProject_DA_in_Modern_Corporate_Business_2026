#!/usr/bin/env python
# Exports the two reporting tables to CSV files for use in Tableau Public.
# Tableau Public Desktop cannot connect directly to BigQuery (no BQ connector
# in the free version on Windows), so CSVs are the easiest path.
#
# Output: D:/Projects/Capstone/exports/
#   - rep_revenue_per_period.csv
#   - rep_revenue_per_customer_and_period.csv

from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Projects/Capstone/credentials/sa-key.json"

PROJECT = "capstoneproject-493015"
DATASET = "reporting_db"
OUT_DIR = "D:/Projects/Capstone/exports"
TABLES = ["rep_revenue_per_period", "rep_revenue_per_customer_and_period"]

os.makedirs(OUT_DIR, exist_ok=True)
client = bigquery.Client(project=PROJECT, location="europe-west1")

for table in TABLES:
    print(f"Exporting {table}...")
    df = client.query(f"SELECT * FROM `{PROJECT}.{DATASET}.{table}`").to_dataframe()
    out_path = os.path.join(OUT_DIR, f"{table}.csv")
    df.to_csv(out_path, index=False)
    print(f"  -> {out_path}  ({len(df):,} rows)")

print("\nDone. Open these CSVs in Tableau Public Desktop:")
for t in TABLES:
    print(f"  {OUT_DIR}/{t}.csv")
