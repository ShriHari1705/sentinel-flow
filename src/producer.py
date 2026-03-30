import boto3
import json
import time
import uuid
import pandas as pd
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError

# CONFIGURATION
BUCKET_NAME = "sentinel-flow-raw-322b0d3a"
REGION = "eu-west-2"
PAYSIM_FILE = "src/paysim1.csv"
BATCH_SIZE = 10
SLEEP_INTERVAL = 0.5

s3 = boto3.client('s3', region_name=REGION)

def load_paysim_data():
    """Load PaySim CSV and return as an iterable of row dicts."""
    print(f"Loading PaySim dataset from {PAYSIM_FILE}...")
    df = pd.read_csv(PAYSIM_FILE, nrows=100000)
    print(f"Loaded {len(df):,} transactions. Starting stream...")
    return df.to_dict(orient='records')

def enrich_record(row):
    """
    Transform a raw PaySim row into a pipeline-ready JSON record.
    Adds a unique transaction_id and a real timestamp derived from step.
    """
    # step = hour offset from a base date
    base_date = datetime(2024, 1, 1)
    transaction_time = base_date + timedelta(hours=int(row['step']))

    return {
        "transaction_id": str(uuid.uuid4()),
        "step": int(row['step']),
        "type": row['type'],
        "amount": round(float(row['amount']), 2),
        "name_orig": row['nameOrig'],
        "old_balance_orig": round(float(row['oldbalanceOrg']), 2),
        "new_balance_orig": round(float(row['newbalanceOrig']), 2),
        "name_dest": row['nameDest'],
        "old_balance_dest": round(float(row['oldbalanceDest']), 2),
        "new_balance_dest": round(float(row['newbalanceDest']), 2),
        "is_fraud": int(row['isFraud']),
        "is_flagged_fraud": int(row['isFlaggedFraud']),
        "timestamp": transaction_time.isoformat(),
        "ingested_at": datetime.now().isoformat()
    }

def stream_data():
    """Read PaySim rows and upload to S3 in batches."""
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print("S3 connection successful.")
    except Exception as e:
        print(f"CRITICAL: Cannot connect to S3 bucket. {e}")
        return

    records = load_paysim_data()
    batch = []

    for row in records:
        enriched = enrich_record(row)
        batch.append(enriched)

        if len(batch) >= BATCH_SIZE:
            # Upload entire batch as one JSON file
            batch_id = enriched['transaction_id']
            file_name = f"raw/batch_{batch_id}.json"

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(batch)
            )
            print(f"Uploaded batch of {BATCH_SIZE} | Last type: {enriched['type']} | Amount: {enriched['amount']}")
            batch = []
            time.sleep(SLEEP_INTERVAL)

    print("All PaySim records streamed successfully.")

if __name__ == "__main__":
    stream_data()