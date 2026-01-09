import boto3
import json
import time
import random
from faker import Faker
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# CONFIGURATION - UPDATE THESE
BUCKET_NAME = "sentinel-flow-raw-322b0d3a" 
REGION = "eu-west-2"

fake = Faker()

# This uses the credentials you set up in 'aws configure'
s3 = boto3.client('s3', region_name=REGION)

def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(100, 999),
        "amount": round(random.uniform(10.0, 2000.0), 2),
        "currency": "GBP",
        "merchant": fake.company(),
        "category": random.choice(["Retail", "Food", "Tech", "Travel"]),
        "timestamp": datetime.now().isoformat(),
        "location": "London"
    }

def stream_data():
    print(f"Checking connection to bucket: {BUCKET_NAME}...")
    try:
        # Initial test to see if we can talk to the bucket
        s3.head_bucket(Bucket=BUCKET_NAME)
        print("Connection Successful! Starting stream...")
        
        while True:
            data = generate_transaction()
            file_name = f"raw/transaction_{data['transaction_id']}.json"
            
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(data)
            )
            
            print(f"SUCCESS: Uploaded {file_name} | Amount: £{data['amount']}")
            time.sleep(5) 
            
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    stream_data()