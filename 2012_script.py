import json
import time
from collections import defaultdict

import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

BUCKET = "spark-demo"
PREFIX = "transactions/"

totals = defaultdict(float)

start = time.time()
records = 0

# List all NDJSON files in MinIO
paginator = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
    for obj in page.get("Contents", []):

        key = obj["Key"]

        print(f"Reading {key}")

        response = s3.get_object(
            Bucket=BUCKET,
            Key=key,
        )

        body = response["Body"]

        for line in body.iter_lines():
            record = json.loads(line)

            totals[record["state"]] += record["amount"]
            records += 1

            if records % 100_000 == 0:
                elapsed = time.time() - start
                rate = records / elapsed

                print(
                    f"{records:,} records | "
                    f"{rate:,.0f} records/sec"
                )

elapsed = time.time() - start

print("\nRevenue by state:")
for state, amount in sorted(totals.items()):
    print(f"{state}: ${amount:,.2f}")

print()
print(f"Processed {records:,} records in {elapsed:.1f}s")
