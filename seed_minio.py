import json
import random
import time
import uuid
from io import BytesIO
from datetime import datetime, timezone

import boto3
from botocore.client import Config


ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password123"

BUCKET = "spark-demo"
PREFIX = "transactions"

NUM_FILES = 1000
RECORDS_PER_FILE = 100_000

states = ["MD", "VA", "PA", "NY", "CA", "TX", "FL", "IL", "WA", "NC"]
categories = ["electronics", "clothing", "books", "home", "grocery", "sports"]


s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)


def ensure_bucket(bucket: str):
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)


def make_record(i: int) -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, 10_000_000),
        "state": random.choice(states),
        "product_category": random.choice(categories),
        "amount": round(random.uniform(5, 500), 2),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    ensure_bucket(BUCKET)

    total_records = NUM_FILES * RECORDS_PER_FILE
    start = time.time()

    print(f"Creating {total_records:,} records")
    print(f"Writing to s3://{BUCKET}/{PREFIX}/")

    for file_num in range(NUM_FILES):
        file_start = time.time()

        buffer = BytesIO()

        base_id = file_num * RECORDS_PER_FILE

        for i in range(RECORDS_PER_FILE):
            record = make_record(base_id + i)
            buffer.write(json.dumps(record).encode("utf-8"))
            buffer.write(b"\n")

        key = f"{PREFIX}/part-{file_num:05d}.ndjson"

        buffer.seek(0)

        s3.upload_fileobj(
            buffer,
            BUCKET,
            key,
            ExtraArgs={"ContentType": "application/x-ndjson"},
        )

        elapsed = time.time() - start
        file_elapsed = time.time() - file_start
        records_done = (file_num + 1) * RECORDS_PER_FILE
        records_per_sec = records_done / elapsed

        print(
            f"Uploaded {key} | "
            f"{records_done:,}/{total_records:,} records | "
            f"file: {file_elapsed:.2f}s | "
            f"total: {elapsed:.2f}s | "
            f"{records_per_sec:,.0f} records/sec"
        )

    total_elapsed = time.time() - start

    print()
    print("Done.")
    print(f"Total records: {total_records:,}")
    print(f"Total files: {NUM_FILES:,}")
    print(f"Total time: {total_elapsed:.2f}s")
    print(f"Average throughput: {total_records / total_elapsed:,.0f} records/sec")


if __name__ == "__main__":
    main()
