# PySpark S3 Lab

This lab sets up a local S3-compatible object store with MinIO, seeds it with a large NDJSON transaction dataset, includes a simple linear processing pipeline example, and adds a Spark version of the same revenue-by-state workload.

Current scope:

- Start MinIO with Docker Compose
- Create a `spark-demo` bucket
- Seed `transactions/` with synthetic data
- Run a linear processing pipeline over the seeded files
- Run the same revenue-by-state aggregation with Spark
- Prepare a dataset that Spark can read from `s3a://spark-demo/transactions/`

## Lab Layout

- [compose.yml](/Users/amoney/pyspark_example/compose.yml) starts MinIO locally
- [seed_minio.py](/Users/amoney/pyspark_example/seed_minio.py) generates and uploads synthetic transaction data
- [setup_test_data.sh](/Users/amoney/pyspark_example/setup_test_data.sh) bootstraps the environment and runs the seed step
- [2012_script.py](/Users/amoney/pyspark_example/2012_script.py) demonstrates a linear processing pipeline by reading the NDJSON files directly from MinIO and summing revenue by state
- [spark_revenue_by_state.py](/Users/amoney/pyspark_example/spark_revenue_by_state.py) reads the same dataset with Spark and computes transaction counts plus total revenue by state
- [requirements.txt](/Users/amoney/pyspark_example/requirements.txt) lists Python dependencies

## Prerequisites

- Python 3
- Docker with `docker compose`
- `curl`

## What Gets Created

The seed script writes newline-delimited JSON files into MinIO:

- Bucket: `spark-demo`
- Prefix: `transactions/`
- File pattern: `part-00000.ndjson`
- Files: `1,000`
- Records per file: `100,000`
- Total records: `100,000,000`
- End product in MinIO: `18.3 GiB - 1000 Objects`

Each record has this shape:

```json
{
  "transaction_id": "uuid",
  "customer_id": 123456,
  "state": "MD",
  "product_category": "electronics",
  "amount": 149.95,
  "created_at": "2026-06-12T12:34:56.000000+00:00"
}
```

## Quick Start

Run the full setup:

```bash
./setup_test_data.sh
```

This script will:

1. Create a local virtual environment in `.venv`
2. Install `boto3`
3. Start MinIO
4. Wait for MinIO to become healthy
5. Seed the dataset

Example output:

```text
$ ./setup_test_data.sh
=====================================
 Spark Demo Setup
=====================================

[1/5] Creating virtual environment...

[2/5] Activating environment...

[3/5] Installing dependencies...
Requirement already satisfied: pip in ./.venv/lib/python3.14/site-packages (25.3)
Collecting pip
  Downloading pip-26.1.2-py3-none-any.whl.metadata (4.6 kB)
Downloading pip-26.1.2-py3-none-any.whl (1.8 MB)
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1.8/1.8 MB 17.1 MB/s  0:00:00
Installing collected packages: pip
  Attempting uninstall: pip
    Found existing installation: pip 25.3
    Uninstalling pip-25.3:
      Successfully uninstalled pip-25.3
Successfully installed pip-26.1.2
Collecting boto3
  Downloading boto3-1.43.29-py3-none-any.whl.metadata (6.6 kB)
Collecting botocore<1.44.0,>=1.43.29 (from boto3)
  Downloading botocore-1.43.29-py3-none-any.whl.metadata (5.6 kB)
Collecting jmespath<2.0.0,>=0.7.1 (from boto3)
  Downloading jmespath-1.1.0-py3-none-any.whl.metadata (7.6 kB)
Collecting s3transfer<0.19.0,>=0.18.0 (from boto3)
  Downloading s3transfer-0.18.0-py3-none-any.whl.metadata (1.7 kB)
Collecting python-dateutil<3.0.0,>=2.1 (from botocore<1.44.0,>=1.43.29->boto3)
  Downloading python_dateutil-2.9.0.post0-py2.py3-none-any.whl.metadata (8.4 kB)
Collecting urllib3!=2.2.0,<3,>=1.25.4 (from botocore<1.44.0,>=1.43.29->boto3)
  Downloading urllib3-2.7.0-py3-none-any.whl.metadata (6.9 kB)
Collecting six>=1.5 (from python-dateutil<3.0.0,>=2.1->botocore<1.44.0,>=1.43.29->boto3)
  Downloading six-1.17.0-py2.py3-none-any.whl.metadata (1.7 kB)
Downloading boto3-1.43.29-py3-none-any.whl (140 kB)
Downloading botocore-1.43.29-py3-none-any.whl (15.2 MB)
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 15.2/15.2 MB 26.5 MB/s  0:00:00
Downloading jmespath-1.1.0-py3-none-any.whl (20 kB)
Downloading python_dateutil-2.9.0.post0-py2.py3-none-any.whl (229 kB)
Downloading s3transfer-0.18.0-py3-none-any.whl (88 kB)
Downloading urllib3-2.7.0-py3-none-any.whl (131 kB)
Downloading six-1.17.0-py2.py3-none-any.whl (11 kB)
Installing collected packages: urllib3, six, jmespath, python-dateutil, botocore, s3transfer, boto3
Successfully installed boto3-1.43.29 botocore-1.43.29 jmespath-1.1.0 python-dateutil-2.9.0.post0 s3transfer-0.18.0 six-1.17.0 urllib3-2.7.0

[4/5] Starting MinIO...
[+] Running 1/1
 ✔ Container minio  Running                                                                                                                                                                     0.0s

Waiting for MinIO to start...
MinIO is ready.

[5/5] Seeding data into MinIO...
Creating 100,000,000 records
Writing to s3://spark-demo/transactions/
Uploaded transactions/part-00000.ndjson | 100,000/100,000,000 records | file: 0.61s | total: 0.61s | 163,834 records/sec
Uploaded transactions/part-00001.ndjson | 200,000/100,000,000 records | file: 0.62s | total: 1.23s | 162,564 records/sec
Uploaded transactions/part-00002.ndjson | 300,000/100,000,000 records | file: 0.62s | total: 1.85s | 162,573 records/sec
Uploaded transactions/part-00003.ndjson | 400,000/100,000,000 records | file: 0.62s | total: 2.47s | 162,054 records/sec
Uploaded transactions/part-00004.ndjson | 500,000/100,000,000 records | file: 0.62s | total: 3.09s | 161,713 records/sec
Uploaded transactions/part-00005.ndjson | 600,000/100,000,000 records | file: 0.60s | total: 3.69s | 162,484 records/sec
Uploaded transactions/part-00006.ndjson | 700,000/100,000,000 records | file: 0.62s | total: 4.31s | 162,467 records/sec
Uploaded transactions/part-00007.ndjson | 800,000/100,000,000 records | file: 0.61s | total: 4.92s | 162,632 records/sec
...
Uploaded transactions/part-00994.ndjson | 99,500,000/100,000,000 records | file: 0.62s | total: 616.72s | 161,337 records/sec
Uploaded transactions/part-00995.ndjson | 99,600,000/100,000,000 records | file: 0.61s | total: 617.34s | 161,339 records/sec
Uploaded transactions/part-00996.ndjson | 99,700,000/100,000,000 records | file: 0.61s | total: 617.95s | 161,341 records/sec
Uploaded transactions/part-00997.ndjson | 99,800,000/100,000,000 records | file: 0.62s | total: 618.57s | 161,340 records/sec
Uploaded transactions/part-00998.ndjson | 99,900,000/100,000,000 records | file: 0.61s | total: 619.18s | 161,341 records/sec
Uploaded transactions/part-00999.ndjson | 100,000,000/100,000,000 records | file: 0.61s | total: 619.79s | 161,345 records/sec

Done.
Total records: 100,000,000
Total files: 1,000
Total time: 619.79s
Average throughput: 161,345 records/sec

=====================================
Setup complete!
=====================================

MinIO Console: http://localhost:9001
Username: admin
Password: password123

Spark can read from:
s3a://spark-demo/transactions/
```

## Manual Setup

If you want to run each step yourself:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
docker compose up -d
python seed_minio.py
```

## Linear Processing Example

The lab also includes [2012_script.py](/Users/amoney/pyspark_example/2012_script.py) as a simple end-to-end example of a linear processing pipeline.

What it does:

1. Lists all objects under `transactions/` in the `spark-demo` bucket
2. Reads each NDJSON file from MinIO
3. Parses each record one line at a time
4. Aggregates total revenue by `state`
5. Prints throughput while processing and a final summary

Run it after the seed step completes:

```bash
source .venv/bin/activate
python 2012_script.py
```

This is intentionally a straightforward Python pipeline. It is useful as a baseline before rewriting the same workload in PySpark.

Sample output:

```text
$ python 2012_script.py
Reading transactions/part-00000.ndjson
100,000 records | 305,220 records/sec
Reading transactions/part-00001.ndjson
200,000 records | 338,730 records/sec
Reading transactions/part-00002.ndjson
...
Reading transactions/part-00997.ndjson
99,800,000 records | 363,998 records/sec
Reading transactions/part-00998.ndjson
99,900,000 records | 364,005 records/sec
Reading transactions/part-00999.ndjson
100,000,000 records | 363,992 records/sec

Revenue by state:
CA: $2,527,275,938.51
FL: $2,525,708,955.14
IL: $2,525,265,743.70
MD: $2,525,816,581.76
NC: $2,523,087,439.59
NY: $2,526,208,987.55
PA: $2,524,619,003.08
TX: $2,524,173,634.13
VA: $2,524,810,057.78
WA: $2,523,041,272.07

Processed 100,000,000 records in 274.7s
```

On this run, the linear Python version took about 4 1/2 minutes to process the full dataset.

## Spark Processing Example

The lab also includes [spark_revenue_by_state.py](/Users/amoney/pyspark_example/spark_revenue_by_state.py), which runs the same revenue-by-state workflow with Spark.

What it does:

1. Creates a `SparkSession`
2. Connects Spark to MinIO with `s3a`
3. Reads all JSON files from `s3a://spark-demo/transactions/`
4. Groups by `state`
5. Computes `transaction_count` and `total_revenue`
6. Displays the aggregated result

This script is the distributed version of the same exercise shown in `2012_script.py`, making it useful for comparing a simple linear pipeline to a Spark-based approach.

To submit the job to the Docker Spark cluster:

```bash
docker compose up -d
docker cp spark_revenue_by_state.py spark-master:/tmp/spark_revenue_by_state.py
docker exec -it spark-master sh -lc '
mkdir -p /tmp/ivy /tmp/ivy-cache &&
export IVY_HOME=/tmp/ivy &&
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.jars.ivy=/tmp/ivy-cache \
  --packages org.apache.hadoop:hadoop-aws:3.4.2 \
  /tmp/spark_revenue_by_state.py
'
```

You can monitor the job in the Spark master UI at `http://localhost:8080`.

If you see this error:

```text
java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found
```

it means the Spark image does not include the S3A connector jars by default. The `--packages` flag in the command above pulls in the required dependencies for reading from MinIO with `s3a://`.

For this lab, the Spark `4.1.2` container includes Hadoop `3.4.2`, so `hadoop-aws:3.4.2` should be used to match the bundled Hadoop version.

If you see this error:

```text
java.io.FileNotFoundException: /nonexistent/.ivy2.5.2/cache/... (No such file or directory)
```

it means Ivy is trying to write dependency metadata into a non-writable home directory inside the container. The command above fixes that by setting `IVY_HOME=/tmp/ivy` and `spark.jars.ivy=/tmp/ivy-cache`.

## Accessing MinIO

- S3 API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Username: `admin`
- Password: `password123`

## Reading From Spark

Once seeded, Spark can read the dataset from:

```text
s3a://spark-demo/transactions/
```

Typical Spark configuration for this lab:

```python
spark.conf.set("fs.s3a.endpoint", "http://localhost:9000")
spark.conf.set("fs.s3a.access.key", "admin")
spark.conf.set("fs.s3a.secret.key", "password123")
spark.conf.set("fs.s3a.path.style.access", "true")
spark.conf.set("fs.s3a.connection.ssl.enabled", "false")
```

## Notes

- The current seed volume is intentionally large and may take significant time and disk space.
- Data is randomly generated each run.
- Timestamps are written in UTC ISO 8601 format.
- The seeded end product in MinIO is expected to be about `18.3 GiB - 1000 Objects`.
- `2012_script.py` is a non-Spark baseline for discussing how a linear pipeline behaves before moving to distributed processing.
- `spark_revenue_by_state.py` performs the same core aggregation in Spark so you can compare the two approaches directly.

## Cleanup

Stop MinIO:

```bash
docker compose down
```

Stop MinIO and remove persisted object storage:

```bash
docker compose down -v
```
