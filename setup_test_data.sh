#!/usr/bin/env bash

set -euo pipefail

echo "====================================="
echo " Spark Demo Setup"
echo "====================================="

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found"
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found"
  exit 1
fi

echo
echo "[1/5] Creating virtual environment..."
python3 -m venv .venv

echo
echo "[2/5] Activating environment..."
source .venv/bin/activate

echo
echo "[3/5] Installing dependencies..."
pip install --upgrade pip
pip install boto3

echo
echo "[4/5] Starting MinIO..."
docker compose up -d

echo
echo "Waiting for MinIO to start..."

for i in {1..30}; do
  if curl -s http://localhost:9000/minio/health/live >/dev/null; then
    echo "MinIO is ready."
    break
  fi

  echo "Waiting..."
  sleep 2
done

echo
echo "[5/5] Seeding data into MinIO..."
python seed_minio.py

echo
echo "====================================="
echo "Setup complete!"
echo "====================================="
echo
echo "MinIO Console: http://localhost:9001"
echo "Username: admin"
echo "Password: password123"
echo
echo "Spark can read from:"
echo "s3a://spark-demo/transactions/"
