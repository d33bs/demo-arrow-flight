#!/bin/bash
#SBATCH --job-name=flight-sender
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8815}
KEY=${KEY:-slurm-ome-dataset}
PARQUET_PATH=${PARQUET_PATH:-/tmp/random_ome_dataset.parquet}
ROWS=${ROWS:-10}
HEIGHT=${HEIGHT:-32}
WIDTH=${WIDTH:-32}
SEED=${SEED:-101}
BATCH_ROWS=${BATCH_ROWS:-3}

echo "[sender] generating dataset at ${PARQUET_PATH}"
uv run demo-arrow-flight parquet-generate \
  --output "${PARQUET_PATH}" \
  --rows "${ROWS}" \
  --height "${HEIGHT}" \
  --width "${WIDTH}" \
  --seed "${SEED}"

echo "[sender] streaming dataset to ${HOST}:${PORT} key=${KEY}"
uv run demo-arrow-flight parquet-stream \
  --host "${HOST}" \
  --port "${PORT}" \
  --key "${KEY}" \
  --parquet-path "${PARQUET_PATH}" \
  --batch-rows "${BATCH_ROWS}"
