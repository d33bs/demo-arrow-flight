#!/bin/bash
#SBATCH --job-name=flight-benchmark
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:20:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G

set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8815}
PARQUET_PATH=${PARQUET_PATH:-/tmp/random_ome_dataset.parquet}
ROWS=${ROWS:-20}
HEIGHT=${HEIGHT:-64}
WIDTH=${WIDTH:-64}
SEED=${SEED:-29}
KEY_PREFIX=${KEY_PREFIX:-benchmark-key}
BATCH_ROWS=${BATCH_ROWS:-4}
REPEATS=${REPEATS:-3}
OUTPUT_CSV=${OUTPUT_CSV:-/tmp/demo_arrow_flight_benchmark.csv}

uv run demo-arrow-flight parquet-generate \
  --output "${PARQUET_PATH}" \
  --rows "${ROWS}" \
  --height "${HEIGHT}" \
  --width "${WIDTH}" \
  --seed "${SEED}"

uv run demo-arrow-flight benchmark-transport \
  --host "${HOST}" \
  --port "${PORT}" \
  --parquet-path "${PARQUET_PATH}" \
  --key-prefix "${KEY_PREFIX}" \
  --batch-rows "${BATCH_ROWS}" \
  --repeats "${REPEATS}" \
  --output-csv "${OUTPUT_CSV}"

uv run demo-arrow-flight benchmark-overhead \
  --host "${HOST}" \
  --port "${PORT}" \
  --rows "${ROWS}" \
  --height "${HEIGHT}" \
  --width "${WIDTH}" \
  --seed "${SEED}" \
  --repeats "${REPEATS}" \
  --key-prefix "${KEY_PREFIX}-overhead" \
  --output-csv "${OUTPUT_CSV%.csv}_overhead.csv"
