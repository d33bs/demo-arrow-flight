#!/bin/bash
#SBATCH --job-name=flight-receiver
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8815}
KEY=${KEY:-slurm-ome-dataset}

echo "[receiver] receiving dataset from ${HOST}:${PORT} key=${KEY}"
uv run demo-arrow-flight parquet-receive \
  --host "${HOST}" \
  --port "${PORT}" \
  --key "${KEY}"
