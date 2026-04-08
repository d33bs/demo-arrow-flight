#!/bin/bash
#SBATCH --job-name=flight-pipe-produce
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8815}
RAW_KEY=${RAW_KEY:-pipeline-raw}
ROWS=${ROWS:-10}
HEIGHT=${HEIGHT:-32}
WIDTH=${WIDTH:-32}
SEED=${SEED:-17}

uv run demo-arrow-flight pipeline-produce \
  --host "${HOST}" \
  --port "${PORT}" \
  --key "${RAW_KEY}" \
  --rows "${ROWS}" \
  --height "${HEIGHT}" \
  --width "${WIDTH}" \
  --seed "${SEED}"
