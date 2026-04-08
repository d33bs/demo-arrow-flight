#!/bin/bash
#SBATCH --job-name=flight-pipe-consume
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8815}
PROCESSED_KEY=${PROCESSED_KEY:-pipeline-processed}

uv run demo-arrow-flight pipeline-consume \
  --host "${HOST}" \
  --port "${PORT}" \
  --key "${PROCESSED_KEY}"
