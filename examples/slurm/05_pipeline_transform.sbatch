#!/bin/bash
#SBATCH --job-name=flight-pipe-transform
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8815}
RAW_KEY=${RAW_KEY:-pipeline-raw}
PROCESSED_KEY=${PROCESSED_KEY:-pipeline-processed}
STAGE_NAME=${STAGE_NAME:-preprocess}

uv run demo-arrow-flight pipeline-transform \
  --host "${HOST}" \
  --port "${PORT}" \
  --input-key "${RAW_KEY}" \
  --output-key "${PROCESSED_KEY}" \
  --stage-name "${STAGE_NAME}"
