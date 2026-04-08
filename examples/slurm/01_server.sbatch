#!/bin/bash
#SBATCH --job-name=flight-server
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:20:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

set -euo pipefail

HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8815}

echo "[server] starting Arrow Flight server at ${HOST}:${PORT}"
uv run demo-arrow-flight server --host "${HOST}" --port "${PORT}"
