#!/bin/bash
#SBATCH --job-name=flight-server-alpine
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:45:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/load_uv_env.sh"

HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8815}

echo "[server] node=$(hostname -s) listening on ${HOST}:${PORT}"
uv run demo-arrow-flight server --host "${HOST}" --port "${PORT}"
