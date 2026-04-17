#!/bin/bash
#SBATCH --job-name=flight-benchmark-alpine
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=00:20:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/load_uv_env.sh"

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8815}

BASE_OUTPUT_DIR=${BASE_OUTPUT_DIR:-${SLURM_SCRATCH:-/scratch/alpine/${USER:-${LOGNAME:-unknown}}}}
OUTPUT_DIR=${OUTPUT_DIR:-${BASE_OUTPUT_DIR}/demo_arrow_flight_benchmark}
PARQUET_PATH=${PARQUET_PATH:-${OUTPUT_DIR}/random_ome_dataset.parquet}
OUTPUT_CSV=${OUTPUT_CSV:-${OUTPUT_DIR}/benchmark_transport.csv}
OVERHEAD_CSV=${OVERHEAD_CSV:-${OUTPUT_DIR}/benchmark_overhead.csv}

ROWS=${ROWS:-20}
HEIGHT=${HEIGHT:-64}
WIDTH=${WIDTH:-64}
SEED=${SEED:-29}
KEY_PREFIX=${KEY_PREFIX:-benchmark-key}
BATCH_ROWS=${BATCH_ROWS:-4}
REPEATS=${REPEATS:-3}

mkdir -p "${OUTPUT_DIR}"

echo "[benchmark] node=$(hostname -s) target=${HOST}:${PORT}"

ready=0
for _ in $(seq 1 60); do
  if bash -c "</dev/tcp/${HOST}/${PORT}" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done

if [[ "${ready}" -ne 1 ]]; then
  echo "[benchmark] server ${HOST}:${PORT} did not become reachable in time" >&2
  exit 1
fi

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
  --output-csv "${OVERHEAD_CSV}"

echo "[benchmark] complete: transport_csv=${OUTPUT_CSV} overhead_csv=${OVERHEAD_CSV}"
