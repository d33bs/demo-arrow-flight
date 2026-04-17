#!/bin/bash
#SBATCH --job-name=flight-benchmark-master
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --time=01:00:00
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/load_uv_env.sh"

PORT=${PORT:-8815}
BASE_OUTPUT_DIR=${BASE_OUTPUT_DIR:-${SLURM_SCRATCH:-/scratch/alpine/${USER:-${LOGNAME:-unknown}}}}
OUTPUT_DIR=${OUTPUT_DIR:-${BASE_OUTPUT_DIR}/demo_arrow_flight_master}
PARQUET_PATH=${PARQUET_PATH:-${OUTPUT_DIR}/random_ome_dataset.parquet}
TRANSPORT_CSV=${TRANSPORT_CSV:-${OUTPUT_DIR}/benchmark_transport.csv}
OVERHEAD_CSV=${OVERHEAD_CSV:-${OUTPUT_DIR}/benchmark_overhead.csv}
PIPELINE_CSV=${PIPELINE_CSV:-${OUTPUT_DIR}/benchmark_pipeline_io.csv}

ROWS=${ROWS:-20}
HEIGHT=${HEIGHT:-64}
WIDTH=${WIDTH:-64}
SEED=${SEED:-29}
BATCH_ROWS=${BATCH_ROWS:-4}
REPEATS=${REPEATS:-3}
KEY_PREFIX=${KEY_PREFIX:-benchmark-key}
PIPELINE_BATCH_COUNTS=${PIPELINE_BATCH_COUNTS:-160,320,640,1280,2560,5120}
PIPELINE_SEED=${PIPELINE_SEED:-41}

mkdir -p "${OUTPUT_DIR}"

mapfile -t ALLOCATED_NODES < <(scontrol show hostnames "${SLURM_JOB_NODELIST}")
if [[ "${#ALLOCATED_NODES[@]}" -lt 2 ]]; then
  echo "[master] need at least 2 allocated nodes; got ${#ALLOCATED_NODES[@]}" >&2
  exit 1
fi

SERVER_NODE="${ALLOCATED_NODES[0]}"
CLIENT_NODE="${ALLOCATED_NODES[1]}"

echo "[master] server node=${SERVER_NODE} client node=${CLIENT_NODE} port=${PORT}"

srun --nodes=1 --ntasks=1 --nodelist="${SERVER_NODE}" \
  uv run demo-arrow-flight server --host 0.0.0.0 --port "${PORT}" &
SERVER_STEP_PID=$!

cleanup() {
  if kill -0 "${SERVER_STEP_PID}" >/dev/null 2>&1; then
    kill "${SERVER_STEP_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_STEP_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

srun --nodes=1 --ntasks=1 --nodelist="${CLIENT_NODE}" bash -lc '
set -euo pipefail
for _ in $(seq 1 60); do
  if bash -c "</dev/tcp/'"${SERVER_NODE}"'/'"${PORT}"'" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done
exit 1
'

echo "[master] generating parquet once for shared transport benchmark"
srun --nodes=1 --ntasks=1 --nodelist="${CLIENT_NODE}" \
  uv run demo-arrow-flight parquet-generate \
    --output "${PARQUET_PATH}" \
    --rows "${ROWS}" \
    --height "${HEIGHT}" \
    --width "${WIDTH}" \
    --seed "${SEED}"

echo "[master] running benchmark-transport"
srun --nodes=1 --ntasks=1 --nodelist="${CLIENT_NODE}" \
  uv run demo-arrow-flight benchmark-transport \
    --host "${SERVER_NODE}" \
    --port "${PORT}" \
    --parquet-path "${PARQUET_PATH}" \
    --key-prefix "${KEY_PREFIX}" \
    --batch-rows "${BATCH_ROWS}" \
    --repeats "${REPEATS}" \
    --output-csv "${TRANSPORT_CSV}"

echo "[master] running benchmark-overhead"
srun --nodes=1 --ntasks=1 --nodelist="${CLIENT_NODE}" \
  uv run demo-arrow-flight benchmark-overhead \
    --host "${SERVER_NODE}" \
    --port "${PORT}" \
    --rows "${ROWS}" \
    --height "${HEIGHT}" \
    --width "${WIDTH}" \
    --seed "${SEED}" \
    --repeats "${REPEATS}" \
    --key-prefix "${KEY_PREFIX}-overhead" \
    --output-csv "${OVERHEAD_CSV}"

echo "[master] running benchmark-pipeline-io against same external server"
srun --nodes=1 --ntasks=1 --nodelist="${CLIENT_NODE}" \
  uv run demo-arrow-flight benchmark-pipeline-io \
    --host "${SERVER_NODE}" \
    --port "${PORT}" \
    --use-existing-server \
    --batch-counts "${PIPELINE_BATCH_COUNTS}" \
    --batch-rows "${BATCH_ROWS}" \
    --height "${HEIGHT}" \
    --width "${WIDTH}" \
    --seed "${PIPELINE_SEED}" \
    --repeats "${REPEATS}" \
    --output-csv "${PIPELINE_CSV}"

echo "[master] complete"
echo "[master] transport_csv=${TRANSPORT_CSV}"
echo "[master] overhead_csv=${OVERHEAD_CSV}"
echo "[master] pipeline_csv=${PIPELINE_CSV}"
