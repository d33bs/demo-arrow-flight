#!/bin/bash
set -euo pipefail

SERVER_SCRIPT=${SERVER_SCRIPT:-examples/alpine_benchmark/01_server.sbatch}
BENCHMARK_SCRIPT=${BENCHMARK_SCRIPT:-examples/alpine_benchmark/02_benchmark_client.sbatch}
PORT=${PORT:-8815}
AUTO_CANCEL_SERVER=${AUTO_CANCEL_SERVER:-1}

SERVER_SBATCH_ARGS=${SERVER_SBATCH_ARGS:-}
BENCHMARK_SBATCH_ARGS=${BENCHMARK_SBATCH_ARGS:-}

if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch was not found in PATH. On Alpine, run: module load slurm/alpine" >&2
  exit 1
fi

server_args=()
benchmark_args=()
if [[ -n "${SERVER_SBATCH_ARGS}" ]]; then
  read -r -a server_args <<<"${SERVER_SBATCH_ARGS}"
fi
if [[ -n "${BENCHMARK_SBATCH_ARGS}" ]]; then
  read -r -a benchmark_args <<<"${BENCHMARK_SBATCH_ARGS}"
fi

server_job_raw=$(sbatch --parsable "${server_args[@]}" "${SERVER_SCRIPT}")
server_job_id=${server_job_raw%%;*}

echo "Submitted server job: ${server_job_raw}"

server_host=""
for _ in $(seq 1 60); do
  server_nodelist=$(squeue -h -j "${server_job_id}" -o "%N" 2>/dev/null || true)
  if [[ -n "${server_nodelist}" ]] && [[ "${server_nodelist}" != "(null)" ]] && [[ "${server_nodelist}" != "n/a" ]]; then
    server_host=$(scontrol show hostnames "${server_nodelist}" | head -n 1)
    if [[ -n "${server_host}" ]]; then
      break
    fi
  fi
  sleep 2
done

if [[ -z "${server_host}" ]]; then
  echo "Could not determine a server host for job ${server_job_id}." >&2
  echo "Cancel the server manually if needed: scancel ${server_job_id}" >&2
  exit 1
fi

benchmark_job_raw=$(HOST="${server_host}" PORT="${PORT}" sbatch --parsable "${benchmark_args[@]}" --dependency="after:${server_job_id}" --exclude="${server_host}" "${BENCHMARK_SCRIPT}")
benchmark_job_id=${benchmark_job_raw%%;*}

echo "Submitted benchmark job: ${benchmark_job_raw}"
echo "Server host pinned to ${server_host}; benchmark excludes that node."

if [[ "${AUTO_CANCEL_SERVER}" == "1" ]]; then
  cleanup_job_raw=$(sbatch --parsable --dependency="afterany:${benchmark_job_id}" --wrap="scancel ${server_job_id}")
  echo "Submitted cleanup job: ${cleanup_job_raw}"
fi

echo "Track with: squeue -j ${server_job_id},${benchmark_job_id}"
echo "Inspect outputs: slurm-<jobid>.out"
