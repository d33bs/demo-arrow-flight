#!/bin/bash
set -euo pipefail

MASTER_SCRIPT=${MASTER_SCRIPT:-examples/alpine_benchmark/03_master_benchmark.sbatch}
MASTER_SBATCH_ARGS=${MASTER_SBATCH_ARGS:-}

if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch was not found in PATH. On Alpine, run: module load slurm/alpine" >&2
  exit 1
fi

master_args=()
if [[ -n "${MASTER_SBATCH_ARGS}" ]]; then
  read -r -a master_args <<<"${MASTER_SBATCH_ARGS}"
fi

master_job=$(sbatch --parsable "${master_args[@]}" "${MASTER_SCRIPT}")
master_job_id=${master_job%%;*}

echo "Submitted master benchmark job: ${master_job}"
echo "Track with: squeue -j ${master_job_id}"
echo "Inspect outputs: slurm-${master_job_id}.out"
