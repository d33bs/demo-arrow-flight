#!/bin/bash
set -euo pipefail

SERVER_SCRIPT=${SERVER_SCRIPT:-examples/slurm/01_server.sbatch}
PRODUCE_SCRIPT=${PRODUCE_SCRIPT:-examples/slurm/04_pipeline_produce.sbatch}
TRANSFORM_SCRIPT=${TRANSFORM_SCRIPT:-examples/slurm/05_pipeline_transform.sbatch}
CONSUME_SCRIPT=${CONSUME_SCRIPT:-examples/slurm/06_pipeline_consume.sbatch}

server_job=$(sbatch --parsable "${SERVER_SCRIPT}")
echo "Submitted server job: ${server_job}"

server_id=$(echo "${server_job}" | cut -d';' -f1)
produce_job=$(sbatch --parsable --dependency=after:${server_id} "${PRODUCE_SCRIPT}")
echo "Submitted produce job: ${produce_job}"

produce_id=$(echo "${produce_job}" | cut -d';' -f1)
transform_job=$(sbatch --parsable --dependency=afterok:${produce_id} "${TRANSFORM_SCRIPT}")
echo "Submitted transform job: ${transform_job}"

transform_id=$(echo "${transform_job}" | cut -d';' -f1)
consume_job=$(sbatch --parsable --dependency=afterok:${transform_id} "${CONSUME_SCRIPT}")
echo "Submitted consume job: ${consume_job}"
