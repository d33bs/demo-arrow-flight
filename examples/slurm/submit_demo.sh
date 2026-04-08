#!/bin/bash
set -euo pipefail

# Usage:
# HOST=10.0.0.5 PORT=8815 KEY=slurm-ome-dataset ./submit_demo.sh

SERVER_SCRIPT=${SERVER_SCRIPT:-examples/slurm/01_server.sbatch}
SENDER_SCRIPT=${SENDER_SCRIPT:-examples/slurm/02_sender.sbatch}
RECEIVER_SCRIPT=${RECEIVER_SCRIPT:-examples/slurm/03_receiver.sbatch}

server_job=$(sbatch --parsable "${SERVER_SCRIPT}")
echo "Submitted server job: ${server_job}"

sender_job=$(sbatch --parsable --dependency=after:$(echo "${server_job}" | cut -d';' -f1) "${SENDER_SCRIPT}")
echo "Submitted sender job: ${sender_job}"

receiver_job=$(sbatch --parsable --dependency=afterok:$(echo "${sender_job}" | cut -d';' -f1) "${RECEIVER_SCRIPT}")
echo "Submitted receiver job: ${receiver_job}"

echo "Use: squeue -j $(echo "${server_job}" | cut -d';' -f1),$(echo "${sender_job}" | cut -d';' -f1),$(echo "${receiver_job}" | cut -d';' -f1)"
