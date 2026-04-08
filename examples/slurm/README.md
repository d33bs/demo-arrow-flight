# Slurm Example

This folder contains a simple staged Arrow Flight workflow from a Slurm user's perspective.

## Files

- `01_server.sbatch`: starts the Flight server.
- `02_sender.sbatch`: generates randomized parquet and streams it to Flight.
- `03_receiver.sbatch`: receives the streamed dataset and prints chunk metadata.
- `04_pipeline_produce.sbatch`: pipeline stage 1 (`raw` Flight key).
- `05_pipeline_transform.sbatch`: pipeline stage 2 (`raw` -> `processed` key).
- `06_pipeline_consume.sbatch`: pipeline stage 3 (consume `processed` key).
- `07_benchmark.sbatch`: baseline-vs-Flight benchmark job with CSV output.
- `submit_demo.sh`: submits jobs with dependencies.
- `submit_pipeline.sh`: submits pipeline stage jobs with dependencies.

## Submit

```bash
HOST=<server-host> PORT=8815 KEY=slurm-ome-dataset ./examples/slurm/submit_demo.sh
```

## Observe

```bash
squeue -u "$USER"
tail -f slurm-<jobid>.out
```

## Pipeline Submit

```bash
HOST=<server-host> PORT=8815 RAW_KEY=pipeline-raw PROCESSED_KEY=pipeline-processed ./examples/slurm/submit_pipeline.sh
```

## Benchmark Job

```bash
HOST=<server-host> PORT=8815 OUTPUT_CSV=/tmp/demo_arrow_flight_benchmark.csv sbatch examples/slurm/07_benchmark.sbatch
```
