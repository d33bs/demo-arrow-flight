# Alpine Slurm Benchmark (Server/Client on Different Nodes)

This folder is a modular Slurm benchmark flow for CURC Alpine where:

- one job runs the Arrow Flight server,
- a second job runs data creation + benchmark commands, and
- the benchmark job is submitted with `--exclude=<server-node>` so it runs on a different node.

This gives a cleaner networked benchmark than running both roles on the same host.

## Files

- `01_server.sbatch`: starts Arrow Flight server.
- `02_benchmark_client.sbatch`: creates data and runs `benchmark-transport` + `benchmark-overhead`.
- `submit_benchmark.sh`: submits both jobs and wires dependency/node exclusion.
- `03_master_benchmark.sbatch`: single 2-node job that runs all benchmarks with shared conditions.
- `submit_master.sh`: submits the single master job.

## Submit

Initialize Slurm and `uv` in your Alpine login shell:

```bash
module load slurm/alpine
module use --append /pl/active/koala/software/lmod-files
module load uv
uv -v
export UV_BASE=/projects/$USER/uv
export UV_CACHE_DIR="$UV_BASE/cache"
export UV_PYTHON_INSTALL_DIR="$UV_BASE/python"
export UV_TOOL_DIR="$UV_BASE/tools"
```

From Alpine login node:

```bash
uv run poe slurm_benchmark_alpine
```

Optional resource/account overrides:

```bash
SERVER_SBATCH_ARGS="--account=<allocation> --partition=amilan --qos=normal --time=00:45:00" \
BENCHMARK_SBATCH_ARGS="--account=<allocation> --partition=amilan --qos=normal --time=00:20:00" \
PORT=8815 \
uv run poe slurm_benchmark_alpine
```

Optional benchmark parameters:

```bash
ROWS=200 BATCH_ROWS=8 REPEATS=5 OUTPUT_DIR=/scratch/alpine/$USER/flight-bench uv run poe slurm_benchmark_alpine
```

## Outputs

The benchmark job writes:

- `${OUTPUT_CSV}` (default: `${OUTPUT_DIR}/benchmark_transport.csv`)
- `${OVERHEAD_CSV}` (default: `${OUTPUT_DIR}/benchmark_overhead.csv`)

These match the benchmark modes already used in this repo (`benchmark-transport` and `benchmark-overhead`).

## Notes for Alpine

- CURC documents loading Alpine Slurm on login nodes via `module load slurm/alpine`.
- CURC notes `$SLURM_SCRATCH` as a high-performance per-job scratch location.
- `submit_benchmark.sh` resolves the server node after submission, then submits the benchmark with `--exclude` to avoid co-location.

## Master Job (Recommended for equal comparisons)

Use one 2-node Slurm allocation where node A runs the server and node B runs all benchmarks:

```bash
uv run poe slurm_benchmark_master
```

Optional resource/account overrides:

```bash
MASTER_SBATCH_ARGS="--account=<allocation> --partition=amilan --qos=normal --time=01:00:00 --nodes=2 --ntasks=2" \
uv run poe slurm_benchmark_master
```

This master job runs:

- `benchmark-transport`
- `benchmark-overhead`
- `benchmark-pipeline-io --use-existing-server`

All run in the same Slurm allocation and target the same Flight server endpoint for fairer comparisons.

All Alpine sbatch scripts in this folder source `load_uv_env.sh`, which applies the same module setup and `UV_*` exports inside each job before running `uv`.
