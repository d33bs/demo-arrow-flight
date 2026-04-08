"""Local simulation of a Slurm-style Arrow Flight workflow."""

from __future__ import annotations

import random
import threading
import time
from pathlib import Path

from .flight_server import InMemoryFlightServer
from .parquet_stream_demo import (
    receive_streamed_chunks,
    stream_parquet_in_chunks,
    write_random_ome_parquet,
)


def simulate_slurm_parquet_workflow(
    output_dir: str | Path,
    host: str,
    port: int,
    key: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
    batch_rows: int,
) -> dict[str, object]:
    """Run a local, Slurm-like staged workflow and write per-job logs."""
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)

    parquet_path = base / "random_ome_dataset.parquet"
    job_server = _job_id()
    job_sender = _job_id()
    job_receiver = _job_id()

    _append(base / f"slurm-{job_server}.out", f"Submitted batch job {job_server}\n")
    _append(base / f"slurm-{job_sender}.out", f"Submitted batch job {job_sender}\n")
    _append(base / f"slurm-{job_receiver}.out", f"Submitted batch job {job_receiver}\n")

    location = f"grpc://{host}:{port}"
    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        write_random_ome_parquet(
            output_path=parquet_path,
            rows=rows,
            height=height,
            width=width,
            seed=seed,
        )
        _append(
            base / f"slurm-{job_sender}.out",
            f"Generated parquet at {parquet_path} rows={rows} seed={seed}\n",
        )

        sent_rows, sent_chunks = stream_parquet_in_chunks(
            location=location,
            key=key,
            parquet_path=parquet_path,
            batch_rows=batch_rows,
        )
        _append(
            base / f"slurm-{job_sender}.out",
            f"Streamed dataset key={key} sent_rows={sent_rows} sent_chunks={sent_chunks}\n",
        )

        recv_chunk_rows, recv_total_rows = receive_streamed_chunks(location=location, key=key)
        _append(
            base / f"slurm-{job_receiver}.out",
            "Received streamed dataset "
            f"key={key} chunk_rows={recv_chunk_rows} total_rows={recv_total_rows}\n",
        )

        return {
            "location": location,
            "dataset": str(parquet_path),
            "job_ids": [job_server, job_sender, job_receiver],
            "sent_rows": sent_rows,
            "sent_chunks": sent_chunks,
            "received_chunk_rows": recv_chunk_rows,
            "received_rows": recv_total_rows,
        }
    finally:
        server.shutdown()
        thread.join(timeout=2)


def _append(path: Path, text: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)


def _job_id() -> int:
    return random.randint(100000, 999999)
