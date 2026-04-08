"""Benchmark helpers for Flight transport versus a simple local baseline."""

from __future__ import annotations

import csv
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_stream_demo import receive_streamed_chunks, stream_parquet_in_chunks
from .transfer import receive_table, send_table


def benchmark_baseline_parquet_read(
    parquet_path: str | Path,
    batch_rows: int,
    repeats: int,
) -> dict[str, float]:
    """Measure local parquet scan time as a simple baseline."""
    path = Path(parquet_path)
    parquet_file = pq.ParquetFile(path)
    start = time.perf_counter()
    total_rows = 0

    for _ in range(repeats):
        for batch in parquet_file.iter_batches(batch_size=batch_rows):
            total_rows += batch.num_rows

    elapsed = time.perf_counter() - start
    mib = (path.stat().st_size * repeats) / (1024 * 1024)
    throughput = mib / elapsed if elapsed > 0 else 0.0
    return {
        "seconds": elapsed,
        "rows": float(total_rows),
        "throughput_mib_s": throughput,
    }


def benchmark_flight_stream(
    location: str,
    key_prefix: str,
    parquet_path: str | Path,
    batch_rows: int,
    repeats: int,
) -> dict[str, float]:
    """Measure Flight do_put + do_get streamed transport performance."""
    path = Path(parquet_path)
    start = time.perf_counter()
    total_rows = 0

    for idx in range(repeats):
        key = f"{key_prefix}-{idx}"
        sent_rows, _sent_chunks = stream_parquet_in_chunks(
            location=location,
            key=key,
            parquet_path=path,
            batch_rows=batch_rows,
        )
        _chunk_rows, received_rows = receive_streamed_chunks(location=location, key=key)
        if sent_rows != received_rows:
            raise RuntimeError(
                f"Row mismatch for key={key}: sent={sent_rows}, received={received_rows}"
            )
        total_rows += received_rows

    elapsed = time.perf_counter() - start
    mib = (path.stat().st_size * repeats) / (1024 * 1024)
    throughput = mib / elapsed if elapsed > 0 else 0.0
    return {
        "seconds": elapsed,
        "rows": float(total_rows),
        "throughput_mib_s": throughput,
    }


def benchmark_parquet_write_read(
    table: pa.Table,
    parquet_path: str | Path,
    repeats: int,
) -> dict[str, float]:
    """Measure end-to-end parquet write+read time for an Arrow table."""
    path = Path(parquet_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()

    for _ in range(repeats):
        pq.write_table(table, path)
        restored = pq.read_table(path)
        if restored.num_rows != table.num_rows:
            raise RuntimeError(
                f"Row mismatch in parquet write/read: {restored.num_rows} != {table.num_rows}"
            )

    elapsed = time.perf_counter() - start
    logical_mib = (table.nbytes * repeats) / (1024 * 1024)
    throughput = logical_mib / elapsed if elapsed > 0 else 0.0
    return {
        "seconds": elapsed,
        "rows": float(table.num_rows * repeats),
        "throughput_mib_s": throughput,
    }


def benchmark_flight_table_roundtrip(
    location: str,
    key_prefix: str,
    table: pa.Table,
    repeats: int,
) -> dict[str, float]:
    """Measure end-to-end Flight send_table + receive_table time."""
    start = time.perf_counter()

    for idx in range(repeats):
        key = f"{key_prefix}-{idx}"
        send_table(location=location, key=key, table=table)
        restored = receive_table(location=location, key=key)
        if restored.num_rows != table.num_rows:
            raise RuntimeError(
                f"Row mismatch in flight roundtrip: {restored.num_rows} != {table.num_rows}"
            )

    elapsed = time.perf_counter() - start
    logical_mib = (table.nbytes * repeats) / (1024 * 1024)
    throughput = logical_mib / elapsed if elapsed > 0 else 0.0
    return {
        "seconds": elapsed,
        "rows": float(table.num_rows * repeats),
        "throughput_mib_s": throughput,
    }


def write_benchmark_csv(output_csv: str | Path, rows: list[dict[str, str]]) -> Path:
    """Write benchmark result rows to CSV."""
    path = Path(output_csv)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError("rows must not be empty")

    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path
