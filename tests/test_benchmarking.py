from __future__ import annotations

import socket
import threading
import time

from demo_arrow_flight.benchmarking import (
    benchmark_baseline_parquet_read,
    benchmark_flight_table_roundtrip,
    benchmark_flight_stream,
    benchmark_pipeline_file_io,
    benchmark_pipeline_flight,
    benchmark_parquet_write_read,
    write_benchmark_csv,
)
from demo_arrow_flight.flight_server import InMemoryFlightServer
from demo_arrow_flight.parquet_stream_demo import build_random_ome_table, write_random_ome_parquet


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_benchmark_baseline_and_flight(tmp_path) -> None:
    parquet_path = tmp_path / "bench.parquet"
    write_random_ome_parquet(
        output_path=parquet_path,
        rows=8,
        height=12,
        width=10,
        seed=9,
    )

    baseline = benchmark_baseline_parquet_read(
        parquet_path=parquet_path,
        batch_rows=3,
        repeats=2,
    )
    assert baseline["rows"] == 16
    assert baseline["seconds"] >= 0

    port = _free_port()
    location = f"grpc://127.0.0.1:{port}"
    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        flight_result = benchmark_flight_stream(
            location=location,
            key_prefix="bench-key",
            parquet_path=parquet_path,
            batch_rows=3,
            repeats=2,
        )
        assert flight_result["rows"] == 16
        assert flight_result["seconds"] >= 0
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_write_benchmark_csv(tmp_path) -> None:
    out = write_benchmark_csv(
        output_csv=tmp_path / "bench.csv",
        rows=[
            {"mode": "baseline", "seconds": "1.0", "rows": "10", "throughput_mib_s": "2.0"},
            {"mode": "flight", "seconds": "0.8", "rows": "10", "throughput_mib_s": "2.5"},
        ],
    )
    assert out.exists()
    assert "mode,seconds,rows,throughput_mib_s" in out.read_text(encoding="utf-8")


def test_benchmark_overhead_parquet_vs_flight(tmp_path) -> None:
    table = build_random_ome_table(rows=16, height=16, width=16, seed=4)

    parquet_result = benchmark_parquet_write_read(
        table=table,
        parquet_path=tmp_path / "overhead.parquet",
        repeats=2,
    )
    assert parquet_result["rows"] == 32
    assert parquet_result["seconds"] >= 0

    port = _free_port()
    location = f"grpc://127.0.0.1:{port}"
    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        flight_result = benchmark_flight_table_roundtrip(
            location=location,
            key_prefix="overhead-key",
            table=table,
            repeats=2,
        )
        assert flight_result["rows"] == 32
        assert flight_result["seconds"] >= 0
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_benchmark_pipeline_io_modes() -> None:
    table = build_random_ome_table(rows=4, height=8, width=8, seed=100)
    file_result = benchmark_pipeline_file_io(table=table, batch_rows=1, repeats=1)
    assert file_result["rows"] == 4
    assert file_result["seconds"] >= 0
    assert file_result["disk_bytes_written"] > 0

    port = _free_port()
    location = f"grpc://127.0.0.1:{port}"
    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)
    try:
        flight_result = benchmark_pipeline_flight(
            location=location,
            table=table,
            batch_rows=1,
            repeats=1,
            key_prefix="pipeline-io-test",
        )
        assert flight_result["rows"] == 4
        assert flight_result["seconds"] >= 0
        assert flight_result["disk_bytes_written"] == 0
    finally:
        server.shutdown()
        thread.join(timeout=2)
