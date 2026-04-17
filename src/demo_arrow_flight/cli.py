"""CLI for Arrow Flight + OME-Arrow demos."""

from __future__ import annotations

import argparse
import socket
import threading
import time
from pathlib import Path

from .benchmarking import (
    benchmark_baseline_parquet_read,
    benchmark_flight_table_roundtrip,
    benchmark_flight_stream,
    benchmark_pipeline_file_io,
    benchmark_pipeline_flight,
    benchmark_parquet_write_read,
    write_benchmark_csv,
)
from .flight_pipeline_demo import (
    pipeline_consume_from_flight,
    pipeline_produce_to_flight,
    pipeline_transform_on_flight,
)
from .flight_server import InMemoryFlightServer
from .ome_image import build_demo_ome_arrow
from .parquet_stream_demo import (
    build_random_ome_table,
    receive_streamed_chunks,
    stream_parquet_in_chunks,
    write_random_ome_parquet,
)
from .slurm_simulation import simulate_slurm_parquet_workflow
from .transfer import clear_keys, receive_ome_arrow, receive_table, send_ome_arrow, send_table


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="demo-arrow-flight")
    subparsers = parser.add_subparsers(dest="command", required=True)

    server = subparsers.add_parser("server", help="Run a Flight server.")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8815)

    send = subparsers.add_parser("send", help="Send a demo OME-Arrow image.")
    send.add_argument("--host", default="127.0.0.1")
    send.add_argument("--port", type=int, default=8815)
    send.add_argument("--key", default="demo-image")

    receive = subparsers.add_parser("receive", help="Receive a demo OME-Arrow image.")
    receive.add_argument("--host", default="127.0.0.1")
    receive.add_argument("--port", type=int, default=8815)
    receive.add_argument("--key", default="demo-image")

    roundtrip = subparsers.add_parser(
        "roundtrip",
        help="Alias for roundtrip-one (single OME-Arrow value).",
    )
    roundtrip.add_argument("--host", default="127.0.0.1")
    roundtrip.add_argument("--key", default="demo-image")

    roundtrip_one = subparsers.add_parser(
        "roundtrip-one",
        help="Run server + send + receive with one OME-Arrow value.",
    )
    roundtrip_one.add_argument("--host", default="127.0.0.1")
    roundtrip_one.add_argument("--key", default="demo-image")

    roundtrip_column = subparsers.add_parser(
        "roundtrip-column",
        help="Run server + send + receive for a multi-row OME-Arrow column.",
    )
    roundtrip_column.add_argument("--host", default="127.0.0.1")
    roundtrip_column.add_argument("--key", default="demo-column")
    roundtrip_column.add_argument("--rows", type=int, default=8)
    roundtrip_column.add_argument("--height", type=int, default=32)
    roundtrip_column.add_argument("--width", type=int, default=32)
    roundtrip_column.add_argument("--seed", type=int, default=19)

    parquet_generate = subparsers.add_parser(
        "parquet-generate",
        help="Generate randomized parquet with an ome_arrow column.",
    )
    parquet_generate.add_argument("--output", required=True)
    parquet_generate.add_argument("--rows", type=int, default=16)
    parquet_generate.add_argument("--height", type=int, default=64)
    parquet_generate.add_argument("--width", type=int, default=64)
    parquet_generate.add_argument("--seed", type=int, default=7)

    parquet_stream = subparsers.add_parser(
        "parquet-stream",
        help="Stream a parquet dataset to Flight in chunked batches.",
    )
    parquet_stream.add_argument("--host", default="127.0.0.1")
    parquet_stream.add_argument("--port", type=int, default=8815)
    parquet_stream.add_argument("--key", default="random-ome-dataset")
    parquet_stream.add_argument("--parquet-path", required=True)
    parquet_stream.add_argument("--batch-rows", type=int, default=4)

    parquet_receive = subparsers.add_parser(
        "parquet-receive",
        help="Receive a chunk-streamed dataset and print chunk sizes.",
    )
    parquet_receive.add_argument("--host", default="127.0.0.1")
    parquet_receive.add_argument("--port", type=int, default=8815)
    parquet_receive.add_argument("--key", default="random-ome-dataset")

    parquet_demo = subparsers.add_parser(
        "parquet-demo",
        help="Run the parquet generate + chunk stream + receive flow locally.",
    )
    parquet_demo.add_argument("--host", default="127.0.0.1")
    parquet_demo.add_argument("--key", default="random-ome-dataset")
    parquet_demo.add_argument("--output", default="/tmp/random_ome_dataset.parquet")
    parquet_demo.add_argument("--rows", type=int, default=10)
    parquet_demo.add_argument("--height", type=int, default=64)
    parquet_demo.add_argument("--width", type=int, default=64)
    parquet_demo.add_argument("--seed", type=int, default=11)
    parquet_demo.add_argument("--batch-rows", type=int, default=3)

    pipeline_produce = subparsers.add_parser(
        "pipeline-produce",
        help="Pipeline stage 1: produce randomized table to a Flight key.",
    )
    pipeline_produce.add_argument("--host", default="127.0.0.1")
    pipeline_produce.add_argument("--port", type=int, default=8815)
    pipeline_produce.add_argument("--key", default="pipeline-raw")
    pipeline_produce.add_argument("--rows", type=int, default=10)
    pipeline_produce.add_argument("--height", type=int, default=32)
    pipeline_produce.add_argument("--width", type=int, default=32)
    pipeline_produce.add_argument("--seed", type=int, default=17)

    pipeline_transform = subparsers.add_parser(
        "pipeline-transform",
        help="Pipeline stage 2: transform one Flight key into another.",
    )
    pipeline_transform.add_argument("--host", default="127.0.0.1")
    pipeline_transform.add_argument("--port", type=int, default=8815)
    pipeline_transform.add_argument("--input-key", default="pipeline-raw")
    pipeline_transform.add_argument("--output-key", default="pipeline-processed")
    pipeline_transform.add_argument("--stage-name", default="preprocess")

    pipeline_consume = subparsers.add_parser(
        "pipeline-consume",
        help="Pipeline stage 3: consume final Flight key and print chunk stats.",
    )
    pipeline_consume.add_argument("--host", default="127.0.0.1")
    pipeline_consume.add_argument("--port", type=int, default=8815)
    pipeline_consume.add_argument("--key", default="pipeline-processed")

    pipeline_demo = subparsers.add_parser(
        "pipeline-demo",
        help="Run local produce -> transform -> consume Flight-key pipeline.",
    )
    pipeline_demo.add_argument("--host", default="127.0.0.1")
    pipeline_demo.add_argument("--raw-key", default="pipeline-raw")
    pipeline_demo.add_argument("--processed-key", default="pipeline-processed")
    pipeline_demo.add_argument("--rows", type=int, default=10)
    pipeline_demo.add_argument("--height", type=int, default=32)
    pipeline_demo.add_argument("--width", type=int, default=32)
    pipeline_demo.add_argument("--seed", type=int, default=17)
    pipeline_demo.add_argument("--stage-name", default="preprocess")

    benchmark_transport = subparsers.add_parser(
        "benchmark-transport",
        help="Compare baseline parquet scan vs Flight transport using existing server.",
    )
    benchmark_transport.add_argument("--host", default="127.0.0.1")
    benchmark_transport.add_argument("--port", type=int, default=8815)
    benchmark_transport.add_argument("--parquet-path", required=True)
    benchmark_transport.add_argument("--key-prefix", default="benchmark-key")
    benchmark_transport.add_argument("--batch-rows", type=int, default=4)
    benchmark_transport.add_argument("--repeats", type=int, default=3)
    benchmark_transport.add_argument("--output-csv", default="")

    benchmark_demo = subparsers.add_parser(
        "benchmark-demo",
        help="Run local server + benchmark comparison (baseline vs Flight).",
    )
    benchmark_demo.add_argument("--host", default="127.0.0.1")
    benchmark_demo.add_argument("--output", default="/tmp/random_ome_benchmark.parquet")
    benchmark_demo.add_argument("--rows", type=int, default=20)
    benchmark_demo.add_argument("--height", type=int, default=64)
    benchmark_demo.add_argument("--width", type=int, default=64)
    benchmark_demo.add_argument("--seed", type=int, default=29)
    benchmark_demo.add_argument("--batch-rows", type=int, default=4)
    benchmark_demo.add_argument("--repeats", type=int, default=3)
    benchmark_demo.add_argument(
        "--output-csv", default="/tmp/demo_arrow_flight_benchmark.csv"
    )

    benchmark_overhead = subparsers.add_parser(
        "benchmark-overhead",
        help="Compare parquet write+read vs Flight table roundtrip for same Arrow table.",
    )
    benchmark_overhead.add_argument("--host", default="127.0.0.1")
    benchmark_overhead.add_argument("--port", type=int, default=8815)
    benchmark_overhead.add_argument("--rows", type=int, default=320)
    benchmark_overhead.add_argument("--height", type=int, default=64)
    benchmark_overhead.add_argument("--width", type=int, default=64)
    benchmark_overhead.add_argument("--seed", type=int, default=31)
    benchmark_overhead.add_argument("--repeats", type=int, default=3)
    benchmark_overhead.add_argument("--key-prefix", default="benchmark-overhead")
    benchmark_overhead.add_argument("--output-csv", default="")

    benchmark_pipeline = subparsers.add_parser(
        "benchmark-pipeline-io",
        help="Scale many-batch pipeline: parquet-intermediate I/O vs Flight in-memory keys.",
    )
    benchmark_pipeline.add_argument("--host", default="127.0.0.1")
    benchmark_pipeline.add_argument("--port", type=int, default=8815)
    benchmark_pipeline.add_argument(
        "--use-existing-server",
        action="store_true",
        help="Use an already running Flight server instead of starting a local one.",
    )
    benchmark_pipeline.add_argument(
        "--batch-counts",
        default="160,320,640,1280,2560,5120",
        help="Comma-separated batch counts.",
    )
    benchmark_pipeline.add_argument("--batch-rows", type=int, default=1)
    benchmark_pipeline.add_argument("--height", type=int, default=64)
    benchmark_pipeline.add_argument("--width", type=int, default=64)
    benchmark_pipeline.add_argument("--seed", type=int, default=41)
    benchmark_pipeline.add_argument("--repeats", type=int, default=1)
    benchmark_pipeline.add_argument(
        "--output-csv",
        default="/tmp/demo_arrow_flight_pipeline_io.csv",
    )

    slurm_sim = subparsers.add_parser(
        "slurm-simulate",
        help="Simulate a Slurm user workflow locally with job logs.",
    )
    slurm_sim.add_argument("--output-dir", default="/tmp/demo_arrow_flight_slurm")
    slurm_sim.add_argument("--host", default="127.0.0.1")
    slurm_sim.add_argument("--port", type=int, default=8825)
    slurm_sim.add_argument("--key", default="slurm-ome-dataset")
    slurm_sim.add_argument("--rows", type=int, default=10)
    slurm_sim.add_argument("--height", type=int, default=32)
    slurm_sim.add_argument("--width", type=int, default=32)
    slurm_sim.add_argument("--seed", type=int, default=101)
    slurm_sim.add_argument("--batch-rows", type=int, default=3)

    return parser


def _location(host: str, port: int) -> str:
    return f"grpc://{host}:{port}"


def _find_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _start_local_server(host: str) -> tuple[InMemoryFlightServer, threading.Thread, str]:
    port = _find_free_port(host)
    location = _location(host, port)
    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)
    return server, thread, location


def _stop_local_server(server: InMemoryFlightServer, thread: threading.Thread) -> None:
    server.shutdown()
    thread.join(timeout=2)


def _ome_scalar_shape(scalar) -> tuple[int, int, int, int, int]:
    pixels = scalar.as_py()["pixels_meta"]
    return (
        int(pixels["size_t"]),
        int(pixels["size_c"]),
        int(pixels["size_z"]),
        int(pixels["size_y"]),
        int(pixels["size_x"]),
    )


def _run_server(host: str, port: int) -> None:
    location = _location(host, port)
    server = InMemoryFlightServer(location)
    print(f"Flight server running at {location}")
    server.serve()


def _run_send(host: str, port: int, key: str) -> None:
    location = _location(host, port)
    payload = build_demo_ome_arrow()
    send_ome_arrow(location, key, payload)
    print(f"Sent demo OME-Arrow image to {location} (key={key}).")


def _run_receive(host: str, port: int, key: str) -> None:
    location = _location(host, port)
    scalar = receive_ome_arrow(location, key)
    shape = _ome_scalar_shape(scalar)
    print(
        "Received OME-Arrow scalar "
        f"shape(T,C,Z,Y,X)={shape}, key={key}, image_id={scalar.as_py().get('id')}"
    )


def _run_roundtrip_one(host: str, key: str) -> None:
    server, thread, location = _start_local_server(host)
    try:
        payload = build_demo_ome_arrow()
        send_ome_arrow(location, key, payload)
        scalar = receive_ome_arrow(location, key)
        print(
            "Roundtrip one successful: "
            f"location={location}, key={key}, shape(T,C,Z,Y,X)={_ome_scalar_shape(scalar)}"
        )
    finally:
        _stop_local_server(server, thread)


def _run_roundtrip_column(
    host: str,
    key: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
) -> None:
    server, thread, location = _start_local_server(host)

    try:
        table = build_random_ome_table(rows=rows, height=height, width=width, seed=seed)
        send_table(location=location, key=key, table=table)
        restored = receive_table(location=location, key=key)
        first_shape = _ome_scalar_shape(restored["ome_arrow"][0])
        print(
            "Roundtrip column successful: "
            f"location={location}, key={key}, rows={restored.num_rows}, "
            f"columns={restored.num_columns}, first_shape(T,C,Z,Y,X)={first_shape}"
        )
    finally:
        _stop_local_server(server, thread)


def _run_parquet_generate(
    output: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
) -> None:
    path = write_random_ome_parquet(
        output_path=output,
        rows=rows,
        height=height,
        width=width,
        seed=seed,
    )
    print(
        f"Wrote parquet dataset at {path} "
        f"(rows={rows}, image_shape=({height}, {width}), seed={seed})"
    )


def _run_parquet_stream(
    host: str,
    port: int,
    key: str,
    parquet_path: str,
    batch_rows: int,
) -> None:
    location = _location(host, port)
    total_rows, total_batches = stream_parquet_in_chunks(
        location=location,
        key=key,
        parquet_path=parquet_path,
        batch_rows=batch_rows,
    )
    print(
        f"Streamed parquet to {location} (key={key}) "
        f"rows={total_rows}, chunks={total_batches}, batch_rows={batch_rows}"
    )


def _run_parquet_receive(host: str, port: int, key: str) -> None:
    location = _location(host, port)
    chunk_rows, total_rows = receive_streamed_chunks(location=location, key=key)
    print(
        f"Received stream from {location} (key={key}) "
        f"chunks={len(chunk_rows)}, chunk_rows={chunk_rows}, total_rows={total_rows}"
    )


def _run_parquet_demo(
    host: str,
    key: str,
    output: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
    batch_rows: int,
) -> None:
    server, thread, location = _start_local_server(host)

    try:
        output_path = write_random_ome_parquet(
            output_path=Path(output),
            rows=rows,
            height=height,
            width=width,
            seed=seed,
        )
        sent_rows, sent_batches = stream_parquet_in_chunks(
            location=location,
            key=key,
            parquet_path=output_path,
            batch_rows=batch_rows,
        )
        recv_chunk_rows, recv_total_rows = receive_streamed_chunks(
            location=location,
            key=key,
        )
        print(
            "Parquet stream demo successful: "
            f"dataset={output_path}, sent_rows={sent_rows}, sent_chunks={sent_batches}, "
            f"received_chunks={len(recv_chunk_rows)}, chunk_rows={recv_chunk_rows}, "
            f"received_rows={recv_total_rows}"
        )
    finally:
        _stop_local_server(server, thread)


def _run_pipeline_produce(
    host: str,
    port: int,
    key: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
) -> None:
    location = _location(host, port)
    produced_rows = pipeline_produce_to_flight(
        location=location,
        key=key,
        rows=rows,
        height=height,
        width=width,
        seed=seed,
    )
    print(
        f"Pipeline produce successful: location={location}, key={key}, rows={produced_rows}"
    )


def _run_pipeline_transform(
    host: str,
    port: int,
    input_key: str,
    output_key: str,
    stage_name: str,
) -> None:
    location = _location(host, port)
    rows = pipeline_transform_on_flight(
        location=location,
        input_key=input_key,
        output_key=output_key,
        stage_name=stage_name,
    )
    print(
        "Pipeline transform successful: "
        f"location={location}, input_key={input_key}, output_key={output_key}, rows={rows}"
    )


def _run_pipeline_consume(host: str, port: int, key: str) -> None:
    location = _location(host, port)
    result = pipeline_consume_from_flight(location=location, key=key)
    print(
        f"Pipeline consume successful: location={location}, key={key}, "
        f"rows={result['rows']}, chunks={result['chunks']}, chunk_rows={result['chunk_rows']}"
    )


def _run_pipeline_demo(
    host: str,
    raw_key: str,
    processed_key: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
    stage_name: str,
) -> None:
    server, thread, location = _start_local_server(host)
    try:
        produced = pipeline_produce_to_flight(
            location=location,
            key=raw_key,
            rows=rows,
            height=height,
            width=width,
            seed=seed,
        )
        transformed = pipeline_transform_on_flight(
            location=location,
            input_key=raw_key,
            output_key=processed_key,
            stage_name=stage_name,
        )
        consumed = pipeline_consume_from_flight(location=location, key=processed_key)
        print(
            "Pipeline demo successful: "
            f"location={location}, raw_key={raw_key}, processed_key={processed_key}, "
            f"produced_rows={produced}, transformed_rows={transformed}, "
            f"consumed_rows={consumed['rows']}, chunk_rows={consumed['chunk_rows']}"
        )
    finally:
        _stop_local_server(server, thread)


def _print_benchmark_result(
    baseline: dict[str, float],
    flight_result: dict[str, float],
    output_csv: str,
) -> None:
    speedup = (
        flight_result["seconds"] / baseline["seconds"]
        if baseline["seconds"] > 0
        else 0.0
    )
    print(
        "Benchmark complete: "
        f"baseline_seconds={baseline['seconds']:.4f}, "
        f"flight_seconds={flight_result['seconds']:.4f}, "
        f"baseline_mib_s={baseline['throughput_mib_s']:.2f}, "
        f"flight_mib_s={flight_result['throughput_mib_s']:.2f}, "
        f"flight_vs_baseline_time_ratio={speedup:.3f}, output_csv={output_csv}"
    )


def _print_overhead_benchmark_result(
    parquet_result: dict[str, float],
    flight_result: dict[str, float],
    output_csv: str,
) -> None:
    ratio = (
        parquet_result["seconds"] / flight_result["seconds"]
        if flight_result["seconds"] > 0
        else 0.0
    )
    print(
        "Benchmark overhead complete: "
        f"parquet_write_read_seconds={parquet_result['seconds']:.4f}, "
        f"flight_roundtrip_seconds={flight_result['seconds']:.4f}, "
        f"parquet_mib_s={parquet_result['throughput_mib_s']:.2f}, "
        f"flight_mib_s={flight_result['throughput_mib_s']:.2f}, "
        f"parquet_vs_flight_time_ratio={ratio:.3f}, output_csv={output_csv}"
    )


def _run_benchmark_transport(
    host: str,
    port: int,
    parquet_path: str,
    key_prefix: str,
    batch_rows: int,
    repeats: int,
    output_csv: str,
) -> None:
    location = _location(host, port)

    baseline = benchmark_baseline_parquet_read(
        parquet_path=parquet_path,
        batch_rows=batch_rows,
        repeats=repeats,
    )
    flight_result = benchmark_flight_stream(
        location=location,
        key_prefix=key_prefix,
        parquet_path=parquet_path,
        batch_rows=batch_rows,
        repeats=repeats,
    )

    csv_path = output_csv or "/tmp/demo_arrow_flight_benchmark.csv"
    write_benchmark_csv(
        output_csv=csv_path,
        rows=[
            {
                "mode": "baseline_parquet_read",
                "seconds": f"{baseline['seconds']:.6f}",
                "rows": str(int(baseline["rows"])),
                "throughput_mib_s": f"{baseline['throughput_mib_s']:.6f}",
            },
            {
                "mode": "flight_stream",
                "seconds": f"{flight_result['seconds']:.6f}",
                "rows": str(int(flight_result["rows"])),
                "throughput_mib_s": f"{flight_result['throughput_mib_s']:.6f}",
            },
        ],
    )
    _print_benchmark_result(baseline, flight_result, csv_path)


def _run_benchmark_overhead(
    host: str,
    port: int,
    rows: int,
    height: int,
    width: int,
    seed: int,
    repeats: int,
    key_prefix: str,
    output_csv: str,
) -> None:
    location = _location(host, port)
    table = build_random_ome_table(rows=rows, height=height, width=width, seed=seed)
    parquet_path = Path("/tmp/demo_arrow_flight_overhead.parquet")

    parquet_result = benchmark_parquet_write_read(
        table=table,
        parquet_path=parquet_path,
        repeats=repeats,
    )
    flight_result = benchmark_flight_table_roundtrip(
        location=location,
        key_prefix=key_prefix,
        table=table,
        repeats=repeats,
    )

    csv_path = output_csv or "/tmp/demo_arrow_flight_overhead.csv"
    write_benchmark_csv(
        output_csv=csv_path,
        rows=[
            {
                "mode": "parquet_write_read",
                "seconds": f"{parquet_result['seconds']:.6f}",
                "rows": str(int(parquet_result["rows"])),
                "throughput_mib_s": f"{parquet_result['throughput_mib_s']:.6f}",
            },
            {
                "mode": "flight_table_roundtrip",
                "seconds": f"{flight_result['seconds']:.6f}",
                "rows": str(int(flight_result["rows"])),
                "throughput_mib_s": f"{flight_result['throughput_mib_s']:.6f}",
            },
        ],
    )
    _print_overhead_benchmark_result(parquet_result, flight_result, csv_path)


def _parse_batch_counts(batch_counts: str) -> list[int]:
    values = [x.strip() for x in batch_counts.split(",") if x.strip()]
    parsed = [int(x) for x in values]
    if not parsed:
        raise ValueError("batch-counts cannot be empty")
    if any(v < 1 for v in parsed):
        raise ValueError("batch-counts values must be >= 1")
    return parsed


def _run_benchmark_pipeline_io(
    host: str,
    port: int,
    use_existing_server: bool,
    batch_counts: str,
    batch_rows: int,
    height: int,
    width: int,
    seed: int,
    repeats: int,
    output_csv: str,
) -> None:
    counts = _parse_batch_counts(batch_counts)
    location = _location(host, port) if use_existing_server else ""
    server: InMemoryFlightServer | None = None
    thread: threading.Thread | None = None
    csv_rows: list[dict[str, str]] = []

    if not use_existing_server:
        server, thread, location = _start_local_server(host)

    try:
        for count in counts:
            total_rows = count * batch_rows
            table = build_random_ome_table(
                rows=total_rows,
                height=height,
                width=width,
                seed=seed,
            )
            file_result = benchmark_pipeline_file_io(
                table=table,
                batch_rows=batch_rows,
                repeats=repeats,
            )
            clear_keys(location=location)
            flight_result = benchmark_pipeline_flight(
                location=location,
                table=table,
                batch_rows=batch_rows,
                repeats=repeats,
                key_prefix=f"pipeline-io-{count}",
            )
            clear_keys(location=location)

            ratio = (
                file_result["seconds"] / flight_result["seconds"]
                if flight_result["seconds"] > 0
                else 0.0
            )
            csv_rows.append(
                {
                    "batch_count": str(count),
                    "rows_per_batch": str(batch_rows),
                    "total_rows": str(total_rows),
                    "file_seconds": f"{file_result['seconds']:.6f}",
                    "flight_seconds": f"{flight_result['seconds']:.6f}",
                    "file_disk_mib": f"{(file_result['disk_bytes_written'] / (1024 * 1024)):.6f}",
                    "flight_disk_mib": "0.000000",
                    "file_vs_flight_time_ratio": f"{ratio:.6f}",
                }
            )

        write_benchmark_csv(output_csv=output_csv, rows=csv_rows)
        print(
            "Pipeline I/O benchmark complete: "
            f"location={location}, output_csv={output_csv}, batch_counts={counts}"
        )
    finally:
        if server is not None and thread is not None:
            _stop_local_server(server, thread)


def _run_benchmark_demo(
    host: str,
    output: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
    batch_rows: int,
    repeats: int,
    output_csv: str,
) -> None:
    server, thread, location = _start_local_server(host)
    try:
        parquet_path = write_random_ome_parquet(
            output_path=Path(output),
            rows=rows,
            height=height,
            width=width,
            seed=seed,
        )
        _run_benchmark_transport(
            host=host,
            port=int(location.rsplit(":", 1)[1]),
            parquet_path=str(parquet_path),
            key_prefix="benchmark-key",
            batch_rows=batch_rows,
            repeats=repeats,
            output_csv=output_csv,
        )
        _run_benchmark_overhead(
            host=host,
            port=int(location.rsplit(":", 1)[1]),
            rows=rows,
            height=height,
            width=width,
            seed=seed,
            repeats=repeats,
            key_prefix="benchmark-overhead",
            output_csv=output_csv.replace(".csv", "_overhead.csv"),
        )
    finally:
        _stop_local_server(server, thread)


def _run_slurm_simulate(
    output_dir: str,
    host: str,
    port: int,
    key: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
    batch_rows: int,
) -> None:
    result = simulate_slurm_parquet_workflow(
        output_dir=output_dir,
        host=host,
        port=port,
        key=key,
        rows=rows,
        height=height,
        width=width,
        seed=seed,
        batch_rows=batch_rows,
    )
    print(
        "Slurm simulation successful: "
        f"location={result['location']}, dataset={result['dataset']}, "
        f"job_ids={result['job_ids']}, sent_rows={result['sent_rows']}, "
        f"chunk_rows={result['received_chunk_rows']}"
    )


def main() -> None:
    """Entry-point for the demo CLI."""
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "server":
        _run_server(args.host, args.port)
    elif args.command == "send":
        _run_send(args.host, args.port, args.key)
    elif args.command == "receive":
        _run_receive(args.host, args.port, args.key)
    elif args.command in {"roundtrip", "roundtrip-one"}:
        _run_roundtrip_one(args.host, args.key)
    elif args.command == "roundtrip-column":
        _run_roundtrip_column(
            host=args.host,
            key=args.key,
            rows=args.rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
        )
    elif args.command == "parquet-generate":
        _run_parquet_generate(
            output=args.output,
            rows=args.rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
        )
    elif args.command == "parquet-stream":
        _run_parquet_stream(
            host=args.host,
            port=args.port,
            key=args.key,
            parquet_path=args.parquet_path,
            batch_rows=args.batch_rows,
        )
    elif args.command == "parquet-receive":
        _run_parquet_receive(host=args.host, port=args.port, key=args.key)
    elif args.command == "parquet-demo":
        _run_parquet_demo(
            host=args.host,
            key=args.key,
            output=args.output,
            rows=args.rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
            batch_rows=args.batch_rows,
        )
    elif args.command == "pipeline-produce":
        _run_pipeline_produce(
            host=args.host,
            port=args.port,
            key=args.key,
            rows=args.rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
        )
    elif args.command == "pipeline-transform":
        _run_pipeline_transform(
            host=args.host,
            port=args.port,
            input_key=args.input_key,
            output_key=args.output_key,
            stage_name=args.stage_name,
        )
    elif args.command == "pipeline-consume":
        _run_pipeline_consume(host=args.host, port=args.port, key=args.key)
    elif args.command == "pipeline-demo":
        _run_pipeline_demo(
            host=args.host,
            raw_key=args.raw_key,
            processed_key=args.processed_key,
            rows=args.rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
            stage_name=args.stage_name,
        )
    elif args.command == "benchmark-transport":
        _run_benchmark_transport(
            host=args.host,
            port=args.port,
            parquet_path=args.parquet_path,
            key_prefix=args.key_prefix,
            batch_rows=args.batch_rows,
            repeats=args.repeats,
            output_csv=args.output_csv,
        )
    elif args.command == "benchmark-overhead":
        _run_benchmark_overhead(
            host=args.host,
            port=args.port,
            rows=args.rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
            repeats=args.repeats,
            key_prefix=args.key_prefix,
            output_csv=args.output_csv,
        )
    elif args.command == "benchmark-pipeline-io":
        _run_benchmark_pipeline_io(
            host=args.host,
            port=args.port,
            use_existing_server=args.use_existing_server,
            batch_counts=args.batch_counts,
            batch_rows=args.batch_rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
            repeats=args.repeats,
            output_csv=args.output_csv,
        )
    elif args.command == "benchmark-demo":
        _run_benchmark_demo(
            host=args.host,
            output=args.output,
            rows=args.rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
            batch_rows=args.batch_rows,
            repeats=args.repeats,
            output_csv=args.output_csv,
        )
    elif args.command == "slurm-simulate":
        _run_slurm_simulate(
            output_dir=args.output_dir,
            host=args.host,
            port=args.port,
            key=args.key,
            rows=args.rows,
            height=args.height,
            width=args.width,
            seed=args.seed,
            batch_rows=args.batch_rows,
        )


if __name__ == "__main__":
    main()
