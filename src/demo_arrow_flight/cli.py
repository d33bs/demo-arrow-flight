"""CLI for the demo Arrow Flight OME-Arrow workflow."""

from __future__ import annotations

import argparse
import socket
import threading
import time
from pathlib import Path

import numpy as np
import ome_arrow
import pyarrow as pa

from .flight_server import InMemoryFlightServer
from .ome_image import build_demo_ome_arrow
from .parquet_stream_demo import (
    build_random_ome_table,
    receive_streamed_chunks,
    stream_parquet_in_chunks,
    write_random_ome_parquet,
)
from .transfer import receive_ome_arrow, receive_table, send_ome_arrow, send_table


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

    return parser


def _location(host: str, port: int) -> str:
    return f"grpc://{host}:{port}"


def _find_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


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
    _scalar, image = receive_ome_arrow(location, key)
    print(
        "Received image "
        f"shape={image.shape}, dtype={image.dtype}, min={image.min()}, max={image.max()}"
    )


def _run_roundtrip_one(host: str, key: str) -> None:
    port = _find_free_port(host)
    location = _location(host, port)
    server = InMemoryFlightServer(location)

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        payload = build_demo_ome_arrow()
        send_ome_arrow(location, key, payload)
        _scalar, image = receive_ome_arrow(location, key)
        checksum = int(image.sum())
        print(
            "Roundtrip one successful: "
            f"location={location}, key={key}, shape={image.shape}, checksum={checksum}"
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)


def _run_roundtrip_column(
    host: str,
    key: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
) -> None:
    port = _find_free_port(host)
    location = _location(host, port)
    server = InMemoryFlightServer(location)

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        table = build_random_ome_table(rows=rows, height=height, width=width, seed=seed)
        send_table(location=location, key=key, table=table)
        restored = receive_table(location=location, key=key)

        first_image = pa.scalar(
            restored["ome_arrow"][0].as_py(),
            type=restored["ome_arrow"][0].type,
        )
        first_shape = tuple(np.squeeze(ome_arrow.to_numpy(first_image)).shape)
        print(
            "Roundtrip column successful: "
            f"location={location}, key={key}, rows={restored.num_rows}, "
            f"columns={restored.num_columns}, first_image_shape={first_shape}"
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)


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
    port = _find_free_port(host)
    location = _location(host, port)
    server = InMemoryFlightServer(location)

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

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
        server.shutdown()
        thread.join(timeout=2)


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
    elif args.command == "roundtrip":
        _run_roundtrip_one(args.host, args.key)
    elif args.command == "roundtrip-one":
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


if __name__ == "__main__":
    main()
