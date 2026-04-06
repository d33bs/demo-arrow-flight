from __future__ import annotations

import socket
import threading
import time

import pyarrow.parquet as pq

from demo_arrow_flight.flight_server import InMemoryFlightServer
from demo_arrow_flight.parquet_stream_demo import (
    receive_streamed_chunks,
    stream_parquet_in_chunks,
    write_random_ome_parquet,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_write_random_ome_parquet(tmp_path) -> None:
    dataset_path = tmp_path / "random_ome.parquet"
    out = write_random_ome_parquet(
        output_path=dataset_path,
        rows=5,
        height=12,
        width=10,
        seed=17,
    )

    assert out.exists()
    table = pq.read_table(out)
    assert table.num_rows == 5
    assert set(table.column_names) == {"row_id", "ome_arrow"}


def test_stream_parquet_in_chunks_and_receive(tmp_path) -> None:
    dataset_path = tmp_path / "stream_me.parquet"
    write_random_ome_parquet(
        output_path=dataset_path,
        rows=7,
        height=16,
        width=16,
        seed=31,
    )

    port = _free_port()
    location = f"grpc://127.0.0.1:{port}"
    key = "random-dataset"

    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        sent_rows, sent_batches = stream_parquet_in_chunks(
            location=location,
            key=key,
            parquet_path=dataset_path,
            batch_rows=3,
        )
        recv_chunk_rows, recv_total_rows = receive_streamed_chunks(location=location, key=key)

        assert sent_rows == 7
        assert sent_batches == 3
        assert recv_chunk_rows == [3, 3, 1]
        assert recv_total_rows == 7
    finally:
        server.shutdown()
        thread.join(timeout=2)
