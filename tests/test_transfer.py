from __future__ import annotations

import socket
import threading
import time

import pyarrow as pa

from demo_arrow_flight.flight_server import InMemoryFlightServer
from demo_arrow_flight.ome_image import build_demo_ome_arrow
from demo_arrow_flight.transfer import (
    clear_keys,
    delete_key,
    receive_ome_arrow,
    receive_table,
    server_stats,
    send_ome_arrow,
    send_table,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_send_receive_roundtrip() -> None:
    port = _free_port()
    location = f"grpc://127.0.0.1:{port}"
    key = "test-image"

    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        payload = build_demo_ome_arrow()

        send_ome_arrow(location, key, payload)
        scalar = receive_ome_arrow(location, key)
        assert scalar.as_py()["name"] == "demo-flight-image"
        assert scalar.as_py()["pixels_meta"]["size_x"] == 128
        assert scalar.as_py()["pixels_meta"]["size_y"] == 96
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_send_receive_table_roundtrip() -> None:
    port = _free_port()
    location = f"grpc://127.0.0.1:{port}"
    key = "test-table"

    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        table = pa.table({"value": [1, 2, 3], "label": ["a", "b", "c"]})
        send_table(location=location, key=key, table=table)
        restored = receive_table(location=location, key=key)
        assert restored.equals(table)
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_delete_clear_and_stats_actions() -> None:
    port = _free_port()
    location = f"grpc://127.0.0.1:{port}"
    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        table = pa.table({"value": [1, 2, 3]})
        send_table(location=location, key="k1", table=table)
        send_table(location=location, key="k2", table=table)

        stats = server_stats(location=location)
        assert stats["current_keys"] == 2
        assert stats["peak_keys"] >= 2

        delete_result = delete_key(location=location, key="k1")
        assert bool(delete_result["ok"])
        assert bool(delete_result["existed"])

        stats = server_stats(location=location)
        assert stats["current_keys"] == 1

        clear_result = clear_keys(location=location)
        assert bool(clear_result["ok"])
        assert int(clear_result["removed"]) == 1

        stats = server_stats(location=location)
        assert stats["current_keys"] == 0
    finally:
        server.shutdown()
        thread.join(timeout=2)
