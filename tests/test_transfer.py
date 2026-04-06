from __future__ import annotations

import socket
import threading
import time

import numpy as np
import pyarrow as pa

from demo_arrow_flight.flight_server import InMemoryFlightServer
from demo_arrow_flight.ome_image import build_demo_image, build_demo_ome_arrow
from demo_arrow_flight.transfer import (
    receive_ome_arrow,
    receive_table,
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
        image = build_demo_image(height=20, width=30)
        payload = build_demo_ome_arrow(image)

        send_ome_arrow(location, key, payload)
        _scalar, restored = receive_ome_arrow(location, key)

        np.testing.assert_array_equal(restored, image)
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
