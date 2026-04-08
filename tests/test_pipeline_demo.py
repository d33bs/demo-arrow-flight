from __future__ import annotations

import socket
import threading
import time

from demo_arrow_flight.flight_pipeline_demo import (
    pipeline_consume_from_flight,
    pipeline_produce_to_flight,
    pipeline_transform_on_flight,
)
from demo_arrow_flight.flight_server import InMemoryFlightServer


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_pipeline_three_stage_flow() -> None:
    port = _free_port()
    location = f"grpc://127.0.0.1:{port}"
    server = InMemoryFlightServer(location)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.25)

    try:
        produced_rows = pipeline_produce_to_flight(
            location=location,
            key="raw",
            rows=7,
            height=12,
            width=10,
            seed=3,
        )
        transformed_rows = pipeline_transform_on_flight(
            location=location,
            input_key="raw",
            output_key="processed",
            stage_name="normalize",
        )
        consumed = pipeline_consume_from_flight(location=location, key="processed")

        assert produced_rows == 7
        assert transformed_rows == 7
        assert consumed["rows"] == 7
        assert consumed["chunks"] >= 1
    finally:
        server.shutdown()
        thread.join(timeout=2)
