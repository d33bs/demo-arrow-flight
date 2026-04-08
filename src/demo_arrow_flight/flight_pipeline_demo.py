"""Multi-stage Arrow Flight pipeline demo (produce -> transform -> consume)."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.flight as flight

from .parquet_stream_demo import build_random_ome_table
from .transfer import receive_table, send_table


def pipeline_produce_to_flight(
    location: str,
    key: str,
    rows: int,
    height: int,
    width: int,
    seed: int,
) -> int:
    """Produce a randomized OME-Arrow table and send it to Flight."""
    table = build_random_ome_table(rows=rows, height=height, width=width, seed=seed)
    send_table(location=location, key=key, table=table)
    return table.num_rows


def pipeline_transform_on_flight(
    location: str,
    input_key: str,
    output_key: str,
    stage_name: str = "preprocess",
) -> int:
    """Read a Flight table by key, add stage metadata, write to new key."""
    table = receive_table(location=location, key=input_key)
    stage_array = pa.array([stage_name] * table.num_rows)
    transformed = table.append_column("pipeline_stage", stage_array)
    send_table(location=location, key=output_key, table=transformed)
    return transformed.num_rows


def pipeline_consume_from_flight(location: str, key: str) -> dict[str, object]:
    """Consume a Flight table and report chunk-level information."""
    client = flight.FlightClient(location)
    info = client.get_flight_info(flight.FlightDescriptor.for_path(key))
    if not info.endpoints:
        raise RuntimeError("FlightInfo had no endpoints.")

    reader = client.do_get(info.endpoints[0].ticket)
    chunk_rows: list[int] = []
    total_rows = 0

    while True:
        try:
            chunk = reader.read_chunk()
        except StopIteration:
            break

        if chunk.data is None:
            continue
        chunk_rows.append(chunk.data.num_rows)
        total_rows += chunk.data.num_rows

    return {
        "rows": total_rows,
        "chunks": len(chunk_rows),
        "chunk_rows": chunk_rows,
    }
