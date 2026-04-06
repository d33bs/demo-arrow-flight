"""Client helpers for sending/receiving OME-Arrow payloads via Flight."""

from __future__ import annotations

import numpy as np
import ome_arrow
import pyarrow as pa
import pyarrow.flight as flight


def send_table(
    location: str,
    key: str,
    table: pa.Table,
    max_chunksize: int | None = None,
) -> None:
    """Send an Arrow table to a Flight server."""
    descriptor = flight.FlightDescriptor.for_path(key)
    client = flight.FlightClient(location)
    writer, _reader = client.do_put(descriptor, table.schema)
    try:
        writer.write_table(table, max_chunksize=max_chunksize)
    finally:
        writer.close()


def receive_table(location: str, key: str) -> pa.Table:
    """Receive an Arrow table from a Flight server by key."""
    client = flight.FlightClient(location)
    info = client.get_flight_info(flight.FlightDescriptor.for_path(key))
    if not info.endpoints:
        raise RuntimeError("FlightInfo had no endpoints.")

    reader = client.do_get(info.endpoints[0].ticket)
    return reader.read_all()


def send_ome_arrow(location: str, key: str, ome_scalar: pa.StructScalar) -> None:
    """Send a one-row Arrow table containing an OME-Arrow scalar."""
    ome_array = pa.array([ome_scalar.as_py()], type=ome_scalar.type)
    table = pa.table({"ome_arrow": ome_array})
    send_table(location=location, key=key, table=table)


def receive_ome_arrow(location: str, key: str) -> tuple[pa.StructScalar, np.ndarray]:
    """Receive OME-Arrow scalar + numpy image array from Flight."""
    table = receive_table(location=location, key=key)
    scalar = table["ome_arrow"][0]
    image = ome_arrow.to_numpy(scalar)
    return scalar, np.squeeze(image)
