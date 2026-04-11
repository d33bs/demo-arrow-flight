"""Client helpers for sending/receiving OME-Arrow payloads via Flight."""

from __future__ import annotations

import json

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


def receive_ome_arrow(location: str, key: str) -> pa.StructScalar:
    """Receive one OME-Arrow scalar from a one-row Flight table."""
    table = receive_table(location=location, key=key)
    return table["ome_arrow"][0]


def delete_key(location: str, key: str) -> dict[str, object]:
    """Delete one table key from the Flight server store."""
    client = flight.FlightClient(location)
    action = flight.Action("delete", key.encode("utf-8"))
    results = list(client.do_action(action))
    if not results:
        return {"ok": True, "deleted": key, "existed": False}
    return json.loads(bytes(results[0].body).decode("utf-8"))


def clear_keys(location: str) -> dict[str, object]:
    """Clear all tables from the Flight server store."""
    client = flight.FlightClient(location)
    results = list(client.do_action(flight.Action("clear", b"")))
    if not results:
        return {"ok": True, "removed": 0}
    return json.loads(bytes(results[0].body).decode("utf-8"))


def server_stats(location: str) -> dict[str, int]:
    """Return current and peak key/memory stats from the Flight server."""
    client = flight.FlightClient(location)
    results = list(client.do_action(flight.Action("stats", b"")))
    if not results:
        return {"current_keys": 0, "current_bytes": 0, "peak_keys": 0, "peak_bytes": 0}
    payload = json.loads(bytes(results[0].body).decode("utf-8"))
    return {
        "current_keys": int(payload.get("current_keys", 0)),
        "current_bytes": int(payload.get("current_bytes", 0)),
        "peak_keys": int(payload.get("peak_keys", 0)),
        "peak_bytes": int(payload.get("peak_bytes", 0)),
    }
