"""In-memory Apache Arrow Flight server for demo image transfer."""

from __future__ import annotations

import json
from typing import Dict
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.flight as flight


class InMemoryFlightServer(flight.FlightServerBase):
    """Small demo server storing Arrow tables by descriptor path key."""

    def __init__(self, location: str) -> None:
        super().__init__(location)
        self._location = location
        self._tables: Dict[str, pa.Table] = {}
        self._peak_bytes = 0
        self._peak_keys = 0

    @property
    def location(self) -> str:
        """Return server location string."""
        return self._location

    def do_put(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.FlightStreamReader,
        writer: flight.FlightMetadataWriter,
    ) -> None:
        del context, writer
        key = _descriptor_to_key(descriptor)
        batches: list[pa.RecordBatch] = []
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            batches.append(chunk.data)

        if not batches:
            raise ValueError("Cannot store an empty stream.")

        self._tables[key] = pa.Table.from_batches(batches)
        self._update_peaks()

    def do_get(
        self,
        context: flight.ServerCallContext,
        ticket: flight.Ticket,
    ) -> flight.FlightDataStream:
        del context
        key = ticket.ticket.decode("utf-8")
        if key not in self._tables:
            raise KeyError(f"No table stored for key '{key}'.")
        return flight.RecordBatchStream(self._tables[key])

    def get_flight_info(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        del context
        key = _descriptor_to_key(descriptor)
        if key not in self._tables:
            raise KeyError(f"No table stored for key '{key}'.")

        table = self._tables[key]
        endpoint = flight.FlightEndpoint(
            ticket=flight.Ticket(key.encode("utf-8")),
            locations=[_location_to_flight(self._location)],
        )
        return flight.FlightInfo(
            table.schema,
            descriptor,
            [endpoint],
            table.num_rows,
            table.nbytes,
        )

    def list_flights(self, context, criteria):  # type: ignore[override]
        del context, criteria
        for key, table in self._tables.items():
            descriptor = flight.FlightDescriptor.for_path(key)
            endpoint = flight.FlightEndpoint(
                ticket=flight.Ticket(key.encode("utf-8")),
                locations=[_location_to_flight(self._location)],
            )
            yield flight.FlightInfo(
                table.schema,
                descriptor,
                [endpoint],
                table.num_rows,
                table.nbytes,
            )

    def list_actions(self, context):  # type: ignore[override]
        del context
        return [
            flight.ActionType("delete", "Delete a stored table by key."),
            flight.ActionType("clear", "Clear all stored tables."),
            flight.ActionType("stats", "Return current and peak memory/key stats."),
        ]

    def do_action(self, context, action):  # type: ignore[override]
        del context
        action_type = action.type
        body = bytes(action.body).decode("utf-8") if action.body is not None else ""

        if action_type == "delete":
            if not body:
                raise ValueError("delete action requires a key body.")
            existed = body in self._tables
            if existed:
                del self._tables[body]
            payload = {"ok": True, "deleted": body, "existed": existed}
            yield flight.Result(pa.py_buffer(json.dumps(payload).encode("utf-8")))
            return

        if action_type == "clear":
            removed = len(self._tables)
            self._tables.clear()
            payload = {"ok": True, "removed": removed}
            yield flight.Result(pa.py_buffer(json.dumps(payload).encode("utf-8")))
            return

        if action_type == "stats":
            payload = self._stats_payload()
            yield flight.Result(pa.py_buffer(json.dumps(payload).encode("utf-8")))
            return

        raise KeyError(f"Unsupported action type: {action_type}")

    def _stats_payload(self) -> dict[str, int]:
        return {
            "current_keys": len(self._tables),
            "current_bytes": sum(table.nbytes for table in self._tables.values()),
            "peak_keys": self._peak_keys,
            "peak_bytes": self._peak_bytes,
        }

    def _update_peaks(self) -> None:
        current = self._stats_payload()
        self._peak_keys = max(self._peak_keys, current["current_keys"])
        self._peak_bytes = max(self._peak_bytes, current["current_bytes"])



def _descriptor_to_key(descriptor: flight.FlightDescriptor) -> str:
    """Convert descriptor path to storage key."""
    if not descriptor.path:
        raise ValueError("Descriptor must provide a non-empty path.")
    return descriptor.path[0].decode("utf-8")


def _location_to_flight(location: str) -> flight.Location:
    """Convert grpc URI string to Flight Location."""
    parsed = urlparse(location)
    if parsed.scheme != "grpc":
        raise ValueError(f"Unsupported Flight URI scheme: {parsed.scheme}")
    if parsed.hostname is None or parsed.port is None:
        raise ValueError(f"Location must include host and port: {location}")
    return flight.Location.for_grpc_tcp(parsed.hostname, parsed.port)
