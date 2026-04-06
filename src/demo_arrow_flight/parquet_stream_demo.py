"""Independent demo: randomized OME-Arrow parquet + chunked Flight streaming."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import ome_arrow
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq


def build_random_ome_table(
    rows: int,
    height: int,
    width: int,
    seed: int,
) -> pa.Table:
    """Build an Arrow table with randomized OME-Arrow rows."""
    if rows < 1:
        raise ValueError("rows must be >= 1")

    rng = np.random.default_rng(seed)
    ome_dicts: list[dict] = []
    ome_type: pa.DataType | None = None

    for idx in range(rows):
        image = rng.integers(0, 65535, size=(height, width), dtype=np.uint16)
        scalar = ome_arrow.from_numpy(
            image,
            dim_order="YX",
            name=f"random-image-{idx}",
            image_id=f"img-{idx}",
            image_type="image",
        )
        ome_type = scalar.type
        ome_dicts.append(scalar.as_py())

    assert ome_type is not None
    return pa.table(
        {
            "row_id": pa.array(list(range(rows)), type=pa.int32()),
            "ome_arrow": pa.array(ome_dicts, type=ome_type),
        }
    )


def write_random_ome_parquet(
    output_path: str | Path,
    rows: int = 16,
    height: int = 64,
    width: int = 64,
    seed: int = 7,
) -> Path:
    """Create randomized OME-Arrow parquet dataset."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    table = build_random_ome_table(rows=rows, height=height, width=width, seed=seed)
    pq.write_table(table, path)
    return path


def stream_parquet_in_chunks(
    location: str,
    key: str,
    parquet_path: str | Path,
    batch_rows: int = 4,
) -> tuple[int, int]:
    """Stream parquet to Flight in multiple record batches."""
    parquet_file = pq.ParquetFile(parquet_path)
    client = flight.FlightClient(location)
    descriptor = flight.FlightDescriptor.for_path(key)

    writer, _reader = client.do_put(descriptor, parquet_file.schema_arrow)
    total_rows = 0
    total_batches = 0

    try:
        for batch in parquet_file.iter_batches(batch_size=batch_rows):
            writer.write_batch(batch)
            total_rows += batch.num_rows
            total_batches += 1
    finally:
        writer.close()

    return total_rows, total_batches


def receive_streamed_chunks(location: str, key: str) -> tuple[list[int], int]:
    """Read streamed Flight record batches and return chunk sizes + total rows."""
    client = flight.FlightClient(location)
    descriptor = flight.FlightDescriptor.for_path(key)
    info = client.get_flight_info(descriptor)
    if not info.endpoints:
        raise RuntimeError("FlightInfo had no endpoints.")

    reader = client.do_get(info.endpoints[0].ticket)
    chunk_rows: list[int] = []

    while True:
        try:
            chunk = reader.read_chunk()
        except StopIteration:
            break

        if chunk.data is None:
            continue
        chunk_rows.append(chunk.data.num_rows)

    return chunk_rows, sum(chunk_rows)
