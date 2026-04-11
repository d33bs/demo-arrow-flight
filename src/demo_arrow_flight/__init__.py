"""demo_arrow_flight package."""

from .benchmarking import (
    benchmark_baseline_parquet_read,
    benchmark_pipeline_file_io,
    benchmark_pipeline_flight,
    benchmark_flight_table_roundtrip,
    benchmark_flight_stream,
    benchmark_parquet_write_read,
    write_benchmark_csv,
)
from .flight_pipeline_demo import (
    pipeline_consume_from_flight,
    pipeline_produce_to_flight,
    pipeline_transform_on_flight,
)
from .ome_image import build_demo_ome_arrow, build_demo_image
from .parquet_stream_demo import (
    build_random_ome_table,
    receive_streamed_chunks,
    stream_parquet_in_chunks,
    write_random_ome_parquet,
)
from .slurm_simulation import simulate_slurm_parquet_workflow
from .transfer import (
    clear_keys,
    delete_key,
    receive_ome_arrow,
    receive_table,
    send_ome_arrow,
    send_table,
    server_stats,
)

__all__ = [
    "benchmark_baseline_parquet_read",
    "benchmark_parquet_write_read",
    "benchmark_flight_stream",
    "benchmark_flight_table_roundtrip",
    "benchmark_pipeline_file_io",
    "benchmark_pipeline_flight",
    "write_benchmark_csv",
    "pipeline_produce_to_flight",
    "pipeline_transform_on_flight",
    "pipeline_consume_from_flight",
    "build_demo_image",
    "build_demo_ome_arrow",
    "build_random_ome_table",
    "write_random_ome_parquet",
    "stream_parquet_in_chunks",
    "receive_streamed_chunks",
    "simulate_slurm_parquet_workflow",
    "send_ome_arrow",
    "receive_ome_arrow",
    "send_table",
    "receive_table",
    "delete_key",
    "clear_keys",
    "server_stats",
]
