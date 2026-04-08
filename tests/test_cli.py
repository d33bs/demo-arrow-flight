from __future__ import annotations

import subprocess


def test_cli_roundtrip_command() -> None:
    result = subprocess.run(
        ["uv", "run", "demo-arrow-flight", "roundtrip"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Roundtrip one successful" in result.stdout


def test_cli_roundtrip_column_command() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "demo-arrow-flight",
            "roundtrip-column",
            "--rows",
            "5",
            "--height",
            "12",
            "--width",
            "10",
            "--seed",
            "23",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Roundtrip column successful" in result.stdout


def test_cli_parquet_demo_command() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "demo-arrow-flight",
            "parquet-demo",
            "--output",
            "/tmp/random_ome_dataset_cli_test.parquet",
            "--rows",
            "7",
            "--height",
            "16",
            "--width",
            "16",
            "--seed",
            "13",
            "--batch-rows",
            "3",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Parquet stream demo successful" in result.stdout


def test_cli_pipeline_demo_command() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "demo-arrow-flight",
            "pipeline-demo",
            "--rows",
            "7",
            "--height",
            "12",
            "--width",
            "10",
            "--seed",
            "9",
            "--stage-name",
            "normalize",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Pipeline demo successful" in result.stdout


def test_cli_benchmark_demo_command() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "demo-arrow-flight",
            "benchmark-demo",
            "--output",
            "/tmp/random_ome_benchmark_cli_test.parquet",
            "--rows",
            "8",
            "--height",
            "12",
            "--width",
            "10",
            "--seed",
            "9",
            "--batch-rows",
            "3",
            "--repeats",
            "1",
            "--output-csv",
            "/tmp/demo_arrow_flight_benchmark_cli_test.csv",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Benchmark complete" in result.stdout


def test_cli_benchmark_overhead_command() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "demo-arrow-flight",
            "benchmark-demo",
            "--output",
            "/tmp/random_ome_benchmark_cli_test.parquet",
            "--rows",
            "40",
            "--height",
            "32",
            "--width",
            "32",
            "--seed",
            "9",
            "--batch-rows",
            "8",
            "--repeats",
            "1",
            "--output-csv",
            "/tmp/demo_arrow_flight_benchmark_cli_test.csv",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Benchmark overhead complete" in result.stdout


def test_cli_slurm_simulate_command() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "demo-arrow-flight",
            "slurm-simulate",
            "--output-dir",
            "/tmp/demo_arrow_flight_slurm_cli_test",
            "--rows",
            "7",
            "--height",
            "12",
            "--width",
            "10",
            "--seed",
            "5",
            "--batch-rows",
            "3",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Slurm simulation successful" in result.stdout
