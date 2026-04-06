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
