from __future__ import annotations

from demo_arrow_flight.slurm_simulation import simulate_slurm_parquet_workflow


def test_simulate_slurm_parquet_workflow(tmp_path) -> None:
    out_dir = tmp_path / "slurm-sim"
    result = simulate_slurm_parquet_workflow(
        output_dir=out_dir,
        host="127.0.0.1",
        port=8835,
        key="slurm-test",
        rows=7,
        height=12,
        width=10,
        seed=5,
        batch_rows=3,
    )

    assert result["sent_rows"] == 7
    assert result["received_rows"] == 7
    assert result["received_chunk_rows"] == [3, 3, 1]

    job_ids = result["job_ids"]
    assert isinstance(job_ids, list)
    assert len(job_ids) == 3

    for job_id in job_ids:
        assert (out_dir / f"slurm-{job_id}.out").exists()

    assert (out_dir / "random_ome_dataset.parquet").exists()
