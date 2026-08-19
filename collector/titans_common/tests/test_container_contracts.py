from pathlib import Path


def test_azure_batch_native_dependencies_are_pinned():
    collector_root = Path(__file__).resolve().parents[2]
    dockerfile = (
        collector_root / "spot-dataset" / "azure" / "batch" / "Dockerfile"
    ).read_text(encoding="utf-8")

    assert "FROM python:3.12.13-slim\n" in dockerfile
    assert "numpy==2.4.4" in dockerfile
    assert "pandas==3.0.2" in dockerfile
    assert "polars==1.40.1" in dockerfile
    assert "pyarrow==24.0.0" in dockerfile
    assert "PYTHONUNBUFFERED=1" in dockerfile
    assert "PYTHONFAULTHANDLER=1" in dockerfile
    assert "ARG AWS_ACCESS_KEY_ID" not in dockerfile
    assert "ARG AWS_SECRET_ACCESS_KEY" not in dockerfile
    assert "ENV AWS_ACCESS_KEY_ID" not in dockerfile
    assert "ENV AWS_SECRET_ACCESS_KEY" not in dockerfile
