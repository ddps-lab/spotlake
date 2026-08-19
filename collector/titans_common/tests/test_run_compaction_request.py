import json
import sys

from titans_common import run_compaction_request
from titans_common.warm_compactor import ConcurrencyConflictError


def test_manifest_conflict_is_returned_as_retryable_failure(monkeypatch, tmp_path):
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps(
            {
                "provider": "azure",
                "hot_key": "parquet_cp_hot/azure/2026/08/03/11-50.parquet",
                "timestamp": "2026-08-03T11:50:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    def raise_conflict(*args, **kwargs):
        raise ConcurrencyConflictError("manifest changed")

    monkeypatch.setattr(run_compaction_request, "run_compaction", raise_conflict)
    monkeypatch.setattr(run_compaction_request.boto3, "client", lambda _: object())
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_compaction_request.py", "--request", str(request_path)],
    )

    assert run_compaction_request.main() == 75
