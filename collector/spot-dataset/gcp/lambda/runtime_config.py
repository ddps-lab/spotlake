from __future__ import annotations

import os
from dataclasses import dataclass

from const_config import GcpCollector, Storage

STORAGE_CONST = Storage()
GCP_CONST = GcpCollector()


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off", ""}


@dataclass(frozen=True)
class GcpRuntimeConfig:
    read_bucket_name: str
    write_bucket_name: str
    latest_read_path: str
    latest_write_path: str
    raw_prefix: str
    query_selector_read_bucket_name: str
    query_selector_write_bucket_name: str
    query_selector_read_path: str
    query_selector_write_path: str
    public_read_enabled: bool
    timestream_enabled: bool
    query_selector_enabled: bool
    compute_api_backend: str
    write_database_name: str
    write_table_name: str


def load_runtime_config() -> GcpRuntimeConfig:
    read_bucket = os.environ.get("GCP_READ_BUCKET_NAME", STORAGE_CONST.BUCKET_NAME)
    write_bucket = os.environ.get("GCP_WRITE_BUCKET_NAME", STORAGE_CONST.BUCKET_NAME)
    latest_path_default = GCP_CONST.S3_LATEST_DATA_SAVE_PATH
    query_selector_path_default = "query-selector/query-selector-gcp.json"
    compute_api_backend = os.environ.get("GCP_COMPUTE_API_BACKEND", "sdk").strip().lower()
    if compute_api_backend not in {"sdk", "rest"}:
        raise ValueError("GCP_COMPUTE_API_BACKEND must be one of: sdk, rest")

    return GcpRuntimeConfig(
        read_bucket_name=read_bucket,
        write_bucket_name=write_bucket,
        latest_read_path=os.environ.get("GCP_LATEST_READ_PATH", latest_path_default),
        latest_write_path=os.environ.get("GCP_LATEST_WRITE_PATH", latest_path_default),
        raw_prefix=os.environ.get("GCP_RAW_PREFIX", "rawdata/gcp").rstrip("/"),
        query_selector_read_bucket_name=os.environ.get(
            "GCP_QUERY_SELECTOR_READ_BUCKET_NAME",
            write_bucket,
        ),
        query_selector_write_bucket_name=os.environ.get(
            "GCP_QUERY_SELECTOR_WRITE_BUCKET_NAME",
            write_bucket,
        ),
        query_selector_read_path=os.environ.get(
            "GCP_QUERY_SELECTOR_READ_PATH",
            query_selector_path_default,
        ),
        query_selector_write_path=os.environ.get(
            "GCP_QUERY_SELECTOR_WRITE_PATH",
            query_selector_path_default,
        ),
        public_read_enabled=_env_bool("GCP_PUBLIC_READ_ENABLED", True),
        timestream_enabled=_env_bool("GCP_TIMESTREAM_ENABLED", True),
        query_selector_enabled=_env_bool("GCP_QUERY_SELECTOR_ENABLED", True),
        compute_api_backend=compute_api_backend,
        write_database_name=os.environ.get(
            "GCP_TIMESTREAM_DATABASE_NAME",
            STORAGE_CONST.DATABASE_NAME,
        ),
        write_table_name=os.environ.get(
            "GCP_TIMESTREAM_TABLE_NAME",
            STORAGE_CONST.GCP_TABLE_NAME,
        ),
    )
