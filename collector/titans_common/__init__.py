"""Lazy exports for TITANS shared utilities."""

from importlib import import_module

__all__ = ["prepare_for_upload", "upload_hot_tier", "run_compaction", "ConcurrencyConflictError"]


def __getattr__(name: str):
    if name == "prepare_for_upload":
        return import_module("titans_common.utils").prepare_for_upload
    if name == "upload_hot_tier":
        return import_module("titans_common.upload_titans").upload_hot_tier
    if name == "run_compaction":
        return import_module("titans_common.warm_compactor").run_compaction
    if name == "ConcurrencyConflictError":
        return import_module("titans_common.warm_compactor").ConcurrencyConflictError
    raise AttributeError(f"module 'titans_common' has no attribute {name!r}")
