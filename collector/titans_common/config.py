"""Provider-specific configuration for TITANS integration."""
import os
from dataclasses import dataclass
from typing import List


def is_test_env() -> bool:
    """Check environment variable dynamically (at call time, not import time)."""
    return os.environ.get("TITANS_ENV", "production") == "test"


@dataclass
class ProviderConfig:
    """Provider-specific TITANS configuration."""
    name: str                          # aws, azure, gcp
    pk_columns: List[str]              # Primary key columns
    value_columns: List[str]           # Value columns
    time_column: str = "Time"
    score_column: str = "SPS"
    titans_bucket: str = "titans-spotlake-data"

    @property
    def hot_prefix(self) -> str:
        """Hot tier prefix (dynamically determined by environment)."""
        env_prefix = "test/" if is_test_env() else ""
        return f"{env_prefix}parquet_cp_hot/{self.name}"

    @property
    def warm_prefix(self) -> str:
        """Warm tier prefix (dynamically determined by environment)."""
        env_prefix = "test/" if is_test_env() else ""
        return f"{env_prefix}parquet_warm/{self.name}/m8"

    @property
    def canonical_columns(self) -> List[str]:
        """Canonical column order: PK + Time + Value + Ceased."""
        return self.pk_columns + [self.time_column] + self.value_columns + ["Ceased"]

    @property
    def schema_dtypes(self) -> dict:
        """Provider-specific canonical dtype definitions."""
        import polars as pl
        base = {
            self.time_column: pl.Datetime("us", "UTC"),
            "Ceased": pl.Boolean,
        }
        for col in self.pk_columns:
            base[col] = pl.Utf8
        for col in self.value_columns:
            if col in ["SPS", "T3", "T2", "Savings", "Score", "InstanceTier", "DesiredCount"]:
                base[col] = pl.Int64
            else:  # IF, OndemandPrice, SpotPrice
                base[col] = pl.Float64
        return base


# Provider configuration definitions
PROVIDER_CONFIGS = {
    "aws": ProviderConfig(
        name="aws",
        pk_columns=["InstanceType", "Region", "AZ"],
        value_columns=["SPS", "T3", "T2", "IF", "OndemandPrice", "SpotPrice", "Savings"],
        score_column="SPS",
    ),
    "azure": ProviderConfig(
        name="azure",
        pk_columns=["InstanceType", "Region", "AvailabilityZone"],
        value_columns=["InstanceTier", "DesiredCount", "Score", "T3", "T2", "IF", "OndemandPrice", "SpotPrice", "Savings"],
        score_column="Score",
    ),
    "gcp": ProviderConfig(
        name="gcp",
        pk_columns=["InstanceType", "Region"],
        value_columns=["Score", "OndemandPrice", "SpotPrice", "Savings"],
        score_column="Score",
    ),
}


def get_config(provider: str) -> ProviderConfig:
    """Return provider configuration."""
    if provider not in PROVIDER_CONFIGS:
        raise ValueError(f"Unknown provider: {provider}. Supported: {list(PROVIDER_CONFIGS.keys())}")
    return PROVIDER_CONFIGS[provider]
