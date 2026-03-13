"""Provider-specific configuration for TITANS integration."""
import os
from dataclasses import dataclass, field
from typing import Dict, List


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
    column_rename: Dict[str, str] = field(default_factory=dict)  # Raw CSV → canonical name

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
        """Provider-specific canonical dtype definitions.

        IMPORTANT: dtypes MUST match cold tier (S3 parquet) exactly for columns
        that exist in both cold and hot/warm tiers. Polars concat rejects dtype
        mismatches on same-named columns.
        """
        import polars as pl
        base = {
            self.time_column: pl.Datetime("us", "UTC"),
            "Ceased": pl.Boolean,
        }
        for col in self.pk_columns:
            base[col] = pl.Utf8
        for col in self.value_columns:
            base[col] = self._value_dtypes.get(col, pl.Float64)
        return base


# Explicit per-provider dtype mappings.
# Each must match the cold tier parquet dtype for columns present in cold tier.
# Columns only in hot/warm (not in cold) can use any appropriate type.
def _make_pl_dtypes():
    """Build dtype dicts using polars types."""
    import polars as pl
    return {
        "aws": {
            # Cold tier: SPS=Int64, T3=Int64, T2=Int64, Savings=Int64
            "SPS": pl.Int64, "T3": pl.Int64, "T2": pl.Int64,
            "IF": pl.Float64, "OndemandPrice": pl.Float64,
            "SpotPrice": pl.Float64, "Savings": pl.Int64,
        },
        "azure": {
            # Cold tier (convert.py bigint_columns): Score=Int64, T2=Int64, T3=Int64
            # Cold tier: DesiredCount=Float64, Savings=Float64
            "DesiredCount": pl.Float64,
            "Score": pl.Int64, "T3": pl.Int64, "T2": pl.Int64,
            "IF": pl.Float64, "OndemandPrice": pl.Float64,
            "SpotPrice": pl.Float64, "Savings": pl.Float64,
        },
        "gcp": {
            # Cold tier: Savings=Float64, Score not in cold
            "Score": pl.Float64,
            "OndemandPrice": pl.Float64, "SpotPrice": pl.Float64,
            "Savings": pl.Float64,
        },
    }

_PL_DTYPES = None

def _get_pl_dtypes(provider: str) -> dict:
    global _PL_DTYPES
    if _PL_DTYPES is None:
        _PL_DTYPES = _make_pl_dtypes()
    return _PL_DTYPES.get(provider, {})


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
        pk_columns=["InstanceTier", "InstanceType", "Region", "AZ"],
        value_columns=["DesiredCount", "Score", "T3", "T2", "IF", "OndemandPrice", "SpotPrice", "Savings"],
        score_column="Score",
        column_rename={"AvailabilityZone": "AZ"},
    ),
    "gcp": ProviderConfig(
        name="gcp",
        pk_columns=["InstanceType", "Region"],
        value_columns=["Score", "OndemandPrice", "SpotPrice", "Savings"],
        score_column="Score",
    ),
}

# Attach per-provider dtype dicts after creation
for _name, _cfg in PROVIDER_CONFIGS.items():
    _cfg._value_dtypes = _get_pl_dtypes(_name)


def get_config(provider: str) -> ProviderConfig:
    """Return provider configuration."""
    if provider not in PROVIDER_CONFIGS:
        raise ValueError(f"Unknown provider: {provider}. Supported: {list(PROVIDER_CONFIGS.keys())}")
    return PROVIDER_CONFIGS[provider]
