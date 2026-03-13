"""Tests for titans_common.config module."""
import os
import pytest

# Force test environment (set before imports)
os.environ["TITANS_ENV"] = "test"

from titans_common.config import is_test_env, get_config, ProviderConfig, PROVIDER_CONFIGS


class TestIsTestEnv:
    """Tests for is_test_env function."""

    def test_returns_true_when_set_to_test(self):
        os.environ["TITANS_ENV"] = "test"
        assert is_test_env() is True

    def test_returns_false_when_set_to_production(self):
        os.environ["TITANS_ENV"] = "production"
        assert is_test_env() is False

    def test_returns_false_when_not_set(self):
        # Remove env var to test default
        original = os.environ.pop("TITANS_ENV", None)
        try:
            assert is_test_env() is False
        finally:
            # Restore
            if original:
                os.environ["TITANS_ENV"] = original


class TestGetConfig:
    """Tests for get_config function."""

    def test_get_config_aws(self):
        config = get_config("aws")
        assert config.name == "aws"
        assert config.pk_columns == ["InstanceType", "Region", "AZ"]
        assert "SPS" in config.value_columns
        assert config.time_column == "Time"

    def test_get_config_azure(self):
        config = get_config("azure")
        assert config.name == "azure"
        assert config.pk_columns == ["InstanceType", "Region", "AvailabilityZone"]
        assert "Score" in config.value_columns

    def test_get_config_gcp(self):
        config = get_config("gcp")
        assert config.name == "gcp"
        assert config.pk_columns == ["InstanceType", "Region"]

    def test_get_config_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown provider"):
            get_config("unknown")


class TestProviderConfig:
    """Tests for ProviderConfig class."""

    def test_hot_prefix_includes_test_when_test_env(self):
        os.environ["TITANS_ENV"] = "test"
        config = get_config("aws")
        assert config.hot_prefix.startswith("test/")
        assert "parquet_cp_hot/aws" in config.hot_prefix

    def test_hot_prefix_no_test_when_production(self):
        os.environ["TITANS_ENV"] = "production"
        config = get_config("aws")
        assert not config.hot_prefix.startswith("test/")
        assert config.hot_prefix == "parquet_cp_hot/aws"

    def test_warm_prefix_includes_test_when_test_env(self):
        os.environ["TITANS_ENV"] = "test"
        config = get_config("aws")
        assert config.warm_prefix.startswith("test/")
        assert "parquet_warm/aws/m8" in config.warm_prefix

    def test_warm_prefix_no_test_when_production(self):
        os.environ["TITANS_ENV"] = "production"
        config = get_config("aws")
        assert not config.warm_prefix.startswith("test/")
        assert config.warm_prefix == "parquet_warm/aws/m8"

    def test_canonical_columns_order(self):
        config = get_config("aws")
        expected_start = ["InstanceType", "Region", "AZ", "Time"]
        assert config.canonical_columns[:4] == expected_start
        assert "Ceased" in config.canonical_columns

    def test_schema_dtypes_has_all_columns(self):
        config = get_config("aws")
        schema = config.schema_dtypes

        # Check PK columns
        for pk in config.pk_columns:
            assert pk in schema

        # Check special columns
        assert "Time" in schema
        assert "Ceased" in schema

    def test_titans_bucket_default(self):
        config = get_config("aws")
        assert config.titans_bucket == "titans-spotlake-data"


class TestProviderConfigsRegistry:
    """Tests for PROVIDER_CONFIGS registry."""

    def test_all_providers_exist(self):
        assert "aws" in PROVIDER_CONFIGS
        assert "azure" in PROVIDER_CONFIGS
        assert "gcp" in PROVIDER_CONFIGS

    def test_all_configs_are_provider_config_instances(self):
        for name, config in PROVIDER_CONFIGS.items():
            assert isinstance(config, ProviderConfig)
            assert config.name == name
