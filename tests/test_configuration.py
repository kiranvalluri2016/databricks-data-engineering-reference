import pytest

from data_engineering_reference.config import ConfigurationError, load_settings


def test_local_mode_requires_explicit_storage_root():
    with pytest.raises(ConfigurationError):
        load_settings({})


def test_databricks_mode_requires_existing_namespace_configuration():
    with pytest.raises(ConfigurationError):
        load_settings({"DATA_PLATFORM_MODE": "databricks"})
    settings = load_settings({"DATA_PLATFORM_MODE": "databricks", "DATABRICKS_CATALOG": "existing", "DATABRICKS_SCHEMA": "retail"})
    assert settings.catalog == "existing"
