"""Configuration boundaries for local and Databricks reference modes."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class ConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    mode: str
    storage_root: Path | None
    catalog: str | None = None
    schema: str | None = None


def load_settings(environment: dict[str, str] | None = None) -> Settings:
    env = environment or os.environ
    mode = env.get("DATA_PLATFORM_MODE", "local")
    if mode == "local":
        root = env.get("DATA_STORAGE_ROOT")
        if not root:
            raise ConfigurationError("DATA_STORAGE_ROOT is required in local mode")
        return Settings(mode, Path(root).resolve())
    if mode == "databricks":
        catalog, schema = env.get("DATABRICKS_CATALOG"), env.get("DATABRICKS_SCHEMA")
        if not catalog or not schema:
            raise ConfigurationError("DATABRICKS_CATALOG and DATABRICKS_SCHEMA are required")
        return Settings(mode, None, catalog, schema)
    raise ConfigurationError("DATA_PLATFORM_MODE must be local or databricks")
