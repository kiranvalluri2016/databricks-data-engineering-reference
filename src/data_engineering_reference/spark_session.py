"""Spark session construction for local Delta Lake execution."""
from __future__ import annotations

import sys
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_local_spark(app_name: str = "data-engineering-reference") -> SparkSession:
    """Create a local Spark session with Delta Lake extensions enabled."""
    # Spark starts Python workers from this environment variable before it can
    # apply SparkConf settings, so keep both driver and workers on Python 3.12.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[2]").appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
