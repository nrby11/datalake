import logging
import os
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(app_name="IcebergLogAnalytics"):
    """
    Create and configure a Spark session for Iceberg operations.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    logger.info(f"Creating Spark session with app name: {app_name}")

    # Initialize builder
    builder = SparkSession.builder.appName(app_name)

    # Set up configurations for Iceberg
    spark = builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "warehouse") \
        .config("spark.sql.warehouse.dir", "warehouse") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark session created successfully")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

    return spark


def stop_spark_session(spark):
    """
    Properly stop a Spark session.

    Args:
        spark (SparkSession): The Spark session to stop
    """
    if spark is not None:
        logger.info("Stopping Spark session")
        spark.stop()
        logger.info("Spark session stopped successfully")