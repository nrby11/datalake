import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(app_name="IcebergLogAnalytics", metastore_uri="thrift://hive-metastore:9083",
    warehouse_dir="s3a://terraform-20250326081517067100000004/hive/warehouse",
    local_mode=False):
    """
    Create and configure a Spark session for Iceberg operations.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Configured Spark session
        :param local_mode:
        :param metastore_uri:
        :param app_name:
        :param warehouse_dir:
    """
    logger.info(f"Creating Spark session with app name: {app_name}")

    if local_mode:
        metastore_uri = "thrift://localhost:9083"

    # Set up configurations for Iceberg
    spark = SparkSession.builder \
        .appName("IcebergDataPipeline") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hive.type", "hive") \
        .config("spark.sql.catalog.hive.uri", metastore_uri) \
        .config("spark.sql.defaultCatalog", "hive") \
        .config("spark.hadoop.hive.metastore.uris", metastore_uri) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.400") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "2") \
        .config("spark.kubernetes.executor.request.cores", "256m") \
        .config("spark.hadoop.fs.s3a.access.key", "AKIA5QV57ZTOSESHYBIY") \
        .config("spark.hadoop.fs.s3a.secret.key", "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") \
        .enableHiveSupport() \
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