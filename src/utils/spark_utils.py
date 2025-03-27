import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from config.config import Config

logger = logging.getLogger(__name__)


def create_spark_session(config, app_name="IcebergLogAnalytics", metastore_uri="thrift://hive-metastore:9083",
    local_mode=False):
    """
    Create and configure a Spark session for Iceberg operations.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Configured Spark session
        :param config:
        :param local_mode:
        :param metastore_uri:
        :param app_name:
    """
    logger.info(f"Creating Spark session with app name: {app_name}")

    access_key=config.access_key

    secret_key=config.secret_key


    warehouse_dir = config.metastore_path
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
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instances", "2") \
        .config("spark.kubernetes.executor.request.cores", "256m") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
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


def optimize_partitioning(partition_columns, df, count_sample=None):
    """
    Optimize partitioning based on data volume and configured strategy.

    Args:
        df: DataFrame to partition
        count_sample: Optional pre-computed count (to avoid counting large datasets)

    Returns:
        Optimally partitioned DataFrame
    """
    if count_sample is None:
        # Sample 1% of data to estimate total size
        sample_ratio = 0.01
        row_count_estimate = df.sample(fraction=sample_ratio).count() / sample_ratio
    else:
        row_count_estimate = count_sample

    logger.info(f"Estimated record count: {row_count_estimate:,}")

    # Calculate optimal partition count based on strategy
    if Config.partition_strategy == "dynamic":
        # Dynamic strategy based on data volume
        optimal_partitions = max(
            Config.base_partitions,
            min(
                Config.max_partitions,
                int(row_count_estimate / Config.target_records_per_partition)
            )
        )

        logger.info(f"Using dynamic partitioning with {optimal_partitions} partitions")

        # Use provided partition columns or default to process_date
        partition_cols = partition_columns

        # For high-volume data (>50M records), add hour-level partitioning
        if row_count_estimate > 50000000 and "process_date" in partition_cols:
            # Add hour-level partitioning for high volume
            df = df.withColumn(
                "process_hour",
                expr("date_format(timestamp, 'HH')")
            )
            partition_cols = ["process_date", "process_hour"] + [
                col for col in partition_cols if col != "process_date"
            ]
            logger.info(f"Added hour-level partitioning for high volume data")

        # Repartition the DataFrame
        return df.repartition(optimal_partitions, *partition_cols)

