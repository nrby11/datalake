import logging

from src.config.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, date_format, regexp_extract,
                                   to_timestamp)

logger = logging.getLogger(__name__)


class LogIngestion:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    def ingest_logs(self):
        """
        Ingest raw logs from S3 into a temporary table.
        The logs are in the format:
        184.87.250.135 - - [06/Nov/2024:23:20:37 +0000] "GET /Integrated/challenge.gif HTTP/1.1" 200 2344 "-" "Mozilla/5.0 (Macintosh; PPC Mac OS X 10_7_2) AppleWebKit/5310 (KHTML, like Gecko) Chrome/39.0.897.0 Mobile Safari/5310"
        """
        logger.info(f"Ingesting logs from {self.config.input_path}")

        # Create database if not exists
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.database_name}")

        # Read raw logs
        raw_logs = self.spark.read.text(self.config.input_path)

        # Define regex patterns for log parsing
        ip_pattern = r"^(\S+)"
        timestamp_pattern = r"\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+\-]\d{4})\]"
        method_pattern = r'"(\S+)'
        endpoint_pattern = r'"(?:\S+)\s+(\S+)'
        protocol_pattern = r'"(?:\S+)\s+(?:\S+)\s+(\S+)"'
        status_pattern = r"\s(\d{3})\s"
        bytes_pattern = r"\s(\d+)\s"
        user_agent_pattern = r'"([^"]*)"$'

        # Parse logs using regex
        parsed_logs = (
            raw_logs.withColumn("ip", regexp_extract(col("value"), ip_pattern, 1))
            .withColumn(
                "timestamp_str", regexp_extract(col("value"), timestamp_pattern, 1)
            )
            .withColumn("method", regexp_extract(col("value"), method_pattern, 1))
            .withColumn("endpoint", regexp_extract(col("value"), endpoint_pattern, 1))
            .withColumn("protocol", regexp_extract(col("value"), protocol_pattern, 1))
            .withColumn(
                "status",
                regexp_extract(col("value"), status_pattern, 1).cast("integer"),
            )
            .withColumn(
                "bytes", regexp_extract(col("value"), bytes_pattern, 1).cast("integer")
            )
            .withColumn(
                "user_agent", regexp_extract(col("value"), user_agent_pattern, 1)
            )
        )

        # Convert timestamp to proper format
        parsed_logs = parsed_logs.withColumn(
            "timestamp", to_timestamp(col("timestamp_str"), "dd/MMM/yyyy:HH:mm:ss Z")
        ).withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))

        # Create or replace the raw logs table
        parsed_logs.write.format("iceberg").mode("overwrite").option(
            "path", self.config.raw_table_path
        ).saveAsTable(self.config.raw_table_full_name)

        logger.info(f"Successfully ingested {parsed_logs.count()} logs")
