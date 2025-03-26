import logging
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, date_format, lit, regexp_extract, udf,
                                   weekofyear)
from pyspark.sql.types import StringType

from src.config.config import Config
logger = logging.getLogger(__name__)


class LogTransformation:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    def extract_device_type(self, user_agent):
        """
        Extract device type from user agent string.
        """
        if user_agent is None:
            return "Unknown"

        user_agent = user_agent.lower()

        if "mobile" in user_agent:
            return "Mobile"
        elif "tablet" in user_agent:
            return "Tablet"
        elif "ipad" in user_agent:
            return "Tablet"
        elif "android" in user_agent and not "mobile" in user_agent:
            return "Tablet"
        elif "windows phone" in user_agent:
            return "Mobile"
        else:
            return "Desktop"

    def transform_logs(self):
        """
        Transform the raw logs into a cleaner format.
        Add additional columns for analytics.
        """
        logger.info("Starting log transformation")

        # Create UDF for device type extraction
        extract_device_udf = udf(self.extract_device_type, StringType())

        # Read raw logs
        raw_logs = self.spark.table(self.config.raw_table_full_name)

        # Transform logs
        transformed_logs = (
            raw_logs.withColumn("device_type", extract_device_udf(col("user_agent")))
            .withColumn("year", date_format(col("timestamp"), "yyyy"))
            .withColumn("month", date_format(col("timestamp"), "MM"))
            .withColumn("day", date_format(col("timestamp"), "dd"))
            .withColumn("week", weekofyear(col("timestamp")))
        )

        # Create or replace the processed logs table
        transformed_logs.write.format("iceberg").mode("overwrite").option(
            "path", self.config.processed_table_path
        ).partitionBy("year", "month", "day").saveAsTable(
            self.config.processed_table_full_name
        )

        logger.info(f"Successfully transformed {transformed_logs.count()} logs")
