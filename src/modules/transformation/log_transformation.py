import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, date_format, udf,
                                   weekofyear)
from pyspark.sql.types import StringType

from src.config.config import Config

logger = logging.getLogger(__name__)


class LogTransformation:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    @staticmethod
    def extract_device_type(user_agent):
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

        self.spark.udf.register("extract_device_type", self.extract_device_type, StringType())

        # Read raw logs
        self.spark.sql(f"SELECT * FROM {self.config.raw_table_full_name}").createOrReplaceTempView("raw_logs")

        drop_table = f"DROP TABLE IF EXISTS {self.config.processed_table_full_name}"

        self.spark.sql(drop_table)
        # Transform logs
        # Transform logs using Spark SQL
        transformation_sql = f"""
                CREATE OR REPLACE TABLE {self.config.processed_table_full_name}
                USING iceberg
                LOCATION '{self.config.processed_table_path}'
                PARTITIONED BY (year, month, day)
                AS
                SELECT
                    *,
                    extract_device_type(user_agent) AS device_type,
                    date_format(timestamp, 'yyyy') AS year,
                    date_format(timestamp, 'MM') AS month,
                    date_format(timestamp, 'dd') AS day,
                    weekofyear(timestamp) AS week
                FROM raw_logs
                """
        self.spark.sql(transformation_sql)

        transformed_count = \
        self.spark.sql(f"SELECT COUNT(*) as count FROM {self.config.processed_table_full_name}").collect()[0]["count"]

        logger.info(f"Successfully transformed {transformed_count} logs")

