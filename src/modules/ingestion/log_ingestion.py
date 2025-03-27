import logging

from pyspark.sql import SparkSession

from config.config import Config
from pyspark.sql.functions import expr

from utils.monitoring.monitoring import \
    analyze_physical_partition_skew

from utils.spark_utils import optimize_partitioning

logger = logging.getLogger(__name__)


class LogIngestion:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

        # Define regex patterns for log parsing
        self.ip_pattern = r"^(\\S+)"
        self.timestamp_pattern = r"\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [+\\-]\\d{4})\\]"
        self.method_pattern = r'"(\\S+)'
        self.endpoint_pattern = r'"(?:\\S+)\\s+(\\S+)'
        self.protocol_pattern = r'"(?:\\S+)\\s+(?:\\S+)\\s+(\\S+)"'
        self.status_pattern = r"\\s(\\d{3})\\s"
        self.bytes_pattern = r"\\s(\\d+)\\s"
        self.user_agent_pattern = r'"([^"]*)"$'

    def extract_log_fields(self):
        """
        Extract fields from raw logs using regexp_extract.
        Returns a DataFrame with extracted fields.
        """
        # Read raw logs
        raw_logs = self.spark.read.text(self.config.input_path)

        # Register the raw logs as a temporary view
        raw_logs.createOrReplaceTempView("raw_logs_temp")

        # Define extraction SQL with the regex patterns
        extracted_df = raw_logs.selectExpr(
            f"regexp_extract(value, '{self.ip_pattern}', 1) AS ip",
            f"regexp_extract(value, '{self.timestamp_pattern}', 1) AS timestamp_str",
            f"regexp_extract(value, '{self.method_pattern}', 1) AS method",
            f"regexp_extract(value, '{self.endpoint_pattern}', 1) AS endpoint",
            f"regexp_extract(value, '{self.protocol_pattern}', 1) AS protocol",
            f"cast(regexp_extract(value, '{self.status_pattern}', 1) AS INT) AS status",
            f"cast(regexp_extract(value, '{self.bytes_pattern}', 1) AS INT) AS bytes",
            f"regexp_extract(value, '{self.user_agent_pattern}', 1) AS user_agent"
        )

        # Extract the basic fields
        extracted_df.createOrReplaceTempView("extracted_fields")

        return extracted_df

    def ingest_logs(self):
        """
        Ingest raw logs from S3 into a temporary table.
        The logs are in the format:
        184.87.250.135 - - [06/Nov/2024:23:20:37 +0000] "GET /Integrated/challenge.gif HTTP/1.1" 200 2344 "-" "Mozilla/5.0 (Macintosh; PPC Mac OS X 10_7_2) AppleWebKit/5310 (KHTML, like Gecko) Chrome/39.0.897.0 Mobile Safari/5310"
        """
        logger.info(f"Ingesting logs from {self.config.input_path}")

        # Create database if not exists
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.database_name}")

        # Extract fields from the raw logs
        self.extract_log_fields()

        # Process the extracted fields to add timestamp and date
        timestamp_sql = """
        SELECT
            *,
            to_timestamp(timestamp_str, 'dd/MMM/yyyy:HH:mm:ss Z') AS timestamp,
            date_format(to_timestamp(timestamp_str, 'dd/MMM/yyyy:HH:mm:ss Z'), 'yyyy-MM-dd') AS process_date
        FROM extracted_fields
        """

        parsed_logs_df = self.spark.sql(timestamp_sql)
        parsed_logs_df.createOrReplaceTempView("parsed_logs_temp")
        partition_columns = ["process_date"]

        # Apply dynamic partitioning based on configuration
        parsed_logs_df = optimize_partitioning(partition_columns, parsed_logs_df)


        # analyze_physical_partition_skew(parsed_logs_df)

        # Check if the table exists
        table_exists = self.spark._jsparkSession.catalog().tableExists(
            self.config.database_name,
            self.config.raw_table_name
        )

        if not table_exists:
            # Create the table with initial data
            logger.info(f"Creating new table: {self.config.raw_table_full_name}")
            parsed_logs_df.write \
                .format("iceberg") \
                .partitionBy("process_date") \
                .option("write-format", "parquet") \
                .saveAsTable(self.config.raw_table_full_name)
        else:
            # For existing table, use dynamic partition overwrite
            logger.info(f"Writing to existing table with dynamic partition overwrite")

            # Enable dynamic partition overwrite
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

            parsed_logs_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("overwrite-mode", "dynamic") \
                .partitionBy("process_date") \
                .saveAsTable(self.config.raw_table_full_name)

        # Count the records
        log_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.config.raw_table_full_name}").collect()[0][
            "count"]

        logger.info(f"Successfully ingested {log_count} logs")