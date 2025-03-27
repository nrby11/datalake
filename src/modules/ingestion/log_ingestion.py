import time
import logging
from pyspark.sql import SparkSession
from config.config import Config
from pyspark.sql.functions import expr
from utils.monitoring.monitoring import analyze_physical_partition_skew
from utils.spark_utils import optimize_partitioning
from utils.utils import timeit

logger = logging.getLogger(__name__)

class LogIngestion:
    def __init__(self, spark: SparkSession, config: Config, iceberg):
        self.iceberg = iceberg
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

    @timeit
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

    @timeit
    def create_database(self):
        """Creates the database if it does not exist."""
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.database_name}")

    @timeit
    def process_parsed_logs(self):
        """
        Process the extracted fields to add timestamp and date fields.
        Returns a DataFrame with parsed logs.
        """
        timestamp_sql = """
        SELECT
            *,
            to_timestamp(timestamp_str, 'dd/MMM/yyyy:HH:mm:ss Z') AS timestamp,
            date_format(to_timestamp(timestamp_str, 'dd/MMM/yyyy:HH:mm:ss Z'), 'yyyy-MM-dd') AS process_date,
            date_format(to_timestamp(timestamp_str, 'dd/MMM/yyyy:HH:mm:ss Z'), 'HH') AS hour
        FROM extracted_fields
        """
        parsed_logs_df = self.spark.sql(timestamp_sql)
        parsed_logs_df.createOrReplaceTempView("parsed_logs_temp")

        partition_columns = ["process_date", "hour"]
        parsed_logs_df = optimize_partitioning(partition_columns, parsed_logs_df)
        return parsed_logs_df

    @timeit
    def write_table(self, parsed_logs_df):
        """
        Write the parsed logs to the target table.
        Uses dynamic partition overwrite if the table already exists.
        """

        # Write to table using the common utility
        self.iceberg.write_to_iceberg_table(
            df=parsed_logs_df,
            database_name=self.config.database_name,
            table_name=self.config.raw_table_full_name,
            partition_columns=["process_date", "hour"]
        )

    @timeit
    def count_logs(self):
        """Count the number of logs ingested into the target table."""
        count_df = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.config.raw_table_full_name}")
        log_count = count_df.collect()[0]["count"]
        logger.info(f"Successfully ingested {log_count} logs")
        return log_count

    @timeit
    def ingest_logs(self):
        """
        Ingest raw logs from S3 into the target table.
        This function orchestrates the process by calling smaller functions.
        """
        logger.info(f"Ingesting logs from {self.config.input_path}")

        self.create_database()
        self.extract_log_fields()
        parsed_logs_df = self.process_parsed_logs()
        self.write_table(parsed_logs_df)
        self.count_logs()
