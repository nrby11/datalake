import logging

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf, weekofyear, hour
from pyspark.sql.types import StringType

from config.config import Config

from utils.utils import timeit

from utils.spark_utils import optimize_partitioning

logger = logging.getLogger(__name__)

class LogTransformation:
    def __init__(self, spark: SparkSession, config: Config, iceberg):
        self.iceberg = iceberg
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
        elif "android" in user_agent and "mobile" not in user_agent:
            return "Tablet"
        elif "windows phone" in user_agent:
            return "Mobile"
        else:
            return "Desktop"

    @timeit
    def register_udfs(self):
        """
        Register UDFs used in the transformation.
        """
        extract_device_udf = udf(self.extract_device_type, StringType())
        self.spark.udf.register("extract_device_type", extract_device_udf)
        return extract_device_udf

    @timeit
    def read_raw_logs(self):
        """
        Read raw logs from the raw table.
        """
        raw_logs = self.spark.table(self.config.raw_table_full_name)
        partition_cols =["process_date", "hour"]
        raw_logs = optimize_partitioning(partition_cols, raw_logs)
        raw_logs.createOrReplaceTempView("raw_logs")
        return raw_logs

    @timeit
    def transform_columns(self, raw_logs, extract_device_udf):
        """
        Apply transformations to add new columns.
        """
        transformed_logs_sql = """SELECT 
                                    *,
                                    extract_device_type(user_agent) AS device_type,
                                    weekofyear(timestamp) AS week
                                FROM raw_logs
                            """
        transformed_logs = self.spark.sql(transformed_logs_sql)
        transformed_logs.persist(StorageLevel.MEMORY_AND_DISK)
        return transformed_logs

    @timeit
    def write_table(self, transformed_logs):
        """
        Write the transformed logs to the processed table.
        Uses dynamic partition overwrite if the table already exists.
        """
        self.iceberg.write_to_iceberg_table(
            df=transformed_logs,
            database_name=self.config.database_name,
            table_name=self.config.processed_table_full_name,
            partition_columns=["process_date", "hour"]
        )

    @timeit
    def count_records(self):
        """
        Count the records in the processed table.
        """
        record_count = self.spark.table(self.config.processed_table_full_name).count()
        logger.info(f"Successfully transformed {record_count} logs")
        return record_count

    @timeit
    def transform_logs(self):
        """
        Orchestrates the log transformation process.
        """
        logger.info("Starting log transformation")
        extract_device_udf = self.register_udfs()
        raw_logs = self.read_raw_logs()
        transformed_logs = self.transform_columns(raw_logs, extract_device_udf)
        self.write_table(transformed_logs)
        self.count_records()
        return transformed_logs
