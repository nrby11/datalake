import logging
from pyspark.sql import SparkSession
from config.config import Config

from utils.spark_utils import optimize_partitioning

from utils.utils import timeit

logger = logging.getLogger(__name__)


class LogAnalytics:
    def __init__(self, spark: SparkSession, config: Config, iceberg):
        self.spark = spark
        self.config = config
        self.iceberg = iceberg

    @timeit
    def read_processed_logs(self):
        """
        Read the processed logs and register a temporary view.
        """
        processed_logs = self.spark.table(self.config.processed_table_full_name)

        return processed_logs

    # --- Daily Analytics Functions ---
    @timeit
    def run_daily_ip_analytics(self):
        """
        Compute daily top 5 IP addresses by request count and write to a table.
        """
        logger.info("Running daily IP analytics")
        daily_top_ips_sql = """
        WITH ranked_ips AS (
            SELECT 
                process_date,
                ip,
                COUNT(*) AS request_count,
                ROW_NUMBER() OVER (PARTITION BY process_date ORDER BY COUNT(*) DESC) AS rank
            FROM processed_logs
            GROUP BY process_date, ip
        )
        SELECT 
            process_date,
            ip,
            request_count
        FROM ranked_ips
        WHERE rank <= 5
        ORDER BY process_date, request_count DESC
        """
        daily_top_ips = self.spark.sql(daily_top_ips_sql)

        self.iceberg.write_to_iceberg_table(
            df=daily_top_ips,
            database_name=self.config.database_name,
            table_name=self.config.daily_ip_analytics_table,
            partition_columns=["process_date"]
        )

        logger.info(f"Daily top IPs analytics saved to {self.config.daily_ip_analytics_table}")

    @timeit
    def run_daily_device_analytics(self):
        """
        Compute daily top 5 devices by request count and write to a table.
        """
        logger.info("Running daily device analytics")
        daily_top_devices_sql = """
        WITH ranked_devices AS (
            SELECT 
                process_date,
                device_type,
                COUNT(*) AS request_count,
                ROW_NUMBER() OVER (PARTITION BY process_date ORDER BY COUNT(*) DESC) AS rank
            FROM processed_logs
            GROUP BY process_date, device_type
        )
        SELECT 
            process_date,
            device_type,
            request_count
        FROM ranked_devices
        WHERE rank <= 5
        ORDER BY process_date, request_count DESC
        """
        daily_top_devices = self.spark.sql(daily_top_devices_sql)

        self.iceberg.write_to_iceberg_table(
            df=daily_top_devices,
            database_name=self.config.database_name,
            table_name=self.config.daily_device_analytics_table,
            partition_columns=["process_date"]
        )

        logger.info(f"Daily top devices analytics saved to {self.config.daily_device_analytics_table}")

    @timeit
    def run_daily_analytics(self, cached_df= None):
        """
        Orchestrates the daily analytics by invoking IP and device analytics functions.
        """
        logger.info("Running daily analytics")
        if cached_df is not None:
            logger.info("Using cached transformed logs")
            processed_logs = cached_df
        else:
            logger.info("Reading processed logs from table")
            processed_logs = self.spark.table(self.config.processed_table_full_name)

        partition_cols = ["process_date", "hour"]
        processed_logs = optimize_partitioning(partition_cols, processed_logs)
        processed_logs.createOrReplaceTempView("processed_logs")

        self.run_daily_ip_analytics()
        self.run_daily_device_analytics()
        logger.info("Daily analytics completed successfully")

    # --- Weekly Analytics Functions ---
    @timeit
    def run_weekly_ip_analytics(self):
        """
        Compute weekly top 5 IP addresses by request count and write to a table.
        """
        logger.info("Running weekly IP analytics")
        weekly_top_ips_sql = """
        WITH weekly_data AS (
            SELECT 
                CONCAT(YEAR(timestamp), '-', LPAD(CAST(WEEKOFYEAR(timestamp) AS STRING), 2, '0')) AS year_week,
                ip,
                COUNT(*) AS request_count
            FROM processed_logs
            GROUP BY year_week, ip
        ),
        ranked_ips AS (
            SELECT 
                year_week,
                ip,
                request_count,
                ROW_NUMBER() OVER (PARTITION BY year_week ORDER BY request_count DESC) AS rank
            FROM weekly_data
        )
        SELECT 
            year_week,
            ip,
            request_count
        FROM ranked_ips
        WHERE rank <= 5
        ORDER BY year_week, request_count DESC
        """
        weekly_top_ips = self.spark.sql(weekly_top_ips_sql)

        self.iceberg.write_to_iceberg_table(
            df=weekly_top_ips,
            database_name=self.config.database_name,
            table_name=self.config.weekly_ip_analytics_table,
            partition_columns=["year_week"]
        )

        logger.info(f"Weekly top IPs analytics saved to {self.config.weekly_ip_analytics_table}")

    @timeit
    def run_weekly_device_analytics(self):
        """
        Compute weekly top 5 devices by request count and write to a table.
        """
        logger.info("Running weekly device analytics")
        weekly_top_devices_sql = """
        WITH weekly_data AS (
            SELECT 
                CONCAT(YEAR(timestamp), '-', LPAD(CAST(WEEKOFYEAR(timestamp) AS STRING), 2, '0')) AS year_week,
                device_type,
                COUNT(*) AS request_count
            FROM processed_logs
            GROUP BY year_week, device_type
        ),
        ranked_devices AS (
            SELECT 
                year_week,
                device_type,
                request_count,
                ROW_NUMBER() OVER (PARTITION BY year_week ORDER BY request_count DESC) AS rank
            FROM weekly_data
        )
        SELECT 
            year_week,
            device_type,
            request_count
        FROM ranked_devices
        WHERE rank <= 5
        ORDER BY year_week, request_count DESC
        """
        weekly_top_devices = self.spark.sql(weekly_top_devices_sql)

        self.iceberg.write_to_iceberg_table(
            df=weekly_top_devices,
            database_name=self.config.database_name,
            table_name=self.config.weekly_device_analytics_table,
            partition_columns=["year_week"]
        )

        logger.info(f"Weekly top devices analytics saved to {self.config.weekly_device_analytics_table}")
    @timeit
    def run_weekly_analytics(self, cached_df):
        """
        Orchestrates the weekly analytics by invoking IP and device analytics functions.
        """
        logger.info("Running weekly analytics")
        if cached_df is not None:
            logger.info("Using cached transformed logs")
            processed_logs = cached_df
        else:
            logger.info("Reading processed logs from table")
            processed_logs = self.spark.table(self.config.processed_table_full_name)
        partition_cols = ["process_date", "hour"]
        processed_logs = optimize_partitioning(partition_cols, processed_logs)
        processed_logs.createOrReplaceTempView("processed_logs")

        self.run_weekly_ip_analytics()
        self.run_weekly_device_analytics()
        logger.info("Weekly analytics completed successfully")

    # --- Display Function ---

    def display_results(self):
        """
        Display the analytics results for daily and weekly top IPs and devices.
        """
        logger.info("Displaying analytics results")

        logger.info("=== Daily Top 5 IP Addresses ===")
        daily_top_ips = self.spark.table(self.config.daily_ip_analytics_table)
        daily_top_ips.show()

        logger.info("=== Daily Top 5 Devices ===")
        daily_top_devices = self.spark.table(self.config.daily_device_analytics_table)
        daily_top_devices.show()

        logger.info("=== Weekly Top 5 IP Addresses ===")
        weekly_top_ips = self.spark.table(self.config.weekly_ip_analytics_table)
        weekly_top_ips.show()

        logger.info("=== Weekly Top 5 Devices ===")
        weekly_top_devices = self.spark.table(self.config.weekly_device_analytics_table)
        weekly_top_devices.show()
