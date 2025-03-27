import logging
from pyspark.sql import SparkSession

from src.config.config import Config

logger = logging.getLogger(__name__)


class LogAnalytics:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    def run_daily_analytics(self):
        """
        Run daily analytics:
        - Top 5 IP addresses by request count
        - Top 5 devices
        """
        logger.info("Running daily analytics")

        # Ensure processed logs table is accessible as a view
        self.spark.sql(f"SELECT * FROM {self.config.processed_table_full_name}").createOrReplaceTempView(
            "processed_logs")

        drop_table = f"""DROP TABLE IF EXISTS {self.config.daily_ip_analytics_table}"""

        self.spark.sql(drop_table)

        # Daily Top 5 IP addresses using SQL window functions
        daily_top_ips_sql = f"""
        CREATE OR REPLACE TABLE {self.config.daily_ip_analytics_table}
        USING iceberg
        AS
        WITH ranked_ips AS (
            SELECT 
                day,
                ip,
                COUNT(*) AS request_count,
                ROW_NUMBER() OVER (PARTITION BY day ORDER BY COUNT(*) DESC) as rank
            FROM processed_logs
            GROUP BY day, ip
        )
        SELECT 
            day,
            ip,
            request_count
        FROM ranked_ips
        WHERE rank <= 5
        ORDER BY day, request_count DESC
        """

        # Execute the daily top IPs analytics
        self.spark.sql(daily_top_ips_sql)

        logger.info(f"Daily top IPs analytics saved to {self.config.daily_ip_analytics_table}")

        drop_table = f"""DROP TABLE IF EXISTS {self.config.daily_device_analytics_table}"""

        self.spark.sql(drop_table)
        # Daily Top 5 devices using SQL window functions
        daily_top_devices_sql = f"""
        CREATE OR REPLACE TABLE {self.config.daily_device_analytics_table}
        USING iceberg
        AS
        WITH ranked_devices AS (
            SELECT 
                day,
                device_type,
                COUNT(*) AS request_count,
                ROW_NUMBER() OVER (PARTITION BY day ORDER BY COUNT(*) DESC) as rank
            FROM processed_logs
            GROUP BY day, device_type
        )
        SELECT 
            day,
            device_type,
            request_count
        FROM ranked_devices
        WHERE rank <= 5
        ORDER BY day, request_count DESC
        """

        # Execute the daily top devices analytics
        self.spark.sql(daily_top_devices_sql)

        logger.info(f"Daily top devices analytics saved to {self.config.daily_device_analytics_table}")
        logger.info("Daily analytics completed successfully")

    def run_weekly_analytics(self):
        """
        Run weekly analytics:
        - Top 5 IP addresses by request count
        - Top 5 devices
        """
        logger.info("Running weekly analytics")

        # Create a week identifier for analytics
        weekly_logs_sql = """
        CREATE OR REPLACE TEMPORARY VIEW weekly_logs AS
        SELECT 
            *,
            CONCAT(YEAR(timestamp), '-', WEEKOFYEAR(timestamp)) AS year_week
        FROM processed_logs
        """

        self.spark.sql(weekly_logs_sql)

        # Weekly Top 5 IP addresses using SQL window functions

        drop_table = f"""DROP TABLE IF EXISTS {self.config.weekly_ip_analytics_table}"""

        self.spark.sql(drop_table)

        weekly_top_ips_sql = f"""
        CREATE OR REPLACE TABLE {self.config.weekly_ip_analytics_table}
        USING iceberg
        AS
        WITH ranked_ips AS (
            SELECT 
                year_week,
                ip,
                COUNT(*) AS request_count,
                ROW_NUMBER() OVER (PARTITION BY year_week ORDER BY COUNT(*) DESC) as rank
            FROM weekly_logs
            GROUP BY year_week, ip
        )
        SELECT 
            year_week,
            ip,
            request_count
        FROM ranked_ips
        WHERE rank <= 5
        ORDER BY year_week, request_count DESC
        """

        # Execute the weekly top IPs analytics
        self.spark.sql(weekly_top_ips_sql)

        logger.info(f"Weekly top IPs analytics saved to {self.config.weekly_device_analytics_table}")

        # Weekly Top 5 devices using SQL window functions
        weekly_top_devices_sql = f"""
        CREATE OR REPLACE TABLE {self.config.weekly_device_analytics_table}
        USING iceberg
        AS
        WITH ranked_devices AS (
            SELECT 
                year_week,
                device_type,
                COUNT(*) AS request_count,
                ROW_NUMBER() OVER (PARTITION BY year_week ORDER BY COUNT(*) DESC) as rank
            FROM weekly_logs
            GROUP BY year_week, device_type
        )
        SELECT 
            year_week,
            device_type,
            request_count
        FROM ranked_devices
        WHERE rank <= 5
        ORDER BY year_week, request_count DESC
        """

        # Execute the weekly top devices analytics
        self.spark.sql(weekly_top_devices_sql)

        logger.info(f"Weekly top devices analytics saved to {self.config.weekly_device_analytics_table}")
        logger.info("Weekly analytics completed successfully")

    def display_results(self):
        """
        Display the results of the analytics for reference
        """
        logger.info("Displaying analytics results")

        # Display daily top IPs
        logger.info("=== Daily Top 5 IP Addresses ===")
        self.spark.sql(f"SELECT * FROM {self.config.daily_ip_analytics_table}").show()

        # Display daily top devices
        logger.info("=== Daily Top 5 Devices ===")
        self.spark.sql(f"SELECT * FROM {self.config.daily_device_analytics_table}").show()

        # Display weekly top IPs
        logger.info("=== Weekly Top 5 IP Addresses ===")
        self.spark.sql(f"SELECT * FROM {self.config.weekly_ip_analytics_table}").show()

        # Display weekly top devices
        logger.info("=== Weekly Top 5 Devices ===")
        self.spark.sql(f"SELECT * FROM {self.config.weekly_device_analytics_table}").show()