import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (col, count, date_format, day, desc, month,
                                   row_number, weekofyear, year)

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

        # Read processed logs
        processed_logs = self.spark.table(self.config.processed_table_full_name)

        # Daily Top 5 IP addresses
        window_spec = Window.partitionBy("date").orderBy(desc("request_count"))

        daily_top_ips = (
            processed_logs.groupBy("date", "ip")
            .agg(count("*").alias("request_count"))
            .withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") <= 5)
            .drop("rank")
            .orderBy("date", desc("request_count"))
        )

        # Save daily top IPs
        daily_top_ips.write.format("iceberg").mode("overwrite").saveAsTable(
            self.config.daily_ip_analytics_table
        )

        logger.info(
            f"Daily top IPs analytics saved to {self.config.daily_ip_analytics_table}"
        )

        # Daily Top 5 devices
        window_spec = Window.partitionBy("date").orderBy(desc("request_count"))

        daily_top_devices = (
            processed_logs.groupBy("date", "device_type")
            .agg(count("*").alias("request_count"))
            .withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") <= 5)
            .drop("rank")
            .orderBy("date", desc("request_count"))
        )

        # Save daily top devices
        daily_top_devices.write.format("iceberg").mode("overwrite").saveAsTable(
            self.config.daily_device_analytics_table
        )

        logger.info(
            f"Daily top devices analytics saved to {self.config.daily_device_analytics_table}"
        )
        logger.info("Daily analytics completed successfully")

    def run_weekly_analytics(self):
        """
        Run weekly analytics:
        - Top 5 IP addresses by request count
        - Top 5 devices
        """
        logger.info("Running weekly analytics")

        # Read processed logs
        processed_logs = self.spark.table(self.config.processed_table_full_name)

        # Create a week identifier column
        weekly_logs = processed_logs.withColumn(
            "year_week",
            year(col("timestamp"))
            .cast("string")
            .concat(lit("-"), weekofyear(col("timestamp")).cast("string")),
        )

        # Weekly Top 5 IP addresses
        window_spec = Window.partitionBy("year_week").orderBy(desc("request_count"))

        weekly_top_ips = (
            weekly_logs.groupBy("year_week", "ip")
            .agg(count("*").alias("request_count"))
            .withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") <= 5)
            .drop("rank")
            .orderBy("year_week", desc("request_count"))
        )

        # Save weekly top IPs
        weekly_top_ips.write.format("iceberg").mode("overwrite").saveAsTable(
            self.config.weekly_ip_analytics_table
        )

        logger.info(
            f"Weekly top IPs analytics saved to {self.config.weekly_ip_analytics_table}"
        )

        # Weekly Top 5 devices
        window_spec = Window.partitionBy("year_week").orderBy(desc("request_count"))

        weekly_top_devices = (
            weekly_logs.groupBy("year_week", "device_type")
            .agg(count("*").alias("request_count"))
            .withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") <= 5)
            .drop("rank")
            .orderBy("year_week", desc("request_count"))
        )

        # Save weekly top devices
        weekly_top_devices.write.format("iceberg").mode("overwrite").saveAsTable(
            self.config.weekly_device_analytics_table
        )

        logger.info(
            f"Weekly top devices analytics saved to {self.config.weekly_device_analytics_table}"
        )
        logger.info("Weekly analytics completed successfully")

    def display_results(self):
        """
        Display the results of the analytics for reference
        """
        logger.info("Displaying analytics results")

        # Display daily top IPs
        logger.info("=== Daily Top 5 IP Addresses ===")
        daily_top_ips = self.spark.table(self.config.daily_ip_analytics_table)
        daily_top_ips.show()

        # Display daily top devices
        logger.info("=== Daily Top 5 Devices ===")
        daily_top_devices = self.spark.table(self.config.daily_device_analytics_table)
        daily_top_devices.show()

        # Display weekly top IPs
        logger.info("=== Weekly Top 5 IP Addresses ===")
        weekly_top_ips = self.spark.table(self.config.weekly_ip_analytics_table)
        weekly_top_ips.show()

        # Display weekly top devices
        logger.info("=== Weekly Top 5 Devices ===")
        weekly_top_devices = self.spark.table(self.config.weekly_device_analytics_table)
        weekly_top_devices.show()
