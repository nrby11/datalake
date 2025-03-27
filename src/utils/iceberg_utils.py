import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class IcebergTableUtils:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_to_iceberg_table(self,
                               df: DataFrame,
                               database_name: str,
                               table_name: str,
                               partition_columns: list = None
                               ):
        """
        Write DataFrame to an Iceberg table with existence check and appropriate write mode.

        Args:
            df: DataFrame to write
            database_name: Database name
            table_name: Table name (without database)
            partition_columns: List of columns to partition by (optional)
        """
        # Check if table exists
        table_exists = self.spark._jsparkSession.catalog().tableExists(table_name)

        # Set up the writer
        writer = df.write.format("iceberg")

        # Set partition columns if provided
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        if not table_exists:
            # Create new table
            logger.info(f"Creating new table: {table_name}")
            writer.saveAsTable(table_name)

        else:
            logger.info(f"Writing to existing table: {table_name} with dynamic overwrite")

            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

            writer = writer.mode("overwrite")

            if partition_columns:
                writer = writer.option("overwrite-mode", "dynamic")

            writer.saveAsTable(table_name)

        return table_name