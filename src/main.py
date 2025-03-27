import argparse
import logging

from modules.analytics.analytics import LogAnalytics
from modules.ingestion.log_ingestion import LogIngestion
from modules.transformation.log_transformation import LogTransformation
from utils.iceberg_utils import IcebergTableUtils
from utils.monitoring.monitoring import monitor_query_performance
from utils.spark_utils import create_spark_session, stop_spark_session

from config.config import Config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Apache Iceberg Log Analytics")
    parser.add_argument("--input-path", required=True, help="S3 path to raw log files")
    parser.add_argument(
        "--output-path", required=True, help="S3 path for Iceberg tables"
    )
    parser.add_argument(
        "--catalog-name", default="hive", help="Iceberg catalog name"
    )
    parser.add_argument("--database-name", default="logs_db", help="Database name")
    parser.add_argument(
        "--raw-table-name", default="raw_logs", help="Raw logs table name"
    )
    parser.add_argument(
        "--processed-table-name",
        default="processed_logs",
        help="Processed logs table name",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    # Create configuration
    config = Config(
        input_path=args.input_path,
        output_path=args.output_path,
        catalog_name=args.catalog_name,
        database_name=args.database_name,
        raw_table_name=args.raw_table_name,
        processed_table_name=args.processed_table_name,
    )

    # Initialize Spark session
    logger.info("Initializing Spark session")
    spark = create_spark_session()
    iceberg = IcebergTableUtils(spark)

    try:
        # Initialize modules
        ingestion = LogIngestion(spark, config, iceberg)
        transformation = LogTransformation(spark, config, iceberg)
        analytics = LogAnalytics(spark, config, iceberg)

        # Execute pipeline
        with monitor_query_performance("ingestion"):
            logger.info("Starting log ingestion")
            ingestion.ingest_logs()

        with monitor_query_performance("transformation"):
            logger.info("Starting log transformation")
            cached_df = transformation.transform_logs()

        with monitor_query_performance("analytics"):
            logger.info("Running analytics queries")
            analytics.run_daily_analytics(cached_df)
            analytics.run_weekly_analytics(cached_df)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Error in pipeline execution: {str(e)}")
        raise
    finally:
        # Cleanup
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
