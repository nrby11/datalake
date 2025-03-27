#!/usr/bin/env python3
"""
Spark Query CLI - A lightweight interface for running Spark SQL queries against
Apache Iceberg tables in the Hive Metastore.
"""
import argparse
import json
import logging

from pandas import pandas
from pyspark.sql import SparkSession
from tabulate import tabulate

from config.config import Config
from utils.monitoring.monitoring import monitor_query_performance
from utils.spark_utils import create_spark_session, stop_spark_session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkQueryCLI:
    def __init__(self, config=None):
        """Initialize the Spark Query CLI with optional config"""
        self.config = config
        self.spark = self._init_spark_session(self.config)
        self._welcome_message()

    def _init_spark_session(self, config):
        """Initialize Spark session with Hive support"""
        # Use the existing utility function if available
        return create_spark_session(config, local_mode=True )


    def _welcome_message(self):
        """Display welcome and help info"""
        print("\n📊 Spark Query CLI (Connected to Hive Metastore)")
        print("Type 'help' for commands, 'exit' to quit\n")

        if self.config:
            print(f"📁 Connected to database: {self.config.database_name}")
            print(f"📦 Using catalog: {self.config.catalog_name}\n")

    def run_query(self, query, output_format='table'):
        """Execute Spark SQL query and display results with performance monitoring"""
        try:
            # Use the monitoring utility if available
            try:
                with monitor_query_performance("ad-hoc-query"):
                    df = self.spark.sql(query)
                    count = df.count()  # Trigger execution
            except:
                # Fallback if monitoring utility not available
                df = self.spark.sql(query)
                count = df.count()  # Trigger execution

            if output_format == 'table':
                # Show first 20 rows as table
                pdf = df.limit(20).toPandas()
                print(tabulate(pdf, headers='keys', tablefmt='psql', showindex=False))
            elif output_format == 'json':
                # Show as JSON
                json_records = [json.loads(row) for row in df.limit(50).toJSON().collect()]
                print(json.dumps(json_records, indent=2))
            elif output_format == 'csv':
                # Show as CSV
                pdf = df.limit(100).toPandas()
                print(pdf.to_csv(index=False))
            else:
                # Default show() output
                df.show(truncate=False)

            print(f"\n📈 Query returned {count} rows")

        except Exception as e:
            print(f"❌ Error executing query: {str(e)}")

    def list_tables(self, database=None):
        """Show available tables in Hive metastore"""
        try:
            if database:
                tables = self.spark.sql(f"SHOW TABLES IN {database}").collect()
                db_display = f"in database '{database}'"
            elif self.config and self.config.database_name:
                tables = self.spark.sql(f"SHOW TABLES IN {self.config.database_name}").collect()
                db_display = f"in database '{self.config.database_name}'"
            else:
                tables = self.spark.sql("SHOW TABLES").collect()
                db_display = "in current database"

            if tables:
                print(f"\n📋 Available Tables {db_display}:")
                for table in tables:
                    db_name = table['database'] if 'database' in table.asDict() else ''
                    table_name = table['tableName']
                    full_name = f"{db_name}.{table_name}" if db_name else table_name
                    print(f"- {full_name}")
            else:
                print(f"No tables found {db_display}")
        except Exception as e:
            print(f"❌ Error listing tables: {str(e)}")

    def list_databases(self):
        """List all available databases in the metastore"""
        try:
            databases = self.spark.sql("SHOW DATABASES").collect()
            if databases:
                print("\n📚 Available Databases:")
                for db in databases:
                    db_name = db['databaseName']
                    if self.config and self.config.database_name == db_name:
                        print(f"- {db_name} (current)")
                    else:
                        print(f"- {db_name}")
            else:
                print("No databases found")
        except Exception as e:
            print(f"❌ Error listing databases: {str(e)}")

    def describe_table(self, table_name):
        """Show schema and metadata of a table"""
        try:
            print(f"\n📝 Schema for {table_name}:")
            self.spark.sql(f"DESCRIBE FORMATTED {table_name}").show(truncate=False)
        except Exception as e:
            print(f"❌ Error describing table: {str(e)}")

    def show_analytics(self):
        """Display the analytics results if available"""
        if not self.config:
            print("❌ Config not available. Cannot show analytics.")
            return

        try:
            print("\n📊 === Daily Top 5 IP Addresses ===")
            daily_ips = self.spark.table(self.config.daily_ip_analytics_table).limit(5)
            daily_ips_pd = daily_ips.toPandas()
            print(tabulate(daily_ips_pd, headers='keys', tablefmt='psql', showindex=False))

            print("\n📊 === Daily Top 5 Devices ===")
            daily_devices = self.spark.table(self.config.daily_device_analytics_table).limit(5)
            daily_devices_pd = daily_devices.toPandas()
            print(tabulate(daily_devices_pd, headers='keys', tablefmt='psql', showindex=False))

            print("\n📊 === Weekly Top 5 IP Addresses ===")
            weekly_ips = self.spark.table(self.config.weekly_ip_analytics_table).limit(20)
            weekly_ips_pd = weekly_ips.toPandas()
            print(tabulate(weekly_ips_pd, headers='keys', tablefmt='psql', showindex=False))

            print("\n📊 === Weekly Top 5 Devices ===")
            weekly_devices = self.spark.table(self.config.weekly_device_analytics_table).limit(20)
            weekly_devices_pd = weekly_devices.toPandas()
            print(tabulate(weekly_devices_pd, headers='keys', tablefmt='psql', showindex=False))
        except Exception as e:
            print(f"❌ Error showing analytics: {str(e)}")

    def show_data_preview(self, limit=5):
        """Show a preview of raw and processed data"""
        if not self.config:
            print("❌ Config not available. Cannot show data preview.")
            return

        try:
            print(f"\n📋 Raw logs preview (first {limit} rows):")
            raw_logs = self.spark.table(self.config.raw_table_full_name).limit(limit)
            raw_logs_pd = raw_logs.toPandas()
            print(tabulate(raw_logs_pd, headers='keys', tablefmt='psql', showindex=False))

            print(f"\n📋 Processed logs preview (first {limit} rows):")
            processed_logs = self.spark.table(self.config.processed_table_full_name).limit(limit)
            processed_logs_pd = processed_logs.toPandas()
            print(tabulate(processed_logs_pd, headers='keys', tablefmt='psql', showindex=False))
        except Exception as e:
            print(f"❌ Error showing data preview: {str(e)}")

    def close(self):
        """Close the Spark session"""
        try:
            stop_spark_session(self.spark)
        except:
            self.spark.stop()
        print("\n👋 Spark session closed. Goodbye!")


def parse_arguments():
    """Parse CLI arguments"""
    parser = argparse.ArgumentParser(description='Spark Query CLI for Apache Iceberg')
    parser.add_argument('--input-path', help='S3 path to raw log files')
    parser.add_argument("--access-keys", required=True, help="Access Keys for AWS")
    parser.add_argument("--secret-keys", required=True, help="Secret keys for AWS")
    parser.add_argument('--output-path', help='S3 path for Iceberg tables')
    parser.add_argument('--catalog-name', default='hive_catalog', help='Iceberg catalog name')
    parser.add_argument('--database-name', default='logs_db', help='Database name')
    parser.add_argument('--s3-bucket-name', help='S3 Bucket name')
    parser.add_argument('--raw-table-name', default='raw_logs', help='Raw logs table name')
    parser.add_argument('--processed-table-name', default='processed_logs', help='Processed logs table name')
    return parser.parse_args()


def main():
    args = parse_arguments()

    # Create configuration if args provided
    config = None
    if args.database_name:
        config = Config(
            input_path=args.input_path or "s3://logs-bucket/raw-logs/",
            output_path=args.output_path or "s3://iceberg-tables/",
            catalog_name=args.catalog_name,
            database_name=args.database_name,
            s3_bucket_name=args.s3_bucket_name,
            raw_table_name=args.raw_table_name,
            processed_table_name=args.processed_table_name,
            access_key = args.access_keys,
            secret_key = args.secret_keys,
            metastore_path=f's3a://{args.s3_bucket_name}/hive/warehouse'
        )

    # Initialize CLI
    cli = SparkQueryCLI(config)

    print("💡 Type your SQL query or enter a command. Press Enter to execute.")

    while True:
        try:
            user_input = input("sparkql> ").strip()

            if not user_input:
                continue

            if user_input.lower() in ('exit', 'quit'):
                break

            if user_input.lower() == 'help':
                print("\n📚 Available commands:")
                print("- [SQL query]: Execute any Spark SQL query")
                print("- list tables [database]: Show available tables")
                print("- list databases: Show available databases")
                print("- describe <database>.<table>: Show table schema")
                print("- show analytics: Display analytics results")
                print("- show data: Preview raw and processed data")
                print("- exit/quit: Exit the CLI")
                continue

            if user_input.lower() == 'list databases':
                cli.list_databases()
                continue

            if user_input.lower().startswith('list tables'):
                parts = user_input.split()
                db = parts[2] if len(parts) > 2 else None
                cli.list_tables(db)
                continue

            if user_input.lower().startswith('describe '):
                table_name = user_input.split(' ', 1)[1]
                cli.describe_table(table_name)
                continue

            if user_input.lower() == 'show analytics':
                cli.show_analytics()
                continue

            if user_input.lower() == 'show data':
                cli.show_data_preview()
                continue

            # Default case - execute as SQL query
            cli.run_query(user_input)

        except KeyboardInterrupt:
            print("\n⚠️ Use 'exit' or 'quit' to end session")
            continue
        except Exception as e:
            print(f"❌ Error: {str(e)}")

    cli.close()


if __name__ == "__main__":
    main()