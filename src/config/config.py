from dataclasses import dataclass


@dataclass
class Config:
    input_path: str
    output_path: str
    catalog_name: str
    database_name: str
    s3_bucket_name: str
    raw_table_name: str
    processed_table_name: str
    metastore_path: str
    partition_strategy: str = "dynamic"  # Options: "dynamic", "hourly", "daily"
    base_partitions: int = 2
    max_partitions: int = 50
    target_records_per_partition: int = 1000000  # Target 1M records per partition
    custom_partition_expr: str = None  # For custom partitioning expressions
    access_key: str = None
    secret_key: str = None

    @property
    def daily_ip_analytics_table(self):
        return f"{self.database_name}.daily_top_ips"

    @property
    def weekly_ip_analytics_table(self):
        return f"{self.database_name}.weekly_top_ips"

    @property
    def daily_device_analytics_table(self):
        return f"{self.database_name}.daily_top_devices"

    @property
    def weekly_device_analytics_table(self):
        return f"{self.database_name}.weekly_top_devices"

    @property
    def raw_table_path(self):
        return f"{self.metastore_path}/{self.raw_table_name}"

    @property
    def processed_table_path(self):
        return f"{self.metastore_path}/{self.processed_table_name}"

    @property
    def raw_table_full_name(self):
        return f"{self.database_name}.{self.raw_table_name}"

    @property
    def processed_table_full_name(self):
        return f"{self.database_name}.{self.processed_table_name}"
