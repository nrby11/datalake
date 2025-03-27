from dataclasses import dataclass


@dataclass
class Config:
    input_path: str
    output_path: str
    catalog_name: str
    database_name: str
    raw_table_name: str
    processed_table_name: str
    metastore_path: str = 's3a://terraform-20250326081517067100000004/hive/warehouse'

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
