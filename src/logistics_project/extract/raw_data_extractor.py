from pyspark.sql import DataFrame
from pyspark.sql import functions as F 
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / 'decorator' ))

from decorator_factory import DecoratorFactory
import functools

class RawDataExtractor:
    def __init__(self, spark, logger, raw_base_path, raw_format, previous_year, previous_month, previous_day):
        self.spark = spark
        self.logger = logger
        
        self.raw_base_path = raw_base_path
        self.raw_format =  raw_format
        
        self.previous_year = previous_year
        self.previous_month = previous_month
        self.previous_day = previous_day

    @DecoratorFactory.decorate_extract_raw_data
    def _extract_raw_data(self, raw_name = str, raw_data_name = str) -> DataFrame:
        raw_df_path = f'{self.raw_base_path}/{raw_data_name}'
        self.logger.info(f"HDFS Path: {raw_df_path}")
        raw_df = (
            self.spark.read
            .format(self.raw_format)
            .option('path', raw_df_path)
            .load()
            .where(
                (F.col('year') == self.previous_year)
                & (F.col('month') == self.previous_month)
                & (F.col('day') == self.previous_day)
            )
            .dropDuplicates()
        )
        return raw_df

    
    def extract_raw_users(self):
        raw_users_name = 'users'
        raw_users_data_name = 'logistics_src.logistics.Users'
        return self._extract_raw_data(raw_users_name, raw_users_data_name)
        
    
    def extract_raw_drivers(self):
        raw_drivers_name = 'drivers'
        raw_drivers_data_name = 'logistics_src.logistics.Drivers'
        return self._extract_raw_data(raw_drivers_name, raw_drivers_data_name)

    def extract_raw_orders(self):
        raw_orders_name = 'orders'
        raw_orders_data_name = 'logistics_src.logistics.Orders'
        return self._extract_raw_data(raw_orders_name, raw_orders_data_name)
        
    def extract_raw_payments(self):
        raw_payments_name = 'payments'
        raw_payments_data_name = 'logistics_src.logistics.Payments'
        return self._extract_raw_data(raw_payments_name, raw_payments_data_name)
        
    def extract_raw_shipments(self):
        raw_shipments_name = 'shipments'
        raw_shipments_data_name = 'logistics_src.logistics.Shipments'
        return self._extract_raw_data(raw_shipments_name, raw_shipments_data_name)