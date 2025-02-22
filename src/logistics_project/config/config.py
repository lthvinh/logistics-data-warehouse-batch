from typing import Optional
import pendulum
import logging
from pyspark.sql import SparkSession
import yaml
from delta import *
from pathlib import Path

class Config:
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        
        self.current_date_details = {
            'date': pendulum.now(tz='Asia/Ho_Chi_Minh')
            , 'year': pendulum.now(tz='Asia/Ho_Chi_Minh').year
            , 'month': pendulum.now(tz='Asia/Ho_Chi_Minh').month
            , 'day': pendulum.now(tz='Asia/Ho_Chi_Minh').day
        }

        # self.one_day_delta = pendulum.timedelta(days=1)
        self.one_day_delta = 1
        
        self.previous_date_details = {
            'date': pendulum.now(tz='Asia/Ho_Chi_Minh').subtract(days = self.one_day_delta)
            , 'year': pendulum.now(tz='Asia/Ho_Chi_Minh').subtract(days = self.one_day_delta).year
            , 'month': pendulum.now(tz='Asia/Ho_Chi_Minh').subtract(days = self.one_day_delta).month
            , 'day': pendulum.now(tz='Asia/Ho_Chi_Minh').subtract(days = self.one_day_delta).day
        }

        self.configs = self._get_configs()

        self.data_lake_configs = self.configs.get('data_lake')
        self.raw_configs = self.data_lake_configs['raw']
        self.enriched_configs = self.data_lake_configs['enriched']
        self.curated_configs = self.data_lake_configs['curated']
        self.spark_configs = self.configs['spark']
        self.logging_configs = self.configs['logging']


    @property
    def spark(self):
        
        spark_config = self.spark_configs.get('config', {})
        spark_packages = self.spark_configs.get('packages')
        spark_app_name = self.spark_configs.get('app_name', 'temp_name')
        
        builder = (
            SparkSession
            .builder
            .config(map = spark_config)
            .appName(spark_app_name)
        )
        spark = configure_spark_with_delta_pip(
            spark_session_builder = builder
            , extra_packages = spark_packages
        ).getOrCreate()
        
        return spark
        
    @property
    def logger(self):
        
        logging_name = self.logging_configs.get('name', 'temp_name')
        logging_level = self.logging_configs.get('level', 'INFO')
        logging_format = self.logging_configs.get('format')
        logging_base_path = self.logging_configs.get(Path('base_path'), Path(__file__).parent.parent / 'logs')

        current_year = self.current_date_details['year']
        current_month = self.current_date_details['month']
        current_day = self.current_date_details['day']
        
        logging_path = logging_base_path / f'{current_year}' / f'{current_month:0>2}'
        logging_file_name = f'{current_year}{current_month:0>2}{current_day:0>2}.log'
        
        logging_path.mkdir(exist_ok=True, parents=True)
        logging_file_path = logging_path / logging_file_name
        
        logging.basicConfig(
            level = logging_level
            , format = logging_format
            , handlers = [
                logging.FileHandler(logging_file_path)
                , logging.StreamHandler()
            ]
        )
        return logging.getLogger(logging_name)
        
    def _get_configs(self) -> dict:
        try:
            if not self.config_path:
                self.config_path = Path(__file__).parent / 'config.yaml'
            with open(self.config_path) as file:
                config = yaml.safe_load(file)
        except Exception as e:
            raise e

        return config
