from pathlib import Path
import sys
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pendulum

sys.path.append(str(Path(__file__).parent.parent / 'decorator'))
sys.path.append(str(Path(__file__).parent.parent / 'config'))
sys.path.append(str(Path(__file__).parent.parent / 'extract'))
sys.path.append(str(Path(__file__).parent.parent / 'load'))

from raw_data_extractor import RawDataExtractor
from enriched_data_loader import EnrichedDataLoader                
from decorator_factory import DecoratorFactory
from config import Config

class EnrichedDataTransformer:
    def __init__(self, config_path = None):
        self.app_config = Config(config_path)
        
        #COMMON
        self.spark = self.app_config.spark
        self.logger = self.app_config.logger

        #RAW CONFIGS
        self.raw_configs = self.app_config.raw_configs
        self.raw_base_path = self.raw_configs.get('base_path')
        self.raw_format =  self.raw_configs.get('format')

        #PREVIOUS DATE DETAILS
        self.previous_date_details = self.app_config.previous_date_details
        self.previous_year = self.previous_date_details['year']
        self.previous_month = self.previous_date_details['month']
        self.previous_day = self.previous_date_details['day']
        
        #RAW DATA EXTRACTOR
        self.raw_data_extractor = RawDataExtractor(
            self.spark
            , self.logger
            , self.raw_base_path
            , self.raw_format
            , self.previous_year
            , self.previous_month
            , self.previous_day
        )
        
        #ENRICHED CONFIGS
        self.enriched_configs = self.app_config.enriched_configs
        self.base_path = self.enriched_configs.get('base_path')
        self.mode = self.enriched_configs.get('mode', 'append')
        self.format = self.enriched_configs.get('format', 'parquet')
        self.partition_columns = self.enriched_configs.get('partition_columns')
        self.compression = self.enriched_configs.get('compression')
        self.extra_configs = self.enriched_configs.get('configs')
        self.tables_configs = self.enriched_configs.get('tables')

        #ENRICHED DATA LOADER
        self.enriched_data_loader = EnrichedDataLoader()
        
    @DecoratorFactory.decorate_select_columns
    def _select_columns(self, raw_df):
        new_raw_df = raw_df.select(
            F.col('after.*')
            , F.col('op')
            , F.col('source.ts_ms')
            , F.from_utc_timestamp(F.to_timestamp(F.col('source.ts_ms') / 1000), 'Asia/Ho_Chi_Minh').alias('event_timestamp')
            , F.col('year')
            , F.col('month')
            , F.col('day')
        )
        return new_raw_df

    
    def _convert_iso_string_to_timestamp(self, iso_string):
        try:
            timestamp = F.from_utc_timestamp(F.to_timestamp(iso_string, "yyyy-MM-dd'T'HH:mm:ss'Z'"), 'Asia/Ho_Chi_Minh')
        except Exception as e:
            self.logger.error(f'Error parsing date: {e}')
            raise
        return timestamp
        
    
    @DecoratorFactory.decorate_process_enriched_data
    def process_enriched_users(self):
        
        self.logger.info('Starting transformation of raw users data.')
        df_name = 'users'
        raw_users_df = self.raw_data_extractor.extract_raw_users()
        
        new_users_df = self._select_columns(raw_users_df)
        new_users_df = (
            new_users_df
            .withColumn(
                'created_at'
                , self._convert_iso_string_to_timestamp(F.col('created_at'))
            )
        )
        
        self.logger.info('Completed transformation of raw users data.')

        enriched_users_configs = self.tables_configs.get(df_name) if self.tables_configs.get(df_name) else {}
        enriched_users_configs['self'] = self
        enriched_users_configs['df'] = new_users_df
        enriched_users_configs['df_name'] = df_name
        enriched_users_configs['output_path'] = self.base_path + f'/{df_name}'
        enriched_users_configs['mode'] =  enriched_users_configs.get('mode', self.mode)
        enriched_users_configs['format'] =  enriched_users_configs.get('format', self.format)
        enriched_users_configs['partition_columns'] =  enriched_users_configs.get('partition_columns', self.partition_columns)
        enriched_users_configs['compression'] =  enriched_users_configs.get('compression', self.compression)
        enriched_users_configs['extra_configs'] =  enriched_users_configs.get('extra_configs', self.extra_configs)
        
        self.enriched_data_loader.load_data(**enriched_users_configs)

    @DecoratorFactory.decorate_process_enriched_data
    def process_enriched_drivers(self):
        
        self.logger.info('Starting transformation of raw drivers data.')
        df_name = 'drivers'
        raw_drivers_df = self.raw_data_extractor.extract_raw_drivers()
        new_drivers_df = self._select_columns(raw_drivers_df)
        
        self.logger.info('Completed transformation of raw drivers data.')
        
        enriched_drivers_configs = self.tables_configs.get(df_name) if self.tables_configs.get(df_name) else {}
        enriched_drivers_configs['self'] = self
        enriched_drivers_configs['df'] = new_drivers_df
        enriched_drivers_configs['df_name'] = df_name
        enriched_drivers_configs['output_path'] = self.base_path + f'/{df_name}'
        enriched_drivers_configs['mode'] =  enriched_drivers_configs.get('mode', self.mode)
        enriched_drivers_configs['format'] =  enriched_drivers_configs.get('format', self.format)
        enriched_drivers_configs['partition_columns'] =  enriched_drivers_configs.get('partition_columns', self.partition_columns)
        enriched_drivers_configs['compression'] =  enriched_drivers_configs.get('compression', self.compression)
        enriched_drivers_configs['extra_configs'] =  enriched_drivers_configs.get('extra_configs', self.extra_configs)
        
        
        self.enriched_data_loader.load_data(**enriched_drivers_configs)
       
    @DecoratorFactory.decorate_process_enriched_data
    def process_enriched_orders(self):
        
        self.logger.info('Starting transformation of raw drivers data.')
        df_name = 'orders'
        
        raw_orders_df = self.raw_data_extractor.extract_raw_orders()
        new_orders_df = self._select_columns(raw_orders_df)
        
        new_orders_df = (
            new_orders_df
            .withColumn(
                'delivery_time'
                , self._convert_iso_string_to_timestamp(F.col('delivery_time'))
            )
            .withColumn(
               'created_at'
                , self._convert_iso_string_to_timestamp(F.col('created_at'))
            )
        )
        self.logger.info('Completed transformation of raw orders data.')
        
        enriched_orders_configs = self.tables_configs.get(df_name) if self.tables_configs.get(df_name) else {}
        enriched_orders_configs['self'] = self
        enriched_orders_configs['df'] = new_orders_df
        enriched_orders_configs['df_name'] = df_name
        enriched_orders_configs['output_path'] = self.base_path + f'/{df_name}'
        enriched_orders_configs['mode'] =  enriched_orders_configs.get('mode', self.mode)
        enriched_orders_configs['format'] =  enriched_orders_configs.get('format', self.format)
        enriched_orders_configs['partition_columns'] =  enriched_orders_configs.get('partition_columns', self.partition_columns)
        enriched_orders_configs['compression'] =  enriched_orders_configs.get('compression', self.compression)
        enriched_orders_configs['extra_configs'] =  enriched_orders_configs.get('extra_configs', self.extra_configs)
        
        
        self.enriched_data_loader.load_data(**enriched_orders_configs)
        
    @DecoratorFactory.decorate_process_enriched_data
    def process_enriched_payments(self):
        self.logger.info('Starting transformation of raw payments data.')
        df_name = 'payments'
        raw_payments_df = self.raw_data_extractor.extract_raw_payments()
        new_payments_df = self._select_columns(raw_payments_df)
        
        new_payments_df = (
            new_payments_df
            .withColumn(
                'payment_date'
                , self._convert_iso_string_to_timestamp(F.col('payment_date'))
            )
        )
        self.logger.info('Completed transformation of raw payments data.')
        
        enriched_payments_configs = self.tables_configs.get(df_name) if self.tables_configs.get(df_name) else {}
        enriched_payments_configs['self'] = self
        enriched_payments_configs['df'] = new_payments_df
        enriched_payments_configs['df_name'] = df_name
        enriched_payments_configs['output_path'] = self.base_path + f'/{df_name}'
        enriched_payments_configs['mode'] =  enriched_payments_configs.get('mode', self.mode)
        enriched_payments_configs['format'] =  enriched_payments_configs.get('format', self.format)
        enriched_payments_configs['partition_columns'] =  enriched_payments_configs.get('partition_columns', self.partition_columns)
        enriched_payments_configs['compression'] =  enriched_payments_configs.get('compression', self.compression)
        enriched_payments_configs['extra_configs'] =  enriched_payments_configs.get('extra_configs', self.extra_configs)
        
        
        self.enriched_data_loader.load_data(**enriched_payments_configs)
        
    @DecoratorFactory.decorate_process_enriched_data
    def process_enriched_shipments(self):
        self.logger.info("Starting transformation of raw shipments data.")
        
        df_name = 'shipments'
        raw_shipments_df = self.raw_data_extractor.extract_raw_shipments()
        new_shipments_df = self._select_columns(raw_shipments_df)
        new_shipments_df = (
            new_shipments_df
            .withColumn(
                'estimated_delivery_time'
                , self._convert_iso_string_to_timestamp(F.col('estimated_delivery_time'))
            )
        )
        
        self.logger.info('Completed transformation of raw shipments data.')
        
        enriched_shipments_configs = self.tables_configs.get(df_name) if self.tables_configs.get(df_name) else {}
        enriched_shipments_configs['self'] = self
        enriched_shipments_configs['df'] = new_shipments_df
        enriched_shipments_configs['df_name'] = df_name
        enriched_shipments_configs['output_path'] = self.base_path + f'/{df_name}'
        enriched_shipments_configs['mode'] =  enriched_shipments_configs.get('mode', self.mode)
        enriched_shipments_configs['format'] =  enriched_shipments_configs.get('format', self.format)
        enriched_shipments_configs['partition_columns'] =  enriched_shipments_configs.get('partition_columns', self.partition_columns)
        enriched_shipments_configs['compression'] =  enriched_shipments_configs.get('compression', self.compression)
        enriched_shipments_configs['extra_configs'] =  enriched_shipments_configs.get('extra_configs', self.extra_configs)
        
        self.enriched_data_loader.load_data(**enriched_shipments_configs)