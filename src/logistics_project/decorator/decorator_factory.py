import functools
from pyspark.sql import functions as F
from pyspark.sql import types as T

class DecoratorFactory:
    
    @staticmethod
    def decorate_extract_raw_data(func):
        @functools.wraps(func)
        def new_function(self, raw_name, raw_data_name):
            self.logger.info(f'Starting to fetch raw {raw_name} data from HDFS.')
            self.logger.info(f"DataFrame: {raw_name}")

            try:
                self.logger.info(f'Executing function: {func.__name__}')
                raw_df = func(self, raw_name, raw_data_name)
                self.logger.info(f'Successfully fetched raw {raw_name} data.')
            except Exception as e:
                self.logger.error(f"Error while fetching {raw_name} from raw layer.")
                self.logger.error(f"DataFrame: {raw_name}")
                self.logger.error(f"Error: {str(e)}")
                raise
            return raw_df
            
        return new_function

    @staticmethod
    def decorate_select_columns(func):
        @functools.wraps(func)
        def new_function(self, raw_df):
            try:
                self.logger.info(f"Selecting columns: ['after.*', 'op', 'source.ts_ms', 'event_timestamp', 'year', 'month', 'day']")
                new_raw_df = func(self, raw_df)
            except Exception as e:
                self.logger.error(f"Error while selecting columns.")
                self.logger.error(f"Error: {str(e)}")
                raise
            return new_raw_df
        return new_function

    @staticmethod
    def decorate_process_enriched_data(func):
        @functools.wraps(func)
        def new_function(self):
            try:
                new_df = func(self)
            except Exception as e:
                self.logger.error(e)
                raise
            return new_df
        return new_function
        
    @staticmethod
    def decorate_load_enriched_data(func):
        @functools.wraps(func)
        def new_function(self, df, df_name, output_path, mode, format, partition_columns, compression, extra_configs):
            self.logger.info(f"Starting to load {df_name} to enriched layer.")
            self.logger.info(f"DataFrame: {df_name}")
            try:
                self.logger.info(f'Executing function: {func.__name__}')
                func(self, df, df_name, output_path, mode, format, partition_columns, compression, extra_configs)
            except Exception as e:
                self.logger.error(f"Error while loading {df_name} to enriched layer.")
                self.logger.error(f"DataFrame: {df_name}")
                self.logger.error(f"Error: {str(e)}")
                raise
            self.logger.info(f"Successfully loaded {df_name} to enriched layer.")
        return new_function