from pyspark.sql import functions as F

class EnrichedDataExtractor:
    def __init__(self, spark, logger, enriched_base_path, enriched_format, previous_year, previous_month, previous_day):
        self.spark = spark
        self.logger = logger
        
        self.enriched_base_path = enriched_base_path
        self.enriched_format =  enriched_format
        
        self.previous_year = previous_year
        self.previous_month = previous_month
        self.previous_day = previous_day

    def _extract_enriched_data(self, source_name):
        enriched_df_path = f'{self.enriched_base_path}/{source_name}'
        self.logger.info(f"HDFS Path: {enriched_df_path}")
        enriched_df = (
            self.spark.read
            .format(self.enriched_format)
            .option('path', enriched_df_path)
            .load()
            .where(
                (F.col('year') == self.previous_year)
                & (F.col('month') == self.previous_month)
                & (F.col('day') == self.previous_day)
            )
            .drop(F.col('ts_ms'), F.col('year'), F.col('month'), F.col('day'))
        )
        return enriched_df

    def extract_enriched_users(self):
        source_name = 'users'
        enriched_users_df = self._extract_enriched_data(source_name)
        return enriched_users_df

    def extract_enriched_drivers(self):
        source_name = 'drivers'
        enriched_drivers_df = self._extract_enriched_data(source_name)
        return enriched_drivers_df

    def extract_enriched_orders(self):
        source_name = 'orders'
        enriched_orders_df = self._extract_enriched_data(source_name)
        return enriched_orders_df

    def extract_enriched_payments(self):
        source_name = 'payments'
        enriched_payments_df = self._extract_enriched_data(source_name)
        return enriched_payments_df

    def extract_enriched_shipments(self):
        source_name = 'shipments'
        enriched_shipments_df = self._extract_enriched_data(source_name)
        return enriched_shipments_df

    
