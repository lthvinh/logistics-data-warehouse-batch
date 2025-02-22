from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta import *

class CuratedDataLoader:

    @staticmethod
    def load_dim_date(self, dim_date):
        dim_date.write.mode('append').format('delta').option('path', self.dim_date_path).save()

    @staticmethod
    def load_dim_locations(self, dim_locations_final):
        
        dim_locations_delta_table = DeltaTable.forPath(self.spark, self.dim_locations_path)
        (
            dim_locations_delta_table.alias('target')
            .merge(
                source = dim_locations_final.alias('source')
                , condition = (F.col('source.location') == F.col('target.location'))
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
    @staticmethod
    def load_dim_users(self, dim_users_final):

        dim_users_delta_table = DeltaTable.forPath(self.spark, self.dim_users_path)
        
        (
            dim_users_delta_table.alias('target')
            .merge(
                source = dim_users_final.alias('source')
                , condition = (
                    (F.col('source.user_id') == F.col('target.user_id'))
                    & (F.col('source.effective_from_timestamp') == F.col('target.effective_from_timestamp'))
                )
            )
            .whenMatchedUpdate(
                set = {
                    'is_current': F.col('source.is_current')
                    , 'effective_to_timestamp': F.col('source.effective_to_timestamp')
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
    @staticmethod
    def load_dim_drivers(self, dim_drivers_final):
        
        dim_drivers_delta_table = DeltaTable.forPath(self.spark, self.dim_drivers_path)
        (
            dim_drivers_delta_table.alias('target')
            .merge(
                source = dim_drivers_final.alias('source')
                , condition = (
                    (F.col('source.driver_id') == F.col('target.driver_id'))
                    & (F.col('source.effective_from_timestamp') == F.col('target.effective_from_timestamp'))
                )
            )
            .whenMatchedUpdate(
                 set = {
                    'is_current': F.col('source.is_current')
                    , 'effective_to_timestamp': F.col('source.effective_from_timestamp')
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    @staticmethod
    def load_fact_orders(self, fact_orders_source):
        
        fact_orders_delta_table = DeltaTable.forPath(self.spark, self.fact_orders_path)

        (
            fact_orders_delta_table.alias('target')
            .merge(
                source = fact_orders_source.alias('source')
                , condition = (F.col('source.order_id') == F.col('target.order_id'))
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
    @staticmethod
    def load_fact_payments(self, fact_payments_source):
        
        fact_payments_delta_table = DeltaTable.forPath(self.spark, self.fact_payments_path)

        (
            fact_payments_delta_table.alias('target')
            .merge(
                source = fact_payments_source.alias('source')
                , condition = (F.col('source.payment_id') == F.col('target.payment_id'))
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
    @staticmethod
    def load_fact_shipments(self, fact_shipments_source):
        
        fact_shipments_delta_table = DeltaTable.forPath(self.spark, self.fact_shipments_path)

        (
            fact_shipments_delta_table.alias('target')
            .merge(
                source = fact_shipments_source.alias('source')
                , condition = (F.col('source.shipment_id') == F.col('target.shipment_id'))
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
