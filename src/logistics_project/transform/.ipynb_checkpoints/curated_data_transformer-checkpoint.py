from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from delta import *
import pendulum
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / 'config'))
sys.path.append(str(Path(__file__).parent.parent / 'extract'))
sys.path.append(str(Path(__file__).parent.parent / 'load'))

from config import Config
from enriched_data_extractor import EnrichedDataExtractor
from curated_data_loader import CuratedDataLoader

#-----------------------------------------TABLE-----------------------------------------

class Table:
    def __init__(self):
        self.app_config = Config()
        
        #COMMON
        self.spark = self.app_config.spark
        self.logger = self.app_config.logger
        
        #ENRICHED CONFIGS
        self.enriched_configs = self.app_config.enriched_configs
        self.enriched_base_path = self.enriched_configs.get('base_path')
        self.enriched_format = self.enriched_configs.get('format', 'parquet')

        #PREVIOUS DATE DETAILS
        self.previous_date_details = self.app_config.previous_date_details
        self.previous_year = self.previous_date_details['year']
        self.previous_month = self.previous_date_details['month']
        self.previous_day = self.previous_date_details['day']

        #ENRICHED DATA EXTRACTOR
        self.enriched_data_extractor = EnrichedDataExtractor(
            self.spark
            , self.logger
            , self.enriched_base_path
            , self.enriched_format
            , self.previous_year
            , self.previous_month
            , self.previous_day
        )

        #CURATED CONFIGS
        self.curated_configs = self.app_config.curated_configs
        self.dimension_base_path = self.curated_configs.get('dimension_base_path')
        self.fact_base_path = self.curated_configs.get('fact_base_path')

        #CURATED DATA LOADER
        self.curated_data_loader = CuratedDataLoader()
        

#-----------------------------------------DIM_DATE-----------------------------------------

class DimDate(Table):
    def __init__(self):
        super().__init__()
        self.dim_date_path = self.dimension_base_path + '/dim_date'
        
    def process_dim_date(self):
        
        is_exists = DeltaTable.isDeltaTable(self.spark, self.dim_date_path)
        if not is_exists:
            
            (
                DeltaTable.create(self.spark)
                .tableName("dim_date")
                .addColumn("date_key", "INT", nullable = False)
                .addColumn("date", "DATE", nullable = False)
                .addColumn("year", "INT", nullable = False)
                .addColumn("quarter", "INT", nullable = False)
                .addColumn("month", "INT", nullable = False)
                .addColumn("day", "INT", nullable = False)
                .addColumn("day_of_week", "INT", nullable = False)
                .addColumn("day_of_month", "INT", nullable = False)
                .addColumn("day_of_year", "INT", nullable = False)
                .addColumn("week_of_year", "INT", nullable = False)
                .addColumn("is_weekend", "BOOLEAN", nullable = False)
                .addColumn("week_name", "STRING", nullable = False)
                .addColumn("month_name", "STRING", nullable = False)
                .addColumn("quarter_name", "STRING", nullable = False)
                .location(self.dim_date_path)
                .execute()
            )
            
            date_range_query = "select explode(sequence(to_date('2024-01-01'), to_date('2044-01-01'), interval 1 days)) as date"
            date_range_df = self.spark.sql(date_range_query)
            
            dim_date = date_range_df.select(
                F.date_format(F.col('date'), 'yyyyMMdd').cast(T.IntegerType()).alias('date_key')
                , F.col('date')
                , F.year(F.col('date')).alias('year')
                , F.quarter(F.col('date')).alias('quarter')
                , F.month(F.col('date')).alias('month')
                , F.day(F.col('date')).alias('day')
                , F.dayofweek(F.col('date')).alias('day_of_week')
                , F.dayofmonth(F.col('date')).alias('day_of_month')
                , F.dayofyear(F.col('date')).alias('day_of_year')
                , F.weekofyear(F.col('date')).alias('week_of_year')
                , F.when(F.dayofweek(F.col('date')).isin(1, 7), F.lit(True)).otherwise(F.lit(False)).alias('is_weekend')
                , F.date_format(F.col('date'), 'EEEE').alias('week_name')
                , F.date_format(F.col('date'), 'MMMM').alias('month_name')
                , F.concat(F.lit('Q'), F.quarter(F.col('date'))).alias('quarter_name')
            )
            
            self.curated_data_loader.load_dim_date(self, dim_date)


#-----------------------------------------DIM_LOCATIONS-----------------------------------------


class DimLocations(Table):
    def __init__(self):
        super().__init__()
        self.dim_locations_path = self.dimension_base_path + '/dim_locations'
        
    def process_dim_locations(self):

        is_exists = DeltaTable.isDeltaTable(self.spark, self.dim_locations_path)
        if not is_exists:
            (
                DeltaTable.create(self.spark)
                .tableName('dim_locations')
                .addColumn('location_key', 'LONG', nullable = False)
                .addColumn('location', 'STRING', nullable = False)
                .location(self.dim_locations_path)
                .execute()
            )

        enriched_locations_df = self.enriched_data_extractor.extract_enriched_orders()

        dim_locations_target = self.spark.read.format('delta').option('path', self.dim_locations_path).load()
        location_key_max = dim_locations_target.selectExpr('ifnull(max(location_key), 0) as location_key_max').first().location_key_max
        
        dim_locations_final = (
            enriched_locations_df.select(F.col('pickup_address').alias('location'))
            .union(enriched_locations_df.select(F.col('delivery_address').alias('location')))
            .dropDuplicates()
            .select(
                (F.row_number().over(Window.orderBy(F.col('location'))) + F.lit(location_key_max)).cast(T.LongType()).alias('location_key') 
                , F.col('location')
            )
        )
        self.curated_data_loader.load_dim_locations(self, dim_locations_final)

#-----------------------------------------DIM_USERS-----------------------------------------


class DimUsers(Table):
    def __init__(self):
        super().__init__()
        self.dim_users_path = self.dimension_base_path + '/dim_users'

    def process_dim_users(self):

        is_exists =  DeltaTable.isDeltaTable(self.spark, self.dim_users_path)
        if not is_exists:
            (
                DeltaTable.create(self.spark)
                .tableName("dim_users")
                .addColumn("user_key", "LONG", nullable = False)
                .addColumn("user_id", "LONG", nullable = False)
                .addColumn("full_name", "STRING", nullable = True)
                .addColumn("email", "STRING", nullable = True)
                .addColumn("password_hash", "STRING", nullable = True)
                .addColumn("phone_number", "STRING", nullable = True)
                .addColumn("address", "STRING", nullable = True)
                .addColumn("role", "STRING", nullable = True)
                .addColumn("created_at", "TIMESTAMP", nullable = True)
                .addColumn("is_current", "BOOLEAN", nullable = False)
                .addColumn("effective_from_timestamp", "TIMESTAMP", nullable = False)
                .addColumn("effective_to_timestamp", "TIMESTAMP", nullable = False)
                .location(self.dim_users_path)
                .execute()
            )
            
        dim_users_target = self.spark.read.format('delta').option('path', self.dim_users_path).load().where(F.col('is_current'))

        enriched_users_df = self.enriched_data_extractor.extract_enriched_users()
        


        timestamp_default = pendulum.datetime(year = 9999, month = 12, day = 31, hour = 0, minute = 0, second = 0, tz = 'Asia/Ho_Chi_Minh')

        user_key_max = dim_users_target.selectExpr('ifnull(max(user_key), 0) as user_key_max').first().user_key_max

        dim_users_source = (
            enriched_users_df
            .select(
                (F.row_number().over(Window.orderBy(F.col('user_id'))) + F.lit(user_key_max)).cast(T.LongType()).alias('user_key')
                , F.col('user_id').cast(T.LongType())
                , F.col('full_name')
                , F.col('email')
                , F.col('password_hash')
                , F.col('phone_number')
                , F.col('address')
                , F.col('role')
                , F.col('created_at')
                , F.lit(True).alias('is_current')
                , F.col('event_timestamp').alias('effective_from_timestamp')
                , F.lit(timestamp_default).alias('effective_to_timestamp')
            ).alias('source')
            .join(dim_users_target.alias('target'), F.col('source.user_id') == F.col('target.user_id'), 'left')
            .where(
                (F.col('source.effective_from_timestamp') > F.col('target.effective_from_timestamp'))
                | F.col('target.user_id').isNull()
            )
            .select(F.col('source.*'))
        )

        dim_users_union = dim_users_target.union(dim_users_source)
    
        dim_users_final = (
            dim_users_union
            .withColumn(
                'temp_effective_to_timestamp'
                , F.lead(F.col('effective_from_timestamp'), default = timestamp_default).over(Window.partitionBy(F.col('user_id')).orderBy(F.col('effective_from_timestamp')))
            )
            .withColumn('effective_to_timestamp', F.col('temp_effective_to_timestamp'))
            .withColumn('is_current', F.when((F.col('temp_effective_to_timestamp') == timestamp_default), F.lit(True)).otherwise(F.lit(False)))
            .drop(F.col('temp_effective_to_timestamp'))
        )
        
        self.curated_data_loader.load_dim_users(self, dim_users_final)

#-----------------------------------------DIM_DRIVERS-----------------------------------------


class DimDrivers(Table):
    def __init__(self):
        super().__init__()
        self.dim_drivers_path = self.dimension_base_path + '/dim_drivers'
        self.dim_users_path = self.dimension_base_path + '/dim_users'
        
    def process_dim_drivers(self):

        is_exists =  DeltaTable.isDeltaTable(self.spark, self.dim_drivers_path)
        if not is_exists:
            (
                DeltaTable.create(self.spark)
                .tableName("dim_drivers")
                .addColumn('driver_key', 'LONG', nullable = False)
                .addColumn("driver_id", "LONG", nullable = False)
                .addColumn("user_id", "LONG", nullable = True)
                .addColumn("full_name", "STRING", nullable = True)
                .addColumn("email", "STRING", nullable = True)
                .addColumn("password_hash", "STRING", nullable = True)
                .addColumn("phone_number", "STRING", nullable = True)
                .addColumn("address", "STRING", nullable = True)
                .addColumn("vehicle_license_plate", "STRING", nullable = True)
                .addColumn("vehicle_type", "STRING", nullable = True)
                .addColumn("vehicle_year", "INT", nullable = True)
                .addColumn("is_current", "BOOLEAN", nullable = False)
                .addColumn("effective_from_timestamp", "TIMESTAMP", nullable = False)
                .addColumn("effective_to_timestamp", "TIMESTAMP", nullable = False)
                .location(self.dim_drivers_path)
                .execute()
            )

 
        dim_drivers_target  = (
            self.spark.read
            .format('delta')
            .option('path', self.dim_drivers_path)
            .load()
            .where((F.col('is_current')))
        )
        
        dim_users_target = (
            self.spark.read
            .format('delta')
            .option('path', self.dim_users_path)
            .load()
            .where((F.col('is_current')))
        )
        
        enriched_drivers_df = self.enriched_data_extractor.extract_enriched_drivers()
        
        enriched_users_df = (
            self.enriched_data_extractor.extract_enriched_users()
            .where(
                (F.col('op') == 'u')
                & (F.col('role') == 'driver')
            )
        )
        
        timestamp_default = pendulum.datetime(year = 9999, month = 12, day = 31, hour = 0, minute = 0, second = 0, tz = 'Asia/Ho_Chi_Minh')

        dim_drivers_source_with_modified_users = (
            dim_drivers_target.alias('dim_drivers')
            .join(enriched_drivers_df.alias('enriched_drivers'), F.col('dim_drivers.driver_id') == F.col('enriched_drivers.driver_id'), 'leftanti')
            .join(enriched_users_df.alias('enriched_users'), F.col('dim_drivers.user_id') == F.col('enriched_users.user_id'), 'inner')
            .select(
                F.col('dim_drivers.driver_id').cast(T.LongType())
                , F.col('dim_drivers.user_id').cast(T.LongType())
                , F.col('enriched_users.full_name')
                , F.col('enriched_users.email')
                , F.col('enriched_users.password_hash')
                , F.col('enriched_users.phone_number')
                , F.col('enriched_users.address')
                , F.col('dim_drivers.vehicle_license_plate')
                , F.col('dim_drivers.vehicle_type')
                , F.col('dim_drivers.vehicle_year')
                , F.lit(True).alias('is_current')
                , F.col('enriched_users.event_timestamp').alias('effective_from_timestamp')
                , F.lit(timestamp_default).alias('effective_to_timestamp')
            )
        )
        
        dim_drivers_source_with_modified_drivers = (
            enriched_drivers_df.alias('enriched_drivers')
            .join(dim_users_target.alias('dim_users'), F.col('enriched_drivers.user_id') == F.col('dim_users.user_id'))
            .select(
                F.col('enriched_drivers.driver_id').cast(T.LongType())
                , F.col('enriched_drivers.user_id').cast(T.LongType())
                , F.col('dim_users.full_name')
                , F.col('dim_users.email')
                , F.col('dim_users.password_hash')
                , F.col('dim_users.phone_number')
                , F.col('dim_users.address')
                , F.col('enriched_drivers.vehicle_license_plate')
                , F.col('enriched_drivers.vehicle_type')
                , F.col('enriched_drivers.vehicle_year')
                , F.lit(True).alias('is_current')
                , F.col('event_timestamp').alias('effective_from_timestamp')
                , F.lit(timestamp_default).alias('effective_to_timestamp')
            )
        )
        
        max_driver_key = dim_drivers_target.selectExpr('ifnull(max(driver_key), 0) as max_driver_key').first().max_driver_key

        dim_drivers_source = (
            dim_drivers_source_with_modified_users
            .union(dim_drivers_source_with_modified_drivers)
            .withColumn(
                'driver_key'
                , (F.row_number().over(Window.orderBy(F.col('driver_id'))) + F.lit(max_driver_key)).cast(T.LongType()).alias('driver_key')
            )
            .select(
                F.col('driver_key')
                , F.col('driver_id')
                , F.col('user_id')
                , F.col('full_name')
                , F.col('email')
                , F.col('password_hash')
                , F.col('phone_number')
                , F.col('address')
                , F.col('vehicle_license_plate')
                , F.col('vehicle_type')
                , F.col('vehicle_year')
                , F.col('is_current')
                , F.col('effective_from_timestamp')
                , F.col('effective_to_timestamp')
            ).alias('source')
            .join(dim_drivers_target.alias('target'), F.col('source.driver_id') == F.col('target.driver_id'), 'left')
            .where(
                (F.col('source.effective_from_timestamp') > F.col('target.effective_from_timestamp'))
                | F.col('target.driver_id').isNull()
            )
            .select(F.col('source.*'))
        )
        
        dim_drivers_union = dim_drivers_target.union(dim_drivers_source)
  
        dim_drivers_final = (
            dim_drivers_union
            .withColumn(
                'temp_effective_to_timestamp'
                , F.lead(F.col('effective_from_timestamp'), default = timestamp_default).over(Window.partitionBy(F.col('driver_id')).orderBy(F.col('effective_from_timestamp')))
            )
            .withColumn('effective_to_timestamp', F.col('temp_effective_to_timestamp'))
            .withColumn('is_current', F.when(F.col('temp_effective_to_timestamp') == timestamp_default, F.lit(True)).otherwise(F.lit(False)))
            .drop(F.col('temp_effective_to_timestamp'))
        )
  
        self.curated_data_loader.load_dim_drivers(self, dim_drivers_final)
        
        

#-----------------------------------------FACT_ORDERS-----------------------------------------

class FactOrders(Table):
    
    def __init__(self):
        super().__init__()
        self.dim_users_path = self.dimension_base_path + '/dim_users'
        self.dim_locations_path = self.dimension_base_path + '/dim_locations'
        self.fact_orders_path = self.fact_base_path + '/fact_orders'

    @property
    def dim_users(self):
        dim_users = (
            self.spark.read.format('delta').option('path', self.dim_users_path).load()
            .where((F.col('is_current')))
            .select(F.col('user_id'), F.col('user_key'))
        )
        return dim_users
        
    @property
    def dim_locations(self):
        dim_locations = self.spark.read.format('delta').option('path', self.dim_locations_path).load()
        return dim_locations
        
    
    def join_orders_with_dimensions(self, orders_df):
        orders_df_with_latest_update = (
            orders_df
            .withColumn('rank', F.rank().over(Window.partitionBy(F.col('order_id'), F.col('status')).orderBy(F.col('event_timestamp').desc())))
            .withColumn('event_timestap', F.min(F.col('event_timestamp')).over(Window.partitionBy(F.col('order_id'), F.col('status'))))
            .where(F.col('rank') == 1)
        )
        joined_orders = (
            orders_df_with_latest_update.alias('orders')
            .join(self.dim_users.alias('dim_users'), F.col('orders.user_id') == F.col('dim_users.user_id'), 'left')
            .join(self.dim_locations.alias('pickup_location'), F.col('orders.pickup_address') == F.col('pickup_location.location'), 'left')
            .join(self.dim_locations.alias('delivery_location'), F.col('orders.delivery_address') == F.col('delivery_location.location'), 'left')
            .select(
                F.col('orders.order_id').cast(T.LongType())
                , F.col('orders.package_description')
                , F.col('orders.package_weight')
                , F.col('orders.delivery_time')
                , F.col('orders.created_at')
                , F.col('orders.event_timestamp')
                , F.col('orders.status')
                , F.col('dim_users.user_key')
                , F.col('pickup_location.location_key').alias('pick_up_location_key')
                , F.col('delivery_location.location_key').alias('delivery_location_key')
            )
        )
        return joined_orders

    @staticmethod
    def make_interval(start_date_key, start_time_key, end_date_key, end_time_key):

        interval = (
            F.to_timestamp(F.concat_ws(' ', end_date_key, end_time_key), 'yyyyMMdd HH:mm:ss') 
            - F.to_timestamp(F.concat_ws(' ', start_date_key, start_time_key), 'yyyyMMdd HH:mm:ss')
        )

        interval_struct = F.named_struct(
            F.lit('days'), F.extract(F.lit('D'), interval)
            , F.lit('hours'), F.extract(F.lit('H'), interval)
            , F.lit('minutes'), F.extract(F.lit('m'), interval)
            , F.lit('seconds'), F.extract(F.lit('s'), interval).cast(T.IntegerType())
            
        )
        return interval_struct
        
 
    def process_fact_orders(self):
        
        enriched_orders_df = self.enriched_data_extractor.extract_enriched_orders()
        joined_orders_df =  self.join_orders_with_dimensions(enriched_orders_df)

        is_exists = DeltaTable.isDeltaTable(self.spark, self.fact_orders_path)
        if not is_exists:
            (
                DeltaTable.create(self.spark)
                .tableName("fact_orders")
                .addColumn("order_id", "LONG", nullable = True)
                .addColumn("user_key", "LONG", nullable = True)
                .addColumn("pick_up_location_key", "LONG", nullable = True)
                .addColumn("delivery_location_key", "LONG", nullable = True)
                .addColumn("dd_package_description", "STRING", nullable = True)
                .addColumn("dd_status", "STRING", nullable = True)
                .addColumn("package_weight", "FLOAT", nullable = True)
                .addColumn("created_order_date_key", "INT", nullable = True)
                .addColumn("created_order_time_key", "STRING", nullable = True)
                .addColumn("accepted_date_key", "INT", nullable = True)
                .addColumn("accepted_time_key", "STRING", nullable = True)
                .addColumn("in_transit_date_key", "INT", nullable = True)
                .addColumn("in_transit_time_key", "STRING", nullable = True)
                .addColumn("delivered_date_key", "INT", nullable = True)
                .addColumn("delivered_time_key", "STRING", nullable = True)
                .addColumn("delivery_date_key", "INT", nullable = True)
                .addColumn("delivery_time_key", "STRING", nullable = True)
                .addColumn("created_to_accepted_lag", "STRUCT<days: INT, hours: INT, minutes: INT, seconds: INT>", nullable = True)
                .addColumn("accepted_to_in_transit_lag", "STRUCT<days: INT, hours: INT, minutes: INT, seconds: INT>", nullable = True)
                .addColumn("in_transit_to_delivered_lag", "STRUCT<days: INT, hours: INT, minutes: INT, seconds: INT>", nullable = True)
                .addColumn("delivered_and_delivery_difference", "STRUCT<days: INT, hours: INT, minutes: INT, seconds: INT>", nullable = True)
                .location(self.fact_orders_path)
                .execute()
            )
   
        processing_enriched_orders_df = joined_orders_df.where(F.col('status') == 'processing')    
        accepted_enriched_orders_df = joined_orders_df.where(F.col('status') == 'accepted')  
        in_transit_enriched_orders_df = joined_orders_df.where(F.col('status') == 'in_transit')  
        delivered_enriched_orders_df = joined_orders_df.where(F.col('status') == 'delivered')  
        fact_orders_df = self.spark.read.format('delta').option('path', self.fact_orders_path).load()
      
        source_orders_df = (
            processing_enriched_orders_df.alias('p')
            .join(accepted_enriched_orders_df.alias('a'), F.col('p.order_id') == F.col('a.order_id'), 'full')
            .join(in_transit_enriched_orders_df.alias('it'), F.col('a.order_id') == F.col('it.order_id'), 'full')
            .join(delivered_enriched_orders_df.alias('d'), F.col('it.order_id') == F.col('d.order_id'), 'full')
            .join(fact_orders_df.alias('o'), F.col('d.order_id') == F.col('o.order_id'), 'left')
        )
        
        final_source_orders_df = source_orders_df.select(
            F.coalesce(F.col('d.order_id'), F.col('it.order_id'), F.col('a.order_id'), F.col('p.order_id')).alias('order_id')
            , F.coalesce(F.col('d.user_key'), F.col('it.user_key'), F.col('a.user_key'), F.col('p.user_key')).alias('user_key')
            , F.coalesce(F.col('d.pick_up_location_key'), F.col('it.pick_up_location_key'), F.col('a.pick_up_location_key'), F.col('p.pick_up_location_key')).alias('pick_up_location_key')
            , F.coalesce(F.col('d.delivery_location_key'), F.col('it.delivery_location_key'), F.col('a.delivery_location_key'), F.col('p.delivery_location_key')).alias('delivery_location_key')
            , F.coalesce(F.col('d.package_description'), F.col('it.package_description'), F.col('a.package_description'), F.col('p.package_description')).alias('dd_package_description')
            , F.coalesce(F.col('d.status'), F.col('it.status'), F.col('a.status'), F.col('p.status')).alias('dd_status')
            , F.coalesce(F.col('d.package_weight'), F.col('it.package_weight'), F.col('a.package_weight'), F.col('p.package_weight')).alias('package_weight')
            , F.date_format(F.coalesce(F.col('d.created_at'), F.col('it.created_at'), F.col('a.created_at'), F.col('p.created_at')), 'yyyyMMdd').cast(T.IntegerType()).alias('created_order_date_key')
            , F.date_format(F.coalesce(F.col('d.created_at'), F.col('it.created_at'), F.col('a.created_at'), F.col('p.created_at')), 'HH:mm:ss').alias('created_order_time_key')
            , F.expr('''
                case 
                    when isnull(o.order_id) and isnull(p.order_id) then cast(99991231 as int)
                    when isnull(o.order_id) or isnull(p.order_id) then coalesce(cast(date_format(p.event_timestamp, 'yyyyMMdd') as int), o.accepted_date_key)
                    when o.accepted_date_key == 99991231 then cast(date_format(p.event_timestamp, 'yyyyMMdd') as int)
                    else o.accepted_date_key
                end
            ''').alias('accepted_date_key')
            , F.expr('''
                case 
                    when isnull(o.order_id) and isnull(p.order_id) then '00:00:00'
                    when isnull(o.order_id) or isnull(p.order_id) then coalesce(date_format(p.event_timestamp, 'HH:mm:ss'), o.accepted_time_key)
                    when o.accepted_time_key == '00:00:00' then date_format(p.event_timestamp, 'HH:mm:ss')
                    else o.accepted_time_key
                end
            ''').alias('accepted_time_key')
            , F.expr('''
                case 
                    when isnull(o.order_id) and isnull(it.order_id) then cast(99991231 as int)
                    when isnull(o.order_id) or isnull(it.order_id) then coalesce(cast(date_format(it.event_timestamp, 'yyyyMMdd') as int), o.in_transit_date_key)
                    when o.in_transit_date_key == 99991231 then cast(date_format(it.event_timestamp, 'yyyyMMdd') as int)
                    else o.in_transit_date_key
                end
            ''').alias('in_transit_date_key')
            , F.expr('''
                case 
                    when isnull(o.order_id) and isnull(it.order_id) then '00:00:00'
                    when isnull(o.order_id) or isnull(it.order_id) then coalesce(date_format(it.event_timestamp, 'HH:mm:ss'), o.in_transit_time_key)
                    when o.in_transit_time_key == '00:00:00' then date_format(it.event_timestamp, 'HH:mm:ss')
                    else o.in_transit_time_key
                end
            ''').alias('in_transit_time_key')
            , F.expr('''
                case 
                    when isnull(o.order_id) and isnull(d.order_id) then cast(99991231 as int)
                    when isnull(o.order_id) or isnull(d.order_id) then coalesce(cast(date_format(d.event_timestamp, 'yyyyMMdd') as int), o.delivered_date_key)
                    when o.delivered_date_key == 99991231 then cast(date_format(d.event_timestamp, 'yyyyMMdd') as int)
                    else o.delivered_date_key
                end
            ''').alias('delivered_date_key')
            , F.expr('''
                case 
                    when isnull(o.order_id) and isnull(d.order_id) then '00:00:00'
                    when isnull(o.order_id) or isnull(d.order_id) then coalesce(date_format(d.event_timestamp, 'HH:mm:ss'), o.delivered_time_key)
                    when o.delivered_time_key == '00:00:00' then date_format(d.event_timestamp, 'HH:mm:ss')
                    else o.delivered_time_key
                end
            ''').alias('delivered_time_key')
            , F.date_format(F.coalesce(F.col('d.delivery_time'), F.col('it.delivery_time'), F.col('a.delivery_time'), F.col('p.delivery_time')), 'yyyyMMdd').cast(T.IntegerType()).alias('delivery_date_key')
            , F.date_format(F.coalesce(F.col('d.delivery_time'), F.col('it.delivery_time'), F.col('a.delivery_time'), F.col('p.delivery_time')), 'HH:mm:ss').alias('delivery_time_key')

        )
            
        final_source_orders_df = final_source_orders_df.select(
            F.col('*')
            , (
                F.when(
                    F.col('accepted_date_key') != F.lit(99991231)
                    , self.make_interval(F.col('created_order_date_key'), F.col('created_order_time_key'), F.col('accepted_date_key'), F.col('accepted_time_key'))
                )
                .otherwise(F.named_struct(F.lit('days'), F.lit(0), F.lit('hours'), F.lit(0), F.lit('minutes'), F.lit(0), F.lit('seconds'), F.lit(0)))
            ).alias('created_to_accepted_lag')
            , (
                F.when(
                    F.col('in_transit_date_key') != F.lit(99991231)
                    , self.make_interval(F.col('accepted_date_key'), F.col('accepted_time_key'), F.col('in_transit_date_key'), F.col('in_transit_time_key'))
                )
                .otherwise(F.named_struct(F.lit('days'), F.lit(0), F.lit('hours'), F.lit(0), F.lit('minutes'), F.lit(0), F.lit('seconds'), F.lit(0)))
            ).alias('accepted_to_in_transit_lag')
            , (
                F.when(
                    F.col('in_transit_date_key') != F.lit(99991231)
                    , self.make_interval(F.col('in_transit_date_key'), F.col('in_transit_time_key'), F.col('delivered_date_key'), F.col('delivery_time_key'))
                )
                .otherwise(F.named_struct(F.lit('days'), F.lit(0), F.lit('hours'), F.lit(0), F.lit('minutes'), F.lit(0), F.lit('seconds'), F.lit(0)))
            ).alias('in_transit_to_delivered_lag')
            , (
                F.when(
                    F.col('in_transit_date_key') != F.lit(99991231)
                    , self.make_interval(F.col('delivered_date_key'), F.col('delivered_time_key'), F.col('delivery_date_key'), F.col('delivery_time_key'))
                )
                .otherwise(F.named_struct(F.lit('days'), F.lit(0), F.lit('hours'), F.lit(0), F.lit('minutes'), F.lit(0), F.lit('seconds'), F.lit(0)))
            ).alias('delivered_and_delivery_difference')
        )
        
        self.curated_data_loader.load_fact_orders(self, final_source_orders_df)

#-----------------------------------------FACT_PAYMENTS-----------------------------------------

class FactPayments(Table):
    def __init__(self):
        super().__init__()
        self.fact_payments_path = self.fact_base_path + '/fact_payments'
        self.fact_orders_path = self.fact_base_path + '/fact_orders'
        
    @property
    def fact_orders(self):
        fact_orders = self.spark.read.format('delta').option('path', self.fact_orders_path).load()
        return fact_orders

    def process_fact_payments(self):
        enriched_payments_df = self.enriched_data_extractor.extract_enriched_payments()

        is_exists = DeltaTable.isDeltaTable(self.spark, self.fact_payments_path)
        if not is_exists:
            (
                DeltaTable.create(self.spark)
                .tableName("payments")
                .addColumn("payment_id", "LONG", nullable = True)
                .addColumn("order_id", "LONG", nullable = True)
                .addColumn("user_key", "LONG", nullable = True)
                .addColumn("pick_up_location_key", "LONG", nullable = True)
                .addColumn("delivery_location_key", "LONG", nullable = True)
                .addColumn("dd_package_description", "STRING", nullable = True)
                .addColumn("created_order_date_key", "INT", nullable = True)
                .addColumn("created_order_time_key", "STRING", nullable = True)
                .addColumn("payment_date_key", "INT", nullable = True)
                .addColumn("payment_time_key", "STRING", nullable = True)
                .addColumn("dd_payment_method", "STRING", nullable = True)
                .addColumn("dd_payment_status", "STRING", nullable = True)
                .addColumn("package_weight", "FLOAT", nullable = True)
                .addColumn("amount", "DECIMAL(10,2)", nullable = True)
                .location(self.fact_payments_path)
                .execute()
            )
        
        joined_payments_df = (
            enriched_payments_df.alias('p')
            .join(self.fact_orders.alias('o'), F.col('o.order_id') == F.col('p.order_id'), 'left' )
            .select(
                F.col('p.payment_id').cast(T.LongType()).alias('payment_id')
                , F.col('p.order_id').cast(T.LongType()).alias('order_id')
                , F.col('o.user_key').cast(T.LongType()).alias('user_key')
                , F.col('o.pick_up_location_key').cast(T.LongType()).alias('pick_up_location_key')
                , F.col('o.delivery_location_key').cast(T.LongType()).alias('delivery_location_key')
                , F.col('o.dd_package_description').cast(T.LongType()).alias('dd_package_description')
                , F.col('o.created_order_date_key').alias('created_order_date_key')
                , F.col('o.created_order_time_key').alias('created_order_time_key')
                , F.date_format(F.col('payment_date'), 'yyyyMMdd').cast(T.IntegerType()).alias('payment_date_key')
                , F.date_format(F.col('payment_date'), 'HH:mm:ss').alias('payment_time_key')
                , F.col('payment_method').alias('dd_payment_method')
                , F.col('payment_status').alias('dd_payment_status')
                , F.col('o.package_weight')
                , F.col('p.amount')
            )
        )
        
        self.curated_data_loader.load_fact_payments(self, joined_payments_df)

class FactShipments(Table):
    def __init__(self):
        super().__init__()
        self.dim_drivers_path = self.dimension_base_path + '/dim_drivers'
        self.fact_shipments_path = self.fact_base_path + '/fact_shipments'
        self.fact_orders_path = self.fact_base_path + '/fact_orders'

    @property
    def dim_drivers(self):
        dim_drivers = self.spark.read.format('delta').option('path', self.dim_drivers_path).load()
        return dim_drivers
        
    @property
    def fact_orders(self):
        fact_orders = self.spark.read.format('delta').option('path', self.fact_orders_path).load()
        return fact_orders

    def process_fact_shipments(self):
        enriched_shipments_df = self.enriched_data_extractor.extract_enriched_shipments()

        is_exists = DeltaTable.isDeltaTable(self.spark, self.fact_shipments_path)
        if not is_exists:
            (
                DeltaTable.create(self.spark)
                .tableName("fact_shipments")
                .addColumn("shipment_id", "LONG", nullable = True)
                .addColumn("order_id", "LONG", nullable = True)
                .addColumn("driver_key", "LONG", nullable = True)
                .addColumn("dd_current_location", "STRING", nullable = True)
                .addColumn("user_key", "LONG", nullable = True)
                .addColumn("pick_up_location_key", "LONG", nullable = True)
                .addColumn("delivery_location_key", "LONG", nullable = True)
                .addColumn("dd_package_description", "STRING", nullable = True)
                .addColumn("created_order_date_key", "INT", nullable = True)
                .addColumn("created_order_time_key", "STRING", nullable = True)
                .addColumn("dd_status", "STRING", nullable = True)
                .addColumn("estimated_delivery_date_key", "INT", nullable = True)
                .addColumn("estimated_delivery_time_key", "STRING", nullable = True)
                .location(self.fact_shipments_path)
                .execute()
            )
            
        joined_shipments_df = (
            enriched_shipments_df.alias('s')
            .join(self.dim_drivers.alias('d'), F.col('d.driver_id') == F.col('s.driver_id'), 'left')
            .join(self.fact_orders.alias('o'), F.col('o.order_id') == F.col('s.order_id'), 'left')
            .select(
                F.col('s.shipment_id').cast(T.LongType()).alias('shipment_id')
                , F.col('s.order_id').cast(T.LongType()).alias('order_id')
                , F.col('d.driver_key').alias('driver_key')
                , F.col('s.current_location').alias('dd_current_location')
                , F.col('o.user_key').cast(T.LongType()).alias('user_key')
                , F.col('o.pick_up_location_key').cast(T.LongType()).alias('pick_up_location_key')
                , F.col('o.delivery_location_key').cast(T.LongType()).alias('delivery_location_key')
                , F.col('o.dd_package_description').alias('dd_package_description')
                , F.col('o.created_order_date_key').alias('created_order_date_key')
                , F.col('o.created_order_time_key').alias('created_order_time_key')
                , F.col('s.status').alias('dd_status')
                , F.date_format(F.col('s.estimated_delivery_time'), 'yyyyMMdd').cast(T.IntegerType()).alias('estimated_delivery_date_key')
                , F.date_format(F.col('s.estimated_delivery_time'), 'HH:mm:ss').alias('estimated_delivery_time_key')
            )
        )
        
        self.curated_data_loader.load_fact_shipments(self, joined_shipments_df)