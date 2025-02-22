from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pathlib import Path
import pendulum

enriched_path = Path('/opt/src/logistics_project_v2/workflows/enriched')
curated_path = Path('/opt/src/logistics_project_v2/workflows/curated')

default_args = {
    'owner': 'vinh'
    , 'email': 'ltvinh1101@gmail.com'
    , 'email_on_failure': True
    , 'email_on_retry': True
    , 'retries': 2
    , 'retry_delay': pendulum.duration(seconds = 1)
}

conn_id = 'my_spark_conn'
packages = 'io.delta:delta-spark_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.5.3'

with DAG(
    dag_id = 'logistics_dag_v1'
    , description = 'this is my first dag'
    , start_date = pendulum.now(tz = 'Asia/Ho_Chi_Minh')
    , max_active_tasks = 1
    , max_active_runs = 1
    , schedule = None
    , tags = ['logistics', 'mysql', 'data_warehouse', 'delta', 'delta_lake']
) as dag:
    # START
    start = EmptyOperator(task_id = 'start')

    
    # ENRICHED

    with TaskGroup(group_id = 'enriched_operators_group') as enriched_group:
        
        process_enirched_apps = [
            'process_enriched_users'
            , 'process_enriched_drivers'
            , 'process_enriched_orders'
            , 'process_enriched_payments'
            , 'process_enriched_shipments'
        ]
        
        process_enriched_operators = []
        
        for app_name in process_enirched_apps:
            temp = SparkSubmitOperator(
                task_id = app_name
                , conn_id = conn_id
                , application = str(enriched_path / f'{app_name}.py')
                , packages = packages
            )
            process_enriched_operators.append(temp)
     
    start >> enriched_group

    # CURATED
    
    with TaskGroup(group_id = 'dim_operatprs_group') as dim_group:
        
        dim_locations_app = 'process_dim_locations' 
        dim_locations_operator = SparkSubmitOperator(
            task_id = dim_locations_app
            , conn_id = conn_id
            , application = str(curated_path / f'{dim_locations_app}.py')
            , packages = packages
        )
        
        dim_date_app = 'process_dim_date' 
        dim_locations_operator = SparkSubmitOperator(
            task_id = dim_locations_app
            , conn_id = conn_id
            , application = str(curated_path / f'{dim_date_app}.py')
            , packages = packages
        )
        
        dim_users_app = 'process_dim_users' 
        dim_users_operator = SparkSubmitOperator(
            task_id = dim_users_app
            , conn_id = conn_id
            , application = str(curated_path / f'{dim_users_app}.py')
            , packages = packages
        )
        
        dim_drivers_app = 'process_dim_drivers' 
        dim_drivers_operator = SparkSubmitOperator(
            task_id = dim_drivers_app
            , conn_id = conn_id
            , application = str(curated_path / f'{dim_drivers_app}.py')
            , packages = packages
        )
        
        dim_users_operator >> dim_drivers_operator
        
    enriched_group >> dim_group
            
    with TaskGroup(group_id = 'fact_operators_groups') as fact_group:

        fact_orders_app = 'process_fact_orders' 
        fact_orders_operator = SparkSubmitOperator(
            task_id = dim_drivers_app
            , conn_id = conn_id
            , application = str(curated_path / f'{fact_orders_app}.py')
            , packages = packages
        )

        fact_payments_app = 'process_fact_payments' 
        fact_payments_operator = SparkSubmitOperator(
            task_id = fact_payments_app
            , conn_id = conn_id
            , application = str(curated_path / f'{fact_payments_app}.py')
            , packages = packages
        )
        fact_orders_operator >> fact_payments_operator
        
        fact_shipments_app = 'process_fact_shipments' 
        fact_shipments_operator = SparkSubmitOperator(
            task_id = fact_shipments_app
            , conn_id = conn_id
            , application = str(curated_path / f'{fact_shipments_app}.py')
            , packages = packages
        )
        fact_orders_operator >> fact_shipments_operator

    dim_group >> fact_group

    
    # END
    
    end = EmptyOperator(task_id = 'end')

    fact_group >> end