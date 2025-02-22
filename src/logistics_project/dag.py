from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent / 'operators'))
from custom_operators import CustomSparkSubmitOperator

import pendulum
import yaml


default_args = {
    'owner': 'vinh'
    , 'depends_on_past': False
    , 'email': 'ltvinh1101@gmail.com'
    , 'email_on_failure': True
    , 'email_on_retry': True
    , 'retries': 0
    , 'retry_delay': pendulum.duration(seconds = 1)
}
   
with DAG(
    dag_id = 'logistics_dag_v3'
    , description = 'This is a dag'
    , start_date = pendulum.now(tz = 'Asia/Ho_Chi_Minh')
    , default_args = default_args
    , catchup = False
    , schedule = '@daily'
    , tags = ['logistics', 'mysql']
) as dag:
    #START
    start = EmptyOperator(task_id = 'start')

    # config_path = Path.cwd() / 'config/config.yaml'
    # with open(config_path) as file:
    #     config = yaml.safe_load(file)
    #     packages = config['spark']['packages']
        
    process_enirched_apps = [
        'process_enriched_users'
        , 'process_enriched_drivers'
        , 'process_enriched_orders'
        , 'process_enriched_paymentss'
        , 'process_enriched_shipments'
    ]
    
    processe_enriched_operators = []
    
    for app_name in process_enirched_apps:
        temp = CustomSparkSubmitOperator(
            task_id = app_name
            , conn_id = 'spark-conn'
            , application = app_name
  
        )
        processe_enriched_operators.append(temp)
        
    #END
    end = EmptyOperator(task_id = 'end')

    start >> processe_enriched_operators >> end