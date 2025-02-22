from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pendulum


profiles_dir = '/opt/dbt/.dbt'
target = 'airflow'
project_dir = '/opt/dbt/projects/logistics'

default_args = {
    'owner': 'vinh'
    , 'email': 'ltvinh1101@gmail.com'
    , 'email_on_failure': False
    , 'email_on_retry': False
    , 'retries': 2
    , 'retry_delay': pendulum.duration(seconds = 1)
}

with DAG(
    dag_id = 'dbt_logistics_dag_v2'
    , description = 'This is dbt dag'
    , default_args = default_args
    , start_date = pendulum.now(tz = 'Asia/Ho_Chi_Minh')
    , max_active_tasks = 1
    , max_active_runs = 1
    , schedule = None
    , tags = ['logistics', 'dbt', 'mysql']
) as dag:

    start = EmptyOperator(task_id = 'start')

    with TaskGroup(group_id = 'staging_operators_group') as staging_group:
        staging_models = [
            'stg_mysql__users'
            , 'stg_mysql__drivers'
            , 'stg_mysql__orders'
            , 'stg_mysql__payments'
            , 'stg_mysql__shipments'
        ]
        
        staging_operators = []

        for model in staging_models:
            temp = BashOperator(
                task_id = model
                , bash_command = f'cd {project_dir} && dbt run --models {model} --profiles-dir {profiles_dir} --target {target}'
            )
            staging_operators.append(temp)

    start >> staging_group

    with TaskGroup(group_id = 'dimensions_operators_group') as dimensions_group:
        dim_date_model_name = 'dim_date'
        dim_date_operator = BashOperator(
            task_id = dim_date_model_name
            , bash_command = f'cd {project_dir} && dbt run --models {dim_date_model_name} --profiles-dir {profiles_dir} --target {target}'
        )
        
        dim_locations_model_name = 'dim_locations'
        dim_locations_operator = BashOperator(
            task_id = dim_locations_model_name
            , bash_command = f'cd {project_dir} && dbt run --models {dim_locations_model_name} --profiles-dir {profiles_dir} --target {target}'
        )
   
        dim_users_model_name = 'dim_users'
        dim_users_operator = BashOperator(
            task_id = dim_users_model_name
            , bash_command = f'cd {project_dir} && dbt run --models {dim_users_model_name} --profiles-dir {profiles_dir} --target {target}'
        )
 
        dim_drivers_model_name = 'dim_drivers'
        dim_drivers_operator = BashOperator(
            task_id = dim_drivers_model_name
            , bash_command = f'cd {project_dir} && dbt run --models {dim_drivers_model_name} --profiles-dir {profiles_dir} --target {target}'
        )

        dim_users_operator >> dim_drivers_operator
        
    staging_group >> dimensions_group

    with TaskGroup(group_id = 'facts_operators_group') as facts_group:
        fact_orders_model_name = 'fact_orders'
        fact_orders_operator = BashOperator(
            task_id = fact_orders_model_name
            , bash_command = f'cd {project_dir} && dbt run --models {fact_orders_model_name} --profiles-dir {profiles_dir} --target {target}'
        )

        fact_payments_model_name = 'fact_payments'
        fact_payments_operator = BashOperator(
            task_id = fact_payments_model_name
            , bash_command = f'cd {project_dir} && dbt run --models {fact_payments_model_name} --profiles-dir {profiles_dir} --target {target}'
        )
        fact_orders_operator >> fact_payments_operator

        fact_shipments_model_name = 'fact_shipments'
        fact_shipments_operator = BashOperator(
            task_id = fact_shipments_model_name
            , bash_command = f'cd {project_dir} && dbt run --models {fact_shipments_model_name} --profiles-dir {profiles_dir} --target {target}'
        )
        fact_orders_operator >> fact_shipments_operator

    dimensions_group >> facts_group

    end = EmptyOperator(task_id = 'end')

    facts_group >> end
