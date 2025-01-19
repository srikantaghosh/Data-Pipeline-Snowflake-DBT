-- THIS IS THE CODE STRUCTURE AND MIGHT NOT BE FULLY ACCURATE

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dbt_pipeline',
    default_args=default_args,
    description='A simple dbt pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define tasks
staging_tasks = [
    'stg_events',
    'stg_lineups',
    'stg_match_info',
    'stg_match_summary',
    'stg_qualifiers',
    'stg_position',
]

transformation_tasks = [
    'dim_position',
    'fct_events',
    'fct_match_positions',
]

# dbt run command for staging tasks
for task in staging_tasks:
    run_staging = BashOperator(
        task_id=f'run_{task}',
        bash_command=f'cd /path/to/your/dbt/project && dbt run --models {task}',
        dag=dag,
    )

# dbt run command for transformation tasks
for task in transformation_tasks:
    run_transformation = BashOperator(
        task_id=f'run_{task}',
        bash_command=f'cd /path/to/your/dbt/project && dbt run --models {task}',
        dag=dag,
    )

# Define dependencies
for i in range(1, len(staging_tasks)):
    staging_tasks[i] >> run_transformation
