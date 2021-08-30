from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

args = {
    'owner': 'Linh',
    'depends_on_past': True,
    'provide_context': True,
}

with DAG (
    dag_id= 'process_dimensions',
    default_args=args,
    start_date=days_ago(2),
    schedule_interval='@daily',
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=['practice']
) as dag:
    wait_for_completion_staging = ExternalTaskSensor(
        task_id='wait_for_completion_staging',
        external_dag_id='completion_staging',
        external_task_id='extract_completion_records',
        execution_delta=None # same day as today
    )

    process_completion_dim = BashOperator(
        task_id='process_completion_dim',
        bash_command='echo process completion dimension'
    )

    wait_for_completion_staging >> process_completion_dim