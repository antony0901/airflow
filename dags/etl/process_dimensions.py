from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from etl.models.dwh_completion import process_completion_record_dim

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

    process_completion_dim = PythonOperator(
        task_id='process_completion_dim',
        python_callable=process_completion_record_dim,
        op_kwargs={
            'sql': r"""
                -- Create a temporary table for customer operations
                CREATE TEMP TABLE merge_completion (LIKE dwh_completion_record);

                -- The customer_key is allocated later.
                -- The staging table should only have at most one record per customer
                INSERT INTO merge_completion
                SELECT 
                    c.completion_record_id, c.learner_id, c.completion_date
                FROM
                    staging_completion_record c
                WHERE 
                    c.staging_at >= '{{ds}}'
                AND c.staging_at < '{{tomorrow_ds}}'
            """,
        }
    )

    wait_for_completion_staging >> process_completion_dim