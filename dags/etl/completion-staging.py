from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from etl.models.completion import insertRandomCompletion

args = {
    'owner': 'Linh',
    'depends_on_past': False,
}

with DAG(
    dag_id = 'completion_etl',
    default_args= args,
    start_date=days_ago(2),
    schedule_interval='@daily',
    tags=['practice']
) as dag:
    process_dag = PythonOperator(
        task_id = 'dag_insert_completion',
        python_callable = insertRandomCompletion,
    )

    process_dag