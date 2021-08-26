from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from etl.models.completion import init_completion_table, extract_completion_records

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
    start = PythonOperator(
        task_id = 'create_table',
        python_callable=init_completion_table
    )

    extract_completion = PythonOperator(
        task_id = 'extract_completion_records',
        python_callable= extract_completion_records,
        op_kwargs={
            'sql': r"select Id as entity_id, LearnerId as learner_id, CompletionDate as completion_date from CompletionRecord order by CompletionDate limit 100",
            'mysql_table': 'completion_record'
        },
    )

    start >> extract_completion