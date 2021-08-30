from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, task
from airflow.operators.bash import BashOperator
from etl.models.completion import init_completion_table, extract_completion_records

args = {
    'owner': 'Linh',
    'depends_on_past': False,
}

with DAG(
    dag_id = 'completion_staging',
    default_args= args,
    start_date=days_ago(2),
    schedule_interval='@daily',
    tags=['practice']
) as dag:
    dag.doc_md = """
    Completion Record ETL
    """

    start = BashOperator(
        task_id = 'start',
        bash_command='echo start completion staging'
    )

    init_table = PythonOperator(
        task_id = 'create_table',
        python_callable=init_completion_table
    )
    
    extract_completion = PythonOperator(
        task_id = 'extract_completion_records',
        execution_timeout = timedelta(minutes=60),
        python_callable= extract_completion_records,
        op_kwargs={
            'sql': r"""
            SELECT Id as completion_record_id, LearnerId as learner_id, 
                CompletionDate as completion_date, '{{execution_date}}' as staging_at
            FROM CompletionRecord 
            WHERE Updated >= '{{ds}}' AND Updated < '{{execution_date}}'
            ORDER BY CompletionDate
            """,
            'mysql_table': 'staging.completion_record'
        },
    )

    start >> init_table >> extract_completion