from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, task
from airflow.operators.bash import BashOperator
from etl.models.staging_completion import (
    init_staging_completion_table, 
    extract_completion_records,
    staging_table_name
)

from etl.models.dwh_completion import (
    init_dwh_completion_table,
)

args = {
    'owner': 'Linh',
    'depends_on_past': False,
}

with DAG(
    dag_id = 'completion_staging',
    default_args= args,
    start_date=days_ago(2),
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=60),
    tags=['practice']
) as dag:
    dag.doc_md = """
    Completion Record ETL
    """

    start = BashOperator(
        task_id = 'start',
        bash_command='echo start completion staging'
    )

    init_staging_table = PythonOperator(
        task_id = 'init_staging_completion_table',
        python_callable=init_staging_completion_table
    )

    init_dwh_table = PythonOperator(
        task_id = 'init_dwh_completion_table',
        python_callable=init_dwh_completion_table
    )
    
    extract_completion = PythonOperator(
        task_id = 'extract_completion_records',
        execution_timeout = timedelta(minutes=60),
        python_callable= extract_completion_records,
        op_kwargs={
            'sql': r"""
            SELECT Id as completion_record_id, LearnerId as learner_id, 
                CompletionDate as completion_date, '{{ds}}' as staging_at
            FROM CompletionRecord 
            WHERE Updated >= '{{ds}}' AND Updated < '{{execution_date}}'
            ORDER BY CompletionDate;
            """,
            'mysql_table': staging_table_name
        },
    )

    start >> init_staging_table >> init_dwh_table >> extract_completion