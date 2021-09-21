from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from .utils import (
    MysqlOperatorWithTemplateParams
)

args = {
    'owner': 'Linh',
    'depends_on_past': True,
    'provide_context': True,
}

with DAG (
    dag_id= 'completion_staging',
    default_args=args,
    start_date=days_ago(2),
    schedule_interval='@daily',
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=['practice'],
    template_searchpath='/opt/airflow/include'
) as dag:
    start = BashOperator(
        task_id='start_extraction',
        bash_command='echo starting extraction'
    )

    completion_extraction = MysqlOperatorWithTemplateParams(
        task_id = 'extract_completions',
        sql = 'completion.sql',
        mysql_conn_id='completion_oltp'
    )

    start >> completion_extraction