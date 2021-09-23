from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from etl.utils import (
    MysqlOperatorWithTemplateParams,
    MysqlToMysqlOperator
)

args = {
    'owner': 'Linh',
    'depends_on_past': False,
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
    dag.doc_md = """
    Processes completion extraction from OLTP data store to staging data store
    v3
    """

    start = BashOperator(
        task_id='start_extraction',
        bash_command='echo starting extraction',
    )

    completion_extraction = MysqlToMysqlOperator(
        sql = 'completion.sql',
        mysql_table= 'CompletionRecord_staging',
        src_mysql_conn_id = 'completion_oltp',
        dest_mysql_conn_id = 'completion_dwh',
        mysql_preoperator="""
        
        DELETE FROM CompletionRecord_staging WHERE
        partition_dtm >= DATE '2021-08-30' AND partition_dtm < DATE '2021-08-31'
        """,
        mysql_postoperator='',
        parameters={
            'window_start_date': '2021-08-30'
        },
        task_id = 'extract_completions',
    )

    start >> completion_extraction