from airflow.providers.mysql.hooks.mysql import MySqlHook

completion_oltp = MySqlHook(mysql_conn_id='completion_oltp')
completion_dwh = MySqlHook(mysql_conn_id='completion_dwh')