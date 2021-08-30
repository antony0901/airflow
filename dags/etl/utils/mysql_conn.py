from airflow.models.variable import Variable
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Connection
from airflow.providers.mysql.hooks.mysql import MySqlHook

host = Variable.get('host')
username = Variable.get('username')
password = Variable.get('password')
db_name = Variable.get('db_name')

conn: Connection = create_engine('mysql://{username}:{password}@{url}/{db_name}?charset=utf8'.format(
    username=username,
    password=password,
    url=host,
    db_name=db_name,
))

completion_oltp = MySqlHook(mysql_conn_id='completion_oltp')
mysql_dwh = MySqlHook(mysql_conn_id='mysql_dwh')