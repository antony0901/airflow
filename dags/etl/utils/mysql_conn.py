from airflow.models.variable import Variable
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Connection

host = Variable.get('host')
username = Variable.get('username')
password = Variable.get('password')
db_name = Variable.get('db_completion')

conn: Connection = create_engine('mysql://{username}:{password}@{url}/{db_name}?charset=utf8'.format(
    username=username,
    password=password,
    url=host,
    db_name=db_name,
))