import logging
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy.engine.base import Connection
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import String, Integer, DateTime
from ..utils.mysql_conn import completion_oltp, mysql_dwh
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import Column, MetaData

Base = declarative_base()
dwh_table_name = 'dwh_completion_record';

class DwhCompletionRecordTable(Base):
    __tablename__=dwh_table_name;
    completion_record_id = Column(String(36), primary_key=True)
    learner_id = Column(String(36), index=True)
    completion_date = Column(String(30))

def init_dwh_completion_table():
    is_run = False
    dwh_conn: Connection = mysql_dwh.get_connection('mysql_dwh')
    if not dwh_conn.dialect.has_table(dwh_conn, dwh_table_name):
        Base.metadata.create_all(bind=dwh_conn)
        is_run = True

def process_completion_record_dim(sql):
    logging.info('Executing...')
    logging.info(sql)
    
    conn = completion_oltp.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    return True