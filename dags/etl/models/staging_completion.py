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
staging_table_name = 'staging_completion_record';
dwh_table_name = 'dwh_completion_record';

class StagingCompletionRecordTable(Base):
    __tablename__=staging_table_name;
    completion_record_id = Column(String(36), primary_key=True)
    learner_id = Column(String(36), index=True)
    completion_date = Column(String(30))
    staging_at = Column(DateTime)

def init_staging_completion_table():
    is_run = False
    dwh_conn: Connection = mysql_dwh.get_connection('mysql_dwh')
    if not dwh_conn.dialect.has_table(dwh_conn, staging_table_name):
        Base.metadata.create_all(bind=dwh_conn)
        is_run = True

def extract_completion_records(sql, mysql_table):
    logging.info('Executing...')
    logging.info(sql)
    
    conn = completion_oltp.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    print(cursor)

    mysql_dwh.insert_rows(mysql_table, rows=cursor)
    print(cursor)
    return True