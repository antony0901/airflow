import logging
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy.engine.base import Connection
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import String, Integer, DateTime
from ..utils.mysql_conn import conn, completion_oltp, mysql_dwh
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import Column, MetaData

Base = declarative_base()
meta = MetaData(conn).reflect()

dwhConnection: Connection = conn.connect()
sessionDwh = sessionmaker(bind=dwhConnection)
sessionDwh = sessionDwh()

class CompletionRecordTable(Base):
    __tablename__='staging.completion_record';
    id = Column(Integer, primary_key=True, autoincrement=True)
    completion_record_id = Column(String(36), index=True)
    learner_id = Column(String(36), index=True)
    completion_date = Column(String(30))
    staging_at = Column(DateTime)


def init_completion_table():
    is_run = False
    if not conn.dialect.has_table(conn, 'staging.completion_record'):
        Base.metadata.create_all(bind=conn)
        is_run = True

def extract_completion_records(sql, mysql_table):
    logging.info('Executing...')
    logging.info(sql)
    
    conn = completion_oltp.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)

    mysql_dwh.insert_rows(mysql_table, rows=cursor)
    print(cursor)
    return True