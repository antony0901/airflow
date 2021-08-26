import logging
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy.engine.base import Connection
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import String
from ..utils.mysql_conn import conn, mysql_oltp_completion, mysql_oltp_dest
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import Column, MetaData

Base = declarative_base()
meta = MetaData(conn).reflect()

dwhConnection: Connection = conn.connect()
sessionDwh = sessionmaker(bind=dwhConnection)
sessionDwh = sessionDwh()

class CompletionRecordTable(Base):
    __tablename__='completion_record';
    entity_id = Column(String(36), primary_key=True)
    learner_id = Column(String(36), index=True)
    completion_date = Column(String(30))


def init_completion_table():
    is_run = False
    if not conn.dialect.has_table(conn, 'completion_record'):
        Base.metadata.create_all(bind=conn)
        is_run = True

def extract_completion_records(sql, mysql_table):
    logging.info('Executing...')

    conn = mysql_oltp_completion.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    mysql_oltp_dest.insert_rows(mysql_table, rows=cursor)
    print(cursor)
    return True