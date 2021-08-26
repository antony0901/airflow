from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import insert
from sqlalchemy.sql.sqltypes import String
from ..utils.mysql_conn import conn
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import Column, MetaData

Base = declarative_base()
meta = MetaData(conn).reflect()

dwhConnection = conn.connect()
sessionDwh = sessionmaker(bind=dwhConnection)
sessionDwh = sessionDwh()

class CompletionTable(Base):
    __tablename__='completion.etl';
    entity_id = Column(String(32), primary_key=True)
    completion_date = Column(String(20))

def initCompletionTable():
    is_run = False
    if not conn.dialect.has_table(conn, 'completion.etl'):
        Base.metadata.create_all(bind=conn)
        is_run = True

def insertRandomCompletion():
    prepareData = []
    now = datetime.now()
    Base.metadata.create_all(bind=conn)
    sessionDwh.add_all(prepareData)
    sessionDwh.commit()
    return True

initCompletionTable()
insertRandomCompletion()
sessionDwh.close()
dwhConnection.close()