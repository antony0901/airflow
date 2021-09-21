from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook

class MysqlToMysqlOperator(BaseOperator):
    template_fields = ('sql', 'parameters', 'mysql_table', 'mysql_preoperator', 'mysql_postoperator')
    template_ext = ('.sql')
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self, 
        sql,
        sql_table,
        src_mysql_conn_id,
        dest_mysql_conn_id,
        mysql_preoperator,
        mysql_postoperator,
        parameters=None,
        *args, **kwargs):
        self.sql = sql
        self.mysql_table = sql_table
        self.src_mysql_conn_id = src_mysql_conn_id,
        self.dest_mysql_conn_id = dest_mysql_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.mysql_postoperator = mysql_postoperator
        self.parameters = parameters
    
    def execute(self, context):
        logging.info('Executing ' + str(self.sql))
        src_mysql = MySqlHook(mysql_conn_id=self.src_mysql_conn_id)
        dest_mysql = MySqlHook(mysql_conn_id=self.dest_mysql_conn_id)

        logging.info('transfering mysql query results')
        conn = src_mysql.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql, self.parameters)

        if self.mysql_preoperator:
            logging.info('Running Mysql preoperator')
            dest_mysql.run(self.mysql_preoperator)
        
        logging.info('insert rows')
        dest_mysql.insert_rows(table=self.mysql_table, rows=cursor)

        if self.mysql_postoperator:
            logging.info('Running mysql postoperator')
            dest_mysql.run(self.mysql_postoperator)

class MysqlOperatorWithTemplateParams(BaseOperator):
    template_fields = ('sql', 'parameters')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            mysql_conn_id='mysql_default', autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(MysqlOperatorWithTemplateParams, self).__init__(*args, **kwargs)
        self.sql = sql
        self.mysql_conn_id = mysql_conn_id
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        self.hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)