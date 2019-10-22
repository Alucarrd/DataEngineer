from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries



class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table="",
                 sql="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.info("Not going to append, time to clean up the fact table {}".format(self.table))
            redshift_hook.run("DELETE FROM {}".format(self.table))
        self.log.info("Time to insert data from staging table into fact {}".format(self.table))
        formatted_sql = getattr(SqlQueries,self.sql).format(self.table)
        redshift_hook.run(formatted_sql)