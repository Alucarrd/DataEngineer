from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql_with_date = """
    COPY {} FROM '{}/{}/{}/'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}'
    {} 'auto';
    
    """
    copy_sql = """
    COPY {} FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}'
    {} 'auto';
    
    """
  

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_conn_id='',
                 table ='',
                 s3_path='',
                 region='',
                 data_format='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_path = s3_path
        self.region= region
        self.data_format=data_format
        self.execution_date = kwargs.get('execution_date')
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clean up the staging table per each load")
        
        redshift_hook.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Time to load data from S3 to RedShift")
        
        if self.execution_date:
            formatted_sql = StageToRedshiftOperator.copy_sql_with_date.format(
                self.table, 
                self.s3_path, 
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                aws_credentials.access_key,
                aws_credentials.secret_key, 
                self.region,
                self.data_format,
                self.execution_date
                
                
            )

        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                self.s3_path, 
                aws_credentials.access_key,
                aws_credentials.secret_key, 
                self.region,
                self.data_format,
                self.execution_date
                
                
            )
        
        redshift_hook.run(formatted_sql)
        
       


