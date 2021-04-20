from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """Loads data from s3 to redshift.

    Args:
        BaseOperator (obj): Inherits from airflow BaseOperator.
    """    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 sql_qry='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql_qry = sql_qry

    def execute(self, context):
        """Executes logic for custom operator.

        Args:
            context (obj): context
        """ 
        self.log.info(f'StageToRedshiftOperator for {self.table} initiated.')
        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Started moving data from S3 to Redshift.")
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        formatted_sql = self.sql_qry.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        redshift.run(formatted_sql)
        self.log.info('Process completed')





