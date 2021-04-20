from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Load dimension tables to db.

    Args:
        BaseOperator (obj): Inherits from airflow BaseOperator.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_qry,
                 truncate,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_qry = sql_qry
        self.truncate = truncate

    def execute(self, context):
        """Executes logic for custom operator.

        Args:
            context (obj): context
        """        
        self.log.info('Extracting credentials')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info('Initiating deletion of records')
            truncate_qry = f"TRUNCATE TABLE {self.table}"
            redshift.run(truncate_qry)
        
        self.log.info(f'Inserting data to {self.table} table')    
        insert_qry =f"""
        INSERT INTO {self.table}
            ({self.sql_qry});
        """
        redshift.run(insert_qry)
        self.log.info('Process completed')
