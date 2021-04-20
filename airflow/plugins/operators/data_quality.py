from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Checks that data is loaded to db table.

    Args:
        BaseOperator (obj): Inherits from airflow BaseOperator.

    Raises:
        ValueError: No results found.
        ValueError: No rows found.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                  redshift_conn_id,
                  tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """Executes logic for custom operator.

        Args:
            context (obj): context

        Raises:
            ValueError: No results found.
            ValueError: No rows found.
        """
        self.log.info('Checking ETL Results')
        redshift =  PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.table:
            
            data = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(data) < 1 | len(data[0])<1:
                
                raise ValueError(f'{table} check failed no results found.')
            
            if data[0][0] < 1:
                
                raise ValueError(f'{table} contained no rows.')
            
            self.log.info(f'Quality checks passed for {table}')