from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Checks that data is loaded to db table.

    Args:
        BaseOperator (obj): Inherits from airflow BaseOperator.

    Raises:
        ValueError: No results found.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                  redshift_conn_id='',
                  tables=[],
                  sql_qry=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = tables
        self.sql_qry = sql_qry

    def execute(self, context):
        """Executes logic for custom operator.

        Args:
            context (obj): context

        Raises:
            ValueError: No results found.
        """
        self.log.info('Checking ETL Results')
        redshift =  PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.table:
            
            for check in self.sql_qry:
                
                sql = check.get('check_sql')
                exp_result = check.get('expected_result')
                
                if redshift.run(sql.format(table)) >exp_result:
                    self.log.info(f'Quality checks passed for {table}')
                else:
                    self.log.info(f'Quality checks Failed for {table}')
                    self.log.info(sql)
                    raise ValueError(f'{table} check failed no results found.')
                    