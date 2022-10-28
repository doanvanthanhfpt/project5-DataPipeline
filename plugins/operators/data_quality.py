from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # conn_id = your-connection-name
                redshift_conn_id="",
                aws_credentials_id="",
                all_tables="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id,
        self.aws_credentials_id = aws_credentials_id,
        self.all_tables = all_tables,

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        self.log.info('Starting DataQualityOperator implementation')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for tbl in range(len(self.all_tables)):
            table = list(self.all_tables.keys())[tbl][0]
            field = list(self.all_tables.values())[tbl][1]
            
            # Check tables - have rows
            check_table_sql = f"SELECT Count(*) FROM {table}"
            rows = redshift.get_first(check_table_sql) 
            self.log.info(f'Table: {table} has {rows} rows')
            
            # Check fields with null entries
            check_table_sql = f"SELECT Count(*) FROM {table} where {field} IS NULL"
            rows = redshift.get_first(check_table_sql) 
            self.log.info(f'Field: "{field}" in table: {table} has {rows} NULL rows')

            self.log.info(f'INFO: Data quality verification finished')