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

        for target_table in range(len(self.all_tables)):
            checking_field = list(self.all_tables.values())[target_table][1]
            checking_table = list(self.all_tables.keys())[target_table][1]
            
            # Check by count number of NULL rows of fields
            count_null_rows_sql = f"SELECT Count(*) FROM {checking_table} where {checking_field} IS NULL"
            number_rows = redshift.get_first(count_null_rows_sql) 
            self.log.info(f'Field quality check: "Field name: {checking_field}" in table name: {checking_table} - Number of NULL rows in filed: {number_rows}')

            # Check by count number of rows of all table
            count_table_rows_sql = f"SELECT Count(*) FROM {checking_table}"
            number_rows = redshift.get_first(count_table_rows_sql)
            self.log.info(f'Table quality check: Table name: {checking_table} - Number of rows: {number_rows}')

            self.log.info(f'INFO: Data quality verification finished')