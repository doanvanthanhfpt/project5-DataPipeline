from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    """
    DataQualityOperator's default parameters definitions
    """
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_access_key_id = "",
                aws_secret_access_key = "",
                all_tables="",
                *args, **kwargs):

        """
        String variable will be used by DataQualityOperator parameters
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.all_tables = all_tables

    def execute(self, context):
        """
        Connect AWS Redshift cluster with value of redshift_conn_id
        """
        self.log.info('Starting DataQualityOperator implementation')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.all_tables:
            """
            Check by count number of rows of all tables
            """
            count_table_rows_sql = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(count_table_rows_sql) < 1 or len(count_table_rows_sql[0]) < 1 or count_table_rows_sql[0][0] < 1:
                self.log.info(f'Data quality check has been failed. Empty {table} returned.')
                raise ValueError(f'Data quality check has been failed. Empty {table} returned.')

            self.log.info(f'DONE: Data quality verification finished')