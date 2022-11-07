from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    """
    LoadDimensionOperator's default parameters definitions
    """
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_access_key_id = "",
                aws_secret_access_key = "",
                table="",
                action = 'append',
                load_dim_sql = '',
                *args, **kwargs):

        """
        String variable will be used by LoadDimensionOperator parameters
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.table = table
        self.action = action
        self.load_dim_sql = load_dim_sql

    def execute(self, context):
        """
        Connect AWS Redshift cluster with value of redshift_conn_id
        """
        self.log.info('Starting LoadDimensionOperator')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        """
        Specify the size (default = 0 byte) of the table to get current position of the dim table
        """
        if self.action == 'truncate':
            self.log.info("Truncating table {}".format(self.table))
            redshift_hook.run("TRUNCATE TABLE {}".format(self.table))

        """
        Create insert sql statement by concate string of "table" and load_dim_sql" variables
        """
        self.log.info("Loading data from StagingTables into DimTables")
        insert_table_sql = "INSERT INTO {} {}".format(self.table, self.load_dim_sql)
        redshift_hook.run(insert_table_sql)

        self.log.info("Done: Data on {} loaded.".format(self.table))
