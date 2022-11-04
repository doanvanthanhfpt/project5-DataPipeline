from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_access_key_id = "",
                aws_secret_access_key = "",
                table="",
                action = 'append',
                load_dim_sql = '',
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.table = table
        self.action = action
        self.load_dim_sql = load_dim_sql

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        self.log.info('Starting LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.action == 'truncate':
            self.log.info("Truncating table {}".format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Loading data from StagingTables into DimTables")
        insert_table_sql = "INSERT INTO {} {}".format(self.table, self.load_dim_sql)
        redshift.run(insert_table_sql)

        self.log.info("Done: Data on {} loaded.".format(self.table))
