from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                table = '',
                redshift_conn_id = '',
                sql = '',
                action = 'append',
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id,
        # self.aws_credentials_id = aws_credentials_id,
        self.table = table,
        self.action = action,
        self.sql = sql,

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        self.log.info('Starting LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.action == 'truncate':
            self.log.info("Truncating table {}".format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Inserting data from StagingTables into DimTables")
        insert_dim_table_sql = "INSERT INTO {} {}".format(self.table, self.sql)
        redshift.run(insert_dim_table_sql)

        self.log.info("Done: Inserting data on {}, {} loaded.".format(self.table))
