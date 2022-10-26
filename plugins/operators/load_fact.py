from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # conn_id = your-connection-name
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                action="append",
                load_fact_sql="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.action = action
        self.load_fact_sql = load_fact_sql

    def execute(self, context):
        # self.log.info('LoadFactOperator not implemented yet')
        self.log.info('Starting LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.action == 'truncate':
            self.log.info("Truncating table {}".format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Inserting data from StagingTable into FactTable")
        insert_table_sql = "INSERT INTO {} {}".format(self.table, self.load_fact_sql)
        redshift.run(insert_table_sql)

        self.log.info("Done: Inserting data on {} loaded.".format(self.table))
