from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    """
    LoadFactOperator's default parameters definitions
    """
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_access_key_id = "",
                aws_secret_access_key = "",
                table="",
                append_only="",
                load_fact_sql="",
                *args, **kwargs):

        """
        String variable will be used by LoadFactOperator parameters
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.table = table
        self.append_only = append_only
        self.load_fact_sql = load_fact_sql

    def execute(self, context):
        """
        Connect AWS Redshift cluster with value of redshift_conn_id
        """
        self.log.info('Starting LoadFactOperator')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        """
        Insert data in fresh mode, from staging tables in to fact table songplays
        """
        if not self.append_only:
            self.log.info(f"Delete {self.table} fact table")
            redshift_hook.run(f"DELETE FROM {self.table}") 
            load_fact_sql = (f"INSERT INTO {self.table} {self.load_fact_sql}")
            redshift_hook.run(load_fact_sql)

        """
        Insert data in continuous mode, from staging tables in to fact table songplays
        """
        self.log.info(f"Loading data from StagingTable {self.table} into FactTable")
        if self.append_only:
            load_fact_sql = (f"INSERT INTO {self.table} {self.load_fact_sql}")
            redshift_hook.run(load_fact_sql)

        self.log.info("Done: Data on {} loaded.".format(self.table))
