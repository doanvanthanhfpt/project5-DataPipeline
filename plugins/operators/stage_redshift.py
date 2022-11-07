from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    """
    templated field that allows it to load timestamped files
    from S3 based on the execution time and run backfills
    """
    template_fields = ("s3_key",)
    """
    Parameters that using for copy sql statement
    copy from Udacity dataset to project redshift
    """
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID {}
        SECRET_ACCESS_KEY {}
        JSON '{}'
        REGION '{}'
    """

    """
    StageToRedshiftOperator's default parameters definitions
    """
    @apply_defaults
    def __init__(self,
        redshift_conn_id="",
        aws_access_key_id = "",
        aws_secret_access_key = "",
        table="",
        jsonlog_path="",
        s3_bucket="",
        s3_key="",
        dataset_format_copy="",
        region="",
        *args, **kwargs):

        """
        String variable will be used by StageToRedshiftOperator parameters
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.table = table
        self.jsonlog_path = jsonlog_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.dataset_format_copy = dataset_format_copy
        self.region = region
        
    def execute(self, context):
        """
        Connect AWS Redshift cluster with value of redshift_conn_id
        """
        self.log.info('StageToRedshiftOperator starting')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        """
        Clean existing staging tables
        """
        self.log.info('Clearing data from destination Redshift table')
        redshift_hook.run("DELETE FROM {}".format(self.table))

        """
        Prepare to copying data from Udacity's S3 to Redshift StagingTables
        """
        self.log.info("Copying data from S3 to Staging Redshift")
        if self.table == 'staging_events':
            """
            Rendering S3 path for staging_events table
            s3_path for staging_events rendered output path shoud be
            "s3://udacity-dend/log_data/2019/10/*.json"
            """
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}/".format(self.s3_bucket, rendered_key)
            # Note: s3_key = "log_data/{execution_date.year}/{execution_date.month}"

        if self.table == 'staging_songs':
            """
            Rendering S3 path for staging_songs table
            s3_path for staging_songs rendered output path should be
            "s3://udacity-dend/song_data/*.json"
            """
            rendered_key = self.s3_key
            s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)

        """
        Run the copying data from Udacity's S3 to Redshift StagingTables
        """
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.jsonlog_path,
            self.region
        )
        redshift_hook.run(formatted_sql)

        self.log.info("Done: Staging tables loaded")
