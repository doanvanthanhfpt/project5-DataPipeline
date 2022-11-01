from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}'
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
        # self.log.info('StageToRedshiftOperator not implemented yet')
        self.log.info('StageToRedshiftOperator starting')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))

        """
        Copying data from Udacity's S3 to Staging Redshift table
        """
        self.log.info("Copying data from S3 to Staging Redshift")
        if self.table == 'staging_events':
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            # s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
            # s3_path for staging_events rendered output path like this "s3://udacity-dend/log_data/2019/10/*.json"

        if self.table == 'staging_songs':
            s3_path = "s3://{}/".format(self.s3_bucket)
            # s3_path for staging_songs rendered output path like this "s3://udacity-dend/song_data/*.json"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.dataset_format_copy,
            self.region
        )
        redshift.run(formatted_sql)

