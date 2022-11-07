from datetime import datetime, timedelta
import configparser

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

config = configparser.ConfigParser()
config.read('/home/workspace/airflow/dags/pl.cfg')

AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']

default_args = {
    'owner': 'Doan_Van_Thanh',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'provide_context': True,
    'depends_on_past': False,
    'catchup' : False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('Doan_Van_Thanh',
            default_args=default_args,
            description='Load and transform data in Redshift with Airflow',
            schedule_interval='0 * * * *',
            concurrency=4, # Limit number of concurrently
            max_active_runs=2 # Limit number of active
        )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key = "log_data/{execution_date.year}/{execution_date.month}",
    region="us-west-2",
    dataset_format_copy="JSON",
    jsonlog_path="s3://udacity-dend/log_json_path.json",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag = dag,
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    redshift_conn_id = 'redshift',
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/',
    region="us-west-2",
    dataset_format_copy="JSON",
    jsonlog_path = 'auto',
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    append_only = True,
    load_fact_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = 'public.users',
    action = 'truncate',
    load_dim_sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = 'public.songs',
    action = 'truncate',
    load_dim_sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = 'public.artists',
    action = 'truncate',
    load_dim_sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = 'public.time',
    action = 'truncate',
    load_dim_sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    all_tables = ["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]
    # tables_fields = ["playid", "userid", "songid", "artistid", "start_time"]
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator  >>  [stage_events_to_redshift, \
                        stage_songs_to_redshift] \
                >>  load_songplays_table \
                >>  [load_user_dimension_table, \
                    load_song_dimension_table, \
                    load_time_dimension_table, \
                    load_artist_dimension_table] \
                >>  run_quality_checks \
                >>  end_operator
