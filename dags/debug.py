from argparse import Action
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# from subdag import load_dim_table_to_redshift_dag
# from airflow.operators.subdag_operator import SubDagOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 2, 12),
    'depends_on_past': False,
    'catchup' : False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
            default_args=default_args,
            description='Load and transform data in Redshift with Airflow',
            schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

create_tables_operator = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    dataset_format_copy="",
    jsonlog_path="log_data/log_json_path.json",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region='us-west-2',
    dataset_format_copy="",
    jsonlog_path="auto",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.songplays",
    action="append",
    # table_columns="(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)",
    load_fact_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    # aws_credentials_id="aws_credentials",
    table = 'public.users',
    action = 'truncate',
    sql=SqlQueries.user_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    all_tables = {'table_field':['public.songplays', 'playid'], \
                    'table_field':['public.users', 'userid'],\
                    'table_field':['public.songs', 'songid'],\
                    'table_field':['public.artists', 'artistid'],\
                    'table_field':['public.time', 'start_time']\
    }
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator  >>  create_tables_operator \
                >> [stage_events_to_redshift, \
                        stage_songs_to_redshift] \
                >>  load_songplays_table \
                >>  [load_user_dimension_table] \
                >>  run_quality_checks \
                >>  end_operator
