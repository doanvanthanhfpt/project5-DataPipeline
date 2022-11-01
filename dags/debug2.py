from argparse import Action
from datetime import datetime, timedelta
import os
import configparser

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

config = configparser.ConfigParser()
config.read('pl.cfg')

AWS_ACCESS_KEY_ID = 'aws_access_key'
AWS_SECRET_ACCESS_KEY = 'aws_secret_key'

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

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key = "log_data/{execution.year}/{execution.month}",
    region="us-west-2",
    dataset_format_copy="JSON",
    jsonlog_path="log_data/log_json_path.json", # s3_json mapped
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
    provide_context = True)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator  >>  [stage_events_to_redshift, \
                        stage_songs_to_redshift] \
                >>  end_operator
