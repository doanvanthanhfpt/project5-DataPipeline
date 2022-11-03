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
config.read('/home/workspace/airflow/dags/pl.cfg')

AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'provide_context': True,
    'depends_on_past': False,
    'catchup' : False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
            default_args=default_args,
            description='Load and transform data in Redshift with Airflow',
            schedule_interval='0 * * * *',
            concurrency=4, 
            max_active_runs=2
        )
# Limit number of active and concurrently across all active runs
# dag = DAG('example2', concurrency=10, max_active_runs=2)

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
    jsonlog_path="log_json_path.json",
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
