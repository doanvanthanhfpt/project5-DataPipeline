from datetime import datetime, timedelta
import os

from Project5-DataPipeline.dags.subdag import load_dim_table_to_redshift_dag
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
# import create_tables

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
    table="staging_events",
    region="us-west-2",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    dataset_format_copy='auto',
    s3_bucket="udacity-dend",
    # s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    s3_key="log_data",
    jsonlog_path="s3://udacity-dend/log_json_path.json",
    inputdata_format="JSON",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    region='us-west-2',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    dataset_format_copy='auto',
    s3_bucket="udacity-dend",
    s3_key="song_data",
    inputdata_format="JSON",
    ignore_headers="0",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="songplays",
    table_columns="(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)",
    sql_select=SqlQueries.songplay_table_insert
)

load_userdimtable_taskid = 'load_userdimtable'
load_userdimtable = SubDagOperator(
    subdag=load_dim_table_to_redshift_dag(
        parent_dag_name=dag,
        task_id=load_userdimtable_taskid,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="public.users",
        load_dimtable_sql=SqlQueries.user_table_insert
    ),
    task_id='load_userdimtable_taskid',
    dag=dag
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)

start_operator >> create_tables_operator
create_tables_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table