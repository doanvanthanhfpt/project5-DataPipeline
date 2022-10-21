import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
import sql
from helpers import SqlQueries


def load_dim_table_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        load_dimtable_sql,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_dim_table_task = LoadDimensionOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        load_dimtable_sql=load_dimtable_sql
    )

    load_dim_table_task

    return dag