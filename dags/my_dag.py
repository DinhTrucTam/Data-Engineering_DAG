from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
# from include.read_1 import spark_session_initialization, data_ingestion, data_ingestion_machine, data_transformation, data_load
# from src.init import create_db_conn
from airflow.decorators import dag, task

postgres_driver_jar = "/usr/local/airflow/include/postgresql-42.7.2.jar"
    
with DAG(
    dag_id='dag_da',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    spark_job_load_postgres = SparkSubmitOperator(
        task_id="spark_job_load_postgres",
        application="/usr/local/airflow/include/scripts/read_1.py", # Spark application path created in airflow and spark cluster
        name="load-postgres",
        conn_id="my_spark_conn",
        verbose=1,
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        dag=dag
    )
    
    spark_job_load_postgres