from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import timedelta, datetime
import sys
import os

#adding scripts directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

from parquetFactLifts import parquetFactLifts # type: ignore
from parquetFactWeights import parquetFactWeights # type: ignore
from parquetDimRoutines import parquetDimRoutines # type: ignore
from parquetDimWorkouts import parquetDimWorkouts # type: ignore

#create args
default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'catchup': False,
}

# Define the DAG
with DAG('postgresToS3Pipeline',
         default_args=default_args,
         description='Extract data from Postgres and load it to S3 in Parquet format',
         schedule_interval=None,  
         start_date=datetime(2025, 1, 1),
         catchup=False) as dag:

    #parquet fact lifts
    task1 = PythonOperator(
        task_id='parquetFactLifts',
        python_callable=parquetFactLifts,
        dag=dag
    )

    #parquet fact weights
    task2 = PythonOperator(
        task_id='parquetFactWeights',
        python_callable=parquetFactWeights,
        dag=dag
    )

    #parquet dim routines
    task3 = PythonOperator(
        task_id='parquetDimRoutines',
        python_callable=parquetDimRoutines,
        dag=dag
    )

    #parquet dim workouts
    task4 = PythonOperator(
        task_id='parquetDimWorkouts',
        python_callable=parquetDimWorkouts,
        dag=dag
    )

    #setting task dependencies
    task1 >> task2 >> task3 >> task4