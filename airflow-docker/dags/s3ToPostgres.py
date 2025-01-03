from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

#adding scripts directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

#import functions to pull data from excel doc in S3 to Postgres
#from importFactLifts import processFactLifts
from importFactWeights import processFactWeights
from importDimRoutines import processDimRoutines
from importDimWorkouts import processDimWorkouts

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Define the DAG
with DAG('s3ToPostgresPipeline',
         default_args=default_args,
         description='A pipeline to pull data from Excel (S3) and load into Postgres tables',
         schedule_interval=None,  
         start_date=datetime(2025, 1, 10),
         catchup=False) as dag:
    
    #process fact lifts
    task1 = PythonOperator(
        task_id='processFactLifts',
        python_callable=processFactLifts,
        dag=dag,
    )

    #process fact weights
    task2 = PythonOperator(
        task_id='processFactWeights',
        python_callable=processFactWeights,
        dag=dag,
    )

    #process dim routines
    task3 = PythonOperator(
        task_id='processDimRoutines',
        python_callable=processDimRoutines,
        dag=dag,
    )

    #process dim routines
    task4 = PythonOperator(
        task_id='processDimWorkouts',
        python_callable=processDimWorkouts,
        dag=dag,
    )

    #setting task dependencies
    task1 >> task2 >> task3 >> task4


