from airflow import DAG  # type: ignore
from airflow.operators.python_operator import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import datetime, timedelta
import boto3
import time

#create args
default_args={
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'catchup': False,
}

#create function to trigger glue jobs
def triggerGlueJob(jobname):
    client = boto3.client('glue',region_name='us-east-2')
    response = client.start_job_run(JobName=jobname)
    print(f'triggered glue job {jobname}')

#temp solution until glue sensor is built
def waitminutes():
    time.sleep(180)

#define the dag
with DAG('glueToSnowflakePipeline',
         default_args=default_args,
         description='Extract parquet data from S3 and load it to snowflake staging tables',
         schedule_interval=None,  
         start_date=datetime(2025, 1, 1),
         catchup=False) as dag:
    
    #glue job for fact lifts
    gluejob1 = PythonOperator(
        task_id='triggerGlueJob1',
        python_callable=triggerGlueJob,
        op_args=['fact lifts to snowflake staging'],
        dag=dag,
    )

    #glue job for fact lifts
    gluejob2 = PythonOperator(
        task_id='triggerGlueJob2',
        python_callable=triggerGlueJob,
        op_args=['fact weights to snowflake staging'],
        dag=dag,
    )
    
    #glue job for fact lifts
    gluejob3 = PythonOperator(
        task_id='triggerGlueJob3',
        python_callable=triggerGlueJob,
        op_args=['dim routines to snowflake staging'],
        dag=dag,
    )

    #glue job for fact lifts
    gluejob4 = PythonOperator(
        task_id='triggerGlueJob4',
        python_callable=triggerGlueJob,
        op_args=['dim workouts to snowflake staging'],
        dag=dag,
    )

    #pause for next job
    wait1 = PythonOperator(
        task_id='wait1',
        python_callable=waitminutes,
        dag=dag,
    )

    #trigger postgres to parquet dag
    triggerStagingtoProdDAG = TriggerDagRunOperator(
        task_id='triggerStagingtoProdDAG',
        trigger_dag_id='stagingToProdPipeline',
    )
    
    gluejob1 >>  gluejob2 >> gluejob3 >> gluejob4 >> wait1 >> triggerStagingtoProdDAG
