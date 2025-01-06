from airflow import DAG # type: ignore
from airflow.operators.bash_operator import BashOperator # type: ignore
from datetime import datetime, timedelta
from airflow.decorators import task, dag # type: ignore

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'catchup': False,
}

# Define the DAG
with DAG('stagingToProdPipeline',
         default_args=default_args,
         description='A pipeline to pull data from snowflake staging tables and load into snowflake prod tables',
         schedule_interval=None,  
         start_date=datetime(2025, 1, 1),
         catchup=False) as dag:
    
    #create dbt task 1
    dbttask1 = BashOperator(
        task_id='dbttask1',
        bash_command='cd /opt/airflow/scripts/dbtStagingToProd && dbt run --models factLifts',
        dag=dag,
    )

    #create dbt task 2
    dbttask2 = BashOperator(
        task_id='dbttask2',
        bash_command='cd /opt/airflow/scripts/dbtStagingToProd && dbt run --models factWeights',
        dag=dag,
    )

    #create dbt task 3
    dbttask3 = BashOperator(
        task_id='dbttask3',
        bash_command='cd /opt/airflow/scripts/dbtStagingToProd && dbt run --models dimRoutines',
        dag=dag,
    )

    #create dbt task 4
    dbttask4 = BashOperator(
        task_id='dbttask4',
        bash_command='cd /opt/airflow/scripts/dbtStagingToProd && dbt run --models dimWorkouts',
        dag=dag,
    )

    #create dbt task 5
    dbttask5 = BashOperator(
        task_id='dbttask5',
        bash_command='cd /opt/airflow/scripts/dbtStagingToProd && dbt run --models dimUsers',
        dag=dag,
    )

    #create dbt task 6
    dbttask6 = BashOperator(
        task_id='dbttask6',
        bash_command='cd /opt/airflow/scripts/dbtStagingToProd && dbt run --models dimMovements',
        dag=dag,
    )

    #set dependencies
    dbttask1 >> dbttask2 >> dbttask3 >> dbttask4 >> dbttask5 >> dbttask6