from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

#adding scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../scripts'))

#import functions to pull data from excel doc in S3 to Postgres
from importFactLifts import processFactLifts