from calendar import c
import os

from datetime import datetime
from asyncio import tasks
from telnetlib import OUTMRK
from airflow import DAG


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 2 * *", 
    start_date=datetime(2021, 1, 1)
)

#url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
# URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data' 
# URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/nytrips_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
# URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data' 
# URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
# OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
# TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_taxi_trips{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_trips{{ execution_date.strftime(\'%Y_%m\') }}'

with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        # bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
        bash_command=f'curl -sSL {url} > {AIRFLOW_HOME}/taxidata.parquet'
    )

    ingest_task = PythonOperator( 
        task_id='ingest',
        python_callable=ingest_callable,
         op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            filename=AIRFLOW_HOME + '/taxidata.parquet',
        ),
          
    )
    list_task = BashOperator(
        task_id='list',
        bash_command=f'ls {AIRFLOW_HOME}',
    )

    wget_task >> list_task >> ingest_task  