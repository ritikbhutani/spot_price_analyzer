import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.dummy_operator import DummyOperator
from operators.boto_nfs import BotoNFSOperator

default_args = {
    'owner': 'ritik-bhutani',
    'depends_on_past': False,
    'email': 'code.rb9@gmail.com',
    'email_on_failure': 'code.rb9@gmail.com',
    'email_on_retry': 'code.rb9@gmail.com',
    'retries': 3
}

with DAG('pipeline', 
        default_args = default_args, 
        schedule_interval = '@daily',
        start_date = datetime.datetime.now(),
        tags = ['boto', 's3', 'snf']) as dag:

    fetch_prices = BotoNFSOperator(task_id = 'FETCH')

    upload_prices = DummyOperator(task_id = 'UPLOAD')

    fetch_prices >> upload_prices