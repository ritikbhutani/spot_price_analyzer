import boto3
import logging

from airflow.models import DAG
from airflow.models import BaseOperator as Base

import settings

class NfsS3Operator(Base):

    def execute(self, context):
        files = context['ti'].xcom_pull(task_ids='FETCH_FROM_BOTO')
        logging.info('Connecting to S3...')

        s3 = boto3.client('s3', 
            aws_access_key_id = settings.aws_access_key_id,
            aws_secret_access_key = settings.aws_secret_access_key)

        logging.info('Uploading files...')
        for file in files:
            s3.upload_file(file, 'spot-prices-stage-bucket', f'prices/{file}')
            logging.info(f'{file} uploaded successfully')