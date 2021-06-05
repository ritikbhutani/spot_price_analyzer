import boto3
import json
import datetime
import pandas as pd

from airflow.models.baseoperator import BaseOperator as Base
from airflow.hooks.S3_hook import S3Hook

import settings

class BotoS3Operator(Base):
    def execute(self, context):
        regions = self.get_regions()
        [self.get_spot_price(region, context) for region in regions]

    def get_regions(self):
        print(settings.aws_access_key_id)
        ec2 = boto3.client('ec2', 
            region_name = 'ap-south-1',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key)
        response = ec2.describe_regions()
        return [region['RegionName'] for region in response['Regions']]

    def get_spot_price(self, region, context):
        ec2 = boto3.client('ec2', 
            region_name = 'ap-south-1',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key)

        today = context['execution_date']
        kwargs = {}
        df = pd.DataFrame()
        n = 1

        logging.info(f'Fetching for region {region} for date {today}')

        while True:
            response = ec2.describe_spot_price_history(
                StartTime = today,
                EndTime = today + datetime.timedelta(days = 1),
                **kwargs
            )
            print('Update {} fetched'.format(n))
            df = df.append(pd.DataFrame(response['SpotPriceHistory']))
            
            print('Update {} done'.format(n))
            print(kwargs)
            n += 1

            if not response['NextToken']:
                break 

            kwargs['NextToken'] = response['NextToken']

        df.to_csv('test.csv')