import boto3
import json
import os
import logging
import datetime
import pandas as pd

from airflow.models.baseoperator import BaseOperator as Base

import settings

class BotoNFSOperator(Base):
    
    def execute(self, context):
        today = context['execution_date'].strftime('%y%m%d')
        regions = self.get_regions()
        for region in regions:
            price_df = self.get_spot_price(region, context)
            price_df.to_csv(f'{region}_{today}.csv', index=False)

    def get_regions(self):
        logging.info('Fetching all region names')
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
        today_str = today.strftime('%y%m%d')
        kwargs = {}
        df = pd.DataFrame()
        n = 1

        logging.info(f'Fetching for region {region} for date {today_str}')

        while True:
            response = ec2.describe_spot_price_history(
                StartTime = today,
                EndTime = today + datetime.timedelta(days = 1),
                **kwargs
            )
            df = df.append(pd.DataFrame(response['SpotPriceHistory']))
            n += 1

            if not response['NextToken']:
                break 

            kwargs['NextToken'] = response['NextToken']

        return df