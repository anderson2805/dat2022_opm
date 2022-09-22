#!/usr/bin/env python
import configparser
from sqlalchemy import create_engine
import logging
from googleapiclient.discovery import build

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'


config = configparser.ConfigParser()
config.read('SETTING.ini')


WAREHOUSE = config['SNOWCONNECTOR']['WAREHOUSE']
DATABASE = config['SNOWCONNECTOR']['DATABASE']
SCHEMA = config['SNOWCONNECTOR']['SCHEMA']
user = config['SNOWCONNECTOR']['USERNAME']
password = config['SNOWCONNECTOR']['PASSWORD']
account = config['SNOWCONNECTOR']['ACCOUNT']

engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/{database}/{schema}?warehouse={warehouse}'.format(
        user= user,
        password= password,
        account_identifier= account,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
)


def create_yt_service(api_key):
    try:
        # Get credentials and create an API client
        service = build(API_SERVICE_NAME, API_VERSION, developerKey=api_key)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None
yt_api_key = config['YOUTUBE']['apikey']
yt_service = create_yt_service(yt_api_key)