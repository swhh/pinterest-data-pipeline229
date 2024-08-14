import datetime
import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
import requests


DB_CREDS = 'db_creds.yaml'
INVOKE_URL = "https://46wmghohz3.execute-api.us-east-1.amazonaws.com/dev/streams/{stream_name}/record"
HEADERS = {'Content-Type': 'application/json'}
USER_ID = '0afff2eeb7e3'
STREAM_NAMES = [f'streaming-{USER_ID}-{stream_end}' for stream_end in ['pin', 'geo', 'user']]
PARTITION_KEY = 'partition-1'



def read_db_creds(yaml_file: str):
    """Read dc_creds.yaml and return creds dictionary"""
    with open(yaml_file) as f:
        creds_dict = yaml.safe_load(f)
    return creds_dict


class AWSDBConnector:

    def __init__(self):
        creds_dict = read_db_creds(DB_CREDS)
        self.HOST = creds_dict['HOST']
        self.USER = creds_dict['USER']
        self.PASSWORD = creds_dict['PASSWORD']
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_kinesis_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)

            post_record_to_kinesis(pin_result, STREAM_NAMES[0])
            post_record_to_kinesis(geo_result, STREAM_NAMES[1])
            post_record_to_kinesis(user_result, STREAM_NAMES[2])
            


def post_record_to_kinesis(data, stream_name):
    payload = json.dumps({
    "StreamName": stream_name,
    "Data": data,
            "PartitionKey": PARTITION_KEY
            }, default=str)
    invoke_url = INVOKE_URL.format(stream_name=stream_name)
    response = requests.request("PUT", invoke_url, headers=HEADERS, data=payload)
    print(response.status_code)

if __name__ == "__main__":
    run_infinite_post_kinesis_data_loop()
    print('Working')