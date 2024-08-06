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
USER_ID = '0afff2eeb7e3'
INVOKE_URL = 'https://46wmghohz3.execute-api.us-east-1.amazonaws.com/dev/topics/{topic_name}'
TOPIC_NAMES = [f'{USER_ID}.{topic_end}' for topic_end in ['pin', 'geo', 'user']]
HEADERS = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
PIN_EXAMPLE = {'index': 75285, 'unique_id': 'fbe53c66-3442-4773-b19e-d3ec6f54dddf', 'title': 'No tittle Data Available', 'description': 'No description available Story format', 'poster_name': 'User Info Error', 'follower_count': 'User Info Error', 'tag_list': 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'is_image_or_video': 'multi-video(story page format)', 'image_src': 'Image src error.', 'downloaded': 0, 'save_location': 'Local save in /data/mens-fashion', 'category': 'mens-fashion'}
GEO_EXAMPLE = {'ind': 7528, 'timestamp': datetime.datetime(2020, 8, 28, 3, 52, 47), 'latitude': -89.9787, 'longitude': -173.293, 'country': 'Albania'}
USER_EXAMPLE = {'ind': 7528, 'first_name': 'Abigail', 'last_name': 'Ali', 'age': 20, 'date_joined': datetime.datetime(2015, 10, 24, 11, 23, 51)}
random.seed(100)


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


def run_infinite_post_data_loop():
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

            post_record_to_kafka(pin_result, TOPIC_NAMES[0])
            post_record_to_kafka(geo_result, TOPIC_NAMES[1])
            post_record_to_kafka(user_result, TOPIC_NAMES[2])
            
            print(pin_result)
            print(geo_result)
            print(user_result)


def post_record_to_kafka(data, topic_name):
    payload = json.dumps({
    "records": [
        {
        #Data should be send as pairs of column_name:value, with different columns separated by commas       
        "value": data
        }
    ]
}, default=str)
    invoke_url = INVOKE_URL.format(topic_name=topic_name)
    print(invoke_url)
    response = requests.request("POST", invoke_url, headers=HEADERS, data=payload)
    print(response.status_code)
    

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    
    


