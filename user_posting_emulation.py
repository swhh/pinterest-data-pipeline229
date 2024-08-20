
import json
import requests
import random
from time import sleep
import sqlalchemy
from sqlalchemy import text
import yaml


DB_CREDS = 'db_creds.yaml'
USER_ID = '0afff2eeb7e3'
INVOKE_URL = 'https://46wmghohz3.execute-api.us-east-1.amazonaws.com/dev/topics/{topic_name}'
TOPIC_NAMES = [f'{USER_ID}.{topic_end}' for topic_end in ['pin', 'geo', 'user']]
HEADERS = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
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
            
            yield pin_result, geo_result, user_result



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
    requests.request("POST", invoke_url, headers=HEADERS, data=payload)
    

def post_records_to_kafka():
    gen = run_infinite_post_data_loop()
    for pin_result, geo_result, user_result in gen:
        post_record_to_kafka(pin_result, TOPIC_NAMES[0])
        post_record_to_kafka(geo_result, TOPIC_NAMES[1])
        post_record_to_kafka(user_result, TOPIC_NAMES[2])

if __name__ == "__main__":
    post_records_to_kafka()
    
    
    


