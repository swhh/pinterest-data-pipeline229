
import json
import random
import requests
from time import sleep
from sqlalchemy import text
from db_connection import AWSDBConnector

USER_ID = '0afff2eeb7e3'
INVOKE_URL = 'https://46wmghohz3.execute-api.us-east-1.amazonaws.com/dev/topics/{topic_name}'
TOPIC_NAMES = [f'{USER_ID}.{topic_end}' for topic_end in ['pin', 'geo', 'user']]
TABLE_NAMES = [f'{table_affix}_data' for table_affix in ['pinterest', 'geolocation', 'user']]
HEADERS = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
random.seed(100)


new_connector = AWSDBConnector()


def generate_row(connection, table, random_row):
    """Fetch randomrow from table with db connection and convert to JSON dict"""
    string = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
    selected_row = connection.execute(string)           
    for row in selected_row:
        print(dict(row._mapping))
        return dict(row._mapping)


def run_infinite_post_data_loop():
    """Fetches Pinterest data records from AWS RDS data and simulates stream via Python generator"""
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            results = (generate_row(connection, table, random_row) for table in TABLE_NAMES)          
            yield results


def post_record_to_kafka(data, topic_name):
    """Posts Pinterest record represented as data to Kafka topic topic_name"""
    payload = json.dumps({
    "records": [
        {     
        "value": data
        }
    ]
    }, default=str)
    invoke_url = INVOKE_URL.format(topic_name=topic_name)
    requests.request("POST", invoke_url, headers=HEADERS, data=payload)
    

def post_records_to_kafka():
    """Streams Pinterest data to Kafka"""
    gen = run_infinite_post_data_loop()
    for pin_result, geo_result, user_result in gen:
        post_record_to_kafka(pin_result, TOPIC_NAMES[0])
        post_record_to_kafka(geo_result, TOPIC_NAMES[1])
        post_record_to_kafka(user_result, TOPIC_NAMES[2])

if __name__ == "__main__":
    post_records_to_kafka()
    
    
    


