import json
import requests
from user_posting_emulation import run_infinite_post_data_loop


INVOKE_URL = "https://46wmghohz3.execute-api.us-east-1.amazonaws.com/dev/streams/{stream_name}/record"
HEADERS = {'Content-Type': 'application/json'}
USER_ID = '0afff2eeb7e3'
STREAM_NAMES = [f'streaming-{USER_ID}-{stream_end}' for stream_end in ['pin', 'geo', 'user']]
PARTITION_KEY = 'partition-1'


def post_records_to_kinesis():
    gen = run_infinite_post_data_loop()
    for pin_result, geo_result, user_result in gen:
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

if __name__ == "__main__":
    post_records_to_kinesis()