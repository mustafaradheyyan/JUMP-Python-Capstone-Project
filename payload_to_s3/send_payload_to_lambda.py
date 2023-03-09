import json
import math
import zipfile
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATA_CLEANING_PATH = 'data_cleaning'
DATA_TYPE = 'Global_Earthquake_Data.csv'
FILE_PATH = f'{DATA_CLEANING_PATH}\{DATA_TYPE}'
COMPRESSED_FILE_PATH = f'{DATA_CLEANING_PATH}\{DATA_TYPE}.zip'

URL = "https://mfcp436giirb7jwlvgizni2w7m0agrmc.lambda-url.us-west-1.on.aws/"
ROWS_PER_PAYLOAD = 2500

def unzip_zip_file():
    with zipfile.ZipFile(COMPRESSED_FILE_PATH, 'r') as zip_ref:
        zip_ref.extractall(DATA_CLEANING_PATH)

def initialize_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount(URL, adapter)
    return session


def get_file_name(num, rows=ROWS_PER_PAYLOAD) -> str:
    return f"Global_Earthquake_Data_{rows}_part{num}.csv"


def send_payload(payload_df: pd.DataFrame):
    i = 0
    num = 1
    
    rows_in_df = len(payload_df)
    row_per_payload_limit = ((math.floor(rows_in_df / ROWS_PER_PAYLOAD)) * ROWS_PER_PAYLOAD)
    
    while i < row_per_payload_limit:
        yield get_file_name(num), payload_df.iloc[i : i + ROWS_PER_PAYLOAD]
        num += 1
        i += ROWS_PER_PAYLOAD
    else:
        yield get_file_name(num, rows_in_df - i), payload_df.iloc[i:]
        
    
def main():
    session = initialize_session()
    
    unzip_zip_file()
    
    earthquake_df = pd.read_csv(FILE_PATH)
    payload_df_generator = send_payload(earthquake_df)
    
    for file_name, payload_df in payload_df_generator:
        payload = {"file_name": file_name, "data": payload_df.to_json()}
        r = session.post(url=URL, data=json.dumps(payload), headers={'Content-type': 'application/json'})
        print(r)

if __name__ == "__main__":
    main()