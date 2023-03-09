import json
import boto3
import botocore
import pandas as pd
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print(event)
    if event['rawPath'] == '/': #since the only goal for this lambda function is to get data we do not need an api gateway
        BUCKET_NAME = 'jan-python-23-capstone-team3' #targeting this specific bucket
        FILE_NAME = 'earthquake_data_' + datetime.utcnow().strftime('%Y%m%d%H%M%SZ') + '.json'
        print(FILE_NAME)
        
        request_json = json.loads(event['body'])

        try:
            s3.put_object(Bucket=BUCKET_NAME, Key=FILE_NAME, Body=json.dumps(request_json)) #this uploads the file to the bucket it will replace/update the file
            return {
                'statusCode': 200,
                'body': "success"
            }
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
    elif event['rawPath'] == '/csv': #since the only goal for this lambda function is to get data we do not need an api gateway
        BUCKET_NAME = 'jan-python-23-capstone-team3' #targeting this specific bucket
        
        request_json = json.loads(event['body'])
        
        FILE_NAME = request_json['file_name'] # replace with your object key
    
        request_data = request_json['data']
        request_df = pd.read_json(request_data)
        request_csv = request_df.to_csv(index=False)

        try:
            s3.put_object(Bucket=BUCKET_NAME, Key=FILE_NAME, Body=request_csv) #this uploads the file to the bucket it will replace/update the file
            return {
                'statusCode': 200,
                'body': "success"
            }
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    return {
        'statusCode': 422,
        'body': "failure"
    }