import json
import boto3
import pandas as pd

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    print(event) #this is to print the payload that the lambda function is getting
    
    if event['rawPath'] == '/': #since the only goal for this lambda function is to get data we do not need an api gateway
        BUCKET_NAME = 'jan-python-23-capstone-team3' #targeting this specific bucket
        FILE_NAME = 'OUTPUT.csv' # replace with your object key
        LOCAL_FILENAME = '/tmp/output.csv'
    
        request_data = event['body']
        request_df = pd.read_json(request_data)
        csv_of_data = request_df.to_csv(LOCAL_FILENAME, index=False)

        try:
            with open(LOCAL_FILENAME) as f:
                string = f.read()
                encoded_string = string.encode("utf-8")
            s3.put_object(Bucket=BUCKET_NAME, Key=FILE_NAME, Body=encoded_string) #this uploads the file to the bucket it will replace/update the file
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