import json
import pandas as pd
import boto3
import sys
import csv
import logging
import pymysql
from io import StringIO
from db_funcs import get_rows

s3 = boto3.client('s3')

# rds settings
rds_host  = "capstone3-earthquakes.c5lobpudlayi.us-west-1.rds.amazonaws.com"
user_name = "admin"
password = "9Sj3xtFraAJCGz9GzeDT"
db_name = "earthquakes"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = pymysql.connect(host=rds_host, user=user_name, passwd=password, db=db_name, connect_timeout=5, cursorclass=pymysql.cursors.DictCursor)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")



def get_data_by_type(query_json):
    data_dict = {}
    query_df = pd.DataFrame(query_json)
    earthquake_types = query_df['type'].unique()
    location_sources = query_df['locationsource'].unique()
    
    COUNT = "count"
    OWNED_COUNT = "ownedCount"
    
    for quake_type in earthquake_types:
        data_dict[quake_type] = {}
        
        data_dict[quake_type][COUNT] = 0
        data_dict[quake_type][OWNED_COUNT] = {}
        
        for location in location_sources:
            
            df_with_quake_type = query_df.loc[(query_df['type'] == quake_type) & (query_df['locationsource'] == location)]
            
            if len(df_with_quake_type) > 0:
                
                data_dict[quake_type][OWNED_COUNT][location] = 0
                data_dict[quake_type][COUNT] += len(df_with_quake_type)
                
                df_with_quake_type['event_time'] = df_with_quake_type['event_time'].astype(str)
                df_with_quake_type['updated'] = df_with_quake_type['updated'].astype(str)
                
                df_with_quake_type['group'] = location
                df_with_quake_type['owner'] = True
                
                data_dict[quake_type][location] = df_with_quake_type.to_dict(orient='records')
    print(data_dict)
    return data_dict



def lambda_handler(event, context):
    print(event)#to check the data in event
        
    with conn.cursor() as cur:
        query = get_rows(100)
        try:
            cur.execute(query)
            query_data = cur.fetchall()
        except pymysql.err.IntegrityError as e:
            pass
        except Exception as e:
            return {
                "statusCode": 422,
                "body": json.dumps("failure")
            }
    
    data_json = get_data_by_type(query_data)
    
    print("Success")
    return {
            "statusCode": 200,
            "body": json.dumps(data_json)
            }