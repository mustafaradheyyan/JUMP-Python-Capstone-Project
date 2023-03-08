import json
import pandas as pd
import boto3
import sys
import csv
import logging
import pymysql
from ETL import data_cleaning

s3 = boto3.client('s3')

# rds settings
rds_host  = "capstone3-earthquakes.c5lobpudlayi.us-west-1.rds.amazonaws.com"
user_name = "admin"
password = "9Sj3xtFraAJCGz9GzeDT"
db_name = "earthquakes"
TABLE_NAME = "seismic_event"

FILE_NAME = '/tmp/output.csv'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = pymysql.connect(host=rds_host, user=user_name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


def prep_insert_qry(colnames):
    """this query is secure as long as `colnames` contains trusted data
    standard parametrized query mechanism secures `args`"""

    binds,use = [],[]

    for colname in colnames:
        use.extend([colname,","])
        binds.extend(["%s",","])

    parts = [f"insert ignore into {TABLE_NAME} ("]
    use = use[:-1]
    binds = binds[:-1]
    
    parts.extend(use)
    parts.append(") values(")
    parts.extend(binds)
    parts.append(")")

    qry = " ".join(parts)

    return qry


def lambda_handler(event, context):
    print(event)#to check the data in event
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]
    
    csv_file = s3.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = csv_file["Body"].read()
    
    with open(FILE_NAME, 'wb') as file: 
        file.write(csv_content)
    
    csv_content_df = data_cleaning(pd.read_csv(FILE_NAME))
    csv_content_df.to_csv(FILE_NAME, index=False)
    
    with open(FILE_NAME, newline='') as file:
        reader = csv.reader(file)
        attributes_list = next(reader, None)
        csv_data = [tuple(line) for line in reader]

    with conn.cursor() as cur:
        query = prep_insert_qry(attributes_list)
        try:
            cur.fast_executemany = True
            cur.executemany(query, csv_data)
        except pymysql.err.IntegrityError as e:
            pass
        except Exception as e:
            raise e
    conn.commit()
    
    print("Success")    

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }