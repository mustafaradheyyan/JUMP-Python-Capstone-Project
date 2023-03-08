import json
import pandas as pd
import boto3
import sys
import csv
import logging
import pymysql

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


def prep_insert_qry(args, colnames):
    """this query is secure as long as `colnames` contains trusted data
    standard parametrized query mechanism secures `args`"""

    binds,use = [],[]

    for colname, value in zip(colnames,args):
        if value is not None:
            use.extend([colname,","])
            binds.extend(["%s",","])


    parts = [f"insert into {TABLE_NAME} ("]
    use = use[:-1]
    binds = binds[:-1]
    
    parts.extend(use)
    parts.append(") values(")
    parts.extend(binds)
    parts.append(")")

    qry = " ".join(parts)

    return qry, tuple([v for v in args if not v is None])


def lambda_handler(event, context):
    print(event)#to check the data in event
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]
    
    csv_file = s3.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = csv_file["Body"].read()
    
    with open(FILE_NAME, 'wb') as file: 
        file.write(csv_content)
    
    with open(FILE_NAME, newline='') as file:
        reader = csv.reader(file)
        next(reader, None)
        csv_data = list(reader)
    
    attributes_list = [
        "event_time", "latitude", "longitude", "depth", "mag", "magtype",
        "nst", "gap", "dmin", "rms", "net", "id", "updated", "type", "horizontalerror",
        "deptherror", "magerror", "magnst", "locationsource", "magsource"
    ]
    
    count = 0
    duplicateCount = 0
    
    for row in csv_data:
        query, data = prep_insert_qry(row, attributes_list)
        with conn.cursor() as cur:
            try:
                cur.execute(query, data)
                count += 1
            except pymysql.err.IntegrityError:
                duplicateCount += 1
            except Exception as e:
                raise e
        conn.commit()
    
    print(f"Added {count} items to RDS MySQL table out of {count + duplicateCount} attempted")
    
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }