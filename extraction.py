import json
import boto3
import psycopg2
import csv
import os
from io import StringIO

# ENVIRONMENT VARIABLES
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # Get bucket and file info
    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = event['Records'][0]['s3']['object']['key']
    
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response['Body'].read().decode('utf-8')
    
    # Assume CSV
    csv_reader = csv.reader(StringIO(body))
    headers = next(csv_reader)  # Skip header

    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    
    for row in csv_reader:
        cur.execute("INSERT INTO staging_table (col1, col2, col3) VALUES (%s, %s, %s)", row)

    conn.commit()
    cur.close()
    conn.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully loaded {key} into staging table.')
    }
