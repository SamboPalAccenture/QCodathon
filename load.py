import psycopg2
import boto3
import os
from io import StringIO

# ENVIRONMENT VARIABLES
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DEST_BUCKET = os.environ['DEST_BUCKET']
DEST_KEY = os.environ['DEST_KEY']

def export_to_s3():
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()

    output = StringIO()
    cur.copy_expert("COPY (SELECT * FROM target_table) TO STDOUT WITH CSV HEADER", output)
    output.seek(0)

    s3 = boto3.client('s3')
    s3.put_object(Bucket=DEST_BUCKET, Key=DEST_KEY, Body=output.getvalue())

    cur.close()
    conn.close()
    print(f"Data exported to s3://{DEST_BUCKET}/{DEST_KEY}")

if __name__ == "__main__":
    export_to_s3()
