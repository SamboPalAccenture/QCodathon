import psycopg2
import os

# ENVIRONMENT VARIABLES
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']

def transform_data():
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()

    # Example: Insert with transformation logic
    cur.execute("""
        INSERT INTO target_table (id, name_upper, amount_usd)
        SELECT id, UPPER(name), amount * 1.1
        FROM staging_table
        WHERE processed = FALSE
    """)

    # Mark rows as processed
    cur.execute("UPDATE staging_table SET processed = TRUE WHERE processed = FALSE")
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    transform_data()
