from dotenv import load_dotenv
import os
import petl as etl
import psycopg2

load_dotenv()

conn = psycopg2.connect(dbname=os.getenv('DB_NAME'),
                        user=os.getenv('DB_USER'),
                        password=os.getenv('DB_PASSWORD'),
                        host=os.getenv('DB_HOST'),
                        port=os.getenv('DB_PORT'))
cursor = conn.cursor()

# GET USER FUNCTION (table: d_user)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
ratings = etl.fromcsv(DATA_SOURCE_DIR + 'ratings.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(ratings, 'userId')
table = etl.convert(table, 'userId', str)
table = etl.selectnotnone(table, 'userId')
table = etl.distinct(table, 'userId')
table = etl.rename(table, 'userId', 'abstract_name')

# LOAD
etl.todb(table, cursor, 'd_user')
