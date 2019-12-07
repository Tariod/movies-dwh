from datetime import datetime
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

# GET DATE FUNCTION (table: d_date)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv', encoding='utf8')
users = etl.fromcsv(DATA_SOURCE_DIR + 'ratings.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'release_date')
table = etl.rename(table, 'release_date', 'date')
table = etl.groupselectfirst(table, 'date')
table = etl.search(table, 'date',
                   '[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])')
dates = etl.cut(users, 'timestamp')
dates = etl.convert(dates, 'timestamp',
                    lambda v: datetime
                    .fromtimestamp(float(v) / 1000)
                    .strftime('%Y-%m-%d'))
dates = etl.distinct(dates)
dates = etl.rename(dates, 'timestamp', 'date')
dates = etl.antijoin(dates, table, key='date')

# LOAD
etl.todb(table, cursor, 'd_date')
etl.appenddb(dates, cursor, 'd_date')
