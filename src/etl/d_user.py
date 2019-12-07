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
movies = etl.fromcsv(DATA_SOURCE_DIR + 'ratings.csv', encoding='utf8')

# TRANSFORMATION
userids = list(range(1, 10657))
vals = list('u' + str(x) for x in range(1, 10657))

table = [userids, vals]
table = etl.fromcolumns(table)
table = etl.rename(table, 'f0', 'id')
table = etl.rename(table, 'f1', 'abstract_name')

# LOAD
etl.todb(table, cursor, 'd_user')
