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

# GET PEOPLE FUNCTION (table: d_people)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'crew')
table = etl.selectcontains(table, 'crew', 'name')
table = etl.splitdown(table, 'crew', '}')
table = etl.split(table, 'crew', '\'name\':', ['job', 'trash'])
table = etl.split(table, 'job', '\'job\':', ['info', 'job'])
table = etl.cut(table, 'job')
table = etl.sub(table, 'job', '[\'\\],"]', '')
table = etl.sub(table, 'job', '(^[ ]+)|([ ]+$)', '')
table = etl.selectnotnone(table, 'job')
table = etl.groupselectfirst(table, 'job')
table = etl.rename(table, 'job', 'name')

# LOAD
etl.todb(table, cursor, 'd_job')
