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

# GET RELEASE_STATUS FUNCTION (table: d_release_status)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'status')
table = etl.groupselectfirst(table, 'status')
table = etl.selectne(table, 'status', '')
table = etl.replace(table, 'status', None, 'Unknown')

values = etl.data(table)
valuesNew = []
vals = list(range(1, etl.nrows(table) + 1))
flat_list = [list(sublist) for sublist in values]
table = [flat_list, vals]
table = etl.fromcolumns(table)
table = etl.unpack(table, 'f0', ['name'])
table = etl.rename(table, 'f1', 'id')

# LOAD
etl.todb(table, cursor, 'd_release_status')
