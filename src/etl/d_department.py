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
table = etl.split(table, 'crew', '\'gender\':', ['department', 'trash'])
table = etl.split(table, 'department', '\'department\':',
                  ['info', 'department'])
table = etl.cut(table, 'department')
table = etl.sub(table, 'department', '[\'\\],]', '')
table = etl.sub(table, 'department', '(^[ ]+)|([ ]+$)', '')
table = etl.selectnotnone(table, 'department')
table = etl.groupselectfirst(table, 'department')
table = etl.rename(table, 'department', 'name')

# LOAD
etl.todb(table, cursor, 'd_department')
