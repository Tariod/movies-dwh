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

# GET STUDIO FUNCTION (table: d_studio)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv',
                     encoding='utf8',
                     errors='ignore')

# TRANSFORMATION
table = etl.cut(movies, 'production_companies')
table = etl.sub(table, 'production_companies', '[\\[\\]]', '')
table = etl.convert(table, 'production_companies', str)
table = etl.selectcontains(table, 'production_companies', 'name')
table = etl.splitdown(table, 'production_companies', '{')
table = etl.sub(table, 'production_companies', '{{', '{')
table = etl.split(table, 'production_companies', '\'id\':', ['name', 'id'])
table = etl.sub(table, 'id', '}', '')
table = etl.split(table, 'name', '\'name\':', ['trash', 'name'])
table = etl.sub(table, 'name', '[\'",]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.selectne(table, 'id', '')
table = etl.sub(table, 'id', ',', '')
table = etl.cut(table, 'id', 'name')
table = etl.convert(table, 'id', int)
table = etl.cut(table, 'name')
table = etl.distinct(table, 'name')

# LOAD
etl.todb(table, cursor, 'd_studio')
