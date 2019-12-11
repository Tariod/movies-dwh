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

# GET COUNTRYFUNCTION (table: d_country)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv',
                     encoding='utf8',
                     errors='ignore')

# TRANSFORMATION
table = etl.cut(movies, 'production_countries')
table = etl.convert(table, 'production_countries', str)
table = etl.splitdown(table, 'production_countries', '}')
table = etl.split(table, 'production_countries', '\'name\':',
                  ['iso_3166_1', 'name'])
table = etl.split(table, 'iso_3166_1', ':', ['trash', 'iso_3166_1'])
table = etl.cut(table, 'name', 'iso_3166_1')
table = etl.sub(table, 'name', '[\'\\]]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'iso_3166_1', '[\',]', '')
table = etl.sub(table, 'iso_3166_1', '[\'\\]]', '')
table = etl.sub(table, 'iso_3166_1', '(^[ ]+)|[ ]+$', '')
table = etl.selectnotnone(table, 'iso_3166_1')
table = etl.sub(table, 'name', '"', '')
table = etl.groupselectfirst(table, 'name')

# LOAD
etl.todb(table, cursor, 'd_country')
