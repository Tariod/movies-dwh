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

# GET LANGUAGE FUNCTION (table: d_language)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv',
                     encoding='utf8',
                     errors='ignore')

# TRANSFORMATION
table = etl.cut(movies, 'spoken_languages')
table = etl.convert(table, 'spoken_languages', str)
table = etl.splitdown(table, 'spoken_languages', '}')
table = etl.split(table, 'spoken_languages', '\'name\':',
                  ['iso_639_1', 'name'])
table = etl.split(table, 'iso_639_1', ':', ['trash', 'iso_639_1'])
table = etl.cut(table, 'name', 'iso_639_1')
table = etl.sub(table, 'name', '[\'\\]]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'iso_639_1', '[\',]', '')
table = etl.sub(table, 'iso_639_1', '[\'\\]]', '')
table = etl.sub(table, 'iso_639_1', '(^[ ]+)|[ ]+$', '')
table = etl.selectnotnone(table, 'iso_639_1')
table = etl.groupselectfirst(table, 'iso_639_1')

# LOAD
etl.todb(table, cursor, 'd_language')
