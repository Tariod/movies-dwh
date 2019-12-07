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

# GET GENRE FUNCTION (table: d_genre)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv',
                     encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'genres')
table = etl.splitdown(table, 'genres', '}')
table = etl.sub(table, 'genres', ',', '')
table = etl.split(table, 'genres', '\'name\':', ['id', 'title'])
table = etl.split(table, 'id', ':', ['trash', 'id'])
table = etl.sub(table, 'title', '\'', '')
table = etl.cut(table, 'id', 'title')
table = etl.selectnotnone(table, 'id')
table = etl.groupselectfirst(table, key='title')
table = etl.cut(table, 'title')
table = etl.sub(table, 'title', '(^[ ]+)|[ ]+$', '')

# LOAD
etl.todb(table, cursor, 'd_genre')
