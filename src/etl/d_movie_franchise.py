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

# GET MOVIE_FRANCHISE FUNCTION (table: d_movie_franchise)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'belongs_to_collection')
table = etl.split(table, 'belongs_to_collection', 'poster_path',
                  ['info', 'trash'])
table = etl.cut(table, 'info')
table = etl.split(table, 'info', '\'name\':', ['id', 'name'])
table = etl.split(table, 'id', ':', ['trash', 'id'])
table = etl.sub(table, 'name', '[\'",]', '')
table = etl.sub(table, 'id', '[\',]', '')
table = etl.cut(table, 'id', 'name')
table = etl.groupselectfirst(table, 'id')
table = etl.selectnotnone(table, 'id')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.cut(table, 'name')

# LOAD
etl.todb(table, cursor, 'd_movie_franchise')
