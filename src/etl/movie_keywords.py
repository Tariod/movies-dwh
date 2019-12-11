from dotenv import load_dotenv
import json
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

# GET CAST FUNCTION (table: d_cast)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'keywords.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'keywords')
table = etl.rename(table, 'id', 'movie_tmdb_id')
table = etl.sub(table, 'keywords', '^\\[', '')
table = etl.sub(table, 'keywords', '\\]$', '')
table = etl.selectnotnone(table, 'keywords')
table = etl.splitdown(table, 'keywords', '(?<=\\}),\\s(?=\\{)')
table = etl.sub(table, 'keywords', '\'', '"')
table = etl.convert(table, 'keywords', lambda row: json.loads(row))
table = etl.unpackdict(table, 'keywords')
table = etl.cutout(table, 'id')

movies = etl.fromdb(conn, 'SELECT id, tmdb_id from d_movie')
movies = etl.rename(movies, 'id', 'id_movie')
table = etl.join(table, movies, lkey='movie_tmdb_id', rkey='tmdb_id')
table = etl.cutout(table, 'movie_tmdb_id')

keywords = etl.fromdb(conn, 'SELECT * from d_keyword')
keywords = etl.rename(keywords, 'id', 'id_keyword')
table = etl.join(table, keywords, key='name')
table = etl.cutout(table, 'name')

# LOAD
etl.todb(table, cursor, 'movie_keywords')
