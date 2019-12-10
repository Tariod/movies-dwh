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
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'production_companies')
table = etl.rename(table, 'id', 'movie_tmdb_id')
table = etl.sub(table, 'production_companies', '^\\[', '')
table = etl.sub(table, 'production_companies', '\\]$', '')
table = etl.selectnotnone(table, 'production_companies')
table = etl.splitdown(table, 'production_companies', '(?<=\\}),\\s(?=\\{)')
table = etl.sub(table, 'production_companies', '\'', '"')
table = etl.convert(table, 'production_companies', lambda row: json.loads(row))
table = etl.unpackdict(table, 'production_companies')
table = etl.cutout(table, 'id')

movies = etl.fromdb(conn, 'SELECT id, tmdb_id from d_movie')
movies = etl.rename(movies, 'id', 'id_movie')
table = etl.join(table, movies, lkey='movie_tmdb_id', rkey='tmdb_id')
table = etl.cutout(table, 'movie_tmdb_id')

studios = etl.fromdb(conn, 'SELECT * from d_studio')
studios = etl.rename(studios, 'id', 'id_studio')
table = etl.join(table, studios, key='name')
table = etl.cutout(table, 'name')

# # LOAD
etl.todb(table, cursor, 'production_studios')
