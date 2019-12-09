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
table = etl.cut(movies, 'id', 'spoken_languages')
table = etl.rename(table, 'id', 'movie_tmdb_id')
table = etl.sub(table, 'spoken_languages', '^\\[', '')
table = etl.sub(table, 'spoken_languages', '\\]$', '')
table = etl.selectnotnone(table, 'spoken_languages')
table = etl.splitdown(table, 'spoken_languages', '(?<=\\}),\\s(?=\\{)')
table = etl.sub(table, 'spoken_languages', '\'', '"')
table = etl.convert(table, 'spoken_languages', lambda row: json.loads(row))
table = etl.unpackdict(table, 'spoken_languages')
table = etl.cutout(table, 'name')

movies = etl.fromdb(conn, 'SELECT id, tmdb_id from d_movie')
movies = etl.rename(movies, 'id', 'id_movie')
table = etl.join(table, movies, lkey='movie_tmdb_id', rkey='tmdb_id')
table = etl.cutout(table, 'movie_tmdb_id')

languages = etl.fromdb(conn, 'SELECT id, iso_639_1 from d_language')
languages = etl.rename(languages, 'id', 'id_language')
table = etl.join(table, languages, key='iso_639_1')
table = etl.cutout(table, 'iso_639_1')

# LOAD
etl.todb(table, cursor, 'spoken_languages')
