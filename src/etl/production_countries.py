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
table = etl.cut(movies, 'id', 'production_countries')
table = etl.rename(table, 'id', 'movie_tmdb_id')
table = etl.sub(table, 'production_countries', '^\\[', '')
table = etl.sub(table, 'production_countries', '\\]$', '')
table = etl.selectnotnone(table, 'production_countries')
table = etl.splitdown(table, 'production_countries', '(?<=\\}),\\s(?=\\{)')
table = etl.sub(table, 'production_countries', '\'', '"')
table = etl.convert(table, 'production_countries', lambda row: json.loads(row))
table = etl.unpackdict(table, 'production_countries')
table = etl.cutout(table, 'name')

movies = etl.fromdb(conn, 'SELECT id, tmdb_id from d_movie')
movies = etl.rename(movies, 'id', 'id_movie')
table = etl.join(table, movies, lkey='movie_tmdb_id', rkey='tmdb_id')
table = etl.cutout(table, 'movie_tmdb_id')

countries = etl.fromdb(conn, 'SELECT id, iso_3166_1 from d_country')
countries = etl.rename(countries, 'id', 'id_country')
table = etl.join(table, countries, key='iso_3166_1')
table = etl.cutout(table, 'iso_3166_1')

# LOAD
etl.todb(table, cursor, 'production_countries')
