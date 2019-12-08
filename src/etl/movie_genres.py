from collections import OrderedDict
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

# GET CAST FUNCTION (table: d_cast)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'genres')
table = etl.splitdown(table, 'genres', '}')
table = etl.selectcontains(table, 'genres', 'id')
table = etl.split(table, 'genres', '\'name\':', ['id_genre', 'name'])
table = etl.cut(table, 'id', 'name')
table = etl.sub(table, 'name', '[,\'\\]]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')

movies = etl.fromdb(conn, 'SELECT * from d_movie')
movies = etl.cut(movies, 'id', 'tmdb_id')
movies = dict(etl.data(movies))
movies_map = {movies[k]: k for k in movies}

genres = etl.fromdb(conn, 'SELECT * from d_genre')
genres = etl.cut(genres, 'id', 'title')
genres = dict(etl.data(genres))
genres_map = {genres[k]: k for k in genres}

mappings = OrderedDict()
mappings['id_movie'] = 'id', movies_map
mappings['id_genre'] = 'name', genres_map
table = etl.fieldmap(table, mappings)

table = etl.convert(table, 'id_movie', str)
table = etl.select(table, lambda rec: '-' not in rec.id_movie)

# LOAD
etl.todb(table, cursor, 'movie_genres')
