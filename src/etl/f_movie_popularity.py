from collections import OrderedDict
from datetime import datetime
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
reviews = etl.fromcsv(DATA_SOURCE_DIR + 'ratings.csv', encoding='utf8')

# TRANSFORMATION
movies = etl.rename(movies, 'id', 'movieId')
movies = etl.convert(movies, 'movieId', int)
reviews = etl.convert(reviews, 'movieId', int)
reviews = etl.join(movies, reviews, key='movieId')
reviews = etl.cut(reviews, 'movieId', 'userId', 'rating', 'timestamp',
                  'revenue', 'runtime', 'popularity', 'budget', 'status')
reviews = etl.convert(reviews, 'timestamp',
                      lambda v: datetime
                      .fromtimestamp(float(v) / 1000)
                      .strftime('%Y-%m-%d'))

timestamps = etl.fromdb(conn, 'SELECT * from d_date')
timestamps = etl.cut(timestamps, 'id', 'date')
timestamps = etl.convert(timestamps, 'date', lambda v: v.strftime('%Y-%m-%d'))
timestamps = dict(timestamps)
timestamps_map = {timestamps[k]: k for k in timestamps}

movies = etl.fromdb(conn, 'SELECT * from d_movie')
movies = etl.cut(movies, 'id', 'tmdb_id')
movies = dict(etl.data(movies))
movies_map = {movies[k]: k for k in movies}

statuses = etl.fromdb(conn, 'SELECT * from d_release_status')
statuses = dict(etl.data(etl.cut(statuses, 'id', 'name')))
statuses_map = {statuses[k]: k for k in statuses}

mappings = OrderedDict()
mappings['id_date'] = 'timestamp', timestamps_map
mappings['id_movie'] = 'movieId', movies_map
mappings['id_release_status'] = 'status', statuses_map
mappings['id_user'] = 'userId'
mappings['f_rating'] = 'rating'
mappings['f_budget'] = 'budget'
mappings['f_revenue'] = 'revenue'
mappings['f_runtime'] = 'runtime'
mappings['f_popularity'] = 'popularity'
table = etl.fieldmap(reviews, mappings)
table = etl.replace(table, 'id_release_status', '', 1)
table = etl.replace(table, 'f_runtime', '', None)

table = etl.convert(table, 'id_movie', str)
table = etl.select(table, lambda rec: '-' not in rec.id_movie)

# LOAD
etl.todb(table, cursor, 'f_movie_popularity')
