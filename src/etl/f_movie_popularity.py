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
movies_data = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv',
                          encoding='utf8',
                          errors='ignore')
reviews = etl.fromcsv(DATA_SOURCE_DIR + 'ratings.csv', encoding='utf8')
links = etl.fromcsv(DATA_SOURCE_DIR + 'links.csv', encoding='utf8')

# TRANSFORMATION
reviews = etl.rename(reviews,
                     {'userId': 'abstract_name',
                      'rating': 'f_rating',
                      'timestamp': 'date'})
reviews = etl.selectnotnone(reviews, 'abstract_name')
reviews = etl.selectnotnone(reviews, 'movieId')
reviews = etl.convert(reviews, 'f_rating', float)
reviews = etl.convert(reviews, 'date',
                      lambda stamp: datetime
                      .fromtimestamp(float(stamp) / 1000)
                      .strftime('%Y-%m-%d %H:%M:%S'))

users = etl.fromdb(conn, 'SELECT * from d_user')
users = etl.rename(users, 'id', 'id_user')
reviews = etl.join(reviews, users, key='abstract_name')
reviews = etl.cutout(reviews, 'abstract_name')

links = etl.cut(links, 'movieId', 'tmdbId')
reviews = etl.join(reviews, links, key='movieId')
reviews = etl.cutout(reviews, 'movieId')
reviews = etl.rename(reviews, 'tmdbId',  'movie_tmdb_id')

movies_data = etl.cut(movies_data,
                      'release_date',
                      'id',
                      'status',
                      'budget',
                      'revenue',
                      'runtime',
                      'popularity')
movies_data = etl.rename(movies_data,
                         {'release_date': 'date',
                          'id': 'movie_tmdb_id',
                          'budget': 'f_budget',
                          'revenue': 'f_revenue',
                          'runtime': 'f_runtime',
                          'popularity': 'f_popularity'})
movies_data = etl.selectnotnone(movies_data, 'movie_tmdb_id')
movies_data = etl.search(movies_data, 'date',
                         '[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])')
movies_data = etl.convert(movies_data, 'date', lambda date: date + ' 00:00:00')
movies_data = etl.convert(movies_data, 'f_budget', int)
movies_data = etl.convert(movies_data, 'f_revenue', int)
movies_data = etl.convert(movies_data, 'f_runtime', float)
movies_data = etl.convert(movies_data, 'f_popularity', float)

release_status = etl.fromdb(conn, 'SELECT * from d_release_status')
release_status = etl.rename(release_status, 'id', 'id_release_status')
movies_data = etl.join(movies_data, release_status, lkey='status', rkey='name')
movies_data = etl.cutout(movies_data, 'status')

table = etl.cat(movies_data, reviews)

dates = etl.fromdb(conn, 'SELECT id, date from d_date')
dates = etl.rename(dates, 'id', 'id_date')
dates = etl.convert(dates, 'date', str)
table = etl.join(table, dates, key='date')
table = etl.cutout(table, 'date')

movies = etl.fromdb(conn, 'SELECT id, tmdb_id from d_movie')
movies = etl.rename(movies, 'id', 'id_movie')
table = etl.convert(table, 'movie_tmdb_id', str)
table = etl.join(table, movies, lkey='movie_tmdb_id', rkey='tmdb_id')
table = etl.cutout(table, 'movie_tmdb_id')

# LOAD
etl.todb(table, cursor, 'f_movie_popularity')
