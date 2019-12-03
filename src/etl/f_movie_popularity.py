import petl as etl
import csv
import psycopg2
from ftfy import fix_encoding
from collections import OrderedDict
from datetime import datetime

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET COUNTRYFUNCTION (table: d_country)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8', errors='ignore')
reviews = etl.fromcsv('dataset/ratings.csv', encoding='utf8')

# TRANSFORMATION
movies = etl.rename(movies, 'id', 'movieId')
movies = etl.convert(movies, 'movieId', int)
reviews = etl.convert(reviews, 'movieId', int)
reviews = etl.join(movies, reviews, key='movieId')
#print(etl.nrows(reviews))
reviews = etl.cut(reviews, 'movieId', 'userId', 'rating', 'timestamp', 'revenue', 'runtime', 'popularity', 'budget', 'status')
reviews = etl.convert(reviews, 'timestamp', lambda v: datetime.fromtimestamp(float(v)/1000).strftime("%Y-%m-%d"))

timestamps = etl.fromdb(conn, 'SELECT * from d_date')
timestamps = etl.cut(timestamps, 'id', 'date')
timestamps = etl.convert(timestamps, 'date', lambda v: v.strftime("%Y-%m-%d"))
timestamps = dict(timestamps)
timestamps_map = {timestamps[k] : k for k in timestamps}

statuses = etl.fromdb(conn, 'SELECT * from d_release_status')
statuses = dict(etl.data(etl.cut(statuses, 'id', 'name')))
statuses_map = {statuses[k] : k for k in statuses}

mappings = OrderedDict()
mappings['id_date'] = 'timestamp', timestamps_map
mappings['id_movie'] = 'movieId'
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
# LOAD
etl.todb(table, cursor, 'f_movie_popularity')