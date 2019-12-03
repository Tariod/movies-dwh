import petl as etl
import csv
import psycopg2
from collections import OrderedDict

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET CAST FUNCTION (table: d_cast)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'genres')
table = etl.splitdown(table, 'genres', '}')
table = etl.selectcontains(table, 'genres', 'id')
table = etl.split(table, 'genres', '\'name\':', ['id_genre', 'trash'])
table = etl.cut(table, 'id', 'id_genre')
table = etl.split(table, 'id_genre', '\'id\':', ['trash', 'id_genre'])
table = etl.cut(table, 'id', 'id_genre')
table = etl.sub(table, 'id_genre', '[ ,\']', '')
table = etl.sub(table, 'id_genre', ' ', '')
table = etl.selectne(table, 'id_genre', None)
table = etl.convert(table, 'id_genre', int)
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectne(table, 'id', None)
table = etl.rename(table, 'id', 'id_movie')

# LOAD
etl.todb(table, cursor, 'movie_genres')