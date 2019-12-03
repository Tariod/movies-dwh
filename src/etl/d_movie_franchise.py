import petl as etl
import csv
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET MOVIE_FRANCHISE FUNCTION (table: d_movie_franchise)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'belongs_to_collection')
table = etl.split(table, 'belongs_to_collection', 'poster_path', ['info', 'trash'])
table = etl.cut(table, 'info')
table = etl.split(table, 'info', '\'name\':', ['id', 'name'])
table = etl.split(table, 'id', ':', ['trash', 'id'])
table = etl.sub(table, 'name', '[\'",]', '')
table = etl.sub(table, 'id', '[\',]', '')
table = etl.cut(table, 'id', 'name')
table = etl.groupselectfirst(table, 'id')
table = etl.selectnotnone(table, 'id')

# LOAD
etl.todb(table, cursor, 'd_movie_franchise')