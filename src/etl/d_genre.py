import petl as etl
import csv
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET GENRE FUNCTION (table: d_genre)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'genres')
table = etl.splitdown(table, 'genres', '}')
table = etl.sub(table, 'genres', ',', '')
table = etl.split(table, 'genres', '\'name\':', ['id', 'title'])
table = etl.split(table, 'id', ':', ['trash', 'id'])
table = etl.sub(table, 'title', '\'', '')
table = etl.cut(table, 'id', 'title')
table = etl.selectnotnone(table, 'id')
table = etl.groupselectfirst(table, key='title')

# LOAD
etl.todb(table, cursor, 'd_genre')