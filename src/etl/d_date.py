import petl as etl
import csv
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET DATE FUNCTION (table: d_date)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'release_date')
table = etl.rename(table, 'release_date', 'date')
table = etl.groupselectfirst(table, 'date')
table = etl.search(table, 'date', '[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])')

# LOAD
etl.todb(table, cursor, 'd_date')