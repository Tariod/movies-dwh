import petl as etl
import csv
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET PEOPLE FUNCTION (table: d_people)
# EXTRACT
movies = etl.fromcsv('dataset/credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'crew')
table = etl.selectcontains(table, 'crew', 'name')
table = etl.splitdown(table, 'crew', '}')
table = etl.split(table, 'crew', '\'name\':', ['job', 'trash'])
table = etl.split(table, 'job', '\'job\':', ['info', 'job'])
table = etl.cut(table, 'job')
table = etl.sub(table, 'job', '[\'\],"]', '')
table = etl.sub(table, 'job', '(^[ ]+)|([ ]+$)', '')
table = etl.selectnotnone(table, 'job')
table = etl.groupselectfirst(table, 'job')
table = etl.rename(table, 'job', 'name')

# LOAD
etl.todb(table, cursor, 'd_job')