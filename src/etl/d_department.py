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
table = etl.split(table, 'crew', '\'gender\':', ['department', 'trash'])
table = etl.split(table, 'department', '\'department\':', ['info', 'department'])
table = etl.cut(table, 'department')
table = etl.sub(table, 'department', '[\'\],]', '')
table = etl.sub(table, 'department', '(^[ ]+)|([ ]+$)', '')
table = etl.selectnotnone(table, 'department')
table = etl.groupselectfirst(table, 'department')
table = etl.rename(table, 'department', 'name')

# LOAD
etl.todb(table, cursor, 'd_department')