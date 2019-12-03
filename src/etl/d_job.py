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


values = etl.data(table)
valuesNew = []
vals = list(range(1, etl.nrows(table) + 1))
flat_list = [list(sublist) for sublist in values]
table = [flat_list, vals]
table = etl.fromcolumns(table)
table = etl.unpack(table, 'f0', ['name'])
table = etl.rename(table, 'f1', 'id')

# LOAD
etl.todb(table, cursor, 'd_job')