import petl as etl
import csv
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET CHARACTER FUNCTION (table: d_character)
# EXTRACT
movies = etl.fromcsv('dataset/credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'cast')
table = etl.splitdown(table, 'cast', '}')
table = etl.split(table, 'cast', 'credit_id', ['name', 'id'])
table = etl.split(table, 'name', '\'character\':', ['id', 'name'])
table = etl.cut(table, 'name')
table = etl.sub(table, 'name', '[,\']', '')
table = etl.convert(table, 'name', str)
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'name', '"', '')
table = etl.selectne(table, 'name', '')
table = etl.groupselectfirst(table, 'name')

values = etl.data(table)
valuesNew = []
vals = list(range(1, etl.nrows(table) + 1))
flat_list = [list(sublist) for sublist in values]
table = [flat_list, vals]
table = etl.fromcolumns(table)
table = etl.unpack(table, 'f0', ['name'])
table = etl.rename(table, 'f1', 'id')

# LOAD
etl.todb(table, cursor, 'd_character')