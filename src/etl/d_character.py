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
table = etl.split(table, 'cast', 'credit_id', ['name', 'id_x'])
table = etl.split(table, 'name', '\'character\':', ['id_y', 'name'])
table = etl.split(table, 'id_x', '\'id\':', ['gender', 'trash'])
table = etl.split(table, 'gender', '\'gender\':', ['trash_x', 'id_gender'])
table = etl.cut(table, 'name', 'id_gender')
table = etl.sub(table, 'name', '[,\']', '')
table = etl.convert(table, 'name', str)
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'name', '"', '')
table = etl.sub(table, 'id_gender', '[ ,\']', '')
table = etl.sub(table, 'id_gender', ' ', '')
table = etl.selectne(table, 'id_gender', None)
table = etl.convert(table, 'id_gender', int)
table = etl.convert(table, 'id_gender', lambda value: value + 1)
table = etl.selectne(table, 'name', '')
table = etl.groupselectfirst(table, 'name')

# LOAD
etl.todb(table, cursor, 'd_character')