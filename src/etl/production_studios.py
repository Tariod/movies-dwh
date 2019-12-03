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
table = etl.cut(movies, 'id', 'production_companies')
table = etl.convert(table, 'production_companies', str)
table = etl.selectcontains(table, 'production_companies', 'id')
table = etl.splitdown(table, 'production_companies', '}')
table = etl.split(table, 'production_companies', '\'id\':', ['trash', 'id_studio'])
table = etl.cut(table, 'id', 'id_studio')
table = etl.sub(table, 'id_studio', '[ ,\']', '')
table = etl.sub(table, 'id_studio', ' ', '')
table = etl.selectne(table, 'id_studio', None)
table = etl.convert(table, 'id_studio', int)
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectne(table, 'id', None)
table = etl.rename(table, 'id', 'id_movie')

# LOAD
etl.todb(table, cursor, 'production_studios')