import petl as etl
import csv
import psycopg2
from ftfy import fix_encoding

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET COUNTRYFUNCTION (table: d_country)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8', errors='ignore')

# TRANSFORMATION
table = etl.cut(movies, 'production_countries')
table = etl.convert(table, 'production_countries', str)
table = etl.splitdown(table, 'production_countries', '}')
table = etl.split(table, 'production_countries', '\'name\':', ['iso_3166_1', 'name'])
table = etl.split(table, 'iso_3166_1', ':', ['trash', 'iso_3166_1'])
table = etl.cut(table, 'name', 'iso_3166_1')
table = etl.sub(table, 'name', '[\'\]]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'iso_3166_1', '[\',]', '')
table = etl.selectnotnone(table, 'iso_3166_1')
table = etl.sub(table, 'name', '"', '')
table = etl.groupselectfirst(table, 'name')

values = etl.data(table)
valuesNew = []
vals = list(range(1, etl.nrows(table) + 1))
flat_list = [list(sublist) for sublist in values]
table = [flat_list, vals]
table = etl.fromcolumns(table)
table = etl.unpack(table, 'f0', ['name', 'iso_3166_1'])
table = etl.rename(table, 'f1', 'id')

# LOAD
etl.todb(table, cursor, 'd_country')