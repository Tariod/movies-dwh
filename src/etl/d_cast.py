import petl as etl
import csv
import psycopg2
from collections import OrderedDict

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET CAST FUNCTION (table: d_cast)
# EXTRACT
movies = etl.fromcsv('dataset/credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'cast')
table = etl.splitdown(table, 'cast', '}')
table = etl.selectcontains(table, 'cast', 'name')
table = etl.split(table, 'cast', '\'name\':', ['info', 'trash'])
table = etl.cut(table, 'id', 'info')
table = etl.split(table, 'info', '\'id\':', ['info', 'id_people'])
table = etl.split(table, 'info', '\'character\':', ['trash', 'name'])
table = etl.cut(table, 'id', 'id_people', 'name')
table = etl.split(table, 'name', '\'credit_id\':', ['name', 'trash'])
table = etl.cut(table, 'id', 'name', 'id_people')
table = etl.sub(table, 'name', '[,\']', '')
table = etl.convert(table, 'name', str)
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'name', '"', '')
table = etl.selectne(table, 'name', '')
table = etl.sub(table, 'id_people', '[ ,\']', '')
table = etl.sub(table, 'id_people', ' ', '')
table = etl.selectne(table, 'id_people', None)
table = etl.convert(table, 'id_people', int)
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectne(table, 'id', None)

characters = etl.fromdb(conn, 'SELECT * from d_character')
characters = dict(etl.data(characters))
characters_map = {characters[k] : k for k in characters}

mappings = OrderedDict()
mappings['id_movie'] = 'id'
mappings['id_character'] = 'name', characters_map
mappings['id_people'] = 'id_people'
table = etl.fieldmap(table, mappings)

# LOAD
etl.todb(table, cursor, 'd_cast')