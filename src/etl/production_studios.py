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
table = etl.split(table, 'production_companies', '\'id\':', ['studio', 'trash'])
table = etl.cut(table, 'id', 'studio')
table = etl.split(table, 'studio', '\'name\':', ['trash', 'studio'])
table = etl.cut(table, 'id', 'studio')
table = etl.sub(table, 'studio', '[",\'\[\]]', '')
table = etl.sub(table, 'studio', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectne(table, 'id', None)
table = etl.selectne(table, 'studio', None)

movies = etl.fromdb(conn, 'SELECT * from d_movie')
movies = etl.cut(movies, 'id', 'tmdb_id')
movies = dict(etl.data(movies))
movies_map = {movies[k] : k for k in movies}

studios = etl.fromdb(conn, 'SELECT * from d_studio')
studios = etl.cut(studios, 'id', 'name')
studios = dict(etl.data(studios))
studios_map = {studios[k] : k for k in studios}

mappings = OrderedDict()
mappings['id_movie'] = 'id', movies_map
mappings['id_studio'] = 'studio', studios_map
table = etl.fieldmap(table, mappings)

table = etl.convert(table, 'id_movie', str)
table = etl.select(table, lambda rec: '-' not in rec.id_movie)

# LOAD
etl.todb(table, cursor, 'production_studios')