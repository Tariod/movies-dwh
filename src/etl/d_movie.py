import petl as etl
import csv
import psycopg2
from ftfy import fix_encoding
from collections import OrderedDict

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET MOVIE FUNCTION (table: d_movie)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8', errors='ignore')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'adult', 'belongs_to_collection', 'original_language', 'original_title', 'title', 'imdb_id')
table = etl.select(table, 'adult', lambda field: field == 'True' or field == 'False')
mappings = OrderedDict()
mappings['id'] = 'id'
mappings['id_age_category'] = 'adult', {'True': 1, 'False': 2}
mappings['belongs_to_collection'] = 'belongs_to_collection'
mappings['original_title'] = 'original_title'
mappings['title'] = 'title'
mappings['imdb_id'] = 'imdb_id'
mappings['original_language'] = 'original_language'
table = etl.fieldmap(table, mappings)

franchise = etl.split(table, 'belongs_to_collection', 'poster_path', ['info', 'trash'])
franchise = etl.split(franchise, 'info', '\'name\':', ['id_movie_franchise', 'name'])
franchise = etl.cut(franchise, 'id', 'id_age_category', 'name', 'original_language', 'original_title', 'title', 'imdb_id')
franchise = etl.sub(franchise, 'name', '[,\'"]', '')
franchise = etl.sub(franchise, 'name', '(^[ ]+)|[ ]+$', '')

languages = etl.fromdb(conn, 'SELECT * from d_language')
languages = dict(etl.data(etl.cut(languages, 'id', 'iso_639_1')))
languages_map = {languages[k] : k for k in languages}

franchises = etl.fromdb(conn, 'SELECT * from d_movie_franchise')
franchises = dict(etl.data(etl.cut(franchises, 'id', 'name')))
franchises_map = {franchises[k] : k for k in franchises}

mappings = OrderedDict()
mappings['id_age_category'] = 'id_age_category'
mappings['id_movie_franchise'] = 'name', franchises_map
mappings['id_original_language'] = 'original_language', languages_map
mappings['original_title'] = 'original_title'
mappings['title'] = 'title'
mappings['imdb_id'] = 'imdb_id'
mappings['tmdb_id'] = 'id'
table = etl.fieldmap(franchise, mappings)

table = etl.groupselectfirst(table, 'tmdb_id')
table = etl.replace(table, 'title', None, 'No translation available')
table = etl.convert(table, 'tmdb_id', int)
table = etl.convert(table, 'id_original_language', str)
table = etl.replace(table, 'id_original_language', '', None)

# LOAD
etl.todb(table, cursor, 'd_movie')