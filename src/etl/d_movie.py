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
franchise = etl.cut(franchise, 'id', 'id_age_category', 'id_movie_franchise', 'original_language', 'original_title', 'title', 'imdb_id')
franchise = etl.replace(franchise, 'id_movie_franchise', None, 'null')
franchise = etl.split(franchise, 'id_movie_franchise', ':', ['trash', 'id_movie_franchise'])
franchise = etl.convert(franchise, 'id_movie_franchise', str)
franchise = etl.sub(franchise, 'id_movie_franchise', '[\', ]', '')
franchise = etl.cut(franchise, 'id', 'id_age_category', 'id_movie_franchise', 'original_language', 'original_title', 'title', 'imdb_id')
franchise = etl.rename(franchise, 'original_language', 'id_original_language')

languages_table = etl.cut(movies, 'spoken_languages')
languages_table = etl.convert(languages_table, 'spoken_languages', str)
languages_table = etl.splitdown(languages_table, 'spoken_languages', '}')
languages_table = etl.split(languages_table, 'spoken_languages', '\'name\':', ['iso_639_1', 'name'])
languages_table = etl.split(languages_table, 'iso_639_1', ':', ['trash', 'iso_639_1'])
languages_table = etl.cut(languages_table, 'name', 'iso_639_1')
languages_table = etl.sub(languages_table, 'name', '[\'\]]', '')
languages_table = etl.sub(languages_table, 'iso_639_1', '[\',]', '')
languages_table = etl.selectnotnone(languages_table, 'iso_639_1')
languages_table = etl.groupselectfirst(languages_table, 'name')

values = etl.data(languages_table)
valuesNew = []
vals = list(range(1, etl.nrows(languages_table) + 1))
flat_list = [list(sublist) for sublist in values]
languages_table = [flat_list, vals]
languages_table = etl.fromcolumns(languages_table)
languages_table = etl.unpack(languages_table, 'f0', ['name', 'iso_639_1'])
languages_table = etl.rename(languages_table, 'f1', 'id')
languages_table = etl.sub(languages_table, 'iso_639_1', '[\' ]', '')

language_to_id = dict(zip(list(etl.values(languages_table, 'iso_639_1')), list(etl.values(languages_table, 'id'))))

mappings = OrderedDict()
mappings['id'] = 'id'
mappings['id_age_category'] = 'id_age_category'
mappings['id_movie_franchise'] = 'id_movie_franchise'
mappings['id_original_language'] = 'id_original_language', language_to_id
mappings['original_title'] = 'original_title'
mappings['title'] = 'title'
mappings['imdb_id'] = 'imdb_id'
table = etl.fieldmap(franchise, mappings)

table = etl.groupselectfirst(table, 'id')
table = etl.replace(table, 'title', None, 'No translation available')
table = etl.convert(table, 'id', int)
table = etl.sub(table, 'id_original_language', '[^0-9]', '')
table = etl.replace(table, 'id_original_language', '', None)

# LOAD
etl.todb(table, cursor, 'd_movie')