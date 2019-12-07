from collections import OrderedDict
from dotenv import load_dotenv
import os
import petl as etl
import psycopg2

load_dotenv()

conn = psycopg2.connect(dbname=os.getenv('DB_NAME'),
                        user=os.getenv('DB_USER'),
                        password=os.getenv('DB_PASSWORD'),
                        host=os.getenv('DB_HOST'),
                        port=os.getenv('DB_PORT'))
cursor = conn.cursor()

# GET CAST FUNCTION (table: d_cast)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'cast')
table = etl.splitdown(table, 'cast', '}')
table = etl.selectcontains(table, 'cast', 'name')
table = etl.split(table, 'cast', '\'name\':', ['info', 'trash'])
table = etl.split(table, 'trash', '\'order\':', ['name_person', 'trash_x'])
table = etl.split(table, 'info', '\'id\':', ['info', 'id_people'])
table = etl.split(table, 'info', '\'character\':', ['trash', 'name'])
table = etl.cut(table, 'id', 'name_person', 'name')
table = etl.split(table, 'name', '\'credit_id\':', ['name', 'trash'])
table = etl.cut(table, 'id', 'name', 'name_person')
table = etl.sub(table, 'name', '[,\']', '')
table = etl.convert(table, 'name', str)
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'name', '"', '')
table = etl.selectne(table, 'name', '')
table = etl.sub(table, 'name_person', '[,\'\\]]', '')
table = etl.sub(table, 'name_person', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectne(table, 'id', None)

characters = etl.fromdb(conn, 'SELECT * from d_character')
characters = dict(etl.data(characters))
characters_map = {characters[k]: k for k in characters}

movies = etl.fromdb(conn, 'SELECT * from d_movie')
movies = etl.cut(movies, 'id', 'tmdb_id')
movies = dict(etl.data(movies))
movies_map = {movies[k]: k for k in movies}

people = etl.fromdb(conn, 'SELECT * from d_people')
people = etl.cut(people, 'id', 'name')
people = dict(etl.data(people))
people_map = {people[k]: k for k in people}

mappings = OrderedDict()
mappings['id_movie'] = 'id', movies_map
mappings['id_character'] = 'name', characters_map
mappings['id_people'] = 'name_person', people_map
table = etl.fieldmap(table, mappings)

table = etl.convert(table, 'id_movie', str)
table = etl.select(table, lambda rec: '-' not in rec.id_movie)

# LOAD
etl.todb(table, cursor, 'd_cast')
