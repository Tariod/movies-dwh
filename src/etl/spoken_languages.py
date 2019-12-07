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
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'spoken_languages')
table = etl.convert(table, 'spoken_languages', str)
table = etl.selectcontains(table, 'spoken_languages', 'name')
table = etl.splitdown(table, 'spoken_languages', '}')
table = etl.split(table, 'spoken_languages', '\'name\':',
                  ['trash', 'id_studio'])
table = etl.cut(table, 'id', 'id_studio')
table = etl.sub(table, 'id_studio', '[\'",]', '')
table = etl.sub(table, 'id_studio', '(^[ ]+)|[ ]+$', '')
table = etl.selectne(table, 'id_studio', None)
table = etl.convert(table, 'id_studio', str)
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectne(table, 'id', None)

movies = etl.fromdb(conn, 'SELECT * from d_movie')
movies = etl.cut(movies, 'id', 'tmdb_id')
movies = dict(etl.data(movies))
movies_map = {movies[k]: k for k in movies}

characters = etl.fromdb(conn, 'SELECT * from d_language')
characters = dict(etl.data(etl.cut(characters, 'id', 'name')))
characters_map = {characters[k]: k for k in characters}

mappings = OrderedDict()
mappings['id_movie'] = 'id', movies_map
mappings['id_language'] = 'id_studio', characters_map
table = etl.fieldmap(table, mappings)

# LOAD
etl.todb(table, cursor, 'spoken_languages')
