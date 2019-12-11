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

# GET PEOPLE FUNCTION (table: d_people)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'cast')
table = etl.selectcontains(table, 'cast', 'name')
table = etl.splitdown(table, 'cast', '}')
table = etl.split(table, 'cast', '\'id\':', ['cast_id', 'info'])
table = etl.split(table, 'cast_id', '\'gender\':', ['trash', 'id_gender'])
table = etl.cut(table, 'info', 'id_gender')
table = etl.split(table, 'info', '\'name\':', ['id', 'name'])
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectne(table, 'id', None)
table = etl.sub(table, 'id_gender', '[ ,\']', '')
table = etl.sub(table, 'id_gender', ' ', '')
table = etl.selectne(table, 'id_gender', None)
table = etl.convert(table, 'id_gender', int)
table = etl.convert(table, 'id_gender', lambda value: value + 1)
table = etl.split(table, 'name', '\'order\':', ['name', 'trash'])
table = etl.cut(table, 'id', 'id_gender', 'name')
table = etl.sub(table, 'name', '[,"\']', '')
table = etl.sub(table, 'name', '(^[ ]+)|([ ]+$)', '')
table = etl.convert(table, 'id', int)
table = etl.distinct(table, 'id')
table = etl.cut(table, 'name', 'id')
table = etl.rename(table, 'id', 'tmdb_id')

crew = etl.cut(movies, 'crew')
crew = etl.selectcontains(crew, 'crew', 'name')
crew = etl.splitdown(crew, 'crew', '}')
crew = etl.split(crew, 'crew', '\'id\':', ['cast_id', 'info'])
crew = etl.split(crew, 'cast_id', '\'gender\':', ['trash', 'id_gender'])
crew = etl.cut(crew, 'info', 'id_gender')
crew = etl.split(crew, 'info', '\'name\':', ['id', 'name'])
crew = etl.split(crew, 'id', '\'job\':', ['id', 'trash'])
crew = etl.sub(crew, 'id', '[ ,\']', '')
crew = etl.sub(crew, 'id', ' ', '')
crew = etl.selectne(crew, 'id', None)
crew = etl.cut(crew, 'id', 'id_gender', 'name')
crew = etl.sub(crew, 'id_gender', '[ ,\']', '')
crew = etl.sub(crew, 'id_gender', ' ', '')
crew = etl.selectne(crew, 'id_gender', None)
crew = etl.convert(crew, 'id_gender', int)
crew = etl.convert(crew, 'id_gender', lambda value: value + 1)
crew = etl.split(crew, 'name', '\'profile_path\':', ['name', 'trash'])
crew = etl.cut(crew, 'id', 'id_gender', 'name')
crew = etl.sub(crew, 'name', '[,"\']', '')
crew = etl.sub(crew, 'name', '(^[ ]+)|([ ]+$)', '')
crew = etl.convert(crew, 'id', int)
crew = etl.distinct(crew, 'id')
crew = etl.cut(crew, 'name', 'id')
crew = etl.rename(crew, 'id', 'tmdb_id')

crew = etl.antijoin(crew, table, key='tmdb_id')

# LOAD
etl.todb(table, cursor, 'd_people')
etl.appenddb(crew, cursor, 'd_people')
