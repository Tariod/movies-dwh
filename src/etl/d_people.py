import petl as etl
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET PEOPLE FUNCTION (table: d_people)
# EXTRACT
movies = etl.fromcsv('dataset/credits.csv', encoding='utf8')

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
table = etl.rename(table, 'id', 'id_tmdb')

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
crew = etl.rename(crew, 'id', 'id_tmdb')

crew = etl.antijoin(crew, table, key='id_tmdb')

# LOAD
etl.todb(table, cursor, 'd_people')
etl.appenddb(crew, cursor, 'd_people')
