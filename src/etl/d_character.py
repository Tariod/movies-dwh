import os
import petl as etl
import psycopg2

conn = psycopg2.connect(dbname=os.getenv('DB_NAME'),
                        user=os.getenv('DB_USER'),
                        password=os.getenv('DB_PASSWORD'),
                        host=os.getenv('DB_HOST'),
                        port=os.getenv('DB_PORT'))
cursor = conn.cursor()

# GET CHARACTER FUNCTION (table: d_character)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'cast')
table = etl.splitdown(table, 'cast', '}')
table = etl.split(table, 'cast', 'credit_id', ['name', 'id_x'])
table = etl.split(table, 'name', '\'character\':', ['id_y', 'name'])
table = etl.split(table, 'id_x', '\'id\':', ['gender', 'trash'])
table = etl.cut(table, 'name', 'gender', 'trash')
table = etl.convert(table, 'gender', str)
table = etl.split(table, 'gender', '\'gender\':', ['trash_x', 'id_gender'])
table = etl.cut(table, 'name', 'id_gender')
table = etl.sub(table, 'name', '[,\']', '')
table = etl.convert(table, 'name', str)
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'name', '"', '')
table = etl.sub(table, 'id_gender', '[ ,\']', '')
table = etl.sub(table, 'id_gender', ' ', '')
table = etl.selectne(table, 'id_gender', None)
table = etl.convert(table, 'id_gender', int)
table = etl.convert(table, 'id_gender', lambda value: value + 1)
table = etl.selectne(table, 'name', '')
table = etl.groupselectfirst(table, 'name')

# LOAD
etl.todb(table, cursor, 'd_character')
