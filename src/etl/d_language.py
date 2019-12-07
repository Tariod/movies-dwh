import petl as etl
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET LANGUAGE FUNCTION (table: d_language)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8',
                     errors='ignore')

# TRANSFORMATION
table = etl.cut(movies, 'spoken_languages')
table = etl.convert(table, 'spoken_languages', str)
table = etl.splitdown(table, 'spoken_languages', '}')
table = etl.split(table, 'spoken_languages', '\'name\':',
                  ['iso_639_1', 'name'])
table = etl.split(table, 'iso_639_1', ':', ['trash', 'iso_639_1'])
table = etl.cut(table, 'name', 'iso_639_1')
table = etl.sub(table, 'name', '[\'\\]]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'iso_639_1', '[\',]', '')
table = etl.sub(table, 'iso_639_1', '[\'\\]]', '')
table = etl.sub(table, 'iso_639_1', '(^[ ]+)|[ ]+$', '')
table = etl.selectnotnone(table, 'iso_639_1')
table = etl.groupselectfirst(table, 'iso_639_1')

# LOAD
etl.todb(table, cursor, 'd_language')
