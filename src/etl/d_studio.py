import petl as etl
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET STUDIO FUNCTION (table: d_studio)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8',
                     errors='ignore')

# TRANSFORMATION
table = etl.cut(movies, 'production_companies')
table = etl.sub(table, 'production_companies', '[\\[\\]]', '')
table = etl.convert(table, 'production_companies', str)
table = etl.selectcontains(table, 'production_companies', 'name')
table = etl.splitdown(table, 'production_companies', '{')
table = etl.sub(table, 'production_companies', '{{', '{')
table = etl.split(table, 'production_companies', '\'id\':', ['name', 'id'])
table = etl.sub(table, 'id', '}', '')
table = etl.split(table, 'name', '\'name\':', ['trash', 'name'])
table = etl.sub(table, 'name', '[\'",]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')
table = etl.selectne(table, 'id', '')
table = etl.sub(table, 'id', ',', '')
table = etl.cut(table, 'id', 'name')
table = etl.convert(table, 'id', int)
table = etl.cut(table, 'name')
table = etl.distinct(table, 'name')

# LOAD
etl.todb(table, cursor, 'd_studio')
