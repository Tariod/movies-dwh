import petl as etl
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET KEYWORD FUNCTION (table: d_keyword)
# EXTRACT
keywords = etl.fromcsv('dataset/keywords.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(keywords, 'id', 'keywords')
table = etl.splitdown(table, 'keywords', '}')
table = etl.split(table, 'keywords', '\'name\': ', ['trash', 'keyword'])
table = etl.split(table, 'trash', ':', ['trash', 'keyword_id'])
table = etl.sub(table, 'keyword_id', ',', '')
table = etl.cut(table, 'keyword_id', 'keyword')
table = etl.sub(table, 'keyword', '[\'"]', '')
table = etl.sub(table, 'keyword', '(^[ ]+)|[ ]+$', '')
table = etl.rename(table, 'keyword', 'name')
table = etl.selectne(table, 'name', None)
table = etl.selectne(table, 'keyword_id', None)
table = etl.rename(table, 'keyword_id', 'id')
table = etl.convert(table, 'id', int)
table = etl.distinct(table, 'id')
table = etl.cut(table, 'name')

# LOAD
etl.todb(table, cursor, 'd_keyword')
