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

# GET KEYWORD FUNCTION (table: d_keyword)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
keywords = etl.fromcsv(DATA_SOURCE_DIR + 'keywords.csv', encoding='utf8')

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
