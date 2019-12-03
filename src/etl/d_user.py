import petl as etl
import csv
import psycopg2
from collections import OrderedDict
from datetime import datetime

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET USER FUNCTION (table: d_user)
# EXTRACT
movies = etl.fromcsv('dataset/ratings.csv', encoding='utf8')

# TRANSFORMATION
#users = etl.cut(movies, 'userId')
#users = etl.groupselectfirst(users, 'userId')
#users = etl.selectne(users, 'userId', '')
timestamps = etl.cut(movies, 'timestamp')
timestamps = etl.convert(timestamps, 'timestamp', lambda t: datetime.fromtimestamp(float(t)/1000.).strftime('%Y-%m-%d'))
timestamps = etl.groupselectfirst(timestamps, 'timestamp')

#print(users)
print(timestamps)

# LOAD
#etl.todb(table, cursor, 'd_user')