import petl as etl
import psycopg2


conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET USER FUNCTION (table: d_user)
# EXTRACT
movies = etl.fromcsv('dataset/ratings.csv', encoding='utf8')

# TRANSFORMATION
userids = list(range(1, 10657))
vals = list('u' + str(x) for x in range(1, 10657))

table = [userids, vals]
table = etl.fromcolumns(table)
table = etl.rename(table, 'f0', 'id')
table = etl.rename(table, 'f1', 'abstract_name')

# LOAD
etl.todb(table, cursor, 'd_user')
