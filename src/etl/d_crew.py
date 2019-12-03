import petl as etl
import csv
import psycopg2
from collections import OrderedDict

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET CREW FUNCTION (table: d_crew)
# EXTRACT
movies = etl.fromcsv('dataset/credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'crew')
table = etl.splitdown(table, 'crew', '}')
table = etl.selectcontains(table, 'crew', 'name')
table = etl.split(table, 'crew', '\'department\':', ['trash', 'info'])
table = etl.cut(table, 'id', 'info')
table = etl.split(table, 'info', '\'job\':', ['department', 'job'])
table = etl.split(table, 'job', '\'name\':', ['job', 'trash'])
table = etl.cut(table, 'id', 'department', 'job')
table = etl.split(table, 'department', '\'id\':', ['department', 'id_people'])
table = etl.split(table, 'department', '\'gender\':', ['department', 'trash'])
table = etl.cut(table, 'id', 'department', 'job', 'id_people')
table = etl.sub(table, 'department', '[,\']', '')
table = etl.convert(table, 'department', str)
table = etl.sub(table, 'department', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'department', '"', '')
table = etl.selectne(table, 'department', '')
table = etl.sub(table, 'job', '[,\']', '')
table = etl.convert(table, 'job', str)
table = etl.sub(table, 'job', '(^[ ]+)|[ ]+$', '')
table = etl.sub(table, 'job', '"', '')
table = etl.selectne(table, 'job', '')
table = etl.sub(table, 'id_people', '[ ,\']', '')
table = etl.sub(table, 'id_people', ' ', '')
table = etl.selectne(table, 'id_people', None)
table = etl.convert(table, 'id_people', int)
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectne(table, 'id', None)

departments = etl.fromdb(conn, 'SELECT * from d_department')
departments = dict(etl.data(departments))
departments_map = {departments[k] : k for k in departments}

jobs = etl.fromdb(conn, 'SELECT * from d_job')
jobs = dict(etl.data(jobs))
jobs_map = {jobs[k] : k for k in jobs}

mappings = OrderedDict()
mappings['id_movie'] = 'id'
mappings['id_department'] = 'department', departments_map
mappings['id_job'] = 'job', jobs_map
mappings['id_people'] = 'id_people'
table = etl.fieldmap(table, mappings)

print(table)
# LOAD
etl.todb(table, cursor, 'd_crew')