from collections import OrderedDict
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

# GET CREW FUNCTION (table: d_crew)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'credits.csv', encoding='utf8')

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
departments_map = {departments[k]: k for k in departments}

jobs = etl.fromdb(conn, 'SELECT * from d_job')
jobs = dict(etl.data(jobs))
jobs_map = {jobs[k]: k for k in jobs}

movies = etl.fromdb(conn, 'SELECT * from d_movie')
movies = etl.cut(movies, 'id', 'tmdb_id')
movies = dict(etl.data(movies))
movies_map = {movies[k]: k for k in movies}

people = etl.fromdb(conn, 'SELECT * from d_people')
people = etl.cut(people, 'id', 'name')
people = dict(etl.data(people))
people_map = {people[k]: k for k in people}

mappings = OrderedDict()
mappings['id_movie'] = 'id', movies_map
mappings['id_department'] = 'department', departments_map
mappings['id_job'] = 'job', jobs_map
mappings['id_people'] = 'id_people', people_map
table = etl.fieldmap(table, mappings)

table = etl.convert(table, 'id_movie', str)
table = etl.select(table, lambda rec: '-' not in rec.id_movie)

# LOAD
etl.todb(table, cursor, 'd_crew')
