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
table = etl.split(table, 'department', '\'id\':', ['department', 'tmdb_id'])
table = etl.split(table, 'department', '\'gender\':', ['department', 'trash'])
table = etl.cut(table, 'id', 'department', 'job', 'tmdb_id')
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
table = etl.sub(table, 'tmdb_id', '[ ,\']', '')
table = etl.sub(table, 'tmdb_id', ' ', '')
table = etl.selectnotnone(table, 'tmdb_id')
table = etl.sub(table, 'id', '[ ,\']', '')
table = etl.sub(table, 'id', ' ', '')
table = etl.selectnotnone(table, 'id')

departments = etl.fromdb(conn, 'SELECT * from d_department')
departments = etl.rename(departments, 'id', 'id_department')
table = etl.join(table, departments, lkey='department', rkey='name')
table = etl.cutout(table, 'department')

jobs = etl.fromdb(conn, 'SELECT * from d_job')
jobs = etl.rename(jobs, 'id', 'id_job')
table = etl.join(table, jobs, lkey='job', rkey='name')
table = etl.cutout(table, 'job')

movies = etl.fromdb(conn, 'SELECT id, tmdb_id from d_movie')
movies = etl.rename(movies, 'id', 'id_movie')
table = etl.join(table, movies, lkey='id', rkey='tmdb_id')
table = etl.cutout(table, 'id')

people = etl.fromdb(conn, 'SELECT id, tmdb_id from d_people')
people = etl.rename(people, 'id', 'id_people')
table = etl.join(table, people, key='tmdb_id')
table = etl.cutout(table, 'tmdb_id')

table = etl.convert(table, 'id_movie', int)

# LOAD
etl.todb(table, cursor, 'd_crew')
