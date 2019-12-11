from dotenv import load_dotenv
import json
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

# GET CAST FUNCTION (table: d_cast)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'credits.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'cast')
table = etl.rename(table, 'id', 'movie_tmdb_id')
table = etl.sub(table, 'cast', '^\\[', '')
table = etl.sub(table, 'cast', '\\]$', '')
table = etl.selectnotnone(table, 'cast')
table = etl.splitdown(table, 'cast', '(?<=\\}),\\s(?=\\{)')
table = etl.sub(table, 'cast', '\'', '"')
table = etl.convert(table, 'cast', lambda row: json.loads(row))
table = etl.unpackdict(table, 'cast')
table = etl.cutout(table, 'cast_id', 'credit_id', 'name', 'order',
                   'profile_path')
table = etl.rename(table, {'id': 'person_tmdb_id', 'gender': 'id_gender'})
table = etl.selectnotnone(table, 'movie_tmdb_id')
table = etl.selectnotnone(table, 'person_tmdb_id')
table = etl.convert(table, 'person_tmdb_id', str)

source_genders = [['id_gender', 'gender'],
                  [0, 'Unspecified'],
                  [1, 'Female'],
                  [2, 'Male']]
table = etl.join(table, source_genders, key='id_gender')
table = etl.cutout(table, 'id_gender')
gender = etl.fromdb(conn, 'SELECT * from d_gender')
gender = etl.rename(gender, {'id': 'id_gender', 'name': 'gender'})
table = etl.join(table, gender, 'gender')
table = etl.cutout(table, 'gender')

table = etl.addfield(table,
                     'fk_character',
                     lambda rec: str(rec['id_gender'])+' '+rec['character'])
table = etl.cutout(table, 'id_gender', 'character')

characters = etl.fromdb(
    conn,
    'SELECT id as id_character, id_gender, name from d_character')
characters = etl.addfield(
    characters,
    'pk_character',
    lambda rec: str(rec['id_gender']) + ' ' + rec['name'])
characters = etl.cutout(characters, 'id_gender', 'name')
table = etl.join(table, characters, lkey='fk_character', rkey='pk_character')
table = etl.cutout(table, 'fk_character')

movies = etl.fromdb(conn, 'SELECT id, tmdb_id from d_movie')
movies = etl.rename(movies, 'id', 'id_movie')
table = etl.join(table, movies, lkey='movie_tmdb_id', rkey='tmdb_id')
table = etl.cutout(table, 'movie_tmdb_id')

people = etl.fromdb(conn, 'SELECT id, tmdb_id from d_people')
people = etl.rename(people, {'id': 'id_people', 'tmdb_id': 'person_tmdb_id'})
table = etl.join(table, people, key='person_tmdb_id')
table = etl.cutout(table, 'person_tmdb_id')

# LOAD
etl.todb(table, cursor, 'd_cast')
