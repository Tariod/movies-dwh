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

# GET MOVIE FUNCTION (table: d_movie)
# EXTRACT
DATA_SOURCE_DIR = os.getenv('DATA_SOURCE_DIR')
movies = etl.fromcsv(DATA_SOURCE_DIR + 'movies_metadata.csv',
                     encoding='utf8',
                     errors='ignore')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'adult', 'belongs_to_collection',
                'original_language', 'original_title', 'title', 'imdb_id')

table = etl.convert(table, 'adult',
                    lambda category: category == 'True' or category == 'False')
age_category = etl.fromdb(conn, 'SELECT * from d_age_category')
age_category = etl.rename(age_category, 'id', 'id_age_category')
table = etl.join(table, age_category, key='adult')
table = etl.cutout(table, 'adult')

table = etl.split(table, 'belongs_to_collection', 'poster_path',
                  ['info', 'trash'])
table = etl.split(table, 'info', '\'name\':',
                  ['id_movie_franchise', 'name'])
table = etl.cut(table, 'id', 'id_age_category', 'name',
                'original_language', 'original_title', 'title', 'imdb_id')
table = etl.sub(table, 'name', '[,\'"]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')

franchises = etl.fromdb(conn, 'SELECT * from d_movie_franchise')
franchises = etl.rename(franchises, 'id', 'id_movie_franchise')
table = etl.outerjoin(table, franchises, 'name')
table = etl.cutout(table, 'name')

languages = etl.fromdb(conn, 'SELECT id, iso_639_1 from d_language')
languages = etl.rename(languages, 'id', 'id_original_language')
table = etl.join(table, languages, lkey='original_language', rkey='iso_639_1')
table = etl.cutout(table, 'original_language')

table = etl.rename(table, 'id', 'tmdb_id')
table = etl.distinct(table, 'tmdb_id')
table = etl.distinct(table, 'imdb_id')
table = etl.replace(table, 'title', None, 'No translation available')

# LOAD
etl.todb(table, cursor, 'd_movie')
