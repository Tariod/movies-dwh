import petl as etl
import psycopg2

conn_string = "dbname='movies_dwh' user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# GET CAST FUNCTION (table: d_cast)
# EXTRACT
movies = etl.fromcsv('dataset/movies_metadata.csv', encoding='utf8')

# TRANSFORMATION
table = etl.cut(movies, 'id', 'genres')
table = etl.splitdown(table, 'genres', '}')
table = etl.selectcontains(table, 'genres', 'id')
table = etl.split(table, 'genres', '\'name\':', ['id_genre', 'name'])
table = etl.cut(table, 'id', 'name')
table = etl.sub(table, 'name', '[,\'\]]', '')
table = etl.sub(table, 'name', '(^[ ]+)|[ ]+$', '')

movies = etl.fromdb(conn, 'SELECT * from d_movie')
movies = etl.cut(movies, 'id', 'tmdb_id')
movies = dict(etl.data(movies))
movies_map = {movies[k] : k for k in movies}

genres = etl.fromdb(conn, 'SELECT * from d_genre')
genres = etl.cut(genres, 'id', 'title')
genres = dict(etl.data(genres))
genres_map = {genres[k] : k for k in genres}

mappings = OrderedDict()
mappings['id_movie'] = 'id', movies_map
mappings['id_genre'] = 'name', genres_map
table = etl.fieldmap(table, mappings)

table = etl.convert(table, 'id_movie', str)
table = etl.select(table, lambda rec: '-' not in rec.id_movie)

# LOAD
etl.todb(table, cursor, 'movie_genres')
