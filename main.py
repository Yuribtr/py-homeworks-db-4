import sqlalchemy
import psycopg2
import csv


def read_query(filename):
    query_file = open(filename, mode='rt', encoding='utf-8')
    query_text = ''.join(query_file.readlines())
    query_file.close()
    return query_text


def read_data(filename):
    data_file = open(filename, mode='rt', encoding='utf-8')
    csv.register_dialect('MyDialect', delimiter=';')
    tmp = []
    reader = csv.DictReader(data_file, dialect='MyDialect')
    for item in reader:
        tmp.append(item)
    return tmp


DATA = read_data('demo-data.csv')

print('Connecting to DB...')
db = None
while True:
    try:
        db_name, login, psw = map(str, (input("""Введите через пробел имя базы, логин и пароль
или нажмите Enter чтобы заполнить их значениями 'test':""") or 'test test test').split())
        db = sqlalchemy.create_engine(f'postgresql://{login}:{psw}@localhost:5432/{db_name}')
        break
    except:
        continue
connection = db.connect()

print('Creating empty tables...')
connection.execute(read_query('create-tables.sql'))

print('\nAdding musicians...')
query = read_query('insert-musicians.sql')
res = connection.execute(query.format(','.join({f"('{x['musician']}')" for x in DATA})))
print(f'Inserted {res.rowcount} musicians.')

print('\nAdding genres...')
query = read_query('insert-genres.sql')
res = connection.execute(query.format(','.join({f"('{x['genre']}')" for x in DATA})))
print(f'Inserted {res.rowcount} genres.')

print('\nLinking musicians with genres...')
# assume that musicians names ar unique
genres_musicians = {x['musician']: x['genre'] for x in DATA}
query = read_query('insert-genre-musician.sql')
# this query can't be run in batch, so execute one by one
res = 0
for key, value in genres_musicians.items():
    res += connection.execute(query.format(value, key)).rowcount
print(f'Inserted {res} connections.')

print('\nAdding albums...')
# assume that albums ar unique
albums = {x['album']: x['album_year'] for x in DATA}
query = read_query('insert-albums.sql')
res = connection.execute(query.format(','.join({f"('{x}', '{y}')" for x, y in albums.items()})))
print(f'Inserted {res.rowcount} albums.')

print('\nLinking musicians with albums...')
# assume that musicians names ar unique
albums_musicians = {x['musician']: x['album'] for x in DATA}
query = read_query('insert-album-musician.sql')
# this query can't be run in batch, so execute one by one
res = 0
for key, value in albums_musicians.items():
    res += connection.execute(query.format(value, key)).rowcount
print(f'Inserted {res} connections.')

print('\nAdding tracks...')
query = read_query('insert-track.sql')
# this query can't be run in batch, so execute one by one
res = 0
for item in DATA:
    res += connection.execute(query.format(item['track'], item['length'], item['album'])).rowcount
print(f'Inserted {res} tracks.')

print('\nAdding collections...')
query = read_query('insert-collections.sql')
res = connection.execute(query.format(','.join({f"('{x['collection']}', {x['collection_year']})" for x in DATA})))
print(f'Inserted {res.rowcount} collections.')

print('\nLinking collections with tracks...')
query = read_query('insert-collection-track.sql')
# this query can't be run in batch, so execute one by one
res = 0
for item in DATA:
    res += connection.execute(query.format(item['collection'], item['track'])).rowcount
print(f'Inserted {res} connections.')
