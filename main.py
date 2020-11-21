import sqlalchemy
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
connection.execute(read_query('queries/create-tables.sql'))

print('\nAdding musicians...')
query = read_query('queries/insert-musicians.sql')
res = connection.execute(query.format(','.join({f"('{x['musician']}')" for x in DATA})))
print(f'Inserted {res.rowcount} musicians.')

print('\nAdding genres...')
query = read_query('queries/insert-genres.sql')
res = connection.execute(query.format(','.join({f"('{x['genre']}')" for x in DATA})))
print(f'Inserted {res.rowcount} genres.')

print('\nLinking musicians with genres...')
# assume that musician + genre has to be unique
genres_musicians = {x['musician'] + x['genre']: [x['musician'], x['genre']] for x in DATA}
query = read_query('queries/insert-genre-musician.sql')
# this query can't be run in batch, so execute one by one
res = 0
for key, value in genres_musicians.items():
    res += connection.execute(query.format(value[1], value[0])).rowcount
print(f'Inserted {res} connections.')

print('\nAdding albums...')
# assume that albums ar unique
albums = {x['album']: x['album_year'] for x in DATA}
query = read_query('queries/insert-albums.sql')
res = connection.execute(query.format(','.join({f"('{x}', '{y}')" for x, y in albums.items()})))
print(f'Inserted {res.rowcount} albums.')

print('\nLinking musicians with albums...')
# assume that musicians + album has to be unique
albums_musicians = {x['musician'] + x['album']: [x['musician'], x['album']] for x in DATA}
query = read_query('queries/insert-album-musician.sql')
# this query can't be run in batch, so execute one by one
res = 0
for key, values in albums_musicians.items():
    res += connection.execute(query.format(values[1], values[0])).rowcount
print(f'Inserted {res} connections.')

print('\nAdding tracks...')
query = read_query('queries/insert-track.sql')
# this query can't be run in batch, so execute one by one
res = 0
for item in DATA:
    res += connection.execute(query.format(item['track'], item['length'], item['album'])).rowcount
print(f'Inserted {res} tracks.')

print('\nAdding collections...')
query = read_query('queries/insert-collections.sql')
res = connection.execute(query.format(','.join({f"('{x['collection']}', {x['collection_year']})" for x in DATA})))
print(f'Inserted {res.rowcount} collections.')

print('\nLinking collections with tracks...')
query = read_query('queries/insert-collection-track.sql')
# this query can't be run in batch, so execute one by one
res = 0
for item in DATA:
    res += connection.execute(query.format(item['collection'], item['track'])).rowcount
print(f'Inserted {res} connections.')

print('\nDatabase ready, let\'s have some fun...')

print('\nAll albums from 2018:')
query = read_query('queries/select-album-by-year.sql')
res = connection.execute(query.format(2018))
print(*res, sep='\n')

print('\nLongest track:')
query = read_query('queries/select-longest-track.sql')
res = connection.execute(query)
print(*res, sep='\n')

print('\nTracks with length not less 3.5min:')
query = read_query('queries/select-tracks-over-length.sql')
res = connection.execute(query.format(310))
print(*res, sep='\n')

print('\nCollections between 2018 and 2020 years (inclusive):')
query = read_query('queries/select-collections-by-year.sql')
res = connection.execute(query.format(2018, 2020))
print(*res, sep='\n')

print('\nMusicians with name that contains not more 1 word:')
query = read_query('queries/select-musicians-by-name.sql')
res = connection.execute(query)
print(*res, sep='\n')

print('\nTracks that contains word "me" in name:')
query = read_query('queries/select-tracks-by-name.sql')
res = connection.execute(query.format('me'))
print(*res, sep='\n')
