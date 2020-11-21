create table if not exists genres (
  id serial primary key,
  name varchar(20)
);

create table if not exists musicians (
  id serial primary key,
  name varchar(50) not null
);

create table if not exists genres_musicians (
  genre_id int references genres(id),
  musician_id int references musicians(id),
  primary key (genre_id, musician_id)
);

create table if not exists albums (
  id serial primary key,
  name varchar(50) not null,
  year int check (year > 1900 and year <= 2021)
);

create table if not exists albums_musicians (
  album_id int references albums(id),
  musician_id int references musicians(id),
  primary key (album_id, musician_id)
);

create table if not exists tracks (
  id serial primary key,
  name varchar(100) not null,
  length int check (length > 0),
  album_id int references albums(id)
);

create table if not exists collections (
  id serial primary key,
  name varchar(50) not null,
  year int check (year > 1900 and year <= 2021)
);

create table if not exists collections_tracks (
  collection_id int references collections(id),
  track_id int references tracks(id),
  primary key (collection_id, track_id)
);

truncate table collections_tracks cascade;
truncate table collections cascade;
truncate table tracks cascade;
truncate table albums_musicians cascade;
truncate table albums cascade;
truncate table genres_musicians cascade;
truncate table musicians cascade;
truncate table genres cascade;