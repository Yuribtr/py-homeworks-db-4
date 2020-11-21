insert into genres_musicians(genre_id, musician_id)
select g.id, m.id
from genres as g, musicians as m
where g.name = '{}' and m.name = '{}';