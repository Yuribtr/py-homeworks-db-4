insert into albums_musicians(album_id, musician_id)
select a.id, m.id
from albums as a, musicians as m
where a.name = '{}' and m.name = '{}';