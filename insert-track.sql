insert into tracks(name, length, album_id)
select '{}', {}, a.id
from albums as a
where a.name = '{}';