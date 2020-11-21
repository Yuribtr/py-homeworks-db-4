insert into collections_tracks(collection_id, track_id)
select c.id, t.id
from collections as c, tracks as t
where c.name = '{}' and t.name = '{}';