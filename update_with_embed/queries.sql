select embed from youtube_comments limit 100;
select embed from youtube_comments where idx = 'cc0a85e4-64dc-45ee-b212-9f4be765b5fe';
select * from youtube_comments where idx = 'b0354709-4fb0-4c4d-9d68-05364eeb3ef8';


ALTER TABLE youtube_comments DROP COLUMN embed; 
ALTER TABLE youtube_comments ADD embed vector(768);

DROP INDEX pk_index;
CREATE UNIQUE INDEX pk_index
    ON public.youtube_comments USING btree
    (idx)
    WITH (deduplicate_items=False)
;

select count(*) from youtube_comments where embed is not null;

create extension if not exists "uuid-ossp";
alter table youtube_comments add column idx uuid default (uuid_generate_v4());