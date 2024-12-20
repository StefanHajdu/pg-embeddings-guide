select * from youtube_comments limit 10;

create extension if not exists "uuid-ossp";
alter table youtube_comments add column idx uuid default (uuid_generate_v4());