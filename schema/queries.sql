
set search_path=digible_schema;


-- matches
with map_count as (
    select count(*) as map_count
    from mapping_table
), sql_count as (
    select count(*) as sql_count
    from sqlserver_table
) select
map_count.map_count,
sql_count.sql_count,
(map_count.map_count::float / sql_count.sql_count::float) * 100.0 as percentage
from map_count
cross join sql_count;


-- histogram
select (address_similarity + apt_name_similarity)::numeric(2,1), count(*) from mapping_table group by 1 order by 1;


-- 1.4 sample
with base as (
select
      q.property_id
    , q.apt_name as qapt_name
    , s.raw_apt_name as sapt_name
    , q.address_line1 as qaddress
    , s.raw_address as saddress
    , q.zip as qzip
    , s.zip as szip
    , m.address_similarity
    , m.apt_name_similarity
    , (apt_name_similarity + address_similarity)::numeric(2,1) as total_score
    , row_number() over (partition by q.property_id) as the_row
 from sqlserver_table q
inner join mapping_table m
   on m.property_id=q.property_id
inner join snowflake_table s 
   on s.address_hash = m.address_hash
),  ranked_scores as (
 select 
      total_score as score
    , qapt_name as sql_server_apt
    , qaddress as sql_server_address
--    , qzip as sql_zip
    , sapt_name as snowflake_apt
    , saddress as snowflake_address
--     , szip as snow_zip
    , row_number() over (partition by total_score order by random()) as score_sample
 from base
where the_row = 1
--  and total_score IN(0.6,  1.4, 1.6, 2.0)
) 
select * from ranked_scores
where score_sample < 5
order by score;

