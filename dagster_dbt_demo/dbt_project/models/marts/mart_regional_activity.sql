with countries as (
    select * from {{ ref('stg_countries') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

posts as (
    select * from {{ ref('stg_posts') }}
),

comments as (
    select * from {{ ref('stg_comments') }}
),

user_post_counts as (
    select
        user_id,
        count(*) as post_count
    from posts
    group by user_id
),

user_comment_counts as (
    select
        p.user_id,
        count(c.comment_id) as comments_received
    from comments c
    join posts p on c.post_id = p.post_id
    group by p.user_id
),

country_stats as (
    select
        c.region,
        c.subregion,
        count(distinct c.country_code_2) as country_count,
        sum(c.population) as total_population,
        sum(c.area_sq_km) as total_area_sq_km,
        round(avg(c.population)::numeric, 0) as avg_population
    from countries c
    group by c.region, c.subregion
)

select
    cs.region,
    cs.subregion,
    cs.country_count,
    cs.total_population,
    cs.total_area_sq_km,
    cs.avg_population,
    case
        when cs.total_area_sq_km > 0
        then round((cs.total_population / cs.total_area_sq_km)::numeric, 2)
        else 0
    end as population_density
from country_stats cs
order by cs.total_population desc
