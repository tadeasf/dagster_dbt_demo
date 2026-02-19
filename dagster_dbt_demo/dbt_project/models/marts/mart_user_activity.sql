with users as (
    select * from {{ ref('stg_users') }}
),

posts as (
    select * from {{ ref('stg_posts') }}
),

comments as (
    select * from {{ ref('stg_comments') }}
),

post_stats as (
    select
        user_id,
        count(*) as total_posts,
        avg(length(body)) as avg_post_length
    from posts
    group by user_id
),

comment_stats as (
    select
        p.user_id,
        count(c.comment_id) as total_comments_received
    from comments c
    join posts p on c.post_id = p.post_id
    group by p.user_id
)

select
    u.user_id,
    u.full_name,
    u.username,
    u.email,
    u.company_name,
    u.city,
    coalesce(ps.total_posts, 0) as total_posts,
    coalesce(ps.avg_post_length, 0)::int as avg_post_length,
    coalesce(cs.total_comments_received, 0) as total_comments_received,
    case
        when coalesce(ps.total_posts, 0) > 0
        then round(coalesce(cs.total_comments_received, 0)::numeric / ps.total_posts, 2)
        else 0
    end as avg_comments_per_post
from users u
left join post_stats ps on u.user_id = ps.user_id
left join comment_stats cs on u.user_id = cs.user_id
