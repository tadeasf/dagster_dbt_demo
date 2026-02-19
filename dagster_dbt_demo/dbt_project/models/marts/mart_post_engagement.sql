with posts as (
    select * from {{ ref('stg_posts') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

comments as (
    select * from {{ ref('stg_comments') }}
),

comment_agg as (
    select
        post_id,
        count(*) as comment_count,
        count(distinct commenter_email) as unique_commenters
    from comments
    group by post_id
)

select
    p.post_id,
    p.title,
    p.user_id,
    u.username as author_username,
    u.company_name as author_company,
    length(p.body) as post_length,
    coalesce(ca.comment_count, 0) as comment_count,
    coalesce(ca.unique_commenters, 0) as unique_commenters,
    case
        when coalesce(ca.comment_count, 0) >= 5 then 'high'
        when coalesce(ca.comment_count, 0) >= 3 then 'medium'
        else 'low'
    end as engagement_tier
from posts p
join users u on p.user_id = u.user_id
left join comment_agg ca on p.post_id = ca.post_id
