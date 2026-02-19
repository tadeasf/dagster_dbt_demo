with source as (
    select * from {{ source('raw', 'raw_posts') }}
),

renamed as (
    select
        id as post_id,
        "userId" as user_id,
        title,
        body
    from source
)

select * from renamed
