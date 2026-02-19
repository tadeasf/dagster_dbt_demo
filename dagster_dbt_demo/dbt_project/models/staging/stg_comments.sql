with source as (
    select * from {{ source('raw', 'raw_comments') }}
),

renamed as (
    select
        id as comment_id,
        "postId" as post_id,
        name as subject,
        email as commenter_email,
        body
    from source
)

select * from renamed
