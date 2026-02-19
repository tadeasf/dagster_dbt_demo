with source as (
    select * from {{ source('raw', 'raw_users') }}
),

renamed as (
    select
        id as user_id,
        name as full_name,
        username,
        email,
        phone,
        website,
        company_name,
        company_bs,
        city,
        street,
        zipcode,
        lat::float as latitude,
        lng::float as longitude
    from source
)

select * from renamed
