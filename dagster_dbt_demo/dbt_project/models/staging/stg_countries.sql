with source as (
    select * from {{ source('raw', 'raw_countries') }}
),

renamed as (
    select
        name_common as country_name,
        name_official as official_name,
        cca2 as country_code_2,
        cca3 as country_code_3,
        region,
        subregion,
        population,
        area_sq_km,
        capital,
        lat::float as latitude,
        lng::float as longitude,
        languages
    from source
)

select * from renamed
