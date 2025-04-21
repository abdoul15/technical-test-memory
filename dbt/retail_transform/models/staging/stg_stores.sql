with source as (
    select * from {{ source('raw', 'stores') }}
),

transformed as (
    select
        id,
        latlng,
        cast(split_part(replace(replace(latlng, '(', ''), ')', ''), ',', 1) as float) as latitude,
        cast(split_part(replace(replace(latlng, '(', ''), ')', ''), ',', 2) as float) as longitude,
        opening,
        closing,
        type
    from source
)

select
    id,
    latlng,
    latitude,
    longitude,
    opening,
    closing,
    type
from transformed
