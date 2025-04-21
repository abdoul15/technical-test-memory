with source as (
    select * from {{ source('raw', 'products') }}
),

transformed as (
    select
        id,
        ean,
        brand,
        description
    from source
)

select
    id,
    ean,
    brand,
    description
from transformed
