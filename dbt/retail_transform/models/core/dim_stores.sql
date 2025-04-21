with stg_stores_data as (
    select * from {{ ref('stg_stores') }}
)

select
    id as store_id,
    latlng,
    latitude,
    longitude,
    opening as opening_hour,
    closing as closing_hour,
    type as store_type
from stg_stores_data
