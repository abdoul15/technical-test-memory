with stg_stores_data as (
    select * from {{ ref('stg_stores') }}
)

select
    id as store_id,
    latlng,
    latitude,
    longitude,
    opening,
    closing,
    type as store_type
from stg_stores_data
