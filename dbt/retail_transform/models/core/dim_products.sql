with stg_products_data as (
    select * from {{ ref('stg_products') }}
)

select
    id as product_id,
    ean,
    brand,
    description
from stg_products_data
