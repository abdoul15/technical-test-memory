with stg_transactions_data as (
    select * from {{ ref('stg_transactions') }}
)

select
    transaction_id,
    client_id,
    product_id,
    store_id,
    quantity,
    transaction_timestamp,
    date as transaction_date,
    hour as transaction_hour,
    minute as transaction_minute,
    account_id,
    is_orphan_store
from stg_transactions_data
