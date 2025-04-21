with stg_transactions_data as (
    select * from {{ ref('stg_transactions') }}
),

stg_stores_data as (
    select * from {{ ref('stg_stores') }}
),

transactions_with_opening_hours as (
    select
        t.transaction_id,
        t.client_id,
        t.product_id,
        t.store_id,
        t.quantity,
        t.transaction_timestamp,
        t.date as transaction_date,
        t.hour as transaction_hour,
        t.minute as transaction_minute,
        t.account_id,
        t.is_orphan_store,
        s.opening,
        s.closing,
        case
            when t.hour between s.opening and s.closing then true
            else false
        end as is_during_opening_hours
    from stg_transactions_data t
    left join stg_stores_data s on t.store_id = s.id
)

select
    transaction_id,
    client_id,
    product_id,
    store_id,
    quantity,
    transaction_timestamp,
    transaction_date,
    transaction_hour,
    transaction_minute,
    account_id,
    is_orphan_store,
    is_during_opening_hours
from transactions_with_opening_hours
