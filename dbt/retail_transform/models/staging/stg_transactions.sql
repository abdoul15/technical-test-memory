with source as (
    select * from {{ source('raw', 'transactions') }}
),

transformed as (
    select
        transaction_id,
        client_id,
        date,
        hour,
        minute,
        product_id,
        quantity,
        store_id,
        try_to_timestamp(concat(date, ' ', lpad(hour, 2, '0'), ':', lpad(minute, 2, '0'), ':00'), 'YYYY-MM-DD HH24:MI:SS') as transaction_timestamp
    from source
),

-- Récupération des account_id des clients
with_account_id as (
    select
        t.*,
        c.account_id
    from transformed t
    left join {{ ref('stg_clients') }} c on t.client_id = c.id
),

-- Vérification des store_id orphelins
with_store_check as (
    select
        w.*,
        case 
            when s.id is null then true
            else false
        end as is_orphan_store
    from with_account_id w
    left join {{ ref('stg_stores') }} s on w.store_id = s.id
),

-- Gestion des doublons de transaction_id
deduplicated as (
    select
        *,
        row_number() over (partition by transaction_id order by transaction_timestamp) as row_num
    from with_store_check
)

select
    transaction_id,
    client_id,
    date,
    hour,
    minute,
    product_id,
    quantity,
    store_id,
    transaction_timestamp,
    account_id,
    is_orphan_store
from deduplicated
where row_num = 1  
