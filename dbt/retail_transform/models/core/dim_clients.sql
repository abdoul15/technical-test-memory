with stg_clients_data as (
    select * from {{ ref('stg_clients') }}
)

select
    id as client_id,
    name,
    job,
    email,
    account_id
from stg_clients_data
