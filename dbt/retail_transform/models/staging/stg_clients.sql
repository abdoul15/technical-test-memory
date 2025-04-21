with source as (
    select * from {{ source('raw', 'clients') }}
),

transformed as (
    select
        id,
        name,
        job,
        email,
        account_id
    from source
),

deduplicated as (
    select
        *,
        row_number() over (partition by id order by name) as row_num
    from transformed
)

select
    id,
    name,
    job,
    email,
    account_id
from deduplicated
where row_num = 1  
