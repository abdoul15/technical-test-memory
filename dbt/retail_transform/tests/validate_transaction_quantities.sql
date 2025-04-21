-- Test pour vérifier que les quantités dans les transactions sont positives
select *
from {{ ref('fct_transactions') }}
where quantity < 0
