-- Test pour vérifier que les heures d'ouverture sont inférieures aux heures de fermeture
select *
from {{ ref('dim_stores') }}
where opening_hour >= closing_hour
