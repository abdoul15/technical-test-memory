-- Ce test échoue si :
-- 1. L'heure d'ouverture est supérieure ou égale à l'heure de fermeture
-- 2. L'heure d'ouverture n'est pas un entier entre 0 et 23
-- 3. L'heure de fermeture n'est pas un entier entre 0 et 23

select
    id,
    opening,
    closing
from {{ ref('stg_stores') }}
where opening >= closing  -- Une boutique n'ouvre pas après sa fermeture
   or opening < 0 or opening > 23  -- L'heure d'ouverture doit être entre 0 et 23
   or closing < 0 or closing > 23  -- L'heure de fermeture doit être entre 0 et 23
