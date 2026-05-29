-- Rahvaalgatuse toorandmete puhastamine.
-- Välistab algatused, mis on juba seemneandmetes olemas (vanad andmed).

select
    'rahvaalgatus'::text                           as allikas,
    doc_id,
    pealkiri,
    tekst,
    staatus                                        as dok_tyyp,
    case when avaldatud > current_timestamp then laetud_kell else avaldatud end as avaldatud,
    allikas_url,
    laetud_kell,
    run_id
from {{ source('staging', 'rahvaalgatus_raw') }}
where tekst is not null
  and length(trim(tekst)) > 0
  and allikas_url not in (
      select allikas_url
      from {{ source('staging', 'rahvaalgatus_raw') }}
      where doc_id like 'seed_%'
  )
