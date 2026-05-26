-- Rahvaalgatuse toorandmete puhastamine.

select
    'rahvaalgatus'::text                           as allikas,
    doc_id,
    pealkiri,
    tekst,
    staatus                                        as dok_tyyp,
    avaldatud,
    allikas_url,
    laetud_kell,
    run_id
from {{ source('staging', 'rahvaalgatus_raw') }}
where tekst is not null
  and length(trim(tekst)) > 0
