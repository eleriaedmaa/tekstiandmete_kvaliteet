-- Wikipedia toorandmete puhastamine.

select
    'wikipedia'::text                              as allikas,
    doc_id,
    pealkiri,
    tekst,
    'artikkel'::text                               as dok_tyyp,
    avaldatud,
    allikas_url,
    laetud_kell,
    run_id
from {{ source('staging', 'wikipedia_raw') }}
where tekst is not null
  and length(trim(tekst)) > 0
