-- Riigikogu toorandmete puhastamine: standardiseerib veerunimed,
-- tühjad tekstid ja pealkirjad filtreerib välja.

select
    'riigikogu'::text                              as allikas,
    doc_id,
    pealkiri,
    tekst,
    dok_tyyp,
    avaldatud,
    allikas_url,
    laetud_kell,
    run_id
from {{ source('staging', 'riigikogu_raw') }}
where tekst is not null
  and length(trim(tekst)) > 0
