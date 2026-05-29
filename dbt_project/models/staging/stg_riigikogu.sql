-- Riigikogu toorandmete puhastamine: standardiseerib veerunimed,
-- tühjad tekstid ja pealkirjad filtreerib välja.
-- Välistab stenogrammid, mis on juba seemneandmetes olemas (vanad andmed).

with seed_koodid as (
    select substring(allikas_url, '\d{12}') as kood
    from {{ source('staging', 'riigikogu_raw') }}
    where doc_id like 'seed_%'
      and allikas_url like '%stenogrammid.riigikogu.ee%'
      and substring(allikas_url, '\d{12}') is not null
)

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
  and not exists (
      select 1 from seed_koodid
      where seed_koodid.kood = substring(riigikogu_raw.allikas_url, '\d{12}')
  )
