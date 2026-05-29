-- Wikipedia toorandmete puhastamine.
-- Iga artikli kõige uuem versioon (revid järgi), et statistikasse ei läheks mitu versiooni.
-- Välistab artiklid, mille pealkiri on seedis (teadaolevad_dokumendid).

with viimased as (
    select *,
        row_number() over (
            partition by allikas_url
            order by revid desc
        ) as rn
    from {{ source('staging', 'wikipedia_raw') }}
    where tekst is not null
      and length(trim(tekst)) > 0
),

seed_pealkirjad as (
    select pealkiri
    from {{ ref('teadaolevad_dokumendid') }}
    where allikas = 'wiki'
      and pealkiri != ''
)

select
    'wikipedia'::text  as allikas,
    doc_id,
    pealkiri,
    tekst,
    'artikkel'::text   as dok_tyyp,
    coalesce(loomise_kuupaev, avaldatud) as avaldatud,
    allikas_url,
    laetud_kell,
    run_id
from viimased
where rn = 1
  and pealkiri not in (select pealkiri from seed_pealkirjad)
