-- Wikipedia toorandmete puhastamine.
-- Iga artikli kõige uuem versioon (revid järgi), et statistikasse ei läheks mitu versiooni.

with viimased as (
    select *,
        row_number() over (
            partition by allikas_url
            order by revid desc
        ) as rn
    from {{ source('staging', 'wikipedia_raw') }}
    where tekst is not null
      and length(trim(tekst)) > 0
)

select
    'wikipedia'::text  as allikas,
    doc_id,
    pealkiri,
    tekst,
    'artikkel'::text   as dok_tyyp,
    avaldatud,
    allikas_url,
    laetud_kell,
    run_id
from viimased
where rn = 1
