-- Päevased mõõdikud allika lõikes — see on näidikulaua põhitabel.
--
-- Veerud:
--   dokumente_kokku          — sissevõetud dokumentide arv päevas
--   sonu_kokku               — kogusõnade arv (toorandmed)
--   sonu_kasutatav           — kvaliteedikontrolli läbinud dokumentide sõnade arv
--   kasutatavuse_pct         — kasutatavate dokumentide osakaal (0–100)
--   liiga_lyhike_pct         — mitu % dokumentidest jäid liiga lühikeseks
--   vale_keel_pct            — mitu % dokumentidest ei tuvastatud eesti keelena
--   duplikaat_pct            — mitu % dokumentidest oli duplikaat
--   peamine_kvaliteedipuudus — kõige sagedasem puudus

with paevased as (
    select
        allikas,
        laetud_kuupaev,
        count(*)                                                       as dokumente_kokku,
        sum(word_count)                                                as sonu_kokku,
        sum(case when kasutatav = 1 then word_count else 0 end)        as sonu_kasutatav,
        sum(kasutatav)                                                 as kasutatav_dokumente,
        sum(case when peamine_pohjus = 'liiga_lyhike' then 1 else 0 end) as liiga_lyhike,
        sum(case when peamine_pohjus = 'vale_keel'   then 1 else 0 end) as vale_keel,
        sum(case when peamine_pohjus = 'duplikaat'   then 1 else 0 end) as duplikaat
    from {{ ref('fct_documents') }}
    group by allikas, laetud_kuupaev
)

select
    allikas,
    laetud_kuupaev,
    dokumente_kokku,
    sonu_kokku,
    sonu_kasutatav,
    round(100.0 * kasutatav_dokumente / nullif(dokumente_kokku, 0), 1) as kasutatavuse_pct,
    round(100.0 * liiga_lyhike       / nullif(dokumente_kokku, 0), 1) as liiga_lyhike_pct,
    round(100.0 * vale_keel          / nullif(dokumente_kokku, 0), 1) as vale_keel_pct,
    round(100.0 * duplikaat          / nullif(dokumente_kokku, 0), 1) as duplikaat_pct,
    case
        when liiga_lyhike >= vale_keel and liiga_lyhike >= duplikaat and liiga_lyhike > 0 then 'liiga_lyhike'
        when vale_keel    >= duplikaat and vale_keel > 0                                   then 'vale_keel'
        when duplikaat > 0                                                                 then 'duplikaat'
        else                                                                                    'puudus_puudub'
    end as peamine_kvaliteedipuudus
from paevased
order by laetud_kuupaev desc, allikas
