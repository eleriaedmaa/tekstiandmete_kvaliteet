-- Dokumendid grupeeritud avaldamiskuupäeva järgi (mitte laadimiskuupäeva).
-- Näitab, millal andmed tegelikult tekkisid — sobib korpuse ajalise katvuse kuvamiseks.

with paevased as (
    select
        allikas,
        avaldatud_kuupaev,
        count(*)                                                       as dokumente_kokku,
        sum(word_count)                                                as sonu_kokku,
        sum(case when kasutatav = 1 then word_count else 0 end)        as sonu_kasutatav,
        sum(kasutatav)                                                 as kasutatav_dokumente
    from {{ ref('fct_documents') }}
    where avaldatud_kuupaev is not null
    group by allikas, avaldatud_kuupaev
)

select
    allikas,
    avaldatud_kuupaev,
    dokumente_kokku,
    sonu_kokku,
    sonu_kasutatav,
    round(100.0 * kasutatav_dokumente / nullif(dokumente_kokku, 0), 1) as kasutatavuse_pct
from paevased
order by avaldatud_kuupaev desc, allikas
