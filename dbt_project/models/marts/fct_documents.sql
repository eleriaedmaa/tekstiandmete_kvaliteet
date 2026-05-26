-- Faktitabel: üks rida = üks dokument kõigist allikatest.
-- Sisaldab kvaliteedilippe — mart_source_quality arvutab nendest mõõdikud.

select
    allikas,
    doc_id,
    pealkiri,
    dok_tyyp,
    avaldatud,
    avaldatud_kuupaev,
    laetud_kell,
    laetud_kuupaev,
    allikas_url,
    word_count,
    is_long_enough,
    is_estonian,
    is_not_duplicate,
    (is_long_enough * is_estonian * is_not_duplicate) as kasutatav,
    peamine_pohjus,
    run_id
from {{ ref('int_documents') }}
