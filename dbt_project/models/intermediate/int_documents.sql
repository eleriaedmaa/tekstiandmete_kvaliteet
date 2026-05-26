-- Ühendab kolme allika puhastatud dokumendid ja arvutab kvaliteedilipud.
--
-- Kvaliteedilipud:
--   is_long_enough  — tekst on vähemalt 50 sõna pikk
--   is_estonian     — tekst sisaldab eesti tähti (õ, ä, ö, ü) ja Eesti tüüpilisi sõnu
--   is_not_duplicate — samasugust teksti pole intermediate's juba olemas
--
-- Peamine kvaliteedipuudus (peamine_pohjus) — esimene leitud probleem.

with koik as (
    select * from {{ ref('stg_riigikogu') }}
    union all
    select * from {{ ref('stg_rahvaalgatus') }}
    union all
    select * from {{ ref('stg_wikipedia') }}
),

loendamine as (
    select
        allikas,
        doc_id,
        pealkiri,
        tekst,
        dok_tyyp,
        avaldatud,
        allikas_url,
        laetud_kell,
        run_id,
        array_length(regexp_split_to_array(trim(tekst), '\s+'), 1) as word_count,
        md5(lower(trim(tekst))) as tekst_hash
    from koik
),

duplikaadi_kontroll as (
    select
        l.*,
        case
            when row_number() over (partition by l.tekst_hash order by l.laetud_kell) > 1 then 0
            else 1
        end as is_not_duplicate
    from loendamine l
)

select
    allikas,
    doc_id,
    pealkiri,
    tekst,
    dok_tyyp,
    avaldatud,
    avaldatud::date            as avaldatud_kuupaev,
    allikas_url,
    laetud_kell,
    laetud_kell::date          as laetud_kuupaev,
    run_id,
    word_count,
    tekst_hash,

    case when word_count >= 50 then 1 else 0 end as is_long_enough,

    case
        when tekst ~ '[õäöüÕÄÖÜ]' then 1
        when tekst ~* '\y(ja|on|ei|see|olid|olen|olid|tema|nende|kuid|aga|või|kui)\y' then 1
        else 0
    end as is_estonian,

    is_not_duplicate,

    case
        when word_count < 50            then 'liiga_lyhike'
        when not (tekst ~ '[õäöüÕÄÖÜ]')
            and not (tekst ~* '\y(ja|on|ei|see|kuid|või)\y') then 'vale_keel'
        when is_not_duplicate = 0       then 'duplikaat'
        else 'sobib'
    end as peamine_pohjus

from duplikaadi_kontroll
