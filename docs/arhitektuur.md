# Arhitektuur

## Äriküsimus

Kui palju kvaliteetset eestikeelset teksti on võimalik regulaarselt koguda valitud avalikest andmeallikatest?

## Mõõdikud

1. Uute sõnade lisandumine ajas allika kohta — näitab mahtu ja aitab tuvastada optimaalse kogumissageduse.
2. Kasutatavuse % allika kohta — kui suur osa kogutud tekstist läbib kvaliteedikontrolli.
3. Peamised kvaliteedipuudused allika kohta — miks tekst ei kvalifitseeru (tekst liiga lühike, vale keel, duplikaat jne).

## Andmeallikad

| Allikas | Tüüp | Muutuvus ajas | Kasutus |
|---|---|---|---|
| Riigikogu API | Avalik HTTP API | Uueneb istungipäevadel | Põhiandmevoog — istungite dokumendid ja stenogrammid |
| Rahvaalgatus.ee API + scraper | Avalik HTTP API + HTML scraper | Uueneb reaalajas | Põhiandmevoog — algatuste metaandmed (API) ja täistekst (scraper) |
| Eesti Wikipedia | MediaWiki HTTP API | Uueneb reaalajas | Põhiandmevoog — artiklite täistekst |
| `seeds/allikad.csv` | Staatiline dbt seed | Muutub ainult kui lisandub uus allikas | Allikate nimekiri, URL-id, kogumissagedus |
| `seeds/teadaolevad_dokumendid.csv` | Staatiline dbt seed | Ei muutu pärast esimest käivitust | Olemasolevate dokumentide URL-id — duplikaatide vältimiseks esimesel ingest-käivitusel |

Kõik kolm allikat on avalikud ja ei nõua autentimist. Rahvaalgatus.ee puhul tagastab API ainult metaandmed; täistekst tõmmatakse eraldi HTTP scraperile avalikelt lehekülgedelt (`robots.txt`: `Disallow:` — kõik lubatud).

## Andmevoog

```mermaid
flowchart LR
    csv[seeds/allikad.csv] -->|dbt seed| allikad[(seeds.allikad)]

    rk[Riigikogu API] -->|Airflow PythonOperator| rk_raw[(staging.riigikogu_raw)]
    ra[Rahvaalgatus API + scraper] -->|Airflow PythonOperator| ra_raw[(staging.rahvaalgatus_raw)]
    wp[Wikipedia API] -->|Airflow PythonOperator| wp_raw[(staging.wikipedia_raw)]

    rk_raw -->|dbt staging| int[intermediate.int_documents]
    ra_raw -->|dbt staging| int
    wp_raw -->|dbt staging| int
    allikad -->|dbt staging| int

    int -->|dbt marts| fct[(marts.fct_documents)]
    int -->|dbt marts| quality[(marts.mart_source_quality)]

    fct --> dashboard[Streamlit näidikulaud]
    quality --> dashboard

    airflow[Airflow scheduler] -->|"@daily"| rk
    airflow -->|"@daily"| ra
    airflow -->|"@daily"| wp
    airflow -->|BashOperator| dbt[dbt run + dbt test]
```

## Andmebaasi kihid

| Kiht | Tüüp | Roll |
|---|---|---|
| `staging` | Tabel | API-st ja scraperilt saadud toorandmed. Iga käivitus lisab ainult uued read (`ON CONFLICT DO NOTHING`). Vanad andmed jäävad alles. |
| `intermediate` | Vaade | Puhastamine + kvaliteedilipud (`is_long_enough`, `is_estonian`, `is_not_duplicate`) + sõnade loendamine (`word_count`). |
| `marts` | Tabel | `fct_documents` ühendab kõik allikad. `mart_source_quality` arvutab mõõdikud allika ja päeva lõikes. |

Iga töövoo käivitus saab unikaalse `run_id`. Staging toorandmed kasvavad kumulatiivselt. Mart tabelid ehitatakse iga käivitusega uuesti — näidikulaud loeb alati viimast seisu.

## Tööjaotus

| Liige | Roll | Vastutus |
|---|---|---|
| Eleri | Andmeallika ja transformatsioonide omanik | Kirjutab sissevõtu loogika Riigikogu API najal, transformatsioonid ja mõõdikute arvutuse, Airflow DAG-id, Docker seadistus |
| Evelin | Kvaliteedi omanik | Rahvaalgatus.ee API kontroll, kirjutab dbt kvaliteeditestid, valmistab ette mõõdikute arvutuse loogika |
| Liis | Näidikulaua omanik | Wikipedia API kontroll, valmistab ette staatilise andmetabeli (seeds), valmistab ette näidikulaua vaated |

## Riskid

| Risk | Mõju | Maandus |
|---|---|---|
| Riigikogu, Rahvaalgatuse ja/või Vikipeedia API ei vasta | (kõiki) andmeid ei lisandu | Airflow `retries=2`; järgmine käivitus proovib uuesti |
| API muudab väljade nimesid | Airflow Python task jookseb kokku | Skript valideerib nõutud väljad enne kirjutamist; vigased read jäävad logidesse |
| dbt testid ebaõnnestuvad | Näidikulaud võib näidata vigaseid andmeid | dbt test task märgib Airflow töövoo ebaõnnestunuks; Streamlit näitab endiselt viimaseid edukaid andmeid |
| Airflow scheduler ei käivitu | Andmed ei värskene | Kontrolli `docker compose logs airflow`; ingest-skripte saab käivitada ka käsitsi |

## Privaatsus ja turve

Projekt kasutab ainult avalikke andmeid. Isikuandmeid ei koguta. Rahvaalgatus.ee algatused on avalik kodanikuplatvorm — scraping on lubatud (`robots.txt: Disallow:` ilma väärtuseta). Andmebaasi kasutajanimi ja parool tulevad `.env` failist. Päris `.env` faili ei tohi reposse lisada — ainult `.env.example`.
