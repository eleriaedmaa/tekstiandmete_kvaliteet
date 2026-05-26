# EKI tekstiandmete grupp - Tekstiandmete kvaliteet

Projekt kogub eestikeelset teksti kolmest avalikust allikast, transformeerib selle ühtsesse vormingusse ja näitab kvaliteedimõõdikuid Streamlit näidikulaual.

## Äriküsimus

Kui palju kvaliteetset eestikeelset teksti on võimalik regulaarselt koguda valitud avalikest andmeallikatest?

**Mõõdikud:**
1. Uute sõnade lisandumine ajas allika kohta
2. Kasutatavuse % allika kohta (kui suur osa läbib kvaliteedikontrolli)
3. Peamised kvaliteedipuudused allika kohta

Täielik arhitektuurikirjeldus: [`docs/arhitektuur.md`](docs/arhitektuur.md)

## Stack

| Komponent | Tööriist |
|---|---|
| Orkestreerimine | Apache Airflow 3.1.8 |
| Transformatsioon | dbt Core 1.10 |
| Andmehoidla | PostgreSQL 16 |
| Näidikulaud | Streamlit |
| Allikad | Riigikogu API, Rahvaalgatus.ee (API + scraper), Eesti Wikipedia API |

## Andmevoog

1. **Sissevõtt** — kolm Airflow DAG-i (üks allika kohta) küsivad iga päev (`@daily`) API-st uusi dokumente ja kirjutavad need `staging.{allikas}_raw` tabelisse (`ON CONFLICT DO NOTHING`).
2. **Transformatsioon** — `dbt run` ehitab kõik kolm staging vaadet, ühendab need `intermediate.int_documents` vaates, arvutab kvaliteedilipud ja sõnade arvu.
3. **Testimine** — `dbt test` kontrollib andmekvaliteedi reegleid (`not_null`, `accepted_values`, unikaalsus).
4. **Mart-kihi ehitus** — `marts.fct_documents` ühendab kõik dokumendid, `marts.mart_source_quality` arvutab päevased mõõdikud allika lõikes.
5. **Näidikulaud** — Streamlit loeb `marts.*` tabeleid ja näitab kogumismahtu, kasutatavust ja kvaliteedipuudusi.

## Projekti struktuur

```
.
├── compose.yml
├── .env.example
├── .gitignore
├── Dockerfile.dashboard
├── airflow/
│   └── dags/
│       ├── riigikogu_pipeline.py
│       ├── rahvaalgatus_pipeline.py
│       ├── wikipedia_pipeline.py
│       └── dbt_pipeline.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── seeds/
│   │   ├── allikad.csv
│   │   └── teadaolevad_dokumendid.csv
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── macros/
│       └── generate_schema_name.sql
├── dashboard/
│   └── app.py
├── init/
│   ├── 01_create_schemas.sql
│   └── 02_historical_data.sql.gz
├── scripts/
│   └── migrate_seed_to_staging.py
└── docs/
    ├── arhitektuur.md
    └── progress.md
```

## Käivitamine

```bash
# 1. Klooni repo
git clone https://github.com/eleriaedmaa/tekstiandmete_kvaliteet.git
cd tekstiandmete_kvaliteet

# 2. Kopeeri keskkonnamuutujad
cp .env.example .env
# Ava .env ja kontrolli, et väärtused on korrektsed (vaikimisi töötavad)

# 3. Käivita teenused
docker compose up -d --build

# 4. Oota ~2 minutit, kuni andmebaas ja Airflow on käivitunud
docker compose ps   # kõik teenused peaksid olema "running"

# 5. Laadi ajaloolised andmed (ühekordne samm — ~35 MB, ~1 min)
gunzip -k init/02_historical_data.sql.gz
docker compose cp init/02_historical_data.sql analytics-db:/tmp/02_historical_data.sql
docker compose exec analytics-db psql -U EKI -d eki_postgres -f /tmp/02_historical_data.sql

# 6. Laadi dbt seemned ja ehita mudelid
docker compose exec airflow-apiserver bash -c "cd /opt/airflow/dbt_project && dbt seed --profiles-dir . && dbt run --profiles-dir ."

# 7. Ava Airflow UI ja käivita DAG-id käsitsi
#    http://localhost:8080  (kasutaja: airflow / parool: airflow)
#    Soovituslik järjekord: riigikogu_pipeline → rahvaalgatus_pipeline → wikipedia_pipeline → dbt_pipeline

# 8. Ava näidikulaud
#    http://localhost:8501
```

## Saladused ja konfiguratsioon

Kõik paroolid on `.env` failis. Reposse läheb ainult `.env.example` — päris `.env` on `.gitignore`-s.

| Muutuja | Tähendus |
|---|---|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Analüütika andmebaas |
| `AIRFLOW_USER` / `AIRFLOW_PASSWORD` | Airflow UI sisselogimine |
| `AIRFLOW_PORT_HOST` | Airflow UI port (vaikimisi 8080) |
| `DASHBOARD_PORT_HOST` | Streamlit port (vaikimisi 8501) |
| `DB_PORT_HOST` | PostgreSQL port hostis (vaikimisi 55432) |

## dbt käsud käsitsi

```bash
docker compose exec airflow-apiserver bash
cd /opt/airflow/dbt_project

dbt seed --profiles-dir .         # laadib seemned (allikad, teadaolevad_dokumendid)
dbt run --profiles-dir .          # käivitab kõik mudelid
dbt test --profiles-dir .         # käivitab kvaliteeditestid
dbt docs generate --profiles-dir .  # genereerib dokumentatsiooni
```

## Andmekvaliteedi testid

1. `stg_*` — `doc_id` ja `tekst` ei ole NULL
2. `int_documents` — `is_long_enough`, `is_estonian`, `is_not_duplicate` on 0 või 1
3. `int_documents` — `word_count` ei ole NULL ja ≥ 0
4. `fct_documents` — `(allikas, doc_id)` paar on unikaalne
5. `fct_documents` — `allikas` on lubatud väärtuste hulgas
6. `mart_source_quality` — `kasutatavuse_pct` jääb vahemikku 0–100

## Tööjaotus

| Liige | Roll | Vastutus |
|---|---|---|
| Eleri | Andmeallika ja transformatsioonide omanik | Kirjutab sissevõtu loogika Riigikogu API najal (ühtlasi kontrollib, kas API töötab), transformatsioonid, Airflow DAG-id, seadistab Dockeri |
| Evelin | Kvaliteedi omanik | Kontrollib rahvaalgatus.ee APIt, kirjutab dbt kvaliteeditestid ja kontrollib nende tööd |
| Liis | Näidikulaua omanik | Kontrollib Vikipeedia APIt, valmistab ette staatilise andmetabeli olemasolevate andmetega, vastutab näidikulaua vaadete eest |

## Privaatsus

Projekt kasutab ainult avalikke andmeid. Isikuandmeid ei koguta. Rahvaalgatus.ee `robots.txt` lubab scraping (`Disallow:` ilma väärtuseta).
