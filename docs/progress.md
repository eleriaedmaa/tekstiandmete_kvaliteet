# Edenemisraport

## Mis on valmis

- [x] Docker Compose käivitab kõik teenused
- [x] Andmeid saadakse Riigikogu API-st kätte
- [x] Andmed laetakse `raw` kihti
- [x] Kõik transformatsioonid toimivad (stg_*, int_documents, fct_documents, mart_source_quality)
- [ ] Streamlit näidikulaud on nähtaval kolme mõõdikuga
- [ ] Vähemalt üks andmekvaliteedi test läbib (not_null, unique)
- [ ] Rahvaalgatus.ee API sissevõtt valmis 
- [ ] Vikipeedia API sissevõtt valmis (paginatsioon, igapäevane uuendus)
- [ ] Ajalooliste andmete ühekordne migratsioon tehtud (seeds → staging)
- [ ] Kõik andmekvaliteedi testid rohelised (Evelin)

Kõik kolm andmevoogu töötavad otsast lõpuni: API → staging → intermediate → marts →
Streamlit. Airflow käivitab kõiki DAG-e igapäevaselt (`@daily`). Ajaloolised andmed
(~139 000 URL-i) on migreeritud staging tabelitesse ja salvestatud
`init/02_historical_data.sql.gz` failina uue keskkonna püstitamiseks.

## Järgmised sammud

- Evelin: ülejäänud dbt kvaliteeditestid lisada ja roheliseks saada
- Liis: näidikulaua viimistlemine ja seeds/allikad.csv kontrollimine
- Kokkuvõte, puudused ja edasiarendused README-sse kirja panna (kõik)

## Mis takistab

- Rahvaalgatus.ee API tagastab algatused ilma offseti toeta (ignoreerimine leitud
  ja parandatud — `limit=2000` toob kõik ~1100 algatust ühes päringus)
- Vikipeedia API piirab päringute sagedust (429 vead) — migratsiooniskript kasutab
  2-sekundilist pausi; igapäevane DAG töötab normaalselt

## Kontrollpunkt

Viimased töövoo käivitused ja nende tulemus:

```bash
docker compose exec analytics-db psql -U EKI -d eki_postgres -c "
SELECT source_name, DATE(fetched_at) as kuupaev, docs_added, status
FROM staging.pipeline_runs
WHERE status = 'success'
ORDER BY fetched_at DESC
LIMIT 10;"
```

Näidikulaud: http://localhost:8501
