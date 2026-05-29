# Edenemisraport

## Mis on valmis

- [x] Kõik kolm andmete sissevõtu Airflow DAG-i käivituvad regulaarselt, kolmest allikast tuuakse iga päev uusi andmeid.
- [x] Andmed laetakse staging kihti (`riigikogu_raw`, `rahvaalgatus_raw`, `wikipedia_raw`)
- [x] dbt transformatsioonid toimivad otsast lõpuni (`stg_*` → `int_documents` → `fct_documents`, `mart_source_quality`, `mart_avaldamise_katvus`)
- [x] Andmekvaliteedi testid on rohelised
- [x] Streamlit näidikulaud näitab kõigi kolme mõõdiku MVPd
- [x] Uus keskkond on võimalik püsti panna README juhendi järgi (sh ajaloolised andmed)
- [x] Wikipedia DAG tõmbab ainult uusi artikleid (muudetud failid välja filtreeritud)

## Järgmised sammud

- Oodata Wikipedia backfilli lõppu, seejärel käivitada `dbt run` ja kontrollida ajaloolist katvust joonisel
- Näidikulaua viimistlemine
- Kokkuvõte, puudused ja edasiarendused README-sse kirja panna

## Mis takistab

- Wikipedia ajalooliste andmete backfill (2016–2026) jookseb veel taustal — kuni see lõpeb, ei kajastu joonisel täielik ajalooline katvus.

### Tehniline võlg
- Airflow DAG-id kasutavad `retries=2`; tootmiskeskkonnas soovitav `retries=3`
- `migrate_seed_to_staging.py` skript on ühekordne tööriist — võiks eemaldada

## Kontrollpunkt

```bash
# Kontrolli, et kõik teenused töötavad
docker compose ps

# Viimased töövoo käivitused
docker compose exec analytics-db psql -U EKI -d eki_postgres -c "
SELECT source_name, DATE(fetched_at) as kuupaev, docs_added, status
FROM staging.pipeline_runs
WHERE status = 'success'
ORDER BY fetched_at DESC
LIMIT 10;"
```

Näidikulaud: http://localhost:8501
